use std::cell::RefCell;
use std::error::Error;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::io::{Error as IoError, ErrorKind, Write};
use std::net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpListener, TcpStream};
use std::ops::Drop;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use lazy_static;
use log;
use parking_lot::RwLock;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::net;

// the callback type for passing closures into a new RPC handler.
pub type Callback<T, U> = Box<dyn Send + Fn(T) -> Result<U, String>>;

#[derive(Clone)]
enum ListenPort {
    TcpPort(u16),
    Unix(PathBuf),
}

#[derive(Clone)]
enum SendPort {
    TcpSocket(SocketAddr),
    Unix(PathBuf),
}

/// Possible responses to RPC calls that do not contain callback output.
#[derive(Debug)]
pub enum RpcError {
    IoError(IoError),
    RpcError(String),
}

const RPC_ERROR: &str = "RpcError";
impl RpcError {
    /// Return True if the `RpcError` contains an IO Error.
    pub fn is_io(&self) -> bool {
        match self {
            RpcError::IoError(_) => true,
            _ => false,
        }
    }

    /// Return True if the `RpcError` contains an RPC Error.
    pub fn is_rpc(&self) -> bool {
        match self {
            RpcError::RpcError(_) => true,
            _ => false,
        }
    }

    /// Unwrap an IO Error if one exists, else panic.
    pub fn unwrap_io(&self) -> &IoError {
        match self {
            RpcError::IoError(err) => err,
            _ => panic!("The RpcError is not an IO Error, got {:?}", self),
        }
    }

    /// Unwrap an RPC error message if one exists, else panic.
    pub fn unwrap_rpc(&self) -> &str {
        match self {
            RpcError::RpcError(err) => err,
            _ => panic!("The RpcError is not an RPC error message, got {:?}", self),
        }
    }
}

impl Display for RpcError {
    /// Display the RPC Error.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, "{:?}", self)
    }
}

impl Error for RpcError {}

/// The RPC server listens for incoming messages of type `T`, processes them, and returns a result
/// containing bytes that can be deserialized to the type `U`.
pub struct RpcServer {
    name: String,
    listen_port: ListenPort,
    stop_requested: Arc<RwLock<bool>>,
    thread: Option<JoinHandle<()>>,
}

impl RpcServer {
    // start a new server
    fn new<T, U>(name: &'static str, listen_port: ListenPort, callback: Callback<T, U>) -> RpcServer
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        let stop_requested = Arc::new(RwLock::new(false));
        let is_stop_requested = stop_requested.clone();
        let thread_listen_port = listen_port.clone();
        let thread = thread::spawn(move || {
            let listener: Box<dyn RpcListener<T, U>> = match thread_listen_port {
                ListenPort::TcpPort(port) => Box::new(TcpRpcListener::new(port)),
                ListenPort::Unix(path) => Box::new(UnixRpcListener::new(&path)),
            };

            while !*is_stop_requested.read() {
                let req = match listener.recv_request() {
                    Ok(req) => req,
                    Err(err) => {
                        log::debug!(
                            "recv_request error on RPC handler '{}' with error:\n{}",
                            name,
                            err
                        );
                        continue;
                    }
                };

                let resp = match (*callback)(req) {
                    Ok(resp) => Ok(resp),
                    Err(err) => Err(err.to_string()),
                };

                if let Err(err) = listener.send_response(resp) {
                    log::debug!(
                        "send_response error on RPC handler '{}' with error:\n{}",
                        name,
                        err
                    );
                }
            }
        });

        RpcServer {
            name: String::from(name),
            listen_port,
            stop_requested,
            thread: Some(thread),
        }
    }

    /// Create an RPC server bound to a TCP port.
    ///
    /// Args:
    /// * `name`: A name to refer to the RPC server.
    /// * `port`: The TCP port to bind the server to.
    /// * `callback`: The function to call on incoming data.
    pub fn with_tcp_port<T, U>(name: &'static str, port: u16, callback: Callback<T, U>) -> RpcServer
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        RpcServer::new(name, ListenPort::TcpPort(port), callback)
    }

    /// Create an RPC server bound to a Unix stream socket.
    ///
    /// Args:
    /// * `name`: A name to refer to the RPC server.
    /// * `path`: The unix socket path to bind the server to.
    /// * `callback`: The function to call on incoming data.
    pub fn with_unix_socket<T, U>(
        name: &'static str,
        path: &Path,
        callback: Callback<T, U>,
    ) -> RpcServer
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        RpcServer::new(name, ListenPort::Unix(PathBuf::from(path)), callback)
    }

    /// Check if the RPC server is running.
    pub fn is_running(&self) -> bool {
        self.thread.is_some()
    }

    /// Stop the RPC server.
    pub fn stop(&mut self) {
        if self.is_running() {
            log::debug!("Stopping RPC handler: {}", self.name);
            *self.stop_requested.write() = true;
            self.send_stop_signal();
            self.thread.take().unwrap().join().unwrap();
        }
    }

    fn send_stop_signal(&self) {
        match &self.listen_port {
            ListenPort::TcpPort(port) => self.send_stop_signal_tcp(*port),
            ListenPort::Unix(path) => self.send_stop_signal_unix(&path),
        }
    }

    fn send_stop_signal_tcp(&self, port: u16) {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        net::write_stop_signal(TcpStream::connect(addr), &self.name);
    }

    fn send_stop_signal_unix(&self, path: &Path) {
        net::write_stop_signal(UnixStream::connect(path), &self.name);
    }
}

// always make sure the server
impl Drop for RpcServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The RPC client sends data of type `T` to a server and expects a response of type `U`.
///
/// It is extremely important that the types on the client and server match, else the RPC calls will
/// likely fail.
pub struct RpcClient<T, U> {
    sender: Box<dyn RpcSender<T, U>>,
}

impl<T, U> RpcClient<T, U>
where
    T: DeserializeOwned + Serialize + 'static,
    U: DeserializeOwned + Serialize + 'static,
{
    fn new(send_port: SendPort) -> RpcClient<T, U> {
        let sender: Box<dyn RpcSender<T, U>> = match send_port {
            SendPort::TcpSocket(addr) => Box::new(TcpRpcSender::new(addr)),
            SendPort::Unix(path) => Box::new(UnixRpcSender::new(&path)),
        };
        RpcClient { sender }
    }

    /// Create an RPC client pointing to a TCP socket address.
    pub fn with_tcp_addr(addr: SocketAddr) -> RpcClient<T, U> {
        RpcClient::new(SendPort::TcpSocket(addr))
    }

    /// Create an RPC client pointing to a Unix stream socket address.
    pub fn with_unix_socket(path: &Path) -> RpcClient<T, U> {
        RpcClient::new(SendPort::Unix(PathBuf::from(path)))
    }

    /// Call the RPC and return the response.
    ///
    /// If the types used to initialize the RPC client differ from the RPC server, some very
    /// cryptic errors can occur. Since `bincode` is used for serialization, the type of error can
    /// differ depending on where the deserialization occurs. In any case, it's important to make
    /// sure types are matched properly when creating RPC servers and clients.
    pub fn call(&self, request: T, timeout: Duration) -> Result<U, RpcError> {
        let now = Instant::now();
        while !self.ping(timeout) {
            thread::sleep(Duration::from_millis(1));
            if now.elapsed() > timeout {
                return Err(RpcError::RpcError(String::from(
                    "RPC call timed out while waiting for ping",
                )));
            }
        }

        match self.sender.send_recv(request, timeout) {
            Ok(response) => Ok(response),
            Err(err) => {
                match err.kind() {
                    ErrorKind::Other => {
                        let err_str = err.to_string();
                        lazy_static::lazy_static! {
                            static ref RE: Regex = Regex::new(&format!("{}: ", RPC_ERROR)).unwrap();
                        }

                        if RE.is_match(&err_str) {
                            // there should be two items in the split, where the second one is the
                            // actual string. this should not actually panic.
                            let mut split = RE.split(&err_str);
                            split.next();
                            Err(RpcError::RpcError(split.next().unwrap().to_string()))
                        } else {
                            Err(RpcError::IoError(err))
                        }
                    }
                    _ => Err(RpcError::IoError(err)),
                }
            }
        }
    }

    /// Check if the corresponding RPC server is online.
    pub fn ping(&self, timeout: Duration) -> bool {
        self.sender.ping(timeout)
    }

    /// Wait indefinitely for the RPC server to come online.
    pub fn wait_for_server(&self, ping_timeout: Duration) {
        while !self.ping(ping_timeout) {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

trait RpcListener<T, U>
where
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn recv_request(&self) -> Result<T, IoError>;

    fn send_response(&self, resp: Result<U, String>) -> Result<(), IoError>;
}

trait RpcSender<T, U>
where
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn send_recv(&self, req: T, timeout: Duration) -> Result<U, IoError>;
    fn ping(&self, timeout: Duration) -> bool;
}

struct TcpRpcListener {
    tcp: TcpListener,
    stream: RefCell<Option<TcpStream>>,
}

impl TcpRpcListener {
    fn new(port: u16) -> TcpRpcListener {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), port);
        let listener =
            TcpListener::bind(addr).expect(&format!("Cannot bind to TCP port: {}", port));
        TcpRpcListener {
            tcp: listener,
            stream: RefCell::new(None),
        }
    }

    fn write_stream<T: Serialize>(&self, data: T) -> Result<usize, IoError> {
        if self.stream.borrow().is_none() {
            return Err(IoError::new(
                ErrorKind::NotFound,
                "TCP stream handler closed",
            ));
        }

        let message = net::construct_message(data);
        let response = self.stream.borrow().as_ref().unwrap().write(&message);
        let _ = self
            .stream
            .borrow()
            .as_ref()
            .take()
            .unwrap()
            .shutdown(Shutdown::Both);
        *self.stream.borrow_mut() = None;
        response
    }
}

impl<T, U> RpcListener<T, U> for TcpRpcListener
where
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn recv_request(&self) -> Result<T, IoError> {
        if self.stream.borrow().is_some() {
            return Err(IoError::new(
                ErrorKind::AlreadyExists,
                "TCP stream handler open",
            ));
        }

        let (mut stream, _) = self.tcp.accept()?;
        let request = net::recv(&mut stream, None, RPC_ERROR, false);
        match request {
            Ok(value) => {
                *self.stream.borrow_mut() = Some(stream);
                Ok(value)
            }
            Err(err) => {
                let err_str = err.to_string();
                let _ = stream.write(net::construct_error(&err_str).as_slice());
                Err(err)
            }
        }
    }

    fn send_response(&self, resp: Result<U, String>) -> Result<(), IoError> {
        match self.write_stream(resp) {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

struct UnixRpcListener {
    unix: UnixListener,
    stream: RefCell<Option<UnixStream>>,
}

impl UnixRpcListener {
    fn new(path: &Path) -> UnixRpcListener {
        let listener =
            UnixListener::bind(path).expect(&format!("Cannot bind to Unix socket: {:?}", path));
        UnixRpcListener {
            unix: listener,
            stream: RefCell::new(None),
        }
    }

    fn write_stream<T: Serialize>(&self, data: T) -> Result<usize, IoError> {
        if self.stream.borrow().is_none() {
            return Err(IoError::new(
                ErrorKind::NotFound,
                "TCP stream handler closed",
            ));
        }

        let message = net::construct_message(data);
        let response = self.stream.borrow().as_ref().unwrap().write(&message);
        let _ = self
            .stream
            .borrow()
            .as_ref()
            .take()
            .unwrap()
            .shutdown(Shutdown::Both);
        *self.stream.borrow_mut() = None;
        response
    }
}

impl<T, U> RpcListener<T, U> for UnixRpcListener
where
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn recv_request(&self) -> Result<T, IoError> {
        if self.stream.borrow().is_some() {
            return Err(IoError::new(
                ErrorKind::AlreadyExists,
                "TCP stream handler open",
            ));
        }

        let (mut stream, _) = self.unix.accept()?;
        let request = net::recv(&mut stream, None, RPC_ERROR, false);
        match request {
            Ok(value) => {
                *self.stream.borrow_mut() = Some(stream);
                Ok(value)
            }
            Err(err) => {
                let err_str = err.to_string();
                let _ = stream.write(net::construct_error(&err_str).as_slice());
                Err(err)
            }
        }
    }

    fn send_response(&self, resp: Result<U, String>) -> Result<(), IoError> {
        match self.write_stream(resp) {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

struct TcpRpcSender {
    addr: SocketAddr,
}

impl TcpRpcSender {
    fn new(addr: SocketAddr) -> TcpRpcSender {
        TcpRpcSender { addr }
    }
}

impl<T, U> RpcSender<T, U> for TcpRpcSender
where
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn send_recv(&self, req: T, timeout: Duration) -> Result<U, IoError> {
        let mut stream = TcpStream::connect(self.addr)?;
        net::write_socket(&mut stream, req)?;
        net::recv(&mut stream, Some(timeout), RPC_ERROR, true)
    }

    fn ping(&self, timeout: Duration) -> bool {
        let mut stream = match TcpStream::connect(self.addr) {
            Ok(stream) => stream,
            Err(_) => return false,
        };

        net::ping(&mut stream, timeout)
    }
}

struct UnixRpcSender {
    path: PathBuf,
}

impl UnixRpcSender {
    fn new(path: &Path) -> UnixRpcSender {
        UnixRpcSender {
            path: PathBuf::from(path),
        }
    }
}

impl<T, U> RpcSender<T, U> for UnixRpcSender
where
    T: DeserializeOwned + Serialize,
    U: DeserializeOwned + Serialize,
{
    fn send_recv(&self, req: T, timeout: Duration) -> Result<U, IoError> {
        let mut stream = UnixStream::connect(&self.path)?;
        net::write_socket(&mut stream, req)?;
        net::recv(&mut stream, Some(timeout), RPC_ERROR, true)
    }

    fn ping(&self, timeout: Duration) -> bool {
        let mut stream = match UnixStream::connect(&self.path) {
            Ok(stream) => stream,
            Err(_) => return false,
        };

        net::ping(&mut stream, timeout)
    }
}

#[cfg(test)]
mod tests {
    use portpicker;
    use serde::Deserialize;
    use tempfile;

    use super::*;

    fn setup_logging() {
        let _ = env_logger::builder()
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{}:{} [{}] - {}",
                    record.file().unwrap_or("unknown"),
                    record.line().unwrap_or(0),
                    record.level(),
                    record.args()
                )
            })
            .is_test(true)
            .try_init();
    }

    #[test]
    fn start_stop_server_tcp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();
        let mut server: RpcServer =
            RpcServer::with_tcp_port::<i32, ()>("test", port, Box::new(|_| Ok(())));
        assert!(server.is_running());
        server.stop();
        assert!(!server.is_running());
    }

    #[test]
    fn start_stop_server_unix() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        let mut server: RpcServer =
            RpcServer::with_unix_socket::<i32, ()>("test", &socket, Box::new(|_| Ok(())));
        assert!(server.is_running());
        server.stop();
        assert!(!server.is_running());
    }

    #[test]
    fn test_client_tcp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer =
            RpcServer::with_tcp_port::<i32, i32>("test", port, Box::new(|x| Ok(x + 1)));
        assert!(server.is_running());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let client: RpcClient<i32, i32> = RpcClient::with_tcp_addr(addr);
        client.wait_for_server(Duration::from_millis(100));
        let response = client.call(1, Duration::from_secs(5));
        assert_eq!(response.unwrap(), 2);
    }

    #[test]
    fn test_client_unix() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        let server: RpcServer =
            RpcServer::with_unix_socket::<i32, i32>("test", &socket, Box::new(|x| Ok(x + 1)));
        assert!(server.is_running());

        let client: RpcClient<i32, i32> = RpcClient::with_unix_socket(&socket);
        client.wait_for_server(Duration::from_millis(100));
        let response = client.call(1, Duration::from_secs(5));
        assert_eq!(response.unwrap(), 2);
    }

    #[test]
    fn test_callback_with_errors_tcp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer = RpcServer::with_tcp_port::<i32, i32>(
            "test",
            port,
            Box::new(|_| Err(String::from("callback example error"))),
        );
        assert!(server.is_running());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let client: RpcClient<i32, i32> = RpcClient::with_tcp_addr(addr);
        let response = client.call(0, Duration::from_secs(5));
        let err = response.unwrap_err();
        assert!(err.is_rpc());
        assert!(!err.is_io());
        let err = err.unwrap_rpc();
        assert_eq!(err, "callback example error");
    }

    #[test]
    fn test_callback_with_errors_unix() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        let server: RpcServer = RpcServer::with_unix_socket::<i32, i32>(
            "test",
            &socket,
            Box::new(|_| Err(String::from("callback example error"))),
        );
        assert!(server.is_running());

        let client: RpcClient<i32, i32> = RpcClient::with_unix_socket(&socket);
        client.wait_for_server(Duration::from_millis(100));
        let response = client.call(0, Duration::from_secs(5));
        let err = response.unwrap_err();
        assert!(err.is_rpc());
        assert!(!err.is_io());
        let err = err.unwrap_rpc();
        assert_eq!(err, "callback example error");
    }

    #[test]
    fn test_mismatched_types_tcp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer =
            RpcServer::with_tcp_port::<i32, i32>("test", port, Box::new(|x| Ok(x + 1)));
        assert!(server.is_running());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let client: RpcClient<String, String> = RpcClient::with_tcp_addr(addr);
        client.wait_for_server(Duration::from_millis(100));

        let response = client.call(String::from("hello"), Duration::from_secs(5));
        let err = response.unwrap_err();
        assert!(!err.is_rpc());
        assert!(err.is_io());
        let err = err.to_string();
        let re = Regex::new(r"failed to deserialize").unwrap();
        assert!(re.is_match(&err));
    }

    #[test]
    fn test_mismatched_types_unix() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        let server: RpcServer =
            RpcServer::with_unix_socket::<i32, i32>("test", &socket, Box::new(|x| Ok(x + 1)));
        assert!(server.is_running());

        let client: RpcClient<String, String> = RpcClient::with_unix_socket(&socket);
        client.wait_for_server(Duration::from_millis(100));

        let response = client.call(String::from("hello"), Duration::from_secs(5));
        let err = response.unwrap_err();
        assert!(!err.is_rpc());
        assert!(err.is_io());
        let err = err.to_string();
        let re = Regex::new(r"failed to deserialize").unwrap();
        assert!(re.is_match(&err));
    }

    #[test]
    fn test_mismatched_struct_types_tcp() {
        #[derive(Debug, Deserialize, Serialize)]
        struct TwoInts {
            num1: i32,
            num2: i32,
        }

        #[derive(Debug, Deserialize, Serialize)]
        struct OneInt {
            num: i32,
        }

        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer = RpcServer::with_tcp_port::<TwoInts, OneInt>(
            "test",
            port,
            Box::new(|_| Ok(OneInt { num: 0 })),
        );
        assert!(server.is_running());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let client: RpcClient<OneInt, TwoInts> = RpcClient::with_tcp_addr(addr);
        client.wait_for_server(Duration::from_millis(100));

        let response = client.call(OneInt { num: 10 }, Duration::from_secs(5));
        let err = response.unwrap_err();
        assert!(err.is_rpc());
        assert!(!err.is_io());
        let err = err.to_string();
        log::trace!("err: {}", err);
    }

    #[test]
    fn test_mismatched_struct_types_unix() {
        #[derive(Debug, Deserialize, Serialize)]
        struct TwoInts {
            num1: i32,
            num2: i32,
        }

        #[derive(Debug, Deserialize, Serialize)]
        struct OneInt {
            num: i32,
        }

        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");

        let server: RpcServer = RpcServer::with_unix_socket::<TwoInts, OneInt>(
            "test",
            &socket,
            Box::new(|_| Ok(OneInt { num: 0 })),
        );
        assert!(server.is_running());

        let client: RpcClient<OneInt, TwoInts> = RpcClient::with_unix_socket(&socket);
        client.wait_for_server(Duration::from_millis(100));

        let response = client.call(OneInt { num: 10 }, Duration::from_secs(5));
        let err = response.unwrap_err();
        assert!(err.is_rpc());
        assert!(!err.is_io());
        let err = err.to_string();
        log::trace!("err: {}", err);
    }

    #[test]
    fn test_large_data_tcp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer =
            RpcServer::with_tcp_port::<String, usize>("test", port, Box::new(|x| Ok(x.len())));
        assert!(server.is_running());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let client: RpcClient<String, usize> = RpcClient::with_tcp_addr(addr);

        let size = 5000;
        let mut request = String::new();
        while request.len() < size {
            request += "adsfadfasdfasdfasdfasdfasdfsadf";
        }
        let size = request.len();

        let response = client.call(request, Duration::from_secs(5));
        assert_eq!(response.unwrap(), size);
    }

    #[test]
    fn test_large_data_unix() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        let server: RpcServer = RpcServer::with_unix_socket::<String, usize>(
            "test",
            &socket,
            Box::new(|x| Ok(x.len())),
        );
        assert!(server.is_running());

        let client: RpcClient<String, usize> = RpcClient::with_unix_socket(&socket);
        client.wait_for_server(Duration::from_millis(100));

        let size = 5000;
        let mut request = String::new();
        while request.len() < size {
            request += "adsfadfasdfasdfasdfasdfasdfsadf";
        }
        let size = request.len();

        let response = client.call(request, Duration::from_secs(5));
        assert_eq!(response.unwrap(), size);
    }

    #[test]
    fn test_timeout_tcp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let stop = Arc::new(RwLock::new(false));
        let stop_requested = Arc::clone(&stop);

        let server: RpcServer = RpcServer::with_tcp_port::<i32, i32>(
            "test",
            port,
            Box::new(move |_| {
                loop {
                    thread::sleep(Duration::from_millis(1));
                    if *stop_requested.read() {
                        break;
                    }
                }
                Ok(0)
            }),
        );
        assert!(server.is_running());

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
        let client: RpcClient<i32, i32> = RpcClient::with_tcp_addr(addr);
        let result = client.call(0, Duration::from_millis(100));
        log::trace!("{:?}", result);
        assert!(result.is_err());
        *stop.write() = true;
    }

    #[test]
    fn test_timeout_unix() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");

        let stop = Arc::new(RwLock::new(false));
        let stop_requested = Arc::clone(&stop);

        let server: RpcServer = RpcServer::with_unix_socket::<i32, i32>(
            "test",
            &socket,
            Box::new(move |_| {
                loop {
                    thread::sleep(Duration::from_millis(1));
                    if *stop_requested.read() {
                        break;
                    }
                }
                Ok(0)
            }),
        );
        assert!(server.is_running());

        let client: RpcClient<i32, i32> = RpcClient::with_unix_socket(&socket);
        let result = client.call(0, Duration::from_millis(100));
        log::trace!("{:?}", result);
        assert!(result.is_err());
        *stop.write() = true;
    }
}
