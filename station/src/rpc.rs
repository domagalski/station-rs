use std::any;
use std::cell::RefCell;
use std::fmt::Debug;
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::mem;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::ops::Drop;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use bincode;
use byteorder::{ByteOrder, BE};
use log;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::Serialize;

// the callback type for passing closures into a new RPC handler.
type Callback<T, U> = Box<dyn Send + Fn(T) -> Result<U, String>>;

trait ReadWrite: Read + Write {}
impl ReadWrite for TcpStream {}

// size of the buffer for incoming messages
const BUFFER_SIZE: usize = 2048;
// Incoming messages may be of multiple purposes, sometimes to encode a message and other times to
// tell a listening thread to shut down. All incoming messages should be prepended with a u32 with
// the message type (message or stop) and a u64 containing the message size, excluding the header
// size. The message type and size parameter are both expected to be encoded as big endian.
const PING_KEYWORD: u32 = 0xC001C0DE;
const MESSAGE_KEYWORD: u32 = 0xC0DEFEED;
const STOP_KEYWORD: u32 = 0xC0DEDEAD;
const HEADER_SIZE: usize = mem::size_of::<u32>() + mem::size_of::<u64>();

// TODO unix domain sockets
#[derive(Clone, Copy)]
enum ListenPort {
    TcpPort(u16),
}

// TODO unix domain sockets
#[derive(Clone, Copy)]
enum SendPort {
    TcpSocket(SocketAddr),
}

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
        let thread = thread::spawn(move || {
            let listener: Box<dyn RpcListener<T, U>> = match listen_port {
                ListenPort::TcpPort(port) => Box::new(TcpRpcListener::new(port)),
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
    /// Incoming messages will not be accepted until `RpcServer::start()` is called.
    ///
    /// Args:
    /// * `name`: A name to refer to the RPC server.
    /// * `port`: The TCP port to bind the server to
    /// * `callback`: The function to call on incoming data.
    pub fn with_tcp_port<T, U>(name: &'static str, port: u16, callback: Callback<T, U>) -> RpcServer
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        RpcServer::new(name, ListenPort::TcpPort(port), callback)
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
        let mut signal: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
        BE::write_u32(&mut signal, STOP_KEYWORD);

        match self.listen_port {
            ListenPort::TcpPort(port) => self.send_stop_signal_tcp(port, &signal),
        }
    }

    fn send_stop_signal_tcp(&self, port: u16, signal: &[u8]) {
        let stream = TcpStream::connect(format!("127.0.0.1:{}", port));
        // the only reason why the TCP endpoint shouldn't connect is that the thread has already
        // been shut down, in which case, not being able to connect to tell it to shut down is fine
        if let Ok(mut stream) = stream {
            match stream.write(signal) {
                Ok(size) => log::trace!(
                    "{}: wrote stop requested signal of {} bytes",
                    self.name,
                    size
                ),
                Err(err) => log::trace!("{}: stop request had error: {}", self.name, err),
            }
        }
    }
}

// always make sure the server
impl Drop for RpcServer {
    fn drop(&mut self) {
        self.stop();
    }
}

pub struct RpcClient<T, U> {
    sender: Box<dyn RpcSender<T, U>>,
}

impl<T, U> RpcClient<T, U>
where
    T: DeserializeOwned + Serialize + 'static,
    U: DeserializeOwned + Serialize + 'static,
{
    fn new(send_port: SendPort) -> RpcClient<T, U> {
        let sender = match send_port {
            SendPort::TcpSocket(addr) => Box::new(TcpRpcSender::new(addr)),
        };
        RpcClient { sender }
    }

    pub fn with_tcp_addr(addr: SocketAddr) -> RpcClient<T, U> {
        RpcClient::new(SendPort::TcpSocket(addr))
    }

    pub fn call(&self, request: T) -> Result<U, IoError> {
        self.sender.send_recv(request)
    }

    pub fn ping(&self) -> bool {
        self.sender.ping()
    }
}

fn construct_message(data: impl Serialize) -> Vec<u8> {
    let mut response_bytes = bincode::serialize(&data).unwrap();
    let mut message: Vec<u8> = Vec::new();
    message.resize(HEADER_SIZE, 0);
    let keyword_bound = mem::size_of::<u32>();
    BE::write_u32(&mut message[..keyword_bound], MESSAGE_KEYWORD);
    BE::write_u64(&mut message[keyword_bound..], response_bytes.len() as u64);
    message.append(&mut response_bytes);
    message
}

fn write_stream(stream: &mut impl ReadWrite, data: impl Serialize) -> Result<usize, IoError> {
    let message = construct_message(data);
    stream.write(&message)
}

fn ping(stream: &mut impl ReadWrite) -> bool {
    let mut signal: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
    BE::write_u32(&mut signal, PING_KEYWORD);
    match stream.write(&signal) {
        Ok(size) => log::trace!("wrote ping requested signal of {} bytes", size),
        Err(err) => {
            log::trace!("ping request had error: {}", err);
            return false;
        }
    }

    // TODO wait for a response

    return true;
}

fn recv<T: DeserializeOwned + Serialize>(
    stream: &mut impl Read,
    is_result_type: bool,
) -> Result<T, IoError> {
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    let n_bytes = stream.read(&mut buffer)?;
    // there must be at least enough bytes for the stop signal
    if n_bytes < HEADER_SIZE {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            "request message too small",
        ));
    }

    let keyword_bound = mem::size_of::<u32>();
    let keyword: u32 = BE::read_u32(&buffer[..keyword_bound]);
    let message_size: usize = match keyword {
        PING_KEYWORD => return Err(IoError::new(ErrorKind::WriteZero, "ping")),
        MESSAGE_KEYWORD => BE::read_u64(&buffer[keyword_bound..HEADER_SIZE]) as usize,
        STOP_KEYWORD => return Err(IoError::new(ErrorKind::Interrupted, "stop requested")),
        _ => {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("unknown message type: {}", keyword),
            ))
        }
    };

    let buffer = &buffer[..n_bytes];
    let mut message_bytes = buffer[HEADER_SIZE..].to_vec();
    if message_size + HEADER_SIZE <= BUFFER_SIZE && message_bytes.len() != message_size {
        let mismatch_error = "request bytes mismatch header length";
        return Err(IoError::new(ErrorKind::InvalidData, mismatch_error));
    }

    while message_size != message_bytes.len() {
        let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
        let n_bytes = stream.read(&mut buffer)?;
        if n_bytes < BUFFER_SIZE && message_bytes.len() + n_bytes != message_size {
            let mismatch_error = "bytes mismatch header length after secondary fetch";
            return Err(IoError::new(ErrorKind::InvalidData, mismatch_error));
        }

        let buffer = &buffer[..n_bytes];
        message_bytes.append(&mut buffer.to_vec());
    }

    if is_result_type {
        let response: Result<T, String> = match bincode::deserialize(&message_bytes) {
            Ok(resp) => resp,
            Err(_) => {
                let err_str = format!(
                    "failed to deserialize to {}",
                    any::type_name::<Result<T, String>>()
                );
                return Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("{}", err_str),
                ));
            }
        };

        match response {
            Ok(resp) => Ok(resp),
            Err(err) => Err(IoError::new(
                ErrorKind::Other,
                format!("RpcError: {}", err.to_string()),
            )),
        }
    } else {
        let message_bytes = message_bytes.as_slice();
        match bincode::deserialize(&message_bytes) {
            Ok(message) => Ok(message),
            Err(_) => {
                let err_str = format!("failed to deserialize to {}", any::type_name::<T>());
                Err(IoError::new(
                    ErrorKind::InvalidInput,
                    format!("{}", err_str),
                ))
            }
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
    fn send_recv(&self, req: T) -> Result<U, IoError>;
    fn ping(&self) -> bool;
}

struct TcpRpcListener {
    tcp: TcpListener,
    stream: RefCell<Option<TcpStream>>,
}

impl TcpRpcListener {
    fn new(port: u16) -> TcpRpcListener {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
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

        let message = construct_message(data);
        let response = self.stream.borrow().as_ref().unwrap().write(&message);
        self.stream
            .borrow()
            .as_ref()
            .take()
            .unwrap()
            .shutdown(Shutdown::Both)
            .unwrap();
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
        let request = recv(&mut stream, false);
        match request {
            Ok(_) => *self.stream.borrow_mut() = Some(stream),
            _ => (),
        }
        // TODO write the error
        request
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
    fn send_recv(&self, req: T) -> Result<U, IoError> {
        let mut stream = TcpStream::connect(self.addr)?;
        write_stream(&mut stream, req)?;
        recv(&mut stream, true)
    }

    fn ping(&self) -> bool {
        let mut stream = match TcpStream::connect(self.addr) {
            Ok(stream) => stream,
            Err(_) => return false,
        };

        ping(&mut stream)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use portpicker;
    use regex::Regex;
    use test_env_log::test;

    use super::*;

    #[test]
    fn start_stop_server_tcp() {
        let port: u16 = portpicker::pick_unused_port().unwrap();
        let mut server: RpcServer =
            RpcServer::with_tcp_port::<i32, ()>("test", port, Box::new(|_| Ok(())));
        assert!(server.is_running());
        //std::thread::sleep(std::time::Duration::from_secs(1));
        server.stop();
        assert!(!server.is_running());
    }

    #[test]
    fn test_client_tcp() {
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer =
            RpcServer::with_tcp_port::<i32, i32>("test", port, Box::new(|x| Ok(x + 1)));
        assert!(server.is_running());

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        let client: RpcClient<i32, i32> = RpcClient::with_tcp_addr(addr);
        while !client.ping() {
            thread::sleep(Duration::from_millis(1));
        }
        let response = client.call(1);
        assert_eq!(response.unwrap(), 2);
    }

    #[test]
    fn test_callback_with_errors_tcp() {
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer = RpcServer::with_tcp_port::<i32, i32>(
            "test",
            port,
            Box::new(|_| Err(String::from("callback example error"))),
        );
        assert!(server.is_running());

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        let client: RpcClient<i32, i32> = RpcClient::with_tcp_addr(addr);
        while !client.ping() {
            thread::sleep(Duration::from_millis(1));
        }
        let response = client.call(0);
        let err = response.unwrap_err().to_string();
        let re = Regex::new(r"RpcError:").unwrap();
        assert!(re.is_match(&err));
    }

    #[test]
    fn test_mismatched_types_tcp() {
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer =
            RpcServer::with_tcp_port::<i32, i32>("test", port, Box::new(|x| Ok(x + 1)));
        assert!(server.is_running());

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        let client: RpcClient<String, String> = RpcClient::with_tcp_addr(addr);
        while !client.ping() {
            thread::sleep(Duration::from_millis(1));
        }

        let response = client.call(String::from("hello"));
        let err = response.unwrap_err().to_string();
        let re = Regex::new(r"failed to deserialize").unwrap();
        assert!(re.is_match(&err));
    }

    #[test]
    fn test_large_data_tcp() {
        let port: u16 = portpicker::pick_unused_port().unwrap();

        let server: RpcServer =
            RpcServer::with_tcp_port::<String, usize>("test", port, Box::new(|x| Ok(x.len())));
        assert!(server.is_running());

        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        let client: RpcClient<String, usize> = RpcClient::with_tcp_addr(addr);
        while !client.ping() {
            thread::sleep(Duration::from_millis(1));
        }

        let size = BUFFER_SIZE + BUFFER_SIZE / 2;
        let mut request = String::new();
        while request.len() < size {
            request += "adsfadfasdfasdfasdfasdfasdfsadf";
        }
        let size = request.len();

        let response = client.call(request);
        assert_eq!(response.unwrap(), size);
    }
}
