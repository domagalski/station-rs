use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::mem;
use std::net::{SocketAddr, UdpSocket};
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use log;
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::net::{self, Udp, UnixUdp};

// the callback type for passing messages into the callback
type Callback<T> = Box<dyn Send + Fn(T)>;

#[derive(Clone)]
enum SubscriberPort {
    UdpPort(u16),
    UnixDatagram(PathBuf),
}

const PUBSUB_ERROR: &str = "PubSubError";

trait PublishHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    fn name(&self) -> &'static str;

    // Send a message
    fn send(&self, message: &T) -> Result<usize, IoError>;

    // return the UDP socket addr
    fn udp(&self) -> Option<SocketAddr> {
        None
    }

    // return the path of the Unix socket
    fn unix_datagram(&self) -> Option<PathBuf> {
        None
    }
}

// TODO have a publish queue
/// The Publisher publishes messages of type `T` to its subscribers.
pub struct Publisher<T> {
    name: String,
    handlers: HashMap<String, Box<dyn PublishHandler<T>>>,
}

fn sockaddr_string(addr: SocketAddr) -> String {
    format!("{}", addr)
}

fn path_string(path: &Path) -> String {
    format!("{}", path.to_str().unwrap_or(""))
}

impl<T> Publisher<T>
where
    T: DeserializeOwned + Serialize,
{
    /// Create a new publisher with a name.
    ///
    /// Args:
    /// * `name`: The name of the publisher.
    pub fn new(name: &str) -> Publisher<T> {
        Publisher {
            name: String::from(name),
            handlers: HashMap::new(),
        }
    }

    /// Publish a message to the subscribers.
    ///
    /// By default, the publisher has no subscribers. Subscribers must be added before the `publish`
    /// method does anything. Without subscribers, `publish` is a no-op.
    ///
    pub fn publish(&mut self, message: &T) {
        let tmp = mem::take(&mut self.handlers);
        for (key, handler) in tmp.into_iter() {
            if let Err(err) = handler.send(message) {
                log::error!(
                    "Failed to publish to '{}' publisher {} with error: {}",
                    self.name,
                    handler.name(),
                    err
                );
            } else {
                assert!(self.handlers.insert(key, handler).is_none());
            }
        }
    }

    /// Add a UDP endpoint to publish to.
    ///
    /// Args:
    /// * `addr`: The address of the UDP endpoint.
    pub fn add_udp_endpoint(&mut self, addr: SocketAddr) -> bool {
        let addr_str = sockaddr_string(addr);
        if self.handlers.contains_key(&addr_str) {
            log::warn!(
                "Pubsub handler for UDP endpoint already exists: {}",
                addr_str
            );
            return false;
        }
        assert!(self
            .handlers
            .insert(addr_str, Box::new(UdpPublisher { addr }))
            .is_none());
        true
    }

    /// Add a Unix Datagram endpoint to publish to.
    ///
    /// Args:
    /// * `path`: The Unix socket path to publish to.
    pub fn add_unix_datagram_endpoint(&mut self, path: &Path) -> bool {
        let addr_str = path_string(path);
        if self.handlers.contains_key(&addr_str) {
            log::warn!(
                "Pubsub handler for Unix datagram endpoint already exists: {}",
                addr_str
            );
            return false;
        }
        assert!(self
            .handlers
            .insert(addr_str, Box::new(UnixDatagramPublisher::new(path)))
            .is_none());
        true
    }

    /// Return the total number of Publish endpoints.
    pub fn num_endpoints(&self) -> usize {
        self.handlers.len()
    }
}

struct UdpPublisher {
    addr: SocketAddr,
}

impl<T> PublishHandler<T> for UdpPublisher
where
    T: DeserializeOwned + Serialize,
{
    fn name(&self) -> &'static str {
        "UDP"
    }

    fn send(&self, message: &T) -> Result<usize, IoError> {
        // TODO bring this into a new() function
        let udp = UdpSocket::bind("0.0.0.0:0").unwrap();
        let msg_bytes = net::construct_message(message);
        udp.send_to(&msg_bytes, self.addr)
    }

    fn udp(&self) -> Option<SocketAddr> {
        Some(self.addr)
    }
}

struct UnixDatagramPublisher {
    path: PathBuf,
    socket: UnixDatagram,
    path_seen: RefCell<bool>,
}

impl UnixDatagramPublisher {
    fn new(path: &Path) -> UnixDatagramPublisher {
        let socket = UnixDatagram::unbound().unwrap();
        UnixDatagramPublisher {
            path: PathBuf::from(path),
            socket,
            path_seen: RefCell::new(false),
        }
    }
}

impl<T> PublishHandler<T> for UnixDatagramPublisher
where
    T: DeserializeOwned + Serialize,
{
    fn name(&self) -> &'static str {
        "Unix Datagram"
    }

    fn send(&self, message: &T) -> Result<usize, IoError> {
        // sometimes the subscriber endpoint is slow to show up. don't treat it as an error to send
        // when the endpoint doesn't exist yet. That said, if the endpoint did exist, assume it
        // always does exist until there's a fatal OS error writing to it.
        if !(self.path.exists() || *self.path_seen.borrow()) {
            return Ok(0);
        } else if self.path.exists() && !*self.path_seen.borrow() {
            *self.path_seen.borrow_mut() = true;
        }

        let msg_bytes = net::construct_message(message);
        self.socket.send_to(&msg_bytes, &self.path)
    }

    fn unix_datagram(&self) -> Option<PathBuf> {
        Some(self.path.clone())
    }
}

/// The Subscriber receives messages of type `T` and processes them with a callback.
pub struct Subscriber {
    name: String,
    subscribe_port: SubscriberPort,
    stop_requested: Arc<RwLock<bool>>,
    thread: Option<JoinHandle<()>>,
}

impl Subscriber {
    fn new<T>(
        name: &'static str,
        subscribe_port: SubscriberPort,
        callback: Callback<T>,
    ) -> Subscriber
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
    {
        let stop_requested = Arc::new(RwLock::new(false));
        let is_stop_requested = stop_requested.clone();
        let thread_subscribe_port = subscribe_port.clone();

        let thread = thread::spawn(move || {
            let subscriber: Box<dyn SubscribeHandler<T>> = match thread_subscribe_port {
                SubscriberPort::UdpPort(port) => Box::new(UdpSubscriber::new(port)),
                SubscriberPort::UnixDatagram(path) => {
                    Box::new(UnixDatagramSubscriber::new(Path::new(&path)))
                }
            };
            while !*is_stop_requested.read() {
                match subscriber.recv() {
                    Ok(msg) => (*callback)(msg),
                    Err(err) => {
                        log::debug!("recv error on Subscriber '{}' with error:\n{}", name, err)
                    }
                };
            }
        });

        Subscriber {
            name: String::from(name),
            subscribe_port,
            stop_requested,
            thread: Some(thread),
        }
    }

    /// Create a subscriber that listens on a UDP port.
    ///
    /// Args:
    /// * `name` The name to refer to the subscriber.
    /// * `port`: The UDP port to listen for new messages on.
    /// * `callback`: The function to call on incoming data.
    pub fn with_udp_port<T>(name: &'static str, port: u16, callback: Callback<T>) -> Subscriber
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
    {
        Subscriber::new(name, SubscriberPort::UdpPort(port), callback)
    }

    /// Create a subscriber that listens on a Unix Datagram socket.
    ///
    /// Args:
    /// * `name` The name to refer to the subscriber.
    /// * `path`: The unix socket path to bind the server to.
    /// * `callback`: The function to call on incoming data.
    pub fn with_unix_datagram<T>(
        name: &'static str,
        path: &Path,
        callback: Callback<T>,
    ) -> Subscriber
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
    {
        Subscriber::new(
            name,
            SubscriberPort::UnixDatagram(PathBuf::from(path)),
            callback,
        )
    }

    /// Check if the Subscriber is running.
    pub fn is_running(&self) -> bool {
        self.thread.is_some()
    }

    /// Stop the Subscriber
    pub fn stop(&mut self) {
        if self.is_running() {
            log::debug!("Stopping Subscriber: {}", self.name);
            *self.stop_requested.write() = true;
            self.send_stop_signal();
            self.thread.take().unwrap().join().unwrap();
        }
    }

    fn send_stop_signal(&self) {
        match &self.subscribe_port {
            SubscriberPort::UdpPort(port) => self.send_stop_signal_udp(*port),
            SubscriberPort::UnixDatagram(path) => self.send_stop_signal_unix(&path),
        }
    }

    fn send_stop_signal_udp(&self, port: u16) {
        let addr = format!("127.0.0.1:{}", port);
        let mut udp = Udp::new(UdpSocket::bind("0.0.0.0:0").unwrap());
        udp.set_write_addr(addr.parse().unwrap());
        net::write_stop_signal(Ok(udp), &self.name);
    }

    fn send_stop_signal_unix(&self, path: &Path) {
        let mut unix = UnixUdp::new(UnixDatagram::unbound().unwrap());
        unix.set_path(path);
        net::write_stop_signal(Ok(unix), &self.name);
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        self.stop();
    }
}

trait SubscribeHandler<T>
where
    T: DeserializeOwned + Serialize,
{
    fn recv(&self) -> Result<T, IoError>;
}

struct UdpSubscriber {
    udp: RefCell<Option<Udp>>,
}

impl UdpSubscriber {
    fn new(port: u16) -> UdpSubscriber {
        let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
        let listener = UdpSocket::bind(addr).expect(&format!("Cannot bind to UDP port: {}", port));

        UdpSubscriber {
            udp: RefCell::new(Some(Udp::new(listener))),
        }
    }
}

impl<T> SubscribeHandler<T> for UdpSubscriber
where
    T: DeserializeOwned + Serialize,
{
    fn recv(&self) -> Result<T, IoError> {
        let mut udp = self.udp.borrow_mut().take().unwrap();
        let response = net::recv(&mut udp, None, PUBSUB_ERROR, false);
        *self.udp.borrow_mut() = Some(udp);
        response
    }
}

struct UnixDatagramSubscriber {
    unix: RefCell<Option<UnixUdp>>,
}

impl UnixDatagramSubscriber {
    fn new(path: &Path) -> UnixDatagramSubscriber {
        UnixDatagramSubscriber {
            unix: RefCell::new(Some(UnixUdp::new(
                UnixDatagram::bind(path)
                    .expect(&format!("Cannot bind to Unix datagram socket: {:?}", path)),
            ))),
        }
    }
}

impl<T> SubscribeHandler<T> for UnixDatagramSubscriber
where
    T: DeserializeOwned + Serialize,
{
    fn recv(&self) -> Result<T, IoError> {
        let mut unix = self.unix.borrow_mut().take().unwrap();
        let response = net::recv(&mut unix, None, PUBSUB_ERROR, false);
        *self.unix.borrow_mut() = Some(unix);
        response
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use parking_lot::Mutex;
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
    fn start_stop_subscriber_udp() {
        setup_logging();
        let port: u16 = portpicker::pick_unused_port().unwrap();
        let mut subscriber: Subscriber =
            Subscriber::with_udp_port::<i32>("test", port, Box::new(|_| {}));
        assert!(subscriber.is_running());
        subscriber.stop();
        assert!(!subscriber.is_running());
    }

    #[test]
    fn start_stop_subscriber_unix_datagram() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        let mut subscriber: Subscriber =
            Subscriber::with_unix_datagram::<i32>("test", &socket, Box::new(|_| {}));
        assert!(subscriber.is_running());
        subscriber.stop();
        assert!(!subscriber.is_running());
    }

    #[test]
    fn one_publisher_many_subscribers() {
        #[derive(Deserialize, Serialize, Debug, Clone)]
        struct TestMessage {
            name: String,
            value: u32,
        }

        impl TestMessage {
            fn eq(&self, other: &TestMessage) -> bool {
                self.name == other.name && self.value == other.value
            }
        }

        let null_message = TestMessage {
            name: String::new(),
            value: 0,
        };

        let test_message = TestMessage {
            name: String::from("test_message"),
            value: 123,
        };

        let mut publisher = Publisher::new("test");

        let udp_port: u16 = portpicker::pick_unused_port().unwrap();
        let udp_sub_msg = Arc::new(Mutex::new(null_message.clone()));
        let udp_msg_clone = Arc::clone(&udp_sub_msg);
        let mut udp_subscriber: Subscriber = Subscriber::with_udp_port::<TestMessage>(
            "test",
            udp_port,
            Box::new(move |msg| {
                let mut data = udp_msg_clone.lock();
                *data = msg.clone();
            }),
        );
        assert!(publisher.add_udp_endpoint(format!("127.0.0.1:{}", udp_port).parse().unwrap()));
        assert!(udp_subscriber.is_running());

        let unix_dgram_sub_msg = Arc::new(Mutex::new(null_message.clone()));
        let unix_dgram_msg_clone = Arc::clone(&unix_dgram_sub_msg);
        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        //let socket = socket.as_path().to_str().unwrap();
        let mut unix_subsciber = Subscriber::with_unix_datagram::<TestMessage>(
            "test",
            &socket,
            Box::new(move |msg| {
                let mut data = unix_dgram_msg_clone.lock();
                *data = msg.clone();
            }),
        );
        assert!(publisher.add_unix_datagram_endpoint(&socket));
        assert!(unix_subsciber.is_running());

        let n_subscribers = 2;

        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        while start.elapsed() < timeout {
            publisher.publish(&test_message);

            let udp_data = udp_sub_msg.lock();
            let unix_dgram_data = unix_dgram_sub_msg.lock();
            if udp_data.eq(&test_message) && unix_dgram_data.eq(&test_message) {
                break;
            }
            if publisher.num_endpoints() != n_subscribers {
                break;
            }

            thread::sleep(Duration::from_millis(10));
        }
        let timed_out = start.elapsed() >= timeout;
        udp_subscriber.stop();
        unix_subsciber.stop();

        assert_eq!(publisher.num_endpoints(), n_subscribers);
        assert!(!timed_out);

        assert!(!udp_subscriber.is_running());
        assert!(!unix_subsciber.is_running());
    }

    #[test]
    fn many_publishers_one_subscriber_udp() {
        let mut publisher = Publisher::new("test");
        let capture = Arc::new(Mutex::new(HashMap::new()));
        let cb_capture_a = Arc::clone(&capture);
        let cb_capture_b = Arc::clone(&capture);

        let udp_port_a: u16 = portpicker::pick_unused_port().unwrap();
        let udp_subscriber_a: Subscriber = Subscriber::with_udp_port::<u32>(
            "test",
            udp_port_a,
            Box::new(move |msg| {
                let mut data = cb_capture_a.lock();
                data.insert(0, msg);
            }),
        );
        assert!(publisher.add_udp_endpoint(format!("127.0.0.1:{}", udp_port_a).parse().unwrap()));

        let udp_port_b: u16 = portpicker::pick_unused_port().unwrap();
        let udp_subscriber_b: Subscriber = Subscriber::with_udp_port::<u32>(
            "test",
            udp_port_b,
            Box::new(move |msg| {
                let mut data = cb_capture_b.lock();
                data.insert(1, msg);
            }),
        );
        assert!(publisher.add_udp_endpoint(format!("127.0.0.1:{}", udp_port_b).parse().unwrap()));

        assert!(udp_subscriber_a.is_running());
        assert!(udp_subscriber_b.is_running());
        let num_endpoints = publisher.num_endpoints();

        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        while start.elapsed() < timeout {
            publisher.publish(&0);
            if capture.lock().len() >= num_endpoints {
                break;
            }

            thread::sleep(Duration::from_millis(10));
        }

        assert!(capture.lock().len() == num_endpoints);
        assert!(start.elapsed() < timeout);
    }

    #[test]
    fn many_publishers_one_subscriber_unix_datagram() {
        let mut publisher = Publisher::new("test");
        let capture = Arc::new(Mutex::new(HashMap::new()));
        let cb_capture_a = Arc::clone(&capture);
        let cb_capture_b = Arc::clone(&capture);

        let tempdir_a = tempfile::tempdir().unwrap();
        let socket_a = tempdir_a.path().join("socket");
        let unix_subscriber_a: Subscriber = Subscriber::with_unix_datagram::<u32>(
            "test",
            &socket_a,
            Box::new(move |msg| {
                let mut data = cb_capture_a.lock();
                data.insert(0, msg);
            }),
        );
        assert!(publisher.add_unix_datagram_endpoint(&socket_a));

        let tempdir_b = tempfile::tempdir().unwrap();
        let socket_b = tempdir_b.path().join("socket");
        let unix_subscriber_b: Subscriber = Subscriber::with_unix_datagram::<u32>(
            "test",
            &socket_b,
            Box::new(move |msg| {
                let mut data = cb_capture_b.lock();
                data.insert(1, msg);
            }),
        );
        assert!(publisher.add_unix_datagram_endpoint(&socket_b));

        assert!(unix_subscriber_a.is_running());
        assert!(unix_subscriber_b.is_running());
        let num_endpoints = publisher.num_endpoints();
        assert_eq!(num_endpoints, 2);

        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        while start.elapsed() < timeout {
            publisher.publish(&0);
            if capture.lock().len() >= num_endpoints {
                break;
            }

            thread::sleep(Duration::from_millis(10));
        }

        assert!(capture.lock().len() == num_endpoints);
        assert!(start.elapsed() < timeout);
    }

    #[test]
    fn no_duplicate_publishers() {
        let mut publisher: Publisher<u32> = Publisher::new("test");

        let udp_port: u16 = portpicker::pick_unused_port().unwrap();
        assert!(publisher.add_udp_endpoint(format!("127.0.0.1:{}", udp_port).parse().unwrap()));
        assert!(!publisher.add_udp_endpoint(format!("127.0.0.1:{}", udp_port).parse().unwrap()));

        let tempdir = tempfile::tempdir().unwrap();
        let socket = tempdir.path().join("socket");
        assert!(publisher.add_unix_datagram_endpoint(&socket));
        assert!(!publisher.add_unix_datagram_endpoint(&socket));

        assert_eq!(publisher.num_endpoints(), 2);
    }
}
