use std::collections::HashMap;
use std::fmt::Debug;
use std::io::{Error as IoError, ErrorKind};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::config::{self, Config, PubSubKind};
pub use crate::pubsub::Callback as PubSubCallback;
use crate::pubsub::{Publisher, Subscriber};
pub use crate::rpc::Callback as RpcCallback;
use crate::rpc::{RpcClient, RpcError, RpcServer};

/// Process helper for RPC and PubSub communication.
///
/// The `station` library's RPC and PubSub clients/servers require a bit of manual tuning. However,
/// the Process helper and configuration system simplifies this. Unless specified in a config,
/// networking for RPCs and PubSub channels are automatically generated Unix sockets based on the
/// process name and the RPC/PubSub channel name. This allows for easy communication between
/// processes on the same machine and being specific about configuring endpoints for processes on
/// two different machines.
pub struct Process {
    run_dir: Option<PathBuf>,
    config: Config,
    name: String,
    rpc: HashMap<String, RpcServer>,
    subscribers: HashMap<String, Subscriber>,
}

impl Process {
    /// Create a new `Process` from a config file.
    ///
    /// Args:
    /// * `name`: The name to associate the process with. Best practice is that process names should
    /// be unique, as per the convention for automatically defining Unix socket paths.
    /// * `config_path`: Path to the YAML process/station configuration. This must exist even if no
    /// RPC or PubSub channels are defined in it. The directory containing this path must be
    /// writable by the user ID creating the `Process` instance. Any RPC method listed in the config
    /// at this path must be named as <process_name>.<rpc_name> in order for `Process` to find TCP
    /// configurations when calling RPCs implemented as TCP sockets.
    pub fn from_config_file(name: &str, config_path: &Path) -> Result<Process, IoError> {
        let config_path = config_path.canonicalize()?;
        let config = Config::read_yaml(&config_path)?;
        let run_directory = config_path.parent().unwrap();
        Process::new(name, run_directory, &config)
    }

    /// Create a new `Process` with a config and run directory.
    ///
    /// Args:
    /// * `name`: The name to associate the process with. Best practice is that process names should
    /// be unique, as per the convention for automatically defining Unix socket paths.
    /// * `run_directory`: Path to where Unix sockets will be created for this process.
    /// * `config`: A process/station configuration defining TCP interfaces.
    pub fn new(name: &str, run_directory: &Path, config: &Config) -> Result<Process, IoError> {
        let run_directory = run_directory.canonicalize()?;
        assert!(config::initialize_run_dir(&run_directory));
        Ok(Process {
            run_dir: Some(run_directory),
            config: config.clone(),
            name: String::from(name),
            rpc: HashMap::new(),
            subscribers: HashMap::new(),
        })
    }

    /// Create a new `Process` from without a run directory.
    ///
    /// Because the config has been pre-loaded and no run directory has been specified, the
    /// `Process` instance returned by this will not support RPC/PubSub with Unix sockets.
    ///
    /// Args:
    /// * `name`: The name to associate the process with. Best practice is that process names should
    /// be unique, as per the convention for automatically defining Unix socket paths.
    /// * `config`: A process/station configuration defining TCP interfaces.
    pub fn without_run_dir(name: &str, config: &Config) -> Process {
        Process {
            run_dir: None,
            config: config.clone(),
            name: String::from(name),
            rpc: HashMap::new(),
            subscribers: HashMap::new(),
        }
    }

    fn unix_socket_base(&self) -> Result<PathBuf, IoError> {
        match &self.run_dir {
            Some(dir) => Ok(config::unix_socket_dir(&dir).as_path().join(&self.name)),
            None => Err(IoError::new(
                ErrorKind::Unsupported,
                format!("Process {} has no run directory", self.name),
            )),
        }
    }

    fn config_name(process_name: &str, channel_name: &str) -> String {
        format!("{}.{}", process_name, channel_name)
    }

    fn get_rpc_from_config(&self, process_name: &str, rpc_name: &str) -> Option<SocketAddr> {
        let config_name = Process::config_name(process_name, rpc_name);
        self.config.get_rpc(&config_name)
    }

    /// Call an RPC.
    ///
    /// This function allows one to make a request to an RPC and get either the result of the call
    /// or an error returned. If the RPC endpoint is not listed in the config, it is assumed to be a
    /// Unix socket. If the endpoint does not exist, the RPC call will time out. The types for
    /// request `T` and response `U` must match waht the RPC server at the endpoint expects or an
    /// error may occur.
    ///
    /// Args:
    /// * `process_name`: The name of the `Process` instance running the RPC server.
    /// * `rpc_name`: The name of the RPC running inside the target `Process`.
    /// * `request`: The data to pass into the RPC request.
    /// * `timeout`: The expected maximum duration of the RPC call.
    ///
    /// Returns either the result of the RPC call on success or an `RpcError` on failure.
    pub fn call_rpc<T, U>(
        &self,
        process_name: &str,
        rpc_name: &str,
        request: T,
        timeout: Duration,
    ) -> Result<U, RpcError>
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        let client: RpcClient<T, U> = match self.get_rpc_from_config(process_name, rpc_name) {
            Some(addr) => RpcClient::with_tcp_addr(addr),
            None => {
                let socket_path = match &self.run_dir {
                    Some(dir) => config::unix_socket_dir(&dir)
                        .as_path()
                        .join(Process::config_name(process_name, rpc_name)),
                    None => {
                        return Err(RpcError::IoError(IoError::new(
                            ErrorKind::Unsupported,
                            format!("Process {} has no run directory", self.name),
                        )))
                    }
                };
                RpcClient::with_unix_socket(&socket_path)
            }
        };
        client.call(request, timeout)
    }

    /// Create an RPC server.
    ///
    /// If the RPC server is defined in the config the `Process` was initialized with, and the IP
    /// address in the config is localhost, then the RPC server is a TCP endpoint. If the address
    /// for the RPC server is not a localhost address, an error is returned. IF the RPC server is
    /// not listed in the config, then the endpoint is a Unix socket based on the RPC name, Process
    /// name, and config path.
    ///
    /// Args:
    /// * `name`: The name to assign the RPC so clients can call it.
    /// * `callback`: The function that is called when the RPC server receives data.
    pub fn create_rpc<T, U>(
        &mut self,
        name: &'static str,
        callback: RpcCallback<T, U>,
    ) -> Result<(), IoError>
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        if self.rpc.contains_key(name) {
            let msg = format!("RPC exists: {}", name);
            log::trace!("{}", msg);
            return Err(IoError::new(ErrorKind::AlreadyExists, msg));
        }

        let server = match self.get_rpc_from_config(&self.name, name) {
            Some(addr) => {
                if addr.ip() != IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)) {
                    let msg = format!(
                        "RPC {} refers to remote endpoint: {}",
                        Process::config_name(&self.name, name),
                        addr
                    );
                    return Err(IoError::new(ErrorKind::AddrNotAvailable, msg));
                }

                RpcServer::with_tcp_port(name, addr.port(), callback)
            }
            None => {
                let socket_path = self.unix_socket_base()?.as_path().with_extension(name);
                RpcServer::with_unix_socket(name, &socket_path, callback)
            }
        };

        assert!(self.rpc.insert(String::from(name), server).is_none());
        Ok(())
    }

    /// Publish a message to a topic.
    ///
    /// The topic must be defined in the `Process` config. Additionally, if publishing to a Unix
    /// socket, the run directory for the proces must exist.
    ///
    /// Args:
    /// * `topic`: The PubSub topic to publish to.
    /// * `message`: The data to publish.
    pub fn publish_to_topic<T>(&self, topic: &str, message: &T) -> Result<(), IoError>
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
    {
        if !self.config.has_pubsub(topic) {
            return Err(IoError::new(
                ErrorKind::NotFound,
                format!("PubSub topic not in config: {}", topic),
            ));
        }

        // TODO make it not necessary to do this every message. Needs refactoring usage of generic
        // parameters in the Publisher struct.
        let topic_name = Process::config_name(&self.name, topic);
        let mut publisher = Publisher::<T>::new(&topic_name);
        for config in self.config.get_pubsub_topic(topic).unwrap().iter() {
            if let Some(addr) = config.udp() {
                publisher.add_udp_endpoint(addr);
            } else if let Some(name) = config.unix() {
                let socket_path = match &self.run_dir {
                    Some(dir) => config::unix_socket_dir(&dir)
                        .as_path()
                        .join(Process::config_name(&name, topic)),
                    None => {
                        return Err(IoError::new(
                            ErrorKind::Unsupported,
                            format!("Process {} has no run directory", self.name),
                        ));
                    }
                };
                publisher.add_unix_datagram_endpoint(&socket_path);
            } else {
                panic!("impossible");
            }
        }

        publisher.publish(message);
        Ok(())
    }

    /// Subscribe to a topic.
    ///
    /// The topic must be defined in the `Process` config and there must be an endpoint listed for
    /// the `Process` name. If the endpoint is a UDP endpoint, it must be a local address.
    /// Additionally, if the topic has already been subscribed to, an error will be returned.
    ///
    /// Args:
    /// * `topic`: The PubSub topic to subscribe to.
    /// * `callback`: The function that is called when the PubSub subscriber receives data.
    pub fn subscribe_to_topic<T>(
        &mut self,
        topic: &str,
        callback: PubSubCallback<T>,
    ) -> Result<(), IoError>
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
    {
        if self.subscribers.contains_key(topic) {
            return Err(IoError::new(
                ErrorKind::AlreadyExists,
                format!("Already subscribed to topic: {}", topic),
            ));
        }

        let topic_config = match self.config.get_pubsub_endpoint(topic, &self.name) {
            Some(config) => config,
            None => {
                return Err(IoError::new(
                    ErrorKind::NotFound,
                    format!("PubSub topic not in config: {}", topic),
                ))
            }
        };

        let topic_name = format!("{}.{}", topic_config.name(), topic);
        match topic_config.config() {
            PubSubKind::Udp(addr) => {
                if addr.ip() != IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)) {
                    let msg = format!(
                        "PubSub topic {} refers to remote endpoint: {}",
                        Process::config_name(&self.name, topic),
                        addr
                    );
                    return Err(IoError::new(ErrorKind::AddrNotAvailable, msg));
                }
                assert!(self
                    .subscribers
                    .insert(
                        String::from(topic),
                        Subscriber::with_udp_port(&topic_name, addr.port(), callback),
                    )
                    .is_none());
            }
            PubSubKind::Unix => {
                let socket_path = self.unix_socket_base()?.with_extension(topic);
                assert!(self
                    .subscribers
                    .insert(
                        String::from(topic),
                        Subscriber::with_unix_datagram(&topic_name, &socket_path, callback),
                    )
                    .is_none());
            }
        }

        log::trace!("Subscribed to topic: {}", topic_name);
        Ok(())
    }

    /// Return the name of the `Process` instance.
    pub fn name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    use parking_lot::Mutex;

    use super::*;
    use crate::config::PubSubEndpoint;

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

    fn create_config(server_name: &str, rpc_name: &str) -> Config {
        let mut cfg = Config::new();
        cfg.add_rpc(
            &format!("{}.{}", server_name, rpc_name),
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                portpicker::pick_unused_port().unwrap(),
            ),
        )
        .unwrap();
        cfg.add_rpc(
            &format!("{}.invalid", server_name),
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                portpicker::pick_unused_port().unwrap(),
            ),
        )
        .unwrap();
        log::trace!("{:?}", cfg);
        cfg
    }

    #[test]
    fn create_process_with_tcp_rpc() {
        setup_logging();

        let rpc_name = "plus-one";
        let server_name = "rpc-test";
        let client_name = "rpc-client";

        let config = create_config(server_name, rpc_name);
        let mut process = Process::without_run_dir(server_name, &config);
        assert!(process
            .create_rpc::<i32, i32>(rpc_name, Box::new(|x| Ok(x + 1)))
            .is_ok());
        assert!(process
            .create_rpc::<i32, i32>(rpc_name, Box::new(|x| Ok(x + 1)))
            .is_err());
        assert!(process
            .create_rpc::<i32, i32>("invalid", Box::new(|x| Ok(x + 1)))
            .is_err());
        assert!(process
            .create_rpc::<i32, i32>("unspecified", Box::new(|x| Ok(x + 1)))
            .is_err());

        let client = Process::without_run_dir(client_name, &config);
        let result = client.call_rpc::<i32, i32>(server_name, rpc_name, 0, Duration::from_secs(5));

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, 1);

        // error because no run durectory associated with client, so no unix sockets.
        assert!(client
            .call_rpc::<i32, i32>(server_name, "unspecified", 0, Duration::from_secs(5))
            .is_err());
    }

    #[test]
    fn create_process_with_unix_rpc() {
        setup_logging();

        let rpc_name = "plus-one";
        let server_name = "rpc-test";
        let client_name = "rpc-client";

        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("config.yaml");
        Config::new().write_yaml(&config_path).unwrap();

        let process = Process::from_config_file(server_name, &config_path);
        assert!(process.is_ok());

        let mut process = process.unwrap();
        assert!(process
            .create_rpc::<i32, i32>(rpc_name, Box::new(|x| Ok(x + 1)))
            .is_ok());
        assert!(process
            .create_rpc::<i32, i32>(rpc_name, Box::new(|x| Ok(x + 1)))
            .is_err());

        let client = Process::from_config_file(client_name, &config_path).unwrap();
        let result = client.call_rpc::<i32, i32>(server_name, rpc_name, 0, Duration::from_secs(5));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn create_process_with_pubsub() {
        setup_logging();

        let mut cfg = Config::new();
        assert!(cfg
            .add_pubsub(
                "counter",
                &PubSubEndpoint::new_udp_endpoint(
                    "udp",
                    SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                        portpicker::pick_unused_port().unwrap(),
                    )
                )
            )
            .is_ok());
        assert!(cfg
            .add_pubsub(
                "counter",
                &PubSubEndpoint::new_udp_endpoint(
                    "invalid",
                    SocketAddr::new(
                        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                        portpicker::pick_unused_port().unwrap(),
                    )
                )
            )
            .is_ok());
        assert!(cfg
            .add_pubsub("counter", &PubSubEndpoint::new_unix_endpoint("unix",))
            .is_ok());
        let cfg = cfg;
        log::trace!(
            "pubsub test config:\n{}",
            serde_yaml::to_string(&cfg).unwrap()
        );
        let tempdir = tempfile::tempdir().unwrap();
        let config_path = tempdir.path().join("config.yaml");
        assert!(cfg.write_yaml(&config_path).is_ok());

        let unix_sub = Process::from_config_file("unix", &config_path);
        assert!(unix_sub.is_ok());
        let mut unix_sub = unix_sub.unwrap();
        let unix_counter = Arc::new(Mutex::new(0 as usize));
        {
            let counter = Arc::clone(&unix_counter);
            assert!(unix_sub
                .subscribe_to_topic::<i32>(
                    "counter",
                    Box::new(move |_| {
                        log::trace!("got a message on the unix subscriber");
                        *counter.lock() += 1;
                    })
                )
                .is_ok());
        }

        // error out because already subscribed
        {
            let counter = Arc::clone(&unix_counter);
            assert!(unix_sub
                .subscribe_to_topic::<i32>(
                    "counter",
                    Box::new(move |_| {
                        log::trace!("got a message on the unix subscriber");
                        *counter.lock() += 1;
                    })
                )
                .is_err());
        }

        let udp_sub = Process::from_config_file("udp", &config_path);
        assert!(udp_sub.is_ok());
        let mut udp_sub = udp_sub.unwrap();
        let udp_counter = Arc::new(Mutex::new(0 as usize));
        {
            let counter = Arc::clone(&udp_counter);
            assert!(udp_sub
                .subscribe_to_topic::<i32>(
                    "counter",
                    Box::new(move |_| {
                        log::trace!("got a message on the udp subscriber");
                        *counter.lock() += 1;
                    })
                )
                .is_ok());

            // topic doesn't exist
            assert!(udp_sub
                .subscribe_to_topic::<i32>("404", Box::new(move |_| { log::trace!("hello") }))
                .is_err());
        }

        {
            let invalid_sub = Process::from_config_file("invalid", &config_path);
            assert!(invalid_sub.is_ok());
            let mut invalid_sub = invalid_sub.unwrap();
            assert!(invalid_sub
                .subscribe_to_topic::<i32>("counter", Box::new(move |_| log::trace!("hello")))
                .is_err());
        }

        let publisher = Process::from_config_file("publisher", &config_path);
        assert!(publisher.is_ok());
        let publisher = publisher.unwrap();
        let start = Instant::now();
        while Instant::now().duration_since(start) < Duration::from_secs(10) {
            assert!(publisher.publish_to_topic("counter", &1).is_ok());
            {
                if *udp_counter.lock() > 0 && *unix_counter.lock() > 0 {
                    break;
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(*unix_counter.lock() > 0);
        assert!(*udp_counter.lock() > 0);
    }
}
