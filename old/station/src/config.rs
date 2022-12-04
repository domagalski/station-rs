//! Basic configuration system for `Process` objects in the `station` crate.
//!
//! The `Config` struct is used to hold RPC and PubSub configurations and can be written/read as
//! YAML. The RPC section is used to configure TCP addresses only, whereas RPC via Unix sockets are
//! automatically determined from a run directory that `Process` uses. The PubSub configuration,
//! however, contains both UDP and Unix datagram socket configurations, as it's used to determine
//! the various endpoints a publisher must publish to.
//!
//! Example configration YAML:
//! ```text
//! rpc:
//!   ## Process named "pets" with "dogs" and "cats" methods.
//!   pets.dogs: "127.0.0.1:15003"
//!   pets.cats: "127.0.0.1:17970"
//!   # Process named "class" with "lecture" method.
//!   class.lecture: "127.0.0.1:16928"
//! pubsub:
//!   ## Topic named "coffee" with subscriptions in the "mocha", "donuts", and "latte" processes.
//!   coffee:
//!     - name: mocha
//!       config:
//!         Udp: "127.0.0.1:21840"
//!     - name: donuts
//!       config: Unix
//!     - name: latte
//!       config:
//!         Udp: "127.0.0.1:18859"
//!   ## Topic named "turtles" with subscriptions in the "rabbits" and "shell" processes.
//!   turtles:
//!     - name: rabbits
//!       config: Unix
//!     - name: shell
//!       config:
//!         Udp: "127.0.0.1:15069"
//! ```

use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::fs::{self, File};
use std::io::{Error as IoError, ErrorKind};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

type ConfigResult<T> = Result<T, ConfigError>;

/// Errors related to creating station configurations.
#[derive(Debug)]
pub struct ConfigError {
    reason: String,
}

impl ConfigError {
    /// Create a new ConfigError
    fn new(reason: &str) -> ConfigError {
        ConfigError {
            reason: String::from(reason),
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
        write!(f, "ConfigError: {}", self.reason)
    }
}

impl Error for ConfigError {}

/// What socket type to use for PubSub.
#[derive(Clone, Debug, Eq, Deserialize, Hash, Serialize, PartialEq)]
pub enum PubSubKind {
    Udp(SocketAddr),
    Unix,
}

/// Network endpoint configuration for a PubSub topic.
#[derive(Clone, Debug, Eq, Deserialize, Hash, Serialize, PartialEq)]
pub struct PubSubEndpoint {
    name: String,
    config: PubSubKind,
}

impl PubSubEndpoint {
    /// Create a PubSub endpoint config with a UDP address.
    pub fn new_udp_endpoint(name: &str, addr: SocketAddr) -> PubSubEndpoint {
        PubSubEndpoint {
            name: String::from(name),
            config: PubSubKind::Udp(addr),
        }
    }

    /// Create a PubSub endpoint config with a name for generating a Unix socket path.
    pub fn new_unix_endpoint(name: &str) -> PubSubEndpoint {
        PubSubEndpoint {
            name: String::from(name),
            config: PubSubKind::Unix,
        }
    }

    /// Return the name of the endpoint.
    pub fn name(&self) -> String {
        self.name.clone()
    }

    /// Return the endpoint configuration.
    pub fn config(&self) -> PubSubKind {
        self.config.clone()
    }

    /// Get the UDP socket address for a UDP endpoint.
    pub fn udp(&self) -> Option<SocketAddr> {
        match &self.config {
            PubSubKind::Udp(addr) => Some(*addr),
            _ => None,
        }
    }

    /// Get the name to use to construct a Unix socket.
    pub fn unix(&self) -> Option<String> {
        match &self.config {
            PubSubKind::Unix => Some(self.name.clone()),
            _ => None,
        }
    }
}

/// RPC and PubSub configuration.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Config {
    rpc: HashMap<String, SocketAddr>,
    pubsub: HashMap<String, HashSet<PubSubEndpoint>>,
}

impl Config {
    /// Create a new config.
    pub fn new() -> Config {
        Config {
            rpc: HashMap::new(),
            pubsub: HashMap::new(),
        }
    }

    fn err_rpc_exists(&self, name: &str) -> ConfigResult<()> {
        if self.rpc.contains_key(name) {
            return Err(ConfigError::new(&format!("RPC exists: {}", name)));
        }
        Ok(())
    }

    /// Add an RPC config entry based on TCP port usage.
    ///
    /// Args:
    /// * `name`: The name to associate the config with.
    /// * `addr`: The TCP socket address to use for the RPC listener.
    pub fn add_rpc(&mut self, name: &str, addr: SocketAddr) -> ConfigResult<()> {
        self.err_rpc_exists(name)?;
        assert!(self.rpc.insert(String::from(name), addr).is_none());
        Ok(())
    }

    /// Get the TCP port number for an RPC listener.
    ///
    /// Args:
    /// * `name`: The name in the config entries.
    pub fn get_rpc(&self, name: &str) -> Option<SocketAddr> {
        match self.rpc.get(name) {
            Some(addr) => Some(*addr),
            None => None,
        }
    }

    /// Add a PubSub endpoint to the config.
    ///
    /// Args:
    /// * `topic`: The PubSub topic to add the endpoint to.
    /// * `endpoint`: An endpoint for a publisher to publish to.
    pub fn add_pubsub(&mut self, topic: &str, endpoint: &PubSubEndpoint) -> ConfigResult<()> {
        let topic_config = self
            .pubsub
            .entry(String::from(topic))
            .or_insert(HashSet::new());

        // disallow duplicate topic names even if the config is different
        for config in topic_config.iter() {
            if config.name() == endpoint.name() {
                return Err(ConfigError::new(&format!(
                    "Endpoint for PubSub topic {} exists: {}",
                    topic,
                    endpoint.name()
                )));
            }
        }

        topic_config.insert(endpoint.clone());
        Ok(())
    }

    /// Get the config for a PubSub endpoint for a topic.
    ///
    /// Args:
    /// * `topic`: The PubSub topic to fetch endpoint configs for.
    /// * `endpoint`: The name of the PubSub endpoint to query.
    pub fn get_pubsub_endpoint(&self, topic: &str, endpoint: &str) -> Option<PubSubEndpoint> {
        let topic_config = match self.get_pubsub_topic(topic) {
            Some(config) => config,
            None => return None,
        };

        for config in topic_config.iter() {
            if config.name() == String::from(endpoint) {
                return Some(config.clone());
            }
        }
        None
    }

    /// Get the set of endpoint configs for a PubSub topic.
    ///
    /// Args:
    /// * `topic`: The PubSub topic to fetch endpoint configs for.
    pub fn get_pubsub_topic(&self, topic: &str) -> Option<HashSet<PubSubEndpoint>> {
        match self.pubsub.get(topic) {
            Some(config) => Some(config.clone()),
            None => None,
        }
    }

    /// Check if a config exists for a PubSub topic.
    pub fn has_pubsub(&self, topic: &str) -> bool {
        self.pubsub.contains_key(topic)
    }

    /// Read a config from a YAML file.
    ///
    /// Args:
    /// * `path`: Path to the file to read.
    pub fn read_yaml(path: &Path) -> Result<Config, IoError> {
        let file = File::open(path)?;
        match serde_yaml::from_reader(file) {
            Ok(config) => Ok(config),
            Err(err) => Err(IoError::new(ErrorKind::InvalidData, err)),
        }
    }

    /// Write the config as a YAML file.
    ///
    /// Args:
    /// * `path`: Path to the file to write.
    pub fn write_yaml(&self, path: &Path) -> Result<(), IoError> {
        let file = File::create(path)?;
        serde_yaml::to_writer(file, self).unwrap();
        Ok(())
    }
}

/// Get the unix socket directory for a run directory.
///
/// This evaluates to `<run_dir>/sockets`.
pub fn unix_socket_dir(run_dir: &Path) -> PathBuf {
    run_dir.join("sockets")
}

/// Create the subdirectories expected to exist in the run directory.
pub fn initialize_run_dir(run_dir: &Path) -> bool {
    let mut success = true;
    let socket_dir = unix_socket_dir(run_dir);
    log::trace!("Unix socket directory: {:?}", socket_dir);
    if let Err(err) = fs::create_dir_all(&socket_dir) {
        log::error!("Failed to create unix socket directory: {}", err);
        success = false;
    }
    success
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::{IpAddr, Ipv4Addr};

    use log;
    use portpicker;

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

    fn pick_unused_addr() -> SocketAddr {
        SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            portpicker::pick_unused_port().unwrap(),
        )
    }

    #[test]
    fn add_items_to_config() {
        setup_logging();
        let mut config = Config::new();
        assert!(config.add_rpc("tcp", pick_unused_addr()).is_ok());

        // should error out, as the config exists.
        let result = config.add_rpc("tcp", pick_unused_addr());
        assert!(result.is_err());
        log::debug!("{}", result.err().unwrap());

        let config = config;
        let yaml = serde_yaml::to_string(&config).unwrap();
        log::debug!("yaml:\n{}", yaml);

        assert!(config.get_rpc("tcp").is_some());
        let addr = config.get_rpc("tcp").unwrap();
        assert!(addr.port() > 0);
    }

    #[test]
    fn load_save_config_file() {
        setup_logging();
        let mut config = Config::new();
        assert!(config.add_rpc("cats", pick_unused_addr()).is_ok());
        assert!(config.add_rpc("dogs", pick_unused_addr()).is_ok());
        assert!(config
            .add_pubsub("turtles", &PubSubEndpoint::new_unix_endpoint("rabbits"))
            .is_ok());
        assert!(config
            .add_pubsub(
                "turtles",
                &PubSubEndpoint::new_udp_endpoint("shell", pick_unused_addr()),
            )
            .is_ok());
        assert!(config
            .add_pubsub("coffee", &PubSubEndpoint::new_unix_endpoint("donuts"))
            .is_ok());
        // coffee.donuts is already inserted, so re-inserting does nothing, but is fine
        assert!(config
            .add_pubsub("coffee", &PubSubEndpoint::new_unix_endpoint("donuts"))
            .is_err());
        assert!(config
            .add_pubsub(
                "coffee",
                &PubSubEndpoint::new_udp_endpoint("latte", pick_unused_addr()),
            )
            .is_ok());
        assert!(config
            .add_pubsub(
                "coffee",
                &PubSubEndpoint::new_udp_endpoint("mocha", pick_unused_addr()),
            )
            .is_ok());
        let config = config;
        assert!(config.get_rpc("cats").is_some());

        let tempdir = tempfile::tempdir().unwrap();
        let yaml_path = tempdir.path().join("config.yaml");
        assert!(config.write_yaml(&yaml_path).is_ok());

        let yaml = serde_yaml::to_string(&config).unwrap();
        log::trace!("config:\n{}", yaml);

        let recovered_config = Config::read_yaml(&yaml_path);
        assert!(recovered_config.is_ok());
        let recovered_config = recovered_config.unwrap();
        log::trace!("recovered config: {:?}", recovered_config);

        assert_eq!(recovered_config, config);
        assert!(recovered_config.get_rpc("dogs").is_some());
        assert!(recovered_config.get_rpc("cats").is_some());
        let addr = recovered_config.get_rpc("cats").unwrap();
        assert!(addr.port() > 0);
        assert!(recovered_config
            .get_pubsub_endpoint("turtles", "rabbits")
            .is_some());
        assert!(recovered_config
            .get_pubsub_endpoint("turtles", "shell")
            .is_some());
        assert!(recovered_config
            .get_pubsub_endpoint("turtles", "shells")
            .is_none());
        assert!(recovered_config
            .get_pubsub_endpoint("coffee", "donuts")
            .is_some());
        assert!(recovered_config
            .get_pubsub_endpoint("coffee", "latte")
            .is_some());
        assert!(recovered_config
            .get_pubsub_endpoint("coffee", "mocha")
            .is_some());
        assert!(recovered_config
            .get_pubsub_endpoint("coffee", "ice-cap")
            .is_none());
    }

    #[test]
    fn try_load_bad_yaml() {
        setup_logging();
        let tempdir = tempfile::tempdir().unwrap();
        let yaml_path = tempdir.path().join("config.yaml");
        {
            let mut file = File::create(&yaml_path).unwrap();
            file.write_all(b"invalid yaml data").unwrap();
        }

        let no_config = Config::read_yaml(&yaml_path);
        assert!(no_config.is_err());
        log::trace!("yaml error: {:?}", no_config.err());

        let path_404 = tempdir.path().join("404.yaml");
        let no_path = Config::read_yaml(&path_404);
        assert!(no_path.is_err());
        log::trace!("path error: {:?}", no_path.err());
    }

    #[test]
    fn test_initialize_run_dir() {
        let tempdir = tempfile::tempdir().unwrap();
        assert!(initialize_run_dir(tempdir.path()));
        let socket_dir = unix_socket_dir(tempdir.path());
        assert!(socket_dir.exists());
    }
}
