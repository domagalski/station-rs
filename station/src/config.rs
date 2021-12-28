use std::collections::{HashMap, HashSet};
use std::env;
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

/// Network endpoint configuration for a PubSub topic.
#[derive(Clone, Debug, Eq, Deserialize, Hash, Serialize, PartialEq)]
pub enum PubSubEndpoint {
    Udp(SocketAddr),
    Unix(String),
}

impl PubSubEndpoint {
    /// Get the UDP socket address for a UDP endpoint.
    pub fn udp(&self) -> Option<SocketAddr> {
        match self {
            PubSubEndpoint::Udp(addr) => Some(*addr),
            _ => None,
        }
    }

    /// Get the name to use to construct a Unix socket.
    pub fn unix(&self) -> Option<String> {
        match self {
            PubSubEndpoint::Unix(name) => Some(String::from(name)),
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
    pub fn add_rpc(&mut self, name: &str, addr: &SocketAddr) -> ConfigResult<()> {
        self.err_rpc_exists(name)?;
        assert!(self.rpc.insert(String::from(name), *addr).is_none());
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
    pub fn add_pubsub(&mut self, topic: &str, endpoint: &PubSubEndpoint) {
        let topic_config = self
            .pubsub
            .entry(String::from(topic))
            .or_insert(HashSet::new());
        topic_config.insert(endpoint.clone());
    }

    /// Get a set of PubSub subscriber endpoints for a topic.
    ///
    /// Args:
    /// * `topic`: The PubSub topic to fetch endpoint configs for.
    pub fn get_pubsub(&self, topic: &str) -> Option<HashSet<PubSubEndpoint>> {
        match self.pubsub.get(topic) {
            Some(config) => Some(config.clone()),
            None => None,
        }
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

pub const STATION_RUN_DIR_ENV_VAR: &str = "STATION_RUN_DIR";
const STATION_RUN_DIR_BASENAME: &str = ".station";
const STATION_UNIX_SOCK_DIR: &str = "sockets";

fn default_run_directory() -> PathBuf {
    dirs::home_dir()
        .unwrap()
        .as_path()
        .join(STATION_RUN_DIR_BASENAME)
}

/// Get the station run directory from the environment.
///
/// If the `STATION_RUN_DIR` variable is set, use it to set the station run directory, else
/// `~/.station` is the station root directory. The station run directory determines paths to
/// configs, logs, and unix sockets.
pub fn run_dir_from_env() -> PathBuf {
    match env::var(STATION_RUN_DIR_ENV_VAR) {
        Ok(value) => {
            if value.len() > 0 {
                PathBuf::from(&value)
            } else {
                default_run_directory()
            }
        }
        Err(_) => default_run_directory(),
    }
}

/// Get the unix socket directory, given a run directory.
pub fn unix_socket_dir(run_dir: &Path) -> PathBuf {
    run_dir.join(STATION_UNIX_SOCK_DIR)
}

/// Get the directory where Unix sockets are expected to be made.
pub fn unix_socket_dir_from_env() -> PathBuf {
    unix_socket_dir(&run_dir_from_env())
}

/// Create the subdirectories expected to exist in the run directory
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
        env::remove_var(STATION_RUN_DIR_ENV_VAR);
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
        assert!(config.add_rpc("tcp", &pick_unused_addr()).is_ok());

        // should error out, as the config exists.
        let result = config.add_rpc("tcp", &pick_unused_addr());
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
        assert!(config.add_rpc("cats", &pick_unused_addr()).is_ok());
        assert!(config.add_rpc("dogs", &pick_unused_addr()).is_ok());
        config.add_pubsub("turtles", &PubSubEndpoint::Unix(String::from("rabbits")));
        config.add_pubsub("turtles", &PubSubEndpoint::Udp(pick_unused_addr()));
        config.add_pubsub("coffee", &PubSubEndpoint::Unix(String::from("donuts")));
        // coffee.donuts is already inserted, so re-inserting does nothing, but is fine
        config.add_pubsub("coffee", &PubSubEndpoint::Unix(String::from("donuts")));
        config.add_pubsub("coffee", &PubSubEndpoint::Udp(pick_unused_addr()));
        config.add_pubsub("coffee", &PubSubEndpoint::Udp(pick_unused_addr()));
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
        assert!(recovered_config.get_pubsub("turtles").is_some());
        assert_eq!(recovered_config.get_pubsub("turtles").unwrap().len(), 2);
        assert!(recovered_config.get_pubsub("coffee").is_some());
        assert_eq!(recovered_config.get_pubsub("coffee").unwrap().len(), 3);
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
    fn get_run_dir() {
        setup_logging();
        let homedir = dirs::home_dir().unwrap();
        let run_dir = run_dir_from_env();
        log::info!("run dir: {:?}", run_dir);
        assert!(run_dir.starts_with(&homedir));

        env::set_var(STATION_RUN_DIR_ENV_VAR, "");
        assert!(env::var(STATION_RUN_DIR_ENV_VAR).is_ok());
        let run_dir = run_dir_from_env();
        log::info!("run dir: {:?}", run_dir);
        assert!(run_dir.starts_with(&homedir));

        env::remove_var(STATION_RUN_DIR_ENV_VAR);
        let tempdir = tempfile::tempdir().unwrap();
        env::set_var(STATION_RUN_DIR_ENV_VAR, tempdir.path().to_str().unwrap());
        assert!(env::var(STATION_RUN_DIR_ENV_VAR).is_ok());
        let run_dir = run_dir_from_env();
        log::info!("run dir: {:?}", run_dir);
        assert!(run_dir.starts_with(tempdir.path()));
        assert!(!run_dir.starts_with(&homedir));

        let socket_dir = unix_socket_dir_from_env();
        log::info!("socket dir: {:?}", socket_dir);
        assert!(socket_dir.starts_with(&run_dir));
        assert!(run_dir.exists());
        assert!(!socket_dir.exists());
    }

    #[test]
    fn test_initialize_run_dir() {
        let tempdir = tempfile::tempdir().unwrap();
        assert!(initialize_run_dir(tempdir.path()));
        let socket_dir = unix_socket_dir(tempdir.path());
        assert!(socket_dir.exists());
    }
}
