use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Error as FmtError, Formatter};
use std::fs::File;
use std::io::{Error as IoError, ErrorKind};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_yaml;

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

/// An entry in a configuration describing the RPC networking.
#[derive(Debug, Deserialize, Serialize)]
pub enum ConfigEntry {
    Port(u16),
    Unix,
}

impl ConfigEntry {
    fn get_port(&self) -> Option<u16> {
        match self {
            ConfigEntry::Port(port) => Some(*port),
            _ => None,
        }
    }

    fn is_port(&self) -> bool {
        match self {
            ConfigEntry::Port(_) => true,
            _ => false,
        }
    }

    fn is_unix(&self) -> bool {
        match self {
            ConfigEntry::Unix => true,
            _ => false,
        }
    }
}

/// RPC configuration.
#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    rpc: HashMap<String, ConfigEntry>,
}

impl Config {
    /// Create a new config.
    pub fn new() -> Config {
        Config {
            rpc: HashMap::new(),
        }
    }

    fn err_rpc_exists(&self, name: &str) -> ConfigResult<()> {
        if self.rpc.contains_key(name) {
            return Err(ConfigError::new(&format!("RPC exists: {}", name)));
        }
        Ok(())
    }

    /// Add a RPC config that uses a TCP port for IPC.
    ///
    /// Args:
    /// * `name`: The name to associate the config with.
    /// * `port`: The TCP port to use for the RPC listener.
    pub fn add_rpc_port(&mut self, name: &str, port: u16) -> ConfigResult<()> {
        let result = self.err_rpc_exists(name);
        if result.is_err() {
            return result;
        }

        assert!(self
            .rpc
            .insert(String::from(name), ConfigEntry::Port(port))
            .is_none());
        Ok(())
    }

    /// Add a RPC config that uses a Unix stream socket for IPC.
    ///
    /// Args:
    /// * `name`: The name to associate the config with, used to construct the socket path.
    pub fn add_rpc_unix(&mut self, name: &str) -> ConfigResult<()> {
        let result = self.err_rpc_exists(name);
        if result.is_err() {
            return result;
        }

        assert!(self
            .rpc
            .insert(String::from(name), ConfigEntry::Unix)
            .is_none());
        Ok(())
    }

    /// Get the port number for an RPC listener.
    ///
    /// Args:
    /// * `name`: The name in the config entries.
    pub fn get_rpc_port(&self, name: &str) -> Option<u16> {
        match self.rpc.get(name) {
            Some(entry) => entry.get_port(),
            None => None,
        }
    }

    /// Determine if there is a named config with a port number.
    ///
    /// Args:
    /// * `name`: The name in the config entries.
    pub fn is_rpc_port(&self, name: &str) -> bool {
        match self.rpc.get(name) {
            Some(entry) => entry.is_port(),
            None => false,
        }
    }

    /// Determine if there is a named config marked as Unix socket.
    ///
    /// Args:
    /// * `name`: The name in the config entries.
    pub fn is_rpc_unix(&self, name: &str) -> bool {
        match self.rpc.get(name) {
            Some(entry) => entry.is_unix(),
            None => false,
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

#[cfg(test)]
mod tests {
    use std::io::Write;

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

    #[test]
    fn add_items_to_config() {
        setup_logging();
        let mut config = Config::new();
        assert!(config
            .add_rpc_port("tcp", portpicker::pick_unused_port().unwrap())
            .is_ok());

        // should error out, as the config exists.
        let result = config.add_rpc_port("tcp", portpicker::pick_unused_port().unwrap());
        assert!(result.is_err());
        log::debug!("{}", result.err().unwrap());

        // add a unix path
        assert!(config.add_rpc_unix("unix").is_ok());

        // also verify that this errors out
        assert!(config.add_rpc_unix("unix").is_err());

        let config = config;
        let yaml = serde_yaml::to_string(&config).unwrap();
        log::debug!("yaml:\n{}", yaml);

        assert!(config.is_rpc_port("tcp"));
        assert!(!config.is_rpc_unix("tcp"));
        assert!(config.get_rpc_port("tcp").is_some());
        let port = config.get_rpc_port("tcp").unwrap();
        assert!(port > 0);
        assert!(!config.is_rpc_port("unix"));
        assert!(config.is_rpc_unix("unix"));

        assert!(!config.is_rpc_port("404"));
        assert!(!config.is_rpc_unix("404"));
    }

    #[test]
    fn load_save_config_file() {
        setup_logging();
        let mut config = Config::new();
        assert!(config
            .add_rpc_port("cats", portpicker::pick_unused_port().unwrap())
            .is_ok());
        assert!(config.add_rpc_unix("dogs").is_ok());
        let config = config;
        let tempdir = tempfile::tempdir().unwrap();
        let yaml_path = tempdir.path().join("config.yaml");
        assert!(config.write_yaml(&yaml_path).is_ok());

        let yaml = serde_yaml::to_string(&config).unwrap();
        log::trace!("config:\n{}", yaml);

        let recovered_config = Config::read_yaml(&yaml_path);
        assert!(recovered_config.is_ok());
        let recovered_config = recovered_config.unwrap();
        log::trace!("recovered config: {:?}", recovered_config);

        assert!(recovered_config.get_rpc_port("cats").is_some());
        assert!(config.is_rpc_port("cats"));
        assert!(config.get_rpc_port("cats").is_some());
        assert_eq!(
            recovered_config.get_rpc_port("cats").unwrap(),
            config.get_rpc_port("cats").unwrap()
        );
        let port = recovered_config.get_rpc_port("cats").unwrap();
        assert!(port > 0);

        assert!(recovered_config.is_rpc_unix("dogs"));
        assert!(!recovered_config.is_rpc_port("404"));
        assert!(!recovered_config.is_rpc_unix("404"));
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
}
