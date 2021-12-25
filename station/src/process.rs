use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config::{self, Config};
use crate::rpc::{Callback, RpcClient, RpcError, RpcServer};

#[derive(Deserialize, Serialize)]
pub enum ProcessAction {
    StartProcess(String),
    StopProcess(String),
}

pub struct Process {
    run_dir: PathBuf,
    config: Config,
    name: String,
    rpc: HashMap<String, RpcServer>,
}

impl Process {
    pub fn new(name: &str, run_directory: &Path, config: &Config) -> Result<Process, IoError> {
        assert!(config::initialize_run_dir(run_directory));
        Ok(Process {
            run_dir: PathBuf::from(run_directory),
            config: config.clone(),
            name: String::from(name),
            rpc: HashMap::new(),
        })
    }

    fn unix_socket_base(&self) -> PathBuf {
        config::unix_socket_dir(&self.run_dir)
            .as_path()
            .join(&self.name)
    }

    /*
    pub fn call_rpc_tcp<T, U>(
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
    }
    */

    pub fn call_rpc_unix<T, U>(
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
        let socket_path = self
            .run_dir
            .as_path()
            .join("sockets")
            .as_path()
            .join(process_name)
            .as_path()
            .with_extension(rpc_name);
        log::trace!("Sending to RPC socket path: {:?}", socket_path);
        let client: RpcClient<T, U> = RpcClient::with_unix_socket(&socket_path);
        client.call(request, timeout)
    }

    pub fn create_rpc_tcp<T, U>(&mut self, name: &'static str, callback: Callback<T, U>) -> bool
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        if self.rpc.contains_key(name) {
            log::trace!("rpc exists: {}", name);
            return false;
        }

        let config_name = format!("{}.{}", self.name, name);
        let config = self.config.get_rpc(&config_name);
        if config.is_none() {
            log::trace!("not in config: {}", config_name);
            return false;
        }

        let port = config.unwrap();
        log::trace!("Listening for {} messages on port: {}", config_name, port);
        assert!(self
            .rpc
            .insert(
                String::from(name),
                RpcServer::with_tcp_port(name, port, callback)
            )
            .is_none());

        true
    }

    pub fn create_rpc_unix<T, U>(&mut self, name: &'static str, callback: Callback<T, U>) -> bool
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        if self.rpc.contains_key(name) {
            log::trace!("rpc exists: {}", name);
            return false;
        }

        let socket_path = self.unix_socket_base().as_path().with_extension(name);
        log::trace!("Listening to RPC on socket path: {:?}", socket_path);
        assert!(self
            .rpc
            .insert(
                String::from(name),
                RpcServer::with_unix_socket(name, &socket_path, callback)
            )
            .is_none());

        true
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

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

    fn create_config(server_name: &str, rpc_name: &str) -> Config {
        let mut cfg = Config::new();
        cfg.add_rpc(
            &format!("{}.{}", server_name, rpc_name),
            portpicker::pick_unused_port().unwrap(),
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
        let tempdir = tempfile::tempdir().unwrap();
        let process = Process::new(server_name, tempdir.path(), &config);
        assert!(process.is_ok());
    }

    #[test]
    fn create_process_with_unix_rpc() {
        setup_logging();

        let rpc_name = "plus-one";
        let server_name = "rpc-test";
        let client_name = "rpc-client";

        let tempdir = tempfile::tempdir().unwrap();
        let process = Process::new(server_name, tempdir.path(), &Config::new());
        assert!(process.is_ok());

        let mut process = process.unwrap();
        assert!(process.create_rpc_unix::<i32, i32>(rpc_name, Box::new(|x| Ok(x + 1))));

        let client = Process::new(client_name, tempdir.path(), &Config::new()).unwrap();
        let result =
            client.call_rpc_unix::<i32, i32>(server_name, rpc_name, 0, Duration::from_secs(5));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }
}
