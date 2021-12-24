use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config;
use crate::rpc::{Callback, RpcClient, RpcError, RpcServer};

#[derive(Deserialize, Serialize)]
pub enum ProcessAction {
    StartProcess(String),
    StopProcess(String),
}

pub struct Process {
    run_dir: PathBuf,
    name: String,
    rpc: HashMap<String, RpcServer>,
}

impl Process {
    pub fn new(name: &str, run_directory: &Path) -> Result<Process, IoError> {
        assert!(config::initialize_run_dir(run_directory));
        Ok(Process {
            run_dir: PathBuf::from(run_directory),
            name: String::from(name),
            rpc: HashMap::new(),
        })
    }

    fn unix_socket_base(&self) -> PathBuf {
        config::unix_socket_dir(&self.run_dir)
            .as_path()
            .join(&self.name)
    }

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

    pub fn initialize_rpc_unix<T, U>(
        &mut self,
        name: &'static str,
        callback: Callback<T, U>,
    ) -> bool
    where
        T: Debug + DeserializeOwned + Serialize + 'static,
        U: Debug + DeserializeOwned + Serialize + 'static,
    {
        if self.rpc.contains_key(name) {
            return false;
        }

        let socket_path = self.unix_socket_base().as_path().with_extension(name);
        log::trace!("Listening to RPC on socket path: {:?}", socket_path);
        self.rpc.insert(
            String::from(name),
            RpcServer::with_unix_socket(name, &socket_path, callback),
        );

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

    #[test]
    fn create_process_with_unix_rpc() {
        setup_logging();

        let rpc_name = "plus-one";
        let server_name = "rpc-test";
        let client_name = "rpc-client";

        let tempdir = tempfile::tempdir().unwrap();
        let process = Process::new(server_name, tempdir.path());
        assert!(process.is_ok());

        let mut process = process.unwrap();
        assert!(process.initialize_rpc_unix::<i32, i32>(rpc_name, Box::new(|x| Ok(x + 1))));

        let client = Process::new(client_name, tempdir.path()).unwrap();
        let result =
            client.call_rpc_unix::<i32, i32>(server_name, rpc_name, 0, Duration::from_secs(5));
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, 1);
    }
}
