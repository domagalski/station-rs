//! Handle IPC events.

use std::any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Result as IoResult, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use mio::net::{UnixDatagram, UnixListener, UnixStream};
use mio::{Events, Interest, Poll, Token};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_EVENTS: usize = 64;
const BUFFER_SIZE: usize = 1_000_000;

const RPC_SOCKET: &str = "rpc";
const RPC_TOKEN: Token = Token(0);

const PUBSUB_SOCKET: &str = "pubsub";
const PUBSUB_TOKEN: Token = Token(1);

#[derive(Deserialize, Serialize)]
pub struct Payload {
    pub id: u32,
    pub data: Vec<u8>,
}

/// The event server is for handling interprocess communication (IPC) via sending messages over a
/// network. There are two main patterns of events: PubSub (see `assign_callback` and RPC (see
/// `assign_response`).
///
/// PubSub events are handled where some callback processes messages it
/// receives, but does not issue a response to the publisher of the message. In EventHander, PubSub
/// events are received on datagram-type sockets. RPC events are where a client sends a message to
/// the event server with the expectation of a response.
///
/// The event server is not thread-safe. The typical usage of it is to define a main loop where the
/// event server waits for events, then processes them. If events need to be processed in another
/// thread, the callbacks or response handlers must handle any multi-threading that is necessary.
/// In any case, callbacks and response handlers should execute quickly, as they are executed
/// serially in order of reception of requests/messages sent to the event server.
pub struct Server<'a> {
    buffer: RefCell<Vec<u8>>,
    rpc_socket: UnixListener,
    pubsub_socket: UnixDatagram,
    poller: Poll,
    events: Events,
    responses: HashMap<u32, Box<dyn Fn(&[u8]) -> Result<Value, String> + 'a>>,
    callbacks: HashMap<u32, Box<dyn Fn(&[u8]) + 'a>>,
}

impl<'a> Server<'a> {
    /// Create an event server. If the event server cannot be created, return an error.
    ///
    /// Args:
    /// * `name`: A name to refer to the event server.
    /// * `socket_dir`: The root directory for which to create unix sockets.
    pub fn new(name: &str, socket_dir: &Path) -> IoResult<Server<'a>> {
        let buffer = RefCell::new(Vec::new());
        buffer.borrow_mut().resize(BUFFER_SIZE, 0);

        let rpc_path = socket_dir.join(format!("{}.{}", name, RPC_SOCKET));
        let pubsub_path = socket_dir.join(format!("{}.{}", name, PUBSUB_SOCKET));

        let mut rpc_socket = UnixListener::bind(rpc_path)?;
        let mut pubsub_socket = UnixDatagram::bind(pubsub_path)?;

        let poller = Poll::new()?;
        poller
            .registry()
            .register(&mut rpc_socket, RPC_TOKEN, Interest::READABLE)?;
        poller
            .registry()
            .register(&mut pubsub_socket, PUBSUB_TOKEN, Interest::READABLE)?;

        Ok(Server {
            buffer,
            rpc_socket,
            pubsub_socket,
            poller,
            events: Events::with_capacity(MAX_EVENTS),
            responses: HashMap::new(),
            callbacks: HashMap::new(),
        })
    }

    /// Assign a callback for when a message of type `T` arrives at the event server, which
    /// processes the messages it receives without returning a response to the message sender.
    ///
    /// Args:
    /// * `id`: An identifier of the callback handler. No two callbacks can share the same ID and if
    /// a callback handler with this ID already exists, `assign_callback` will return `false`.
    /// * `callback`: A function to process a message of type `T` upon reception.
    pub fn assign_callback<T: DeserializeOwned + 'a>(
        &mut self,
        id: u32,
        callback: Box<dyn Fn(T) + 'a>,
    ) -> bool {
        if self.callbacks.contains_key(&id) {
            return false;
        }

        self.callbacks.insert(
            id,
            Box::new(move |data| {
                let data: T = match serde_json::from_slice(data) {
                    Ok(data) => data,
                    Err(_) => {
                        log::error!(
                            "Failed to deserialize input for callback {} as {}",
                            id,
                            any::type_name::<T>()
                        );
                        return;
                    }
                };
                callback(data);
            }),
        );
        true
    }

    /// Assign a response for when a message of type `T` arrives at the event server, which either
    /// returns a response of type `U` or a string describing some error.
    ///
    /// Args:
    /// * `id`: An identifier of the response handler. No two response handlers can share the same
    /// ID and if a response handler with this ID already exists, `assign_callback` will return
    /// `false`.
    /// * `callback`: A function to process a message of type `T` upon reception.
    pub fn assign_response<T, U>(
        &mut self,
        id: u32,
        response: Box<dyn Fn(T) -> Result<U, String> + 'a>,
    ) -> bool
    where
        T: DeserializeOwned + 'a,
        U: Serialize + 'a,
    {
        if self.responses.contains_key(&id) {
            return false;
        }

        self.responses.insert(
            id,
            Box::new(move |data| {
                let data: T = match serde_json::from_slice(data) {
                    Ok(data) => data,
                    Err(_) => {
                        return Err(format!(
                            "Failed to deserialize input for callback {} as {}",
                            id,
                            any::type_name::<T>()
                        ));
                    }
                };
                match response(data) {
                    Ok(value) => Ok(serde_json::to_value(value).unwrap()),
                    Err(err) => Err(err),
                }
            }),
        );
        true
    }

    fn handle_pubsub(&self) {
        let size = match self.pubsub_socket.recv(&mut self.buffer.borrow_mut()) {
            Ok(size) => size,
            Err(err) => {
                log::error!("Failed to receive message on subscriber: {}", err);
                return;
            }
        };

        let payload =
            match serde_json::from_slice::<Payload>(&self.buffer.borrow().as_slice()[0..size]) {
                Ok(payload) => payload,
                Err(err) => {
                    log::error!("Failed to deserialize PubSub message: {}", err);
                    return;
                }
            };

        match self.callbacks.get(&payload.id) {
            Some(callback) => callback(&payload.data),
            None => {
                log::error!("Dropping PubSub with unknown ID: {}", payload.id);
                return;
            }
        }
    }

    fn handle_rpc(&self) {
        loop {
            match self.rpc_socket.accept() {
                Ok((mut connection, _)) => self.respond_to_rpc(&mut connection),
                Err(err) => {
                    if err.kind() == ErrorKind::WouldBlock {
                        break;
                    } else {
                        panic!("failed to accept connection: {}", err);
                    }
                }
            }
        }
    }

    /// Process all events that are currently awaiting processing. If there are registered events
    /// that are not a part of the event server (e.g. responses from some network resource), the
    /// associated event tokens for those events are returned.
    pub fn process_events(&self) -> Vec<Token> {
        let mut unknown_tokens = Vec::new();
        for event in self.events.iter() {
            let token = event.token();
            match token {
                RPC_TOKEN => self.handle_rpc(),
                PUBSUB_TOKEN => self.handle_pubsub(),
                token => {
                    unknown_tokens.push(token);
                }
            }
        }
        unknown_tokens
    }

    fn respond_to_rpc(&self, connection: &mut UnixStream) {
        let size = match connection.read(&mut self.buffer.borrow_mut()) {
            Ok(size) => size,
            Err(err) => {
                log::error!("Failed to read RPC request: {}", err);
                return;
            }
        };

        let response =
            match serde_json::from_slice::<Payload>(&self.buffer.borrow().as_slice()[0..size]) {
                Ok(payload) => match self.responses.get(&payload.id) {
                    Some(handler) => handler(&payload.data),
                    None => Err(format!("Unknown request ID: {}", payload.id)),
                },
                Err(err) => Err(format!("Failed to deserialize request: {}", err)),
            };

        let response = serde_json::to_vec(&response).unwrap();
        match connection.write(&response) {
            Ok(_) => (),
            Err(err) => log::error!("Failed to write response: {}", err),
        }
    }

    /// Wait for events to arrive at the handler. If there is at least one event ready, this will
    /// return `true`. If no event has arrived within the timeout, then `false` is returned.
    ///
    /// Args:
    /// * `timeout`: The maximum amount of time to wait for events.
    pub fn wait(&mut self, timeout: Duration) -> IoResult<bool> {
        self.poller.poll(&mut self.events, Some(timeout))?;
        Ok(!self.events.is_empty())
    }
}

pub struct Client {
    socket_dir: PathBuf,
}

impl Client {
    pub fn new(socket_dir: &Path) -> Client {
        Client {
            socket_dir: socket_dir.to_path_buf(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use serde::Deserialize;

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
    fn test_pubsub() {
        setup_logging();

        struct Counter {
            count: RefCell<usize>,
        }

        impl Counter {
            fn new() -> Counter {
                Counter {
                    count: RefCell::new(0),
                }
            }

            fn increment(&self, value: usize) {
                *self.count.borrow_mut() += value;
            }

            fn count(&self) -> usize {
                *self.count.borrow()
            }
        }

        let counter = Counter::new();
        let increment = 5 as usize;

        let tempdir = tempfile::tempdir().unwrap();
        let mut server = Server::new("test", tempdir.path()).unwrap();
        assert!(!server.wait(Duration::from_millis(0)).unwrap());
        assert!(server.assign_callback(0, Box::new(|value| counter.increment(value))));

        let publisher = UnixDatagram::unbound().unwrap();
        let pubsub_path = tempdir.path().join("test.pubsub");
        let payload = Payload {
            id: 0,
            data: serde_json::to_vec(&increment).unwrap(),
        };
        let payload = serde_json::to_vec(&payload).unwrap();
        let result = publisher.send_to(&payload, pubsub_path).unwrap();
        assert_eq!(result, payload.len());

        assert!(server.wait(Duration::from_millis(0)).unwrap());
        assert_eq!(server.process_events().len(), 0);

        assert_eq!(counter.count(), increment);
    }

    #[test]
    fn test_rpc() {
        setup_logging();

        #[derive(Serialize, Deserialize)]
        struct AddRequest {
            x: i32,
            y: i32,
        }

        // struct that counts how many requests have been made to it.
        struct Adder {
            n_reqs: RefCell<usize>,
        }

        impl Adder {
            fn new() -> Adder {
                Adder {
                    n_reqs: RefCell::new(0),
                }
            }

            fn add(&self, request: &AddRequest) -> i32 {
                *self.n_reqs.borrow_mut() += 1;
                return request.x + request.y;
            }

            fn num_requests(&self) -> usize {
                *self.n_reqs.borrow()
            }
        }

        let adder = Adder::new();

        let tempdir = tempfile::tempdir().unwrap();
        let mut server = Server::new("test", tempdir.path()).unwrap();
        assert!(!server.wait(Duration::from_millis(0)).unwrap());
        assert!(server.assign_response(0, Box::new(|req| Ok(adder.add(&req)))));

        let rpc_path = tempdir.path().join("test.rpc");
        let mut client = UnixStream::connect(&rpc_path).unwrap();
        let request = AddRequest { x: 2, y: 4 };
        let payload = Payload {
            id: 0,
            data: serde_json::to_vec(&request).unwrap(),
        };
        let payload = serde_json::to_vec(&payload).unwrap();
        let result = client.write(&payload).unwrap();
        assert_eq!(result, payload.len());

        assert!(server.wait(Duration::from_millis(0)).unwrap());
        assert_eq!(server.process_events().len(), 0);
        let mut buffer: [u8; 1000] = [0; 1000];
        let size = client.read(&mut buffer).unwrap();
        let result: Result<i32, String> = serde_json::from_slice(&buffer[0..size]).unwrap();
        let result = result.unwrap();
        assert_eq!(result, request.x + request.y);
        assert_eq!(adder.num_requests(), 1);
    }
}
