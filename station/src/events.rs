//! Handle IPC events

use std::any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Result as IoResult;
use std::path::Path;
use std::time::Duration;

use mio::net::{UnixDatagram, UnixListener};
use mio::{Events, Interest, Poll, Token};
use serde::de::DeserializeOwned;

use crate::messages::Payload;

const MAX_EVENTS: usize = 64;
const BUFFER_SIZE: usize = 1_000_000;

const RPC_SOCKET: &str = "rpc";
const RPC_TOKEN: Token = Token(0);

const PUBSUB_SOCKET: &str = "pubsub";
const PUBSUB_TOKEN: Token = Token(1);

pub trait Response {}

pub struct EventHandler<'a> {
    buffer: RefCell<Vec<u8>>,
    rpc_socket: UnixListener,
    pubsub_socket: UnixDatagram,
    poller: Poll,
    events: Events,
    responses: HashMap<u32, Box<dyn Response>>,
    callbacks: HashMap<u32, Box<dyn Fn(&[u8]) + 'a>>,
}

impl<'a> EventHandler<'a> {
    pub fn new(socket_path: &Path) -> IoResult<EventHandler> {
        let buffer = RefCell::new(Vec::new());
        buffer.borrow_mut().resize(BUFFER_SIZE, 0);

        let mut rpc_socket = UnixListener::bind(socket_path.join(RPC_SOCKET))?;
        let mut pubsub_socket = UnixDatagram::bind(socket_path.join(PUBSUB_SOCKET))?;

        let poller = Poll::new()?;
        poller
            .registry()
            .register(&mut rpc_socket, RPC_TOKEN, Interest::READABLE)?;
        poller
            .registry()
            .register(&mut pubsub_socket, PUBSUB_TOKEN, Interest::READABLE)?;

        Ok(EventHandler {
            buffer,
            rpc_socket,
            pubsub_socket,
            poller,
            events: Events::with_capacity(MAX_EVENTS),
            responses: HashMap::new(),
            callbacks: HashMap::new(),
        })
    }

    pub fn wait(&mut self, timeout: Duration) -> IoResult<bool> {
        self.poller.poll(&mut self.events, Some(timeout))?;
        Ok(!self.events.is_empty())
    }

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

    pub fn assign_response(&mut self, id: u32, response: Box<dyn Response>) -> bool {
        if self.responses.contains_key(&id) {
            return false;
        }

        self.responses.insert(id, response);
        true
    }

    fn handle_rpc(&self) {
        panic!("RPC not implemented");
    }

    fn handle_pubsub(&self) {
        let size = match self.pubsub_socket.recv(&mut self.buffer.borrow_mut()) {
            Ok(size) => size,
            Err(err) => {
                log::error!("Failed to receive message on subscriber: {}", err);
                return;
            }
        };

        let payload: Payload =
            match serde_json::from_slice(&self.buffer.borrow().as_slice()[0..size]) {
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
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use mio::net::UnixDatagram;

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
        let mut event_handler = EventHandler::new(tempdir.path()).unwrap();
        assert!(!event_handler.wait(Duration::from_millis(0)).unwrap());
        assert!(event_handler.assign_callback(0, Box::new(|value| counter.increment(value))));

        let publisher = UnixDatagram::unbound().unwrap();
        let pubsub_path = tempdir.path().join("pubsub");
        let payload = Payload {
            id: 0,
            data: serde_json::to_vec(&increment).unwrap(),
        };
        let data = serde_json::to_vec(&payload).unwrap();
        let result = publisher.send_to(&data, pubsub_path).unwrap();
        assert_eq!(result, data.len());

        assert!(event_handler.wait(Duration::from_millis(0)).unwrap());
        assert_eq!(event_handler.process_events().len(), 0);

        assert_eq!(counter.count(), increment);
    }
}
