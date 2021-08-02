use std::any;
use std::io::{Error as IoError, ErrorKind, Read, Write};
use std::mem;
use std::net::{SocketAddrV4, TcpStream, UdpSocket};
use std::os::unix::net::{UnixDatagram, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use bincode;
use byteorder::{ByteOrder, BE};
use log;
use serde::de::DeserializeOwned;
use serde::Serialize;

// newtype for UDP socket so Read/Write can be implemented
pub struct Udp(UdpSocket, SocketAddrV4);

impl Udp {
    pub fn new(udp: UdpSocket) -> Udp {
        Udp(udp, "0.0.0.0:0".parse().unwrap())
    }

    pub fn set_write_addr(&mut self, addr: SocketAddrV4) {
        self.1 = addr;
    }
}

impl Read for Udp {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IoError> {
        let (n_bytes, _) = self.0.recv_from(buffer)?;
        Ok(n_bytes)
    }
}

impl Write for Udp {
    fn write(&mut self, buffer: &[u8]) -> Result<usize, IoError> {
        self.0.send_to(buffer, self.1)
    }

    fn flush(&mut self) -> Result<(), IoError> {
        log::warn!("UDP flush is a no-op");
        Ok(())
    }
}

pub struct UnixUdp(UnixDatagram, PathBuf);

impl UnixUdp {
    pub fn new(socket: UnixDatagram) -> UnixUdp {
        UnixUdp(socket, PathBuf::new())
    }

    pub fn set_path(&mut self, path: &Path) {
        self.1 = PathBuf::from(path);
    }
}

impl Read for UnixUdp {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, IoError> {
        self.0.recv(buffer)
    }
}

impl Write for UnixUdp {
    fn write(&mut self, buffer: &[u8]) -> Result<usize, IoError> {
        if self.1.to_str().unwrap_or("").len() == 0 {
            return Err(IoError::new(
                ErrorKind::NotFound,
                "Unix datagram path not set or is invalid",
            ));
        }

        self.0.send_to(buffer, &self.1)
    }

    fn flush(&mut self) -> Result<(), IoError> {
        log::warn!("Unix Datagram flush is a no-op");
        Ok(())
    }
}

// Trait for grouping together socket operations
pub trait Socket: Read + Write {
    fn set_timeout(&self, timeout: Option<Duration>) -> Result<(), IoError>;
}

impl Socket for TcpStream {
    fn set_timeout(&self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.set_write_timeout(timeout)?;
        self.set_read_timeout(timeout)
    }
}

impl Socket for UnixStream {
    fn set_timeout(&self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.set_write_timeout(timeout)?;
        self.set_read_timeout(timeout)
    }
}

impl Socket for Udp {
    fn set_timeout(&self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.0.set_write_timeout(timeout)?;
        self.0.set_read_timeout(timeout)
    }
}

impl Socket for UnixUdp {
    fn set_timeout(&self, timeout: Option<Duration>) -> Result<(), IoError> {
        self.0.set_write_timeout(timeout)?;
        self.0.set_read_timeout(timeout)
    }
}

// size of the buffer for incoming messages
const BUFFER_SIZE: usize = 2048;
// Incoming messages may be of multiple purposes, sometimes to encode a message and other times to
// tell a listening thread to shut down. All incoming messages should be prepended with a u32 with
// the message type (message or stop) and a u64 containing the message size, excluding the header
// size. The message type and size parameter are both expected to be encoded as big endian.
const PING_KEYWORD: u32 = 0xC001C0DE;
const MESSAGE_KEYWORD: u32 = 0xC0DEFEED;
const STOP_KEYWORD: u32 = 0xC0DEDEAD;
const ERROR_KEYWORD: u32 = 0xC0DEEEEE;
const HEADER_SIZE: usize = mem::size_of::<u32>() + mem::size_of::<u64>();

// Construct a payload message
// Message format:
//  keyword: u32
//  size: u64
//  data: bytes
pub fn construct_payload(data: impl Serialize, keyword: u32) -> Vec<u8> {
    let mut message_bytes = bincode::serialize(&data).unwrap();
    let mut message: Vec<u8> = Vec::new();
    message.resize(HEADER_SIZE, 0);
    let keyword_bound = mem::size_of::<u32>();
    BE::write_u32(&mut message[..keyword_bound], keyword);
    BE::write_u64(&mut message[keyword_bound..], message_bytes.len() as u64);
    message.append(&mut message_bytes);
    message
}

// Construct a payload message containing a serde message.
pub fn construct_message(data: impl Serialize) -> Vec<u8> {
    construct_payload(data, MESSAGE_KEYWORD)
}

// Construct a payload message containing an error string
pub fn construct_error(error: &str) -> Vec<u8> {
    construct_payload(error, ERROR_KEYWORD)
}

// Write data to a socket stream
pub fn write_socket(socket: &mut impl Socket, data: impl Serialize) -> Result<usize, IoError> {
    let message = construct_message(data);
    socket.write(&message)
}

//pub fn write_stop_signal(socket: &mut impl Socket, name: &str) {
pub fn write_stop_signal<S: Socket>(socket: Result<S, IoError>, name: &str) {
    // the only reason why the socket endpoint shouldn't connect is that the server thread it is
    // connecting to for sending a shutdown signal has already been shut down, in which case, not
    // being able to connect to tell it to shut down is fine.
    if let Ok(mut socket) = socket {
        match socket.write(construct_payload((), STOP_KEYWORD).as_slice()) {
            Ok(size) => log::trace!("{}: wrote stop requested signal of {} bytes", name, size),
            Err(err) => log::trace!("{}: stop request had error: {}", name, err),
        }
    }
}

// ping a socket
pub fn ping(socket: &mut impl Socket, timeout: Duration) -> bool {
    let _ = socket.set_timeout(Some(timeout));
    let mut signal: [u8; HEADER_SIZE] = [0; HEADER_SIZE];
    BE::write_u32(&mut signal, PING_KEYWORD);
    match socket.write(&signal) {
        Ok(size) => log::trace!("wrote ping requested signal of {} bytes", size),
        Err(err) => {
            log::trace!("ping request had error: {}", err);
            return false;
        }
    }

    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    if let Ok(n_bytes) = socket.read(&mut buffer) {
        log::trace!("ping response: {:?}", &buffer[..n_bytes]);
    } else {
        return false;
    }

    return true;
}

pub fn recv<S: Socket, T: DeserializeOwned + Serialize>(
    socket: &mut S,
    timeout: Option<Duration>,
    error_keyword: &str,
    is_result_type: bool,
) -> Result<T, IoError> {
    let _ = socket.set_timeout(timeout);
    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    let n_bytes = socket.read(&mut buffer)?;
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
        MESSAGE_KEYWORD => BE::read_u64(&buffer[keyword_bound..HEADER_SIZE]) as usize,
        PING_KEYWORD => return Err(IoError::new(ErrorKind::WriteZero, "ping")),
        STOP_KEYWORD => return Err(IoError::new(ErrorKind::Interrupted, "stop requested")),
        ERROR_KEYWORD => {
            let error_msg: &str = bincode::deserialize(&buffer[HEADER_SIZE..n_bytes]).unwrap();
            let error_msg = format!("{}: {}", error_keyword, error_msg);
            return Err(IoError::new(ErrorKind::Other, error_msg));
        }
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
        let n_bytes = socket.read(&mut buffer)?;
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
                format!("{}: {}", error_keyword, &err.to_string()),
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
