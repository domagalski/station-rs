# station
A networked interprocess communication (IPC) library written in Rust.

I'm writing this to be useful to me. It might not be useful for you. Some
things mentioned here might not be fully implemented or subject to change. v1.0
shall be the first release that is meant to be stable, usable and fully
featured. Anything before that is subject to be whatever I feel like it is.

## Structure

The `station` design is fairly straightforward. It implements two types of IPC,
namely Remote Procedure Calls (RPC) and PubSub. RPC communication is performed
over a stream socket (TCP or Unix stream sockets) and PubSub communication is
performed over datagram sockets (UDP or Unix datagram sockets). This allows
some flexibility in defining how two processes that may or may not be running
on the same machine can talk to each other.

See the unit tests in `process.rs` for some examples.
