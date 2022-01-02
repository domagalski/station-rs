# station
A network-based interprocess communication (IPC) library written in Rust.

NOTE: this is heavily under development and not everything is available.
Anything version less than v0.1.0 is incomplete.

I'm writing this to be useful to me. It might not be useful for you.

## Structure

The `station` design is fairly straightforward. It implements two types of IPC,
namely Remote Procedure Calls (RPC) and PubSub. RPC communication is performed
over a stream socket (TCP or Unix stream sockets) and PubSub communication is
performed over datagram sockets (UDP or Unix datagram sockets). This allows
some flexibility in defining how two processes that may or may not be running
on the same machine can talk to each other.

The `station` module has several main types:
* `Process`: A `Process` instance is a helper around RPC and PubSub patterns.
* `Publisher`: Publish a message with the `Serialize` and `Deserialize` traits
to a topic.
* `Subscriber`: Subscribe to a topic and process messages via a callback.
* `RpcServer`: Listen for requests and send the result back to the sender.
* `RpcClient` Send requests to a server and wait for the response.

## Status

* [x] TCP RPC
* [x] Unix Stream Socket RPC
* [x] UDP PubSub
* [x] Unix Datagram Socket PubSub
* [x] Process with RPC configuration.
* [x] Process with PubSub configuration.
