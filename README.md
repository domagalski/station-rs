# station
A network-based interprocess communication (IPC) library written in Rust.

NOTE: this is heavily under development

## Structure

The `station` design is heavily influenced by [ROS](https://www.ros.org/) and
aims to create an interface with similar patterns. `station` can run either on
one machine or many machines and the networking protocol used to transmit
messages between processes is designed to be flexible.

The `station` module has several main types:
* `Station`: A `Station` tracks and managers all incoming and outgoing messages
on a machine and connects to other machines running `station`. There is
typically one instance of `station` running per machine.
* `Process`: A `Process` connects to the local `station` instance and registers
connections to pubsub topics and RPC channels.
* `Publisher`: Publish a message with the `Serialize` and `Deserialize` traits
to a topic.
* `Subscriber`: Subscribe to a topic and process messages via a callback.
* `RpcServer`: Listen for requests and send the result back to the sender.
* `RpcClient` Send requests to a server and wait for the response.
