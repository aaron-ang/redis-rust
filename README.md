[![progress-banner](https://backend.codecrafters.io/progress/redis/5c1a4d4c-40a0-4434-9ff4-610a670222bf)](https://app.codecrafters.io/users/codecrafters-bot?r=2qF)

This project is a Rust implementation of a Redis clone, developed as part of the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

It aims to replicate core Redis functionalities, including event loops and the Redis Serialization Protocol (RESP).

**Note**: If you're viewing this repo on GitHub, head over to
[codecrafters.io](https://codecrafters.io) to try the challenge.

# Features ✨

This Redis clone supports the following features:
-   **Basic Commands** ⌨️:
    -   `PING`: Checks server responsiveness.
    -   `ECHO`: Returns the provided string.
    -   `SET`: Stores a key-value pair.
    -   `GET`: Retrieves the value associated with a key.
    -   `INFO`: Provides information about replication.
    -   `REPLCONF`: Used for replication configuration.
    -   `PSYNC`: Facilitates partial or full resynchronization with a replica.
    -   `WAIT`: Blocks until all previous write commands are successfully transferred and acknowledged by the specified number of replicas.
    -   `CONFIG GET`: Retrieves configuration parameters.
    -   `KEYS`: Returns all keys matching a pattern.
    -   `TYPE`: Returns the string representation of the type of the value stored at key.
    -   `XADD`: Appends a new entry to a stream.
    -   `XRANGE`: Returns a range of entries from a stream.
    -   `XREAD`: Reads entries from one or more streams.
    -   `PUBLISH`: Posts a message to a channel.
    -   `SUBSCRIBE`: Subscribes the client to the given channels.
    -   `UNSUBSCRIBE`: Unsubscribes the client from the given channels.
-   **Concurrency** ⚡: Utilizes an event loop model to handle multiple client connections concurrently.
-   **RESP Protocol** 🗣️: Communicates with clients using the Redis Serialization Protocol (RESP).
-   **Persistence** 💾: Supports RDB file persistence to save and load data.
-   **Replication** 🔄: Implements master-replica replication for data redundancy and read scaling.
-   **Streams** 🌊: Supports Redis Streams, a data structure that models a log.
-   **Pub/Sub** 📢: Implements the publish/subscribe messaging paradigm.
