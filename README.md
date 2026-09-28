# Implementing Gossip Glomers (WIP)

Rust implementations of [Fly.io's distributed systems challenges](https://fly.io/dist-sys/), using [Maelstrom](https://github.com/jepsen-io/maelstrom) to run nodes and simulate their network. Echo, Unique ID Generation, and Broadcast share the default executable. Further challenges are a work in progress.

## Node architecture and message delivery

This is a Cargo workspace: the root executable runs the input/output loop, `broadcast_node` implements Echo, Unique IDs, and Broadcast, and `node_common` provides the message envelope and reusable Outbox. `g_counter_node` contains the counter work in progress.

Each node owns its identity, cluster membership, local state, topology, and Outbox. An `init` request sets the node identity and membership. Requests are handled sequentially by `Node::next`, which dispatches on the message body's `type`.

```text
Maelstrom → stdin JSON lines → Node::next → workload handler
                                                │
                          replies / gossip → mpsc channel
                                                │
                                      writer thread → stdout → Maelstrom
```

Messages have `src`, `dest`, and `body` fields. Replies reverse the addresses and use `in_reply_to` to identify the original request. Handlers send typed messages into an `mpsc` channel; a dedicated writer serializes them as newline-delimited JSON on stdout. Debug output goes to stderr. Maelstrom routes the messages between processes—nodes do not open network sockets themselves.

Sending into the channel only confirms local submission. Reliable peer delivery is handled separately by `Outbox<NodeId, ItemId, Payload, MessageId>`:

1. `enqueue(peer, item_id, payload)` records pending work for a destination. Enqueuing the same pending item again keeps its original payload.
2. `poll(now, timeout, next_message_id)` expires timed-out attempts and returns batches to send, allowing only one outstanding batch per peer. Items remain queued while in flight.
3. The receiver applies a gossip batch and returns `gossip_ok`. `acknowledge(in_reply_to)` removes the items covered by that attempt.
4. If submitting a message to the channel fails, `mark_send_failed(message_id)` releases its reservation. If no peer ACK arrives, a later poll retries after the timeout.

An item ID identifies logical work; a message ID identifies a delivery attempt. Retries use fresh message IDs, so receivers must tolerate duplicate delivery. The Outbox is in memory and does not survive a process restart.

Implementation: [runtime](src/main.rs), [node and dispatch](src/broadcast_node/src/lib.rs), [shared messages and Outbox](src/node_common/src/lib.rs).

## Challenge 1: Echo

[Challenge specification](https://fly.io/dist-sys/1/) · [Implementation](src/broadcast_node/src/echo.rs)

The handler copies the incoming `echo` text into an `echo_ok` response, adds a response message ID, and sets `in_reply_to` to the request ID. `into_reply` swaps the source and destination, and the response follows the shared output channel. No replication or persistent state is needed.

## Challenge 2: Unique ID Generation

[Challenge specification](https://fly.io/dist-sys/2/) · [Implementation](src/broadcast_node/src/unique_id.rs)

Each `generate` request creates a ULID locally with `Ulid::new()` and returns its string representation in `generate_ok`. Nodes do not coordinate, so a network partition does not prevent ID generation. Uniqueness relies on ULID's timestamp and randomness rather than a centralized sequence.

The generated application ID is the full ULID string. Protocol message IDs are separate: the current helper takes the final four ULID bytes as a `u32`. Those IDs are probabilistic and can collide; they are not a strictly increasing counter.

## Challenge 3: Broadcast

All stages use the same [broadcast implementation](src/broadcast_node/src/broadcast.rs). Each node stores received numbers in a `HashSet`, making repeated delivery harmless. Reads return the locally known set; a broadcast acknowledgement confirms local acceptance, not replication to every node.

### 3a: Single-Node Broadcast

[Challenge specification](https://fly.io/dist-sys/3a/)

`broadcast` inserts the value and returns `broadcast_ok`. `read` returns the stored values in `read_ok`; their order is unspecified. `topology` saves the supplied neighbor map and returns `topology_ok`. With one node, these local operations provide the required behavior.

### 3b: Multi-Node Broadcast

[Challenge specification](https://fly.io/dist-sys/3b/)

New values are queued for up to two topology neighbors, excluding the sender. The selection takes the first two eligible neighbors; it is not random. Each value serves as both the Outbox item ID and payload.

A receiving node inserts unseen gossip values and queues those new values for onward propagation. Already-known values are acknowledged without being forwarded again, limiting repeated traffic. This spreads values along the selected neighbor links; cluster-wide coverage depends on those links reaching all nodes.

### 3c: Fault-Tolerant Broadcast

[Challenge specification](https://fly.io/dist-sys/3c/)

Pending gossip remains in the Outbox until acknowledged. Attempts expire after 300 ms and become eligible for retransmission. If a partition drops a message or its acknowledgement, subsequent attempts can deliver the queued values after connectivity returns. A receiver acknowledges duplicate batches too, allowing a sender whose earlier ACK was lost to finish the delivery.


### 3d: Efficient Broadcast, Part I

[Challenge specification](https://fly.io/dist-sys/3d/)

Rather than sending every value immediately in its own message, the Outbox accumulates pending values per peer. The runtime checks a 50 ms fanout interval, and a poll packages all eligible values for each peer into one gossip message. One ACK covers the entire batch, reducing message overhead when multiple values arrive between sends.

### 3e: Efficient Broadcast, Part II

[Challenge specification](https://fly.io/dist-sys/3e/)

The same implementation combines batching, fanout capped at two neighbors, forwarding only newly seen values, and at most one outstanding batch per peer. These choices reduce traffic while the short fanout interval aims to keep propagation latency low. There is no separate 3e algorithm or runtime mode; the actual latency and message count should be measured with the challenge's Maelstrom workload.


## Build and test

```sh
cargo build --release
cargo test --workspace
```

The default `broadcast` feature includes handlers for Echo, Unique IDs, and Broadcast. Pass `target/release/dist-challenge` as Maelstrom's `--bin` argument, using an absolute path if running Maelstrom from another directory. Each linked challenge provides its workload command and evaluation parameters.

Unit tests cover replies, local storage, gossip forwarding, acknowledgements, retries, and Outbox bookkeeping. Maelstrom runs are needed to verify behavior under partitions and the performance targets of Broadcast 3d/3e.

Challenge 4 (Grow only counter) is currently wip. 
