use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

use crate::{BroadcastNodeTrait, Message, MessageBody, Node, NodeTrait};

// ── Helpers ──────────────────────────────────────────────────────────────────

fn make_node() -> Node<u32> {
    let mut node = Node::<u32>::new();
    node.id = "n1".to_string();
    node.node_ids = vec!["n1".to_string(), "n2".to_string(), "n3".to_string()];
    node
}

fn channel() -> (Sender<Message>, Receiver<Message>) {
    mpsc::channel()
}

fn msg(src: &str, dest: &str, body: MessageBody) -> Message {
    Message {
        src: src.to_string(),
        dest: dest.to_string(),
        body,
    }
}

fn drain(rx: &Receiver<Message>) -> Vec<Message> {
    let mut msgs = vec![];
    while let Ok(m) = rx.try_recv() {
        msgs.push(m);
    }
    msgs
}

// ── Init ─────────────────────────────────────────────────────────────────────

#[test]
fn init_sets_node_state_and_replies() {
    let mut node = make_node();
    let (tx, rx) = channel();

    let incoming = msg(
        "c1",
        "n1",
        MessageBody::init {
            msg_id: 1,
            node_id: "n1".to_string(),
            node_ids: vec!["n1".to_string(), "n2".to_string()],
        },
    );
    node.handle_init_message(incoming, tx).unwrap();

    assert_eq!(node.id, "n1");
    assert_eq!(node.node_ids, vec!["n1", "n2"]);

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].src, "n1");
    assert_eq!(sent[0].dest, "c1");
    assert!(matches!(
        sent[0].body,
        MessageBody::init_ok { in_reply_to: 1 }
    ));
}

// ── Echo ──────────────────────────────────────────────────────────────────────

#[test]
fn echo_replies_with_same_text() {
    let mut node = make_node();
    let (tx, rx) = channel();

    let incoming = msg(
        "c1",
        "n1",
        MessageBody::echo {
            msg_id: 42,
            echo: "hello".to_string(),
        },
    );
    node.handle_echo_message(incoming, tx).unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);

    let reply = &sent[0];
    assert_eq!(reply.src, "n1");
    assert_eq!(reply.dest, "c1");
    match &reply.body {
        MessageBody::echo_ok {
            in_reply_to, echo, ..
        } => {
            assert_eq!(*in_reply_to, 42);
            assert_eq!(echo, "hello");
        }
        other => panic!("expected echo_ok, got {:?}", other),
    }
}

// ── Unique ID (generate) ─────────────────────────────────────────────────────

#[test]
fn generate_replies_with_unique_non_empty_id() {
    let mut node = make_node();
    let (tx, rx) = channel();

    let incoming = msg("c1", "n1", MessageBody::generate { msg_id: 7 });
    node.handle_generate_message(incoming, tx).unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);

    let reply = &sent[0];
    assert_eq!(reply.src, "n1");
    assert_eq!(reply.dest, "c1");
    match &reply.body {
        MessageBody::generate_ok {
            in_reply_to, id, ..
        } => {
            assert_eq!(*in_reply_to, 7);
            assert!(!id.is_empty(), "generated id must not be empty");
        }
        other => panic!("expected generate_ok, got {:?}", other),
    }
}

#[test]
fn generate_ids_are_unique() {
    let mut node = make_node();
    let (tx, rx) = channel();

    for i in 0..10u32 {
        node.handle_generate_message(
            msg("c1", "n1", MessageBody::generate { msg_id: i }),
            tx.clone(),
        )
        .unwrap();
    }

    let ids: Vec<String> = drain(&rx)
        .into_iter()
        .map(|m| match m.body {
            MessageBody::generate_ok { id, .. } => id,
            _ => panic!("unexpected body"),
        })
        .collect();

    let unique: std::collections::HashSet<_> = ids.iter().collect();
    assert_eq!(ids.len(), unique.len(), "all generated ids must be unique");
}

// ── Broadcast ─────────────────────────────────────────────────────────────────

#[test]
fn broadcast_new_message_acks_and_fans_out_to_neighbours() {
    let mut node = make_node();
    // topology: n1 -> [n2, n3]
    node.topology
        .insert("n1".to_string(), vec!["n2".to_string(), "n3".to_string()]);
    let (tx, rx) = channel();

    // Message arrives from a client (not a neighbour)
    let incoming = msg(
        "c1",
        "n1",
        MessageBody::broadcast {
            message: 99,
            msg_id: 1,
        },
    );
    node.handle_broadcast_message(incoming, tx).unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);

    let ack = &sent[0];
    assert_eq!(ack.dest, "c1");
    assert!(
        matches!(ack.body, MessageBody::broadcast_ok { in_reply_to: 1, .. }),
        "ack should be broadcast_ok in_reply_to=1"
    );

    for dest in ["n2", "n3"] {
        assert!(
            node.msg_outbox
                .get(dest)
                .is_some_and(|messages| messages.contains(&99)),
            "fanout payload should be queued for {dest}"
        );
    }

    // Value stored
    assert!(node.store.contains(&99u32));
}

#[test]
fn broadcast_does_not_fan_out_back_to_sender() {
    let mut node = make_node();
    // n2 is both the sender and a topology neighbour
    node.topology
        .insert("n1".to_string(), vec!["n2".to_string(), "n3".to_string()]);
    let (tx, rx) = channel();

    let incoming = msg(
        "n2",
        "n1",
        MessageBody::broadcast {
            message: 55,
            msg_id: 2,
        },
    );
    node.handle_broadcast_message(incoming, tx).unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert!(
        sent.iter()
            .any(|m| m.dest == "n2" && matches!(m.body, MessageBody::broadcast_ok { .. })),
        "should ack n2"
    );
    assert!(
        node.msg_outbox
            .get("n3")
            .is_some_and(|messages| messages.contains(&55)),
        "should queue fanout to n3"
    );
    assert!(
        !node.msg_outbox.contains_key("n2"),
        "must not queue fanout back to sender n2"
    );
}

#[test]
fn broadcast_duplicate_message_acks_without_fanout() {
    let mut node = make_node();
    node.topology
        .insert("n1".to_string(), vec!["n2".to_string()]);
    let (tx, rx) = channel();

    let make_msg = || {
        msg(
            "c1",
            "n1",
            MessageBody::broadcast {
                message: 7,
                msg_id: 1,
            },
        )
    };

    node.handle_broadcast_message(make_msg(), tx.clone())
        .unwrap();
    drain(&rx); // discard first delivery

    // Send the exact same value again
    node.handle_broadcast_message(make_msg(), tx).unwrap();
    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert!(matches!(
        sent[0].body,
        MessageBody::broadcast_ok { in_reply_to: 1, .. }
    ));
    assert!(
        node.msg_outbox
            .get("n2")
            .is_none_or(|messages| messages.len() == 1),
        "duplicate broadcast should not queue another fanout payload"
    );
}

#[test]
fn broadcast_new_message_added_to_outbox() {
    let mut node = make_node();
    node.topology
        .insert("n1".to_string(), vec!["n2".to_string()]);
    let (tx, _rx) = channel();

    node.handle_broadcast_message(
        msg(
            "c1",
            "n1",
            MessageBody::broadcast {
                message: 42,
                msg_id: 1,
            },
        ),
        tx,
    )
    .unwrap();

    assert!(
        node.retry_outbox.contains_key("n2"),
        "fanout message to n2 should be in outbox keyed by destination n2"
    );
    assert!(node.retry_outbox["n2"].contains(&42));
}

// ── Read ─────────────────────────────────────────────────────────────────────

#[test]
fn read_returns_empty_store() {
    let mut node = make_node();
    let (tx, rx) = channel();

    node.handle_read_message(msg("c1", "n1", MessageBody::read { msg_id: 3 }), tx)
        .unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].src, "n1");
    assert_eq!(sent[0].dest, "c1");
    match &sent[0].body {
        MessageBody::read_ok {
            messages,
            in_reply_to,
            ..
        } => {
            assert_eq!(*in_reply_to, 3);
            assert!(messages.is_empty());
        }
        other => panic!("expected read_ok, got {:?}", other),
    }
}

#[test]
fn read_returns_all_stored_values() {
    let mut node = make_node();
    node.store.insert(1u32);
    node.store.insert(2u32);
    node.store.insert(3u32);
    let (tx, rx) = channel();

    node.handle_read_message(msg("c1", "n1", MessageBody::read { msg_id: 5 }), tx)
        .unwrap();

    let sent = drain(&rx);
    match &sent[0].body {
        MessageBody::read_ok { messages, .. } => {
            let mut got = messages.clone();
            got.sort();
            assert_eq!(got, vec![1, 2, 3]);
        }
        other => panic!("expected read_ok, got {:?}", other),
    }
}

// ── Topology ──────────────────────────────────────────────────────────────────

#[test]
fn topology_updates_node_and_acks() {
    let mut node = make_node();
    let (tx, rx) = channel();

    let topo: HashMap<String, Vec<String>> =
        [("n1".to_string(), vec!["n2".to_string(), "n3".to_string()])]
            .into_iter()
            .collect();

    node.handle_topology_message(
        msg(
            "c1",
            "n1",
            MessageBody::topology {
                topology: topo.clone(),
                msg_id: 8,
            },
        ),
        tx,
    )
    .unwrap();

    assert_eq!(node.topology, topo, "node topology should be updated");

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].src, "n1");
    assert_eq!(sent[0].dest, "c1");
    assert!(
        matches!(
            sent[0].body,
            MessageBody::topology_ok { in_reply_to: 8, .. }
        ),
        "should reply topology_ok in_reply_to=8"
    );
}

// ── Sync ─────────────────────────────────────────────────────────────────────

#[test]
fn sync_merges_their_data_and_replies_with_ours() {
    let mut node = make_node();
    node.store.insert(1u32);
    node.store.insert(2u32);
    node.store.insert(3u32);
    let (tx, rx) = channel();

    // Peer has 2, 3, 4 — we have 1, 2, 3
    let incoming = msg(
        "n2",
        "n1",
        MessageBody::sync {
            msg_id: 10,
            messages: vec![2, 3, 4],
        },
    );
    node.handle_sync_message(incoming, tx).unwrap();

    // Node should now also contain 4
    assert!(node.store.contains(&4u32), "should insert value from peer");
    assert_eq!(node.store.len(), 4);

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].src, "n1");
    assert_eq!(sent[0].dest, "n2");
    match &sent[0].body {
        MessageBody::sync_ok {
            in_reply_to,
            messages,
            ..
        } => {
            assert_eq!(*in_reply_to, 10);
            // We reply with what we had that they didn't: [1]
            assert_eq!(messages, &vec![1u32], "sync_ok should contain only value 1");
        }
        other => panic!("expected sync_ok, got {:?}", other),
    }
}

#[test]
fn sync_with_identical_stores_sends_empty_sync_ok() {
    let mut node = make_node();
    node.store.insert(5u32);
    let (tx, rx) = channel();

    node.handle_sync_message(
        msg(
            "n2",
            "n1",
            MessageBody::sync {
                msg_id: 1,
                messages: vec![5],
            },
        ),
        tx,
    )
    .unwrap();

    let sent = drain(&rx);
    match &sent[0].body {
        MessageBody::sync_ok { messages, .. } => {
            assert!(messages.is_empty(), "nothing new to send back");
        }
        other => panic!("expected sync_ok, got {:?}", other),
    }
}

#[test]
fn sync_ok_inserts_new_values_no_reply() {
    let mut node = make_node();
    node.store.insert(1u32);
    let (tx, rx) = channel();

    node.handle_sync_ok_message(
        msg(
            "n2",
            "n1",
            MessageBody::sync_ok {
                msg_id: 1,
                in_reply_to: 0,
                messages: vec![2, 3],
            },
        ),
        tx,
    )
    .unwrap();

    assert!(node.store.contains(&2u32));
    assert!(node.store.contains(&3u32));
    assert_eq!(node.store.len(), 3);
    assert!(drain(&rx).is_empty(), "sync_ok must not send any reply");
}

// ── Gossip OK (outbox management) ────────────────────────────────────────────

#[test]
fn gossip_ok_removes_messages_from_outbox() {
    let mut node = make_node();
    let (tx, rx) = channel();

    node.retry_outbox
        .entry("n2".to_string())
        .or_default()
        .insert(10);
    node.retry_outbox
        .entry("n2".to_string())
        .or_default()
        .insert(11);
    node.in_flight_gossip
        .insert(5, ("n2".to_string(), [10].into_iter().collect()));

    assert_eq!(node.retry_outbox["n2"].len(), 2);

    node.handle_gossip_ok_message(
        msg("n2", "n1", MessageBody::gossip_ok { in_reply_to: 5 }),
        tx,
    )
    .unwrap();

    assert!(!node.retry_outbox["n2"].contains(&10));
    assert!(node.retry_outbox["n2"].contains(&11));
    assert!(
        drain(&rx).is_empty(),
        "gossip_ok handler must not send messages"
    );
}

// ── Retry ─────────────────────────────────────────────────────────────────────

#[test]
fn retry_resends_all_outbox_messages() {
    let mut node = make_node();
    let (tx, rx) = channel();

    node.retry_outbox
        .entry("n2".to_string())
        .or_default()
        .insert(1);
    node.retry_outbox
        .entry("n3".to_string())
        .or_default()
        .insert(2);

    node.retry_messages(tx).unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 2, "retry should resend all 2 outbox messages");

    let dests: std::collections::HashSet<_> = sent.iter().map(|m| m.dest.as_str()).collect();
    assert!(dests.contains("n2"));
    assert!(dests.contains("n3"));
    for message in sent {
        assert!(matches!(message.body, MessageBody::gossip { .. }));
    }
}

#[test]
fn retry_skips_peer_with_in_flight_gossip() {
    let mut node = make_node();
    let (tx, rx) = channel();

    node.retry_outbox
        .entry("n2".to_string())
        .or_default()
        .insert(1);
    node.retry_outbox
        .entry("n3".to_string())
        .or_default()
        .insert(2);
    node.in_flight_gossip
        .insert(100, ("n2".to_string(), [1].into_iter().collect()));

    node.retry_messages(tx).unwrap();

    let sent = drain(&rx);
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].dest, "n3");
    assert!(matches!(sent[0].body, MessageBody::gossip { .. }));
}

// ── Request sync with random peers ───────────────────────────────────────────

#[test]
fn request_sync_creates_sync_messages_from_this_node() {
    let mut node = make_node();
    // node has 3 peers: n1 (self), n2, n3
    node.store.insert(10u32);
    node.store.insert(20u32);

    let messages = node.request_sync_with_random_peers();

    // Should pick at most 2 peers (or fewer if node_ids has < 2)
    assert!(messages.len() <= 2);

    for m in &messages {
        assert_eq!(m.src, "n1", "sync message src must be this node");
        match &m.body {
            MessageBody::sync { messages, .. } => {
                let mut vals = messages.clone();
                vals.sort();
                assert_eq!(vals, vec![10u32, 20u32], "sync should carry current store");
            }
            other => panic!("expected sync body, got {:?}", other),
        }
    }
}
