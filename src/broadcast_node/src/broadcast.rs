use crate::{Message, MessageBody, Node, NodeTrait};
use anyhow::Result;
use std::hash::Hash;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

const GOSSIP_ACK_TIMEOUT: Duration = Duration::from_millis(300);

pub fn handle_broadcast_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    if let MessageBody::broadcast { message, msg_id } = msg.body {
        let reply = Message {
            src: msg.dest.clone(),
            dest: msg.src.clone(),
            body: MessageBody::broadcast_ok {
                in_reply_to: msg_id,
                msg_id: node.get_and_increment_msg_id(),
            },
        };

        // Always ack client/peer broadcast RPCs, but only fan out newly seen values.
        if node.insert_if_absent(Data::from(message)).is_none() {
            reply.send(tx)?;
            return Ok(());
        }

        let neighbours: Option<&Vec<String>> = node.topology.get(&node.id);
        if let Some(neighbours) = neighbours {
            let fanout_peers: Vec<String> = neighbours
                .iter()
                .filter(|n| **n != msg.src)
                .take(2)
                .cloned()
                .collect();
            for peer in fanout_peers {
                //Since we are storing a unique set of values broadcasted, we can get away with having the value itself as ItemId
                node.outbox.enqueue(peer, message, message);
            }
        }

        reply.send(tx)?;
    }
    Ok(())
}

pub fn handle_read_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    if let MessageBody::read { msg_id } = msg.body {
        let messages: Vec<u32> = node.read();
        let payload = MessageBody::read_ok {
            messages,
            in_reply_to: msg_id,
            msg_id: node.get_and_increment_msg_id(),
        };
        let reply = msg.into_reply(payload);
        reply.send(tx)?;
    }
    Ok(())
}

pub fn handle_topology_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    if let MessageBody::topology {
        ref topology,
        msg_id,
    } = msg.body
    {
        node.topology = topology.clone();
        let payload = MessageBody::topology_ok {
            msg_id: node.get_and_increment_msg_id(),
            in_reply_to: msg_id,
        };
        let reply = msg.into_reply(payload);
        reply.send(tx)?;
    }
    Ok(())
}

pub fn handle_broadcast_ok_message<Data>(
    _node: &mut Node<Data>,
    _msg: Message,
    _tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    Ok(())
}

pub fn handle_gossip_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    let src = msg.src.clone();
    if let MessageBody::gossip { msg_id, messages } = msg.body {
        let mut newly_seen = Vec::new();
        for message in messages {
            if node.insert_if_absent(Data::from(message)).is_some() {
                newly_seen.push(message);
            }
        }

        if !newly_seen.is_empty() {
            let neighbours: Option<&Vec<String>> = node.topology.get(&node.id);
            if let Some(neighbours) = neighbours {
                let fanout_peers: Vec<String> = neighbours
                    .iter()
                    .filter(|n| **n != src)
                    .take(2)
                    .cloned()
                    .collect();
                for peer in fanout_peers {
                    for message in &newly_seen {
                        node.outbox.enqueue(peer.clone(), *message, *message);
                    }
                }
            }
        }

        Message {
            src: node.id.clone(),
            dest: src,
            body: MessageBody::gossip_ok {
                in_reply_to: msg_id,
            },
        }
        .send(tx)?;
    }

    Ok(())
}

pub fn handle_gossip_ok_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    _tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    if let MessageBody::gossip_ok { in_reply_to } = msg.body {
        node.outbox.acknowledge(&in_reply_to);
    }

    Ok(())
}

//We do bulk retries
pub fn retry_messages<Data>(node: &mut Node<Data>, tx: Sender<Message>) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    send_ready_batches(node, tx)
}

//We do bulk fanouts
pub fn fanout_messages<Data>(node: &mut Node<Data>, tx: Sender<Message>) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    send_ready_batches(node, tx)
}

fn send_ready_batches<Data>(node: &mut Node<Data>, tx: Sender<Message>) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    let batches = node.outbox.poll(
        Instant::now(),
        GOSSIP_ACK_TIMEOUT,
        crate::unique_id::generate_message_id,
    );
    for batch in batches {
        let message_id = batch.message_id;

        let messages = batch
            .items
            .into_iter()
            .map(|(_, payload)| payload)
            .collect();

        let outgoing = Message {
            src: node.id.clone(),
            dest: batch.peer,
            body: MessageBody::gossip {
                msg_id: message_id,
                messages,
            },
        };
        if let Err(error) = outgoing.send(tx.clone()) {
            node.outbox.mark_send_failed(&message_id);
            return Err(error);
        }
    }
    Ok(())
}
