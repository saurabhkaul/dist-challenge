use crate::{Message, MessageBody, Node, NodeTrait};
use anyhow::Result;
use rand::seq::IndexedRandom;
use std::collections::HashSet;
use std::hash::Hash;
use std::sync::mpsc::Sender;

pub fn handle_broadcast_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
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
                node.add_to_outbox(crate::OutboxKind::FanoutMsg, &peer, message)?;
                node.add_to_outbox(crate::OutboxKind::RetryMsg, &peer, message)?;
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
    Node<Data>: NodeTrait,
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
    Node<Data>: NodeTrait,
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

pub fn handle_sync_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
{
    if let MessageBody::sync {
        msg_id,
        ref messages,
    } = msg.body
    {
        let messages: HashSet<Data> = messages.into_iter().map(|m| Data::from(*m)).collect();
        let i_have: HashSet<Data> = node.store.difference(&messages).cloned().collect();
        let they_have: HashSet<Data> = messages.difference(&node.store).cloned().collect();

        let i_have: Vec<u32> = i_have.into_iter().map(|m| Data::into(m)).collect();
        //insert the data we dont have
        for data in they_have {
            node.store.insert(data);
        }
        //send back the data they dont have
        let payload = MessageBody::sync_ok {
            msg_id: node.get_and_increment_msg_id(),
            in_reply_to: msg_id,
            messages: i_have,
        };
        let reply = msg.into_reply(payload);
        reply.send(tx)?;
    }
    Ok(())
}

pub fn handle_sync_ok_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    _tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    if let MessageBody::sync_ok {
        msg_id: _,
        in_reply_to: _,
        messages,
    } = msg.body
    {
        //We might have received data we didn't have the the syncing node has
        //So we simply insert this new data and dont send any acknowledgement
        for m in messages {
            node.store.insert(Data::from(m));
        }
    }
    Ok(())
}

// To combat network partitions, a node calls this function to pick random
// nodes for their messages,while it sends its own. Once we get theirs we can
// copy values we dont have, while they can copy values from us
// This function acts as a initiator for the sync process, piggybacking on
// maelstroms messaging protocol, by injecting custom message types.
pub fn request_sync_with_random_peers<Data>(node: &mut Node<Data>) -> Vec<Message>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
{
    let all_nodes: Vec<String> = node.node_ids.clone();
    let mut rng = rand::rng();
    let messages = all_nodes
        .choose_multiple(&mut rng, 2)
        .map(|node_id| Message {
            src: node.id.clone(),
            dest: node_id.to_owned(),
            body: MessageBody::sync {
                msg_id: node.get_and_increment_msg_id(),
                messages: node.read(),
            },
        })
        .collect();
    messages
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
    Node<Data>: NodeTrait,
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
                        node.add_to_outbox(crate::OutboxKind::FanoutMsg, &peer, *message)?;
                        node.add_to_outbox(crate::OutboxKind::RetryMsg, &peer, *message)?;
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
        node.acknowledge_gossip_batch(in_reply_to);
    }

    Ok(())
}

//We do bulk retries
pub fn retry_messages<Data>(node: &mut Node<Data>, tx: Sender<Message>) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
{
    let retries: Vec<(String, HashSet<u32>)> = node
        .retry_outbox
        .iter()
        .filter(|(_, messages)| !messages.is_empty())
        .filter(|(node_id, _)| !node.has_in_flight_gossip_for(node_id))
        .map(|(node_id, messages)| (node_id.clone(), messages.clone()))
        .collect();

    for (node_id, messages) in retries {
        let msg_id = node.get_and_increment_msg_id();
        node.track_gossip_batch(msg_id, node_id.clone(), messages.clone());
        Message {
            src: node.id.clone(),
            dest: node_id,
            body: MessageBody::gossip {
                msg_id,
                messages: messages.iter().copied().collect(),
            },
        }
        .send(tx.clone())?;
    }

    Ok(())
}

//We do bulk fanouts
pub fn fanout_messages<Data>(node: &mut Node<Data>, tx: Sender<Message>) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
{
    let pending: Vec<(String, HashSet<u32>)> = node.msg_outbox.drain().collect();
    for (node_id, messages) in pending {
        if messages.is_empty() {
            continue;
        }

        let msg_id = node.get_and_increment_msg_id();
        node.track_gossip_batch(msg_id, node_id.clone(), messages.clone());
        Message {
            src: node.id.clone(),
            dest: node_id,
            body: MessageBody::gossip {
                msg_id,
                messages: messages.iter().copied().collect(),
            },
        }
        .send(tx.clone())?;
    }

    Ok(())
}
