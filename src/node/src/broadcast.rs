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
        //Check and see if we have received the message before, only gossip if its a new message
        if let Some(_data) = node.check_and_push_to_store(Data::from(message)) {
            let reply_payload = MessageBody::broadcast_ok {
                in_reply_to: msg_id,
                msg_id: node.get_and_increment_msg_id(),
            };
            let reply = msg.clone().into_reply(reply_payload);
            reply.send(tx.clone())?;

            //Send the new message to our neighbours in the topology,
            // and also add them to our outbox so that we can retry later
            let neighbours: Option<&Vec<String>> = node.topology.get(&node.id);
            if let Some(neighbours) = neighbours {
                let fanout_messages: Vec<Message> = neighbours
                    .iter()
                    .filter(|n| **n != msg.src)
                    .map(|node_id| Message {
                        src: node.id.clone(),
                        dest: node_id.to_owned(),
                        body: MessageBody::broadcast { message, msg_id },
                    })
                    .collect();
                for msg in fanout_messages {
                    node.add_to_outbox(&msg)?;
                    msg.send(tx.clone())?;
                }
            }
        };
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
    node: &mut Node<Data>,
    msg: Message,
    _tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    if let MessageBody::broadcast_ok { in_reply_to, .. } = msg.body {
        node.remove_from_outbox(msg.src, &in_reply_to)?
    }
    Ok(())
}

pub fn retry_messages<Data>(node: &mut Node<Data>, tx: Sender<Message>) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    for n in node.outbox.keys() {
        for msg in node.outbox.get(n).unwrap().iter() {
            msg.clone().send(tx.clone())?
        }
    }
    Ok(())
}
