use crate::{Message, MessageBody, Node, NodeTrait};
use anyhow::Result;
use std::hash::Hash;
use std::sync::mpsc::Sender;
use ulid::Ulid;

pub fn handle_generate_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
{
    if let MessageBody::generate { msg_id } = msg.body {
        let unique_id = Ulid::new().to_string();
        let payload = MessageBody::generate_ok {
            msg_id: node.get_and_increment_msg_id(),
            in_reply_to: msg_id,
            id: unique_id,
        };
        let reply = msg.into_reply(payload);
        reply.send(tx)?;
    }
    Ok(())
}
