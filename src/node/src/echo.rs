use crate::{Message, MessageBody, Node, NodeTrait};
use anyhow::Result;
use std::hash::Hash;
use std::sync::mpsc::Sender;

pub fn handle_echo_message<Data>(
    node: &mut Node<Data>,
    msg: Message,
    tx: Sender<Message>,
) -> Result<()>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
    Node<Data>: NodeTrait,
{
    if let MessageBody::echo { msg_id, ref echo } = msg.body {
        let payload = MessageBody::echo_ok {
            msg_id: node.get_and_increment_msg_id(),
            in_reply_to: msg_id,
            echo: echo.clone(),
        };
        let reply = msg.into_reply(payload);
        reply.send(tx)?;
    }
    Ok(())
}
