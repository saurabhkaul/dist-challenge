use anyhow::{Ok, Result};
mod message_body;
use node_common::NodeTrait;
use std::sync::mpsc::Sender;

pub use crate::message_body::MessageBody;
pub type Message = node_common::Message<MessageBody>;

pub trait GCounterNodeTrait: NodeTrait<Message = Message> {
    fn handle_read_message(&mut self, msg: Self::Message, tx: Sender<Self::Message>) -> Result<()>;
    fn handle_add_message(&mut self, msg: Self::Message, tx: Sender<Self::Message>) -> Result<()>;
    fn fanout_messages(&mut self, tx: Sender<Message>) -> Result<()>;
    fn next(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        match msg.body{
            MessageBody::read => self.handle_read_message(msg, tx),
            MessageBody::add { delta } => self.handle_add_message(msg, tx),
            MessageBody::add_ok{ .. } => unreachable!(),
            MessageBody::read_ok { value } => unreachable!(),
        }
    }
    
}

// pub struct Node{
//     pub id: 
// }
