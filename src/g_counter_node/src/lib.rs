use anyhow::Result;
mod message_body;
use node_common::Outbox;
use std::{
    collections::{HashMap, HashSet},
    sync::mpsc::Sender,
};

pub use crate::message_body::MessageBody;
pub type Message = node_common::Message<MessageBody>;
pub type GCounterOutBox = Outbox<String, u32, u32, u32>;

#[derive(Clone)]
pub struct Node<Data> {
    pub id: String,
    pub node_ids: Vec<String>,
    pub store: HashSet<Data>,
    pub topology: HashMap<String, Vec<String>>,
    pub outbox: GCounterOutBox,
}

pub struct IdempotentDispatchFromOutBox {
    //node that the message is supposed to go for
    pub owner: String,
    pub value: u64,
}

impl<Data> Node<Data> {
    /// Workload implementation is pending.
    pub fn handle_read_message(&mut self, _msg: Message, _tx: Sender<Message>) -> Result<()> {
        anyhow::bail!("G-counter read handler is not implemented")
    }
    /// Workload implementation is pending.
    pub fn handle_add_message(&mut self, _msg: Message, _tx: Sender<Message>) -> Result<()> {
        anyhow::bail!("G-counter add handler is not implemented")
    }
    /// Workload implementation is pending.
    pub fn fanout_messages(&mut self, _tx: Sender<Message>) -> Result<()> {
        anyhow::bail!("G-counter fanout is not implemented")
    }
    pub fn next(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        match msg.body {
            MessageBody::read => self.handle_read_message(msg, tx),
            MessageBody::add { .. } => self.handle_add_message(msg, tx),
            MessageBody::add_ok { .. } => unreachable!(),
            MessageBody::read_ok { .. } => unreachable!(),
        }
    }
}
