mod broadcast;
mod echo;
mod message_body;
#[cfg(test)]
mod tests;
mod unique_id;

use anyhow::Result;
use node_common::Outbox;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::mpsc::Sender;

pub use crate::message_body::MessageBody;
pub use node_common::NodeTrait;

pub type Message = node_common::Message<MessageBody>;
pub type BroadCastOutbox = Outbox<String, u32, u32, u32>;

#[derive(Clone)]
pub struct Node<Data> {
    pub id: String,
    pub node_ids: Vec<String>,
    pub store: HashSet<Data>,
    pub topology: HashMap<String, Vec<String>>,
    pub outbox: BroadCastOutbox,
}

impl<Data> Node<Data>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Eq + Hash,
{
    pub(crate) fn insert_if_absent(&mut self, payload: Data) -> Option<Data> {
        if !self.store.contains(&payload) {
            self.store.insert(payload.clone());
            Some(payload)
        } else {
            None
        }
    }
    pub(crate) fn read(&self) -> Vec<u32> {
        self.store.iter().map(|data| data.clone().into()).collect()
    }
}

impl<Data> Default for Node<Data> {
    fn default() -> Self {
        Self {
            id: Default::default(),
            node_ids: Default::default(),
            store: HashSet::new(),
            topology: HashMap::new(),
            outbox: BroadCastOutbox::default(),
        }
    }
}

impl<Data> NodeTrait for Node<Data>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    type Message = Message;

    fn new() -> Self {
        Self {
            id: String::new(),
            node_ids: vec![],
            store: HashSet::new(),
            topology: HashMap::new(),
            outbox: BroadCastOutbox::default(),
        }
    }
    fn handle_init_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::init {
            msg_id,
            node_ids,
            node_id,
        } = msg.body
        {
            (self.id, self.node_ids) = (node_id.clone(), node_ids.clone());

            let reply = Message {
                src: node_id,
                dest: msg.src,
                body: MessageBody::init_ok {
                    in_reply_to: msg_id,
                },
            };
            reply.send(tx)?;
        }
        Ok(())
    }

    fn get_and_increment_msg_id(&self) -> u32 {
        unique_id::generate_message_id()
    }

    fn handle_gossip_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_gossip_message(self, msg, tx)
    }

    fn handle_gossip_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_gossip_ok_message(self, msg, tx)
    }
}

impl<Data> Node<Data>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    pub fn handle_echo_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        echo::handle_echo_message(self, msg, tx)
    }

    pub fn handle_generate_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        unique_id::handle_generate_message(self, msg, tx)
    }

    pub fn handle_broadcast_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_broadcast_message(self, msg, tx)
    }

    pub fn handle_read_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_read_message(self, msg, tx)
    }

    pub fn handle_topology_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_topology_message(self, msg, tx)
    }

    pub fn handle_broadcast_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_broadcast_ok_message(self, msg, tx)
    }

    pub fn retry_messages(&mut self, tx: Sender<Message>) -> Result<()> {
        broadcast::retry_messages(self, tx)
    }
    pub fn fanout_messages(&mut self, tx: Sender<Message>) -> Result<()> {
        broadcast::fanout_messages(self, tx)
    }

    pub fn next(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        match msg.body {
            MessageBody::echo { .. } => self.handle_echo_message(msg, tx),
            MessageBody::init { .. } => self.handle_init_message(msg, tx),
            MessageBody::generate { .. } => self.handle_generate_message(msg, tx),
            MessageBody::broadcast { .. } => self.handle_broadcast_message(msg, tx),
            MessageBody::topology { .. } => self.handle_topology_message(msg, tx),
            MessageBody::read { .. } => self.handle_read_message(msg, tx),
            MessageBody::broadcast_ok { .. } => self.handle_broadcast_ok_message(msg, tx),
            MessageBody::gossip { .. } => self.handle_gossip_message(msg, tx),
            MessageBody::gossip_ok { .. } => self.handle_gossip_ok_message(msg, tx),
            MessageBody::init_ok { .. }
            | MessageBody::topology_ok { .. }
            | MessageBody::read_ok { .. }
            | MessageBody::generate_ok { .. }
            | MessageBody::echo_ok { .. } => unreachable!(),
        }
    }
}
