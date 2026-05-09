mod broadcast;
mod echo;
#[cfg(test)]
mod tests;
mod unique_id;

use anyhow::Ok;
use anyhow::Result;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::mpsc::Sender;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

impl Message {
    pub fn send(self, tx: Sender<Message>) -> Result<(), anyhow::Error> {
        tx.send(self)?;
        Ok(())
    }
    pub fn into_reply(self, payload: MessageBody) -> Message {
        Message {
            src: self.dest,
            dest: self.src,
            body: payload,
        }
    }
    pub fn into_message(self, payload: MessageBody, new_dest: &str) -> Message {
        Message {
            src: self.dest,
            dest: new_dest.to_owned(),
            body: payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MessageBody {
    #[serde(rename_all = "snake_case")]
    broadcast {
        message: u32,
        msg_id: u32,
    },

    broadcast_ok {
        in_reply_to: u32,
        msg_id: u32,
    },
    topology {
        topology: HashMap<String, Vec<String>>,
        msg_id: u32,
    },
    topology_ok {
        msg_id: u32,
        in_reply_to: u32,
    },
    read {
        msg_id: u32,
    },
    read_ok {
        messages: Vec<u32>,
        in_reply_to: u32,
        msg_id: u32,
    },

    generate {
        msg_id: u32,
    },

    generate_ok {
        msg_id: u32,
        in_reply_to: u32,
        id: String,
    },

    echo {
        msg_id: u32,
        echo: String,
    },

    echo_ok {
        msg_id: u32,
        in_reply_to: u32,
        echo: String,
    },

    init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },
    init_ok {
        in_reply_to: u32,
    },
    //Custom messages not part of the protocol
    sync {
        msg_id: u32,
        messages: Vec<u32>,
    },
    sync_ok {
        msg_id: u32,
        in_reply_to: u32,
        messages: Vec<u32>,
    },
    gossip {
        msg_id: u32,
        messages: Vec<u32>,
    },
    gossip_ok {
        in_reply_to: u32,
    },
}

pub trait NodeTrait {
    fn new() -> Self;
    fn handle_init_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_echo_message(&mut self, msg: Message, tx: Sender<Message>) -> anyhow::Result<()>;
    fn handle_generate_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_broadcast_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_broadcast_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_read_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_topology_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn get_and_increment_msg_id(&self) -> u32;
    fn handle_sync_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_sync_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn request_sync_with_random_peers(&mut self) -> Vec<Message>;
    fn retry_messages(&mut self, tx: Sender<Message>) -> Result<()>;
    fn fanout_messages(&mut self, tx: Sender<Message>) -> Result<()>;
    fn handle_gossip_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_gossip_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn next(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        match msg.body {
            MessageBody::echo { .. } => self.handle_echo_message(msg, tx),
            MessageBody::init { .. } => self.handle_init_message(msg, tx),
            MessageBody::generate { .. } => self.handle_generate_message(msg, tx),
            MessageBody::broadcast { .. } => self.handle_broadcast_message(msg, tx),
            MessageBody::topology { .. } => self.handle_topology_message(msg, tx),
            MessageBody::read { .. } => self.handle_read_message(msg, tx),
            MessageBody::broadcast_ok { .. } => self.handle_broadcast_ok_message(msg, tx),
            MessageBody::sync { .. } => self.handle_sync_message(msg, tx),
            MessageBody::sync_ok { .. } => self.handle_sync_ok_message(msg, tx),
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

type Outbox = HashMap<String, HashSet<u32>>;
#[derive(Debug, Clone, Copy)]
pub enum OutboxKind {
    RetryMsg,
    FanoutMsg,
}

#[derive(Clone)]
pub struct Node<Data> {
    pub id: String,
    pub node_ids: Vec<String>,
    pub store: HashSet<Data>,
    pub topology: HashMap<String, Vec<String>>,
    //We track our retries here
    pub retry_outbox: Outbox,
    //We collect fanout messages we have to send for each node, and send in one go
    pub msg_outbox: Outbox,
    pub in_flight_gossip: HashMap<u32, (String, HashSet<u32>)>,
}

impl<Data> Node<Data>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Eq + Hash,
    Self: NodeTrait,
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
    fn outbox_mut(&mut self, kind: OutboxKind) -> &mut Outbox {
        match kind {
            OutboxKind::RetryMsg => &mut self.retry_outbox,
            OutboxKind::FanoutMsg => &mut self.msg_outbox,
        }
    }

    pub(crate) fn add_to_outbox(
        &mut self,
        kind: OutboxKind,
        node_id: &str,
        message: u32,
    ) -> Result<()> {
        self.outbox_mut(kind)
            .entry(node_id.to_owned())
            .or_default()
            .insert(message);

        Ok(())
    }

    pub(crate) fn track_gossip_batch(
        &mut self,
        msg_id: u32,
        node_id: String,
        messages: HashSet<u32>,
    ) {
        self.in_flight_gossip.insert(msg_id, (node_id, messages));
    }

    pub(crate) fn has_in_flight_gossip_for(&self, node_id: &str) -> bool {
        self.in_flight_gossip
            .values()
            .any(|(peer, _)| peer == node_id)
    }

    pub(crate) fn acknowledge_gossip_batch(&mut self, msg_id: u32) {
        if let Some((node_id, acked_messages)) = self.in_flight_gossip.remove(&msg_id) {
            if let Some(node_outbox) = self.retry_outbox.get_mut(&node_id) {
                for message in acked_messages {
                    node_outbox.remove(&message);
                }
                if node_outbox.is_empty() {
                    self.retry_outbox.remove(&node_id);
                }
            }
        }
    }
}

impl<Data> Default for Node<Data> {
    fn default() -> Self {
        Self {
            id: Default::default(),
            node_ids: Default::default(),
            store: HashSet::new(),
            topology: HashMap::new(),
            retry_outbox: HashMap::new(),
            msg_outbox: HashMap::new(),
            in_flight_gossip: HashMap::new(),
        }
    }
}

impl<Data> NodeTrait for Node<Data>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Hash + Eq,
{
    fn new() -> Self {
        Self {
            id: String::new(),
            node_ids: vec![],
            store: HashSet::new(),
            topology: HashMap::new(),
            retry_outbox: HashMap::new(),
            msg_outbox: HashMap::new(),
            in_flight_gossip: HashMap::new(),
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

    fn handle_echo_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        echo::handle_echo_message(self, msg, tx)
    }

    fn handle_generate_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        unique_id::handle_generate_message(self, msg, tx)
    }

    fn handle_broadcast_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_broadcast_message(self, msg, tx)
    }

    fn handle_read_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_read_message(self, msg, tx)
    }

    fn handle_topology_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_topology_message(self, msg, tx)
    }

    fn get_and_increment_msg_id(&self) -> u32 {
        unique_id::generate_message_id()
    }

    fn handle_sync_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_sync_message(self, msg, tx)
    }

    fn handle_sync_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_sync_ok_message(self, msg, tx)
    }

    fn request_sync_with_random_peers(&mut self) -> Vec<Message> {
        broadcast::request_sync_with_random_peers(self)
    }

    fn handle_broadcast_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_broadcast_ok_message(self, msg, tx)
    }

    fn retry_messages(&mut self, tx: Sender<Message>) -> Result<()> {
        broadcast::retry_messages(self, tx)
    }
    fn fanout_messages(&mut self, tx: Sender<Message>) -> Result<()> {
        broadcast::fanout_messages(self, tx)
    }

    fn handle_gossip_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_gossip_message(self, msg, tx)
    }

    fn handle_gossip_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        broadcast::handle_gossip_ok_message(self, msg, tx)
    }
}
