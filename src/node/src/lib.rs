use anyhow::Ok;
use anyhow::Result;
use rand::seq::IndexedRandom;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::mpsc::Sender;
use ulid::Ulid;

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
}

impl MessageBody {
    fn msg_id(&self) -> &u32 {
        match self {
            MessageBody::broadcast { message, msg_id } => msg_id,
            MessageBody::broadcast_ok {
                in_reply_to,
                msg_id,
            } => msg_id,
            MessageBody::topology { topology, msg_id } => msg_id,
            MessageBody::topology_ok {
                msg_id,
                in_reply_to,
            } => msg_id,
            MessageBody::read { msg_id } => msg_id,
            MessageBody::read_ok {
                messages,
                in_reply_to,
                msg_id,
            } => msg_id,
            MessageBody::generate { msg_id } => msg_id,
            MessageBody::generate_ok {
                msg_id,
                in_reply_to,
                id,
            } => msg_id,
            MessageBody::echo { msg_id, echo } => msg_id,
            MessageBody::echo_ok {
                msg_id,
                in_reply_to,
                echo,
            } => msg_id,
            MessageBody::init {
                msg_id,
                node_id,
                node_ids,
            } => msg_id,
            MessageBody::init_ok { in_reply_to } => unreachable!(),
            MessageBody::sync { msg_id, messages } => msg_id,
            MessageBody::sync_ok {
                msg_id,
                in_reply_to,
                messages,
            } => msg_id,
        }
    }
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
    fn get_and_increment_msg_id(&mut self) -> u32;
    fn handle_sync_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_sync_ok_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn request_sync_with_random_peers(&mut self) -> Vec<Message>;
    fn retry_messages(&mut self,tx: Sender<Message>) -> Result<()>;
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

            _ => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct Node<Data> {
    pub id: String,
    pub msg_id: u32,
    pub node_ids: Vec<String>,
    pub store: HashSet<Data>,
    pub topology: HashMap<String, Vec<String>>,
    pub outbox: HashMap<String, Vec<Message>>,
}

impl<Data> Node<Data>
where
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32> + Eq + Hash,
    Self: NodeTrait,
{
    fn check_and_push_to_store(&mut self, payload: Data) -> Option<Data> {
        if !self.store.contains(&payload) {
            self.store.insert(payload.clone());
            Some(payload)
        } else {
            None
        }
    }
    fn read(&self) -> Vec<u32> {
        self.store.iter().map(|data| data.clone().into()).collect()
    }
    fn add_to_outbox(&mut self, msg: &Message) -> Result<()> {
        let node_id = msg.src.clone();
        self.outbox.entry(node_id).or_default().push(msg.clone());
        Ok(())
    }
    fn remove_from_outbox(&mut self, node_id: String, msg_id: &u32) -> Result<()> {
        if let Some(node_outbox) = self.outbox.get_mut(&node_id) {
            if let Some(index) = node_outbox.iter().position(|m| m.body.msg_id() == msg_id) {
                node_outbox.swap_remove(index);
            }
        }
        Ok(())
    }
}

impl<Data> Default for Node<Data> {
    fn default() -> Self {
        Self {
            id: Default::default(),
            msg_id: Default::default(),
            node_ids: Default::default(),
            store: HashSet::new(),
            topology: HashMap::new(),
            outbox: HashMap::new(),
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
            msg_id: 0,
            node_ids: vec![],
            store: HashSet::new(),
            topology: HashMap::new(),
            outbox: HashMap::new(),
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
    // fn into_reply(self,mut msg:MessageBody,&mut StdoutLock){

    fn handle_echo_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::echo { msg_id, ref echo } = msg.body {
            let payload = MessageBody::echo_ok {
                msg_id: self.get_and_increment_msg_id(),
                in_reply_to: msg_id,
                echo: echo.clone(),
            };
            let reply = msg.into_reply(payload);
            reply.send(tx)?;
        }
        Ok(())
    }

    fn handle_generate_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::generate { msg_id } = msg.body {
            let unique_id = Ulid::new().to_string();
            let payload = MessageBody::generate_ok {
                msg_id: self.get_and_increment_msg_id(),
                in_reply_to: msg_id,
                id: unique_id,
            };
            let reply = msg.into_reply(payload);
            reply.send(tx)?;
        }
        Ok(())
    }
    fn handle_broadcast_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::broadcast { message, msg_id } = msg.body {
            //Check and see if we have received the message before, only gossip if its a new message
            if let Some(_data) = self.check_and_push_to_store(Data::from(message)) {
                let reply_payload = MessageBody::broadcast_ok {
                    in_reply_to: msg_id,
                    msg_id: self.get_and_increment_msg_id(),
                };
                let reply = msg.clone().into_reply(reply_payload);
                reply.send(tx.clone())?;

                //Send the new message to our neighbours in the topology,
                // and also add them to our outbox so that we can retry later
                let neighbours: Option<&Vec<String>> = self.topology.get(&self.id);
                if let Some(neighbours) = neighbours {
                    let fanout_messages: Vec<Message> = neighbours
                        .iter()
                        .filter(|n| **n != msg.src)
                        .map(|node_id| Message {
                            src: self.id.clone(),
                            dest: node_id.to_owned(),
                            body: MessageBody::broadcast { message, msg_id },
                        })
                        .collect();
                    for msg in fanout_messages {
                        self.add_to_outbox(&msg)?;
                        msg.send(tx.clone())?;
                        
                    }
                }
            };
        }
        Ok(())
    }
    fn handle_read_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::read { msg_id } = msg.body {
            let messages: Vec<u32> = self.read();
            let payload = MessageBody::read_ok {
                messages,
                in_reply_to: msg_id,
                msg_id: self.get_and_increment_msg_id(),
            };
            let reply = msg.into_reply(payload);
            reply.send(tx)?;
        }
        Ok(())
    }
    fn handle_topology_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::topology {
            ref topology,
            msg_id,
        } = msg.body
        {
            self.topology = topology.clone();
            let payload = MessageBody::topology_ok {
                msg_id: self.get_and_increment_msg_id(),
                in_reply_to: msg_id,
            };
            let reply = msg.into_reply(payload);
            reply.send(tx)?;
        }
        Ok(())
    }
    fn get_and_increment_msg_id(&mut self) -> u32 {
        let id = self.msg_id;
        self.msg_id += 1;
        id
    }

    fn handle_sync_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::sync {
            msg_id,
            ref messages,
        } = msg.body
        {
            let messages: HashSet<Data> = messages.into_iter().map(|m| Data::from(*m)).collect();
            let i_have: HashSet<Data> = self.store.difference(&messages).cloned().collect();
            let they_have: HashSet<Data> = messages.difference(&self.store).cloned().collect();

            let i_have: Vec<u32> = i_have.into_iter().map(|m| Data::into(m)).collect();
            //insert the data we dont have
            for data in they_have {
                self.store.insert(data);
            }
            //send back the data they dont have
            let payload = MessageBody::sync_ok {
                msg_id: self.get_and_increment_msg_id(),
                in_reply_to: msg_id,
                messages: i_have,
            };
            let reply = msg.into_reply(payload);
            reply.send(tx)?;
        }
        Ok(())
    }

    fn handle_sync_ok_message(&mut self, msg: Message, _tx: Sender<Message>) -> Result<()> {
        if let MessageBody::sync_ok {
            msg_id: _,
            in_reply_to: _,
            messages,
        } = msg.body
        {
            //We might have received data we didn't have the the syncing node has
            //So we simply insert this new data and dont send any acknowledgement
            for m in messages {
                self.store.insert(Data::from(m));
            }
        }
        Ok(())
    }

    // To combat network partitions, a node calls this function to pick random
    // nodes for their messages,while it sends its own. Once we get theirs we can
    // copy values we dont have, while they can copy values from us
    // This function acts as a initiator for the sync process, piggybacking on
    // maelstroms messaging protocol, by injecting custom message types.
    fn request_sync_with_random_peers(&mut self) -> Vec<Message> {
        let all_nodes: Vec<String> = self.node_ids.clone();
        let mut rng = rand::rng();
        let messages = all_nodes
            .choose_multiple(&mut rng, 2)
            .map(|node| Message {
                src: self.id.clone(),
                dest: node.to_owned(),
                body: MessageBody::sync {
                    msg_id: self.get_and_increment_msg_id(),
                    messages: self.read(),
                },
            })
            .collect();
        messages
    }

    fn handle_broadcast_ok_message(&mut self, msg: Message, _tx: Sender<Message>) -> Result<()> {
        if let MessageBody::broadcast_ok { in_reply_to,.. } = msg.body {
            self.remove_from_outbox(msg.src, &in_reply_to)?
        }      
        Ok(())
    }

    fn retry_messages(&mut self,tx: Sender<Message>) -> Result<()> {
        for node in self.outbox.keys(){
            for msg in self.outbox.get(node).unwrap().iter(){
                msg.clone().send(tx.clone())?
            }
        }
        Ok(())
    }
}
