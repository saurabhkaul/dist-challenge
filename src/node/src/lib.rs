use anyhow::Ok;
use anyhow::{Result};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::HashMap;
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
    pub fn into_message(self, payload: MessageBody,new_dest:&str)  -> Message {
        Message{
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
    topology{
        topology:HashMap<String,Vec<String>>,
        msg_id: u32,
    },
    topology_ok{
        msg_id: u32,
        in_reply_to: u32,
        
    },
    read{
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
   
}

pub trait NodeTrait {
    fn new() -> Self;
    fn handle_init_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_echo_message(&mut self, msg: Message, tx: Sender<Message>) -> anyhow::Result<()>;
    fn handle_generate_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_broadcast_message(&mut self, msg: Message,tx: Sender<Message>) -> Result<()>;
    fn handle_read_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn handle_topology_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()>;
    fn get_and_increment_msg_id(&mut self) -> u32;
    fn next(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        match msg.body {
            MessageBody::echo { .. } => self.handle_echo_message(msg, tx),
            MessageBody::init { .. } => self.handle_init_message(msg, tx),
            MessageBody::generate { .. } => self.handle_generate_message(msg, tx),
            MessageBody::broadcast { .. } => self.handle_broadcast_message(msg, tx),
            MessageBody::topology { .. } => self.handle_topology_message(msg, tx),
            MessageBody::read { .. } => self.handle_read_message(msg,tx),
            MessageBody::broadcast_ok { .. } => {
                //Do nothing, we know our fanout message was successful
                Ok(())
            }
            _ => unreachable!()
        }
    }
}



#[derive(Clone,PartialEq,Eq)]
pub struct Node<MsgId,Data> {
    pub id: String,
    pub msg_id: u32,
    pub node_ids: Vec<String>,
    pub store:Vec<(MsgId,Data)>,
    pub topology : HashMap<String,Vec<String>>
}

impl<MsgId,Data> Node<MsgId,Data> 
where
    MsgId: PartialEq,
    Data: PartialEq + Clone + Copy + From<u32> + Into<u32>,
{
    fn check_and_push_to_store(&mut self,payload:(MsgId,Data)){
        if !self.store.contains(&payload){
            self.store.push(payload);
        }
    }
    fn read(&self)->Vec<u32>{
        self.store.iter().map(|(_id,data)|data.clone().into()).collect()
    }
}

impl<MsgId,Data> Default for Node<MsgId,Data>{
    fn default() -> Self {
        Self {
            id: Default::default(),
            msg_id: Default::default(),
            node_ids: Default::default(),
            store: Vec::new(),
            topology: HashMap::new(),
        }
    }
}

// impl<MsgId: PartialEq, Data: PartialEq> PartialEq for Node<MsgId, Data> {
//     fn eq(&self, other: &Self) -> bool {
//         self.first == other.first && self.second == other.second
//     }
// }

impl <MsgId,Data> NodeTrait for Node<MsgId,Data>
where 
    Data:PartialEq + Clone + Copy + From<u32> + Into<u32>,
    MsgId:From<u32> + Clone + Into<u32> + PartialEq
{
    fn new() -> Self {
        Self {
            id: String::new(),
            msg_id: 0,
            node_ids: vec![],
            store: Vec::new(),
            topology: HashMap::new(),
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
        if let MessageBody::broadcast { message, msg_id } = msg.body{
            self.check_and_push_to_store((MsgId::from(msg_id),Data::from(message)));
            let reply_payload = MessageBody::broadcast_ok { in_reply_to: msg_id, msg_id:self.get_and_increment_msg_id()};
            let reply = msg.into_reply(reply_payload);
            reply.send(tx.clone())?;
            
            
            let neighbours:Option<&Vec<String>> = self.topology.get(&self.id);
            if let Some(neighbours) = neighbours{
                let fanout_messages:Vec<Message> = neighbours.iter().map(|node_id|{
                    Message{
                        src: self.id.clone(),
                        dest: node_id.to_owned(),
                        body: MessageBody::broadcast { message, msg_id },
                    }
                }).collect();
                for msg in fanout_messages{
                    msg.send(tx.clone())?;
                }
            }
            
        }
        Ok(())
    }
    fn handle_read_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::read { msg_id } = msg.body{
            let messages:Vec<u32> = self.read();
            let payload = MessageBody::read_ok { messages, in_reply_to: msg_id,msg_id:self.get_and_increment_msg_id() };
            let reply = msg.into_reply(payload);
            reply.send(tx)?;
        }
        Ok(())
    }
    fn handle_topology_message(&mut self, msg: Message, tx: Sender<Message>) -> Result<()> {
        if let MessageBody::topology { ref topology, msg_id } = msg.body{
            self.topology = topology.clone();
            let payload = MessageBody::topology_ok { msg_id: self.get_and_increment_msg_id(), in_reply_to: msg_id};
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
 
}
