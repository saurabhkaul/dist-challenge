use anyhow::Ok;
use anyhow::{Context, Result};
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::collections::HashMap;
use std::io::{StdoutLock, Write};
use ulid::Ulid;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: MessageBody,
}

impl Message {
    pub fn send(&self, output: &mut impl Write) -> Result<(), anyhow::Error> {
        eprintln!("Sending: src={}, dest={}", self.src, self.dest);
        serde_json::to_writer(&mut *output, self).context("serializing response")?;
        output.write_all(b"\n").context("write trailing newline")?;
        Ok(())
    }
    pub fn into_reply(self, payload: MessageBody) -> Message {
        Message {
            src: self.dest,
            dest: self.src,
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
    fn handle_init_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()>;
    fn handle_echo_message(&mut self, msg: Message, output: &mut StdoutLock) -> anyhow::Result<()>;
    fn handle_generate_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()>;
    fn handle_broadcast_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()>;
    fn handle_read_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()>;
    fn handle_topology_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()>;
    fn get_and_increment_msg_id(&mut self) -> u32;
    fn next(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        match msg.body {
            MessageBody::echo { .. } => self.handle_echo_message(msg, output),
            MessageBody::init { .. } => self.handle_init_message(msg, output),
            MessageBody::generate { .. } => self.handle_generate_message(msg, output),
            MessageBody::broadcast { .. } => self.handle_broadcast_message(msg, output),
            MessageBody::topology { .. } => self.handle_topology_message(msg, output),
            MessageBody::read { .. } => self.handle_read_message(msg,output),
            _ => unreachable!()
        }
    }
}



#[derive(Clone)]
pub struct Node<Data> {
    pub id: String,
    pub msg_id: u32,
    pub node_ids: Vec<String>,
    pub store:Vec<Data>,
    pub topology : HashMap<String,Vec<String>>
}

impl<Data> Default for Node<Data>{
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

impl <Data> NodeTrait for Node<Data>
where 
    Data:From<u32> + Clone + Into<u32>
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
    // }
    fn handle_init_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
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
            reply.send(output)?;
        }
        Ok(())
    }
    // fn into_reply(self,mut msg:MessageBody,&mut StdoutLock){

    fn handle_echo_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let MessageBody::echo { msg_id, ref echo } = msg.body {
            let payload = MessageBody::echo_ok {
                msg_id: self.get_and_increment_msg_id(),
                in_reply_to: msg_id,
                echo: echo.clone(),
            };
            let reply = msg.into_reply(payload);
            reply.send(output)?;
        }
        Ok(())
    }

    fn handle_generate_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let MessageBody::generate { msg_id } = msg.body {
            let unique_id = Ulid::new().to_string();
            let payload = MessageBody::generate_ok {
                msg_id: self.get_and_increment_msg_id(),
                in_reply_to: msg_id,
                id: unique_id,
            };
            let reply = msg.into_reply(payload);
            reply.send(output)?;
        }
        Ok(())
    }
    fn handle_broadcast_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let MessageBody::broadcast { message, msg_id } = msg.body{
            self.store.push(Data::from(message));
            let payload = MessageBody::broadcast_ok { in_reply_to: msg_id, msg_id:self.get_and_increment_msg_id()};
            let reply = msg.into_reply(payload);
            reply.send(output)?;
        }
        
        Ok(())
    }
    fn handle_read_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let MessageBody::read { msg_id } = msg.body{
            let messages:Vec<u32> = self.store.clone().into_iter().map(Into::into).collect();
            let payload = MessageBody::read_ok { messages, in_reply_to: msg_id,msg_id:self.get_and_increment_msg_id() };
            let reply = msg.into_reply(payload);
            reply.send(output)?;
        }
        Ok(())
    }
    fn handle_topology_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let MessageBody::topology { ref topology, msg_id } = msg.body{
            self.topology = topology.clone();
            let payload = MessageBody::topology_ok { msg_id: self.get_and_increment_msg_id(), in_reply_to: msg_id};
            let reply = msg.into_reply(payload);
            reply.send(output)?;
        }
        Ok(())
    }

    fn get_and_increment_msg_id(&mut self) -> u32 {
        let id = self.msg_id;
        self.msg_id += 1;
        id
    }
 
}
