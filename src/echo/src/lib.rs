use anyhow::{Context, Result};
use serde::Serialize as S;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::Serializer;
use std::io::StdoutLock;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

// impl Message {
//     pub fn reply(&mut self)->Message{
//         Message { src: self.dest, dest: self.src, body:  }
//     }
// }

// #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
// #[serde(untagged)]
// struct Body {
//     r#type: String,
//     msg_id: Option<usize>,
//     in_reply_to: Option<usize>,
//     #[serde(flatten)]
//     payload: Option<Payload>,
// }
//
//
// #[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
// // #[serde(tag = "type")]
// #[serde(rename_all = "snake_case")]
// enum Payload {
//     Echo { echo: String },
//     EchoOk { echo: String },
//     Init {
//         node_id: String,
//         node_ids: Vec<String>,
//     },
//
// }

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Body {
    #[serde(rename_all = "snake_case")]
    echo { msg_id: Option<usize>, echo: String },
    #[serde(rename_all = "snake_case")]
    echo_ok {
        msg_id: Option<usize>,
        in_reply_to: Option<usize>,
        echo: String,
    },
    #[serde(rename_all = "snake_case")]
    init {
        msg_id: Option<usize>,
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename_all = "snake_case")]
    init_ok {
        msg_id: Option<usize>,
        in_reply_to: Option<usize>,
    },
}

pub trait Node {
    fn handle_message(
        &mut self,
        msg: Message,
        output: &mut Serializer<StdoutLock>,
    ) -> anyhow::Result<()>;
}

#[derive(Clone, Copy)]
pub struct EchoNode {
    pub id: usize,
}

impl Node for EchoNode {
    fn handle_message(&mut self, msg: Message, output: &mut Serializer<StdoutLock>) -> Result<()> {
        match msg.body {
            Body::echo { msg_id, echo } => {
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body::echo_ok {
                        msg_id: Some(self.id),
                        in_reply_to: msg_id,
                        echo,
                    },
                };
                reply.serialize(output)?;
            }
            Body::echo_ok { .. } => unreachable!("We hit EchoOk"),
            Body::init {
                msg_id,
                node_id,
                node_ids,
            } => {
                self.id = str::parse::<usize>(&node_id)
                    .context("Failed to parse Maelstrom init node id string into usize")?;
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body::init_ok {
                        msg_id: None,
                        in_reply_to: msg_id,
                    },
                };
                reply.serialize(output)?;
            }
            Body::init_ok { .. } => unreachable!("We hit InitOk"),
        }
        Ok(())
    }
}

/*
match msg.body.payload {
            Some(Payload::Echo { echo }) => {
                let reply = Message {
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        r#type: "echo_ok".to_string(),
                        msg_id: Some(self.id),
                        in_reply_to: msg.body.msg_id,
                        payload: Some(Payload::EchoOk { echo }),
                    },
                };
                reply.serialize(output)?;
            }
            Some(Payload::EchoOk { .. }) => unreachable!(),

           Some(Payload::Init {node_id, node_ids }) =>{
                self.id = str::parse::<usize>(&node_id).context("Failed to parse Maelstrom init node id string into usize")?;
                let reply = Message{
                    src: msg.dest,
                    dest: msg.src,
                    body: Body {
                        r#type: "init_ok".to_string(),
                        msg_id: None,
                        in_reply_to: msg.body.msg_id,
                        payload:None
                    },
                };
                reply.serialize(output)?;
            },
            None => unreachable!(),
        }


*/
