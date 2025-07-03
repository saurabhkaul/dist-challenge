use anyhow::{Context, Result};
use serde::Serialize as S;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use std::io::{StdoutLock, Write};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

impl Message {
    pub fn send(&self, output: &mut impl Write) -> Result<(), anyhow::Error> {
        serde_json::to_writer(&mut *output, self).context("serializing response")?;
        output.write_all(b"\n").context("write trailing newline")?;
        Ok(())
    }
    pub fn into_reply(self, payload: Body) -> Message {
        Message {
            src: self.src,
            dest: self.dest,
            body: payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Body {
    #[serde(rename_all = "snake_case")]
    echo { msg_id: usize, echo: String },
    #[serde(rename_all = "snake_case")]
    echo_ok {
        msg_id: usize,
        in_reply_to: usize,
        echo: String,
    },
    #[serde(rename_all = "snake_case")]
    init {
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename_all = "snake_case")]
    init_ok { in_reply_to: usize },
}

pub trait Node {
    fn new() -> Self;
    fn handle_init_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()>;
    fn handle_echo_message(&mut self, msg: Message, output: &mut StdoutLock) -> anyhow::Result<()>;
    fn handle_any_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        match msg.body {
            Body::echo { .. } => self.handle_echo_message(msg, output),
            Body::echo_ok { .. } => unreachable!(),
            Body::init { .. } => self.handle_init_message(msg, output),
            Body::init_ok { .. } => unreachable!(),
        }
    }
}

#[derive(Clone)]
pub struct EchoNode {
    pub id: String,
}

impl Node for EchoNode {
    fn new() -> Self {
        Self { id: String::new() }
    }

    fn handle_init_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let Body::init {
            msg_id,
            node_ids,
            node_id,
        } = msg.body
        {
            self.id = node_id.clone();
            let reply = Message {
                src: node_id,
                dest: msg.src,
                body: Body::init_ok {
                    in_reply_to: msg_id,
                },
            };
            reply.send(output)?;
        }
        Ok(())
    }

    fn handle_echo_message(&mut self, msg: Message, output: &mut StdoutLock) -> Result<()> {
        if let Body::echo { msg_id, echo } = msg.body {
            let reply = Message {
                src: self.id.clone(),
                dest: msg.src,
                body: Body::echo_ok {
                    msg_id,
                    in_reply_to: msg_id,
                    echo,
                },
            };
            reply.send(output)?;
        }
        Ok(())
    }
}
