use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::Sender;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Message<Body> {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

impl<Body> Message<Body> {
    pub fn send(self, tx: Sender<Self>) -> Result<()>
    where
        Body: Send + Sync + 'static,
    {
        tx.send(self)?;
        Ok(())
    }

    pub fn into_reply(self, payload: Body) -> Self {
        Self {
            src: self.dest,
            dest: self.src,
            body: payload,
        }
    }

    pub fn into_message(self, payload: Body, new_dest: &str) -> Self {
        Self {
            src: self.dest,
            dest: new_dest.to_owned(),
            body: payload,
        }
    }
}

pub trait NodeTrait {
    type Message;

    fn new() -> Self;
    fn handle_init_message(&mut self, msg: Self::Message, tx: Sender<Self::Message>) -> Result<()>;
    fn handle_gossip_message(
        &mut self,
        msg: Self::Message,
        tx: Sender<Self::Message>,
    ) -> Result<()>;
    fn handle_gossip_ok_message(
        &mut self,
        msg: Self::Message,
        tx: Sender<Self::Message>,
    ) -> Result<()>;
}
