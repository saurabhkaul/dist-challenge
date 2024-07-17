use serde_derive::Deserialize;
use serde_derive::Serialize;
use anyhow::{Result,Error};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Message <Body>{
    pub src: String,
    pub dest: String,
    pub body: Body,
}


trait Node{
    type Item;
    async fn handle(msg:Message<Self::Item>) ->Result<Message<Self::Item>,Error>;
}

pub struct EchoNode{
    id:String
}

impl<Body> Node<Body> for EchoNode{
    async fn handle(msg: Message<Body>) -> Result<(), ()> {
        let 
        let return_message = Message { src: msg.src, dest: msg.body, body: val }
    }
}




#[derive(Clone, Debug, PartialEq)]
enum RequestMessageType{
    Echo,
}
impl RequestMessageType{
    fn as_str(&self) -> &'static str {
        match self {
            RequestMessageType::Echo => "echo",

        }
    }
}
enum ResponseMessageType{
    EchoOk,
}

impl ResponseMessageType{
    fn as_str(&self) -> &'static str {
        match self {
            ResponseMessageType::EchoOk => "echo_ok",

        }
    }
}

