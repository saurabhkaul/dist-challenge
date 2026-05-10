use serde::{Deserialize, Serialize};

pub type Message = node_common::Message<MessageBody>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum MessageBody {
    read,
    read_ok {},
}
