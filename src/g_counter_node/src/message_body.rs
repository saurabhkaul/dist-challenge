use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum MessageBody {
    read,
    read_ok { value: u32 },
    add { delta: u32 },
    add_ok,
}
