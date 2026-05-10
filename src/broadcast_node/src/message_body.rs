use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
