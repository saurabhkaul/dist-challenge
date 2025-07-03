use anyhow::Context;
use echo::{EchoNode, Node};
use serde_path_to_error::deserialize;
use std::io::{stdin, stdout, BufRead};

fn main() -> anyhow::Result<()> {
    let stdin = stdin().lock().lines();
    let mut stdout = stdout().lock();

    let mut echo_node = EchoNode { id: String::new() };

    // eprintln!("Waiting for input...");
    for line in stdin {
        let input = match line {
            Ok(l) => {
                eprintln!("Received line: '{}'", l);
                l
            }
            Err(e) => panic!("{e}"),
        };
        // eprintln!("Attempting to deserialize: {}", input);
        let deser = &mut serde_json::Deserializer::from_str(&input);
        let result = deserialize(deser);
        let input = match result {
            Ok(msg) => {
                // eprintln!("Successfully deserialized message: {:?}", msg);
                msg
            }
            Err(e) => {
                eprintln!("Deserialization failed: {}", e);
                return Err(e).context("Failed to deserialize STDIN input from Maelstrom");
            }
        };
        match echo_node.handle_any_message(input, &mut stdout) {
            Ok(_) => eprintln!("Message handled successfully"),
            Err(e) => eprintln!("Failed to handle message: {}", e),
        }
    }

    Ok(())
}
