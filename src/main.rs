use anyhow::Context;
use echo::{EchoNode, Message, Node};
use serde_path_to_error::deserialize;
use std::fs::File;
use std::io::stdout;
use std::io::{stdin, Read, Write};

fn main() -> anyhow::Result<()> {
    let stdin = stdin().lines();
    let stdout = stdout().lock();
    // let mut file = File::create("maelstorm_logs.txt")?;
    // Write a string to the file
    // file.write_all(b"Logs Start")?;

    // let inputs = serde_json::Deserializer::from_reader(stdin).into_iter::<Message>();

    let mut output = serde_json::Serializer::new(stdout);
    let mut echo_node = EchoNode { id: 0 };

    for line in stdin {
        // let input = lines.context("Couldn't deserialize stdin input to string").unwrap();
        let input = match line {
            Ok(l) => l,
            Err(e) => panic!("{e}"),
        };
        let deser = &mut serde_json::Deserializer::from_str(&input);
        let input = deserialize(deser);
        let input = input.context("Failed to deserialize STDIN input from Maelstrom")?;
        let _ = echo_node
            .handle_message(input, &mut output)
            .context("Failed to serialise out to STDOUT ");
    }
    Ok(())
}
