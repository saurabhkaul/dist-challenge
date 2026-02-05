use anyhow::Context;
use node::{Node, NodeTrait,Message};
use serde_path_to_error::deserialize;
use std::{io::{BufRead, stdin, stdout,Write}, sync::mpsc, thread};


fn main() -> anyhow::Result<()> {
    let stdin = stdin().lock().lines();
    let mut node: Node<u32,u32> = Node::default();
    let (tx,rx) = mpsc::channel::<Message>();
    
    let print_thread_handle = thread::spawn(move ||{
        let mut stdout = stdout().lock();
        while let Ok(message) = rx.recv(){
            eprintln!("Sending: src={}, dest={}", message.src, message.dest);
            let _ = serde_json::to_writer(&mut stdout, &message).context("serializing response");
            let _ = stdout.write_all(b"\n").context("write trailing newline").context("Couldn't write to stdout");
        }
    });

    
    for line in stdin {
        let input = match line {
            Ok(l) => {
                eprintln!("Received line: '{}'", l);
                l
            }
            Err(e) => panic!("{e}"),
        };
        let deser = &mut serde_json::Deserializer::from_str(&input);
        let result = deserialize(deser);
        let input = match result {
            Ok(msg) => {
                msg
            }
            Err(e) => {
                eprintln!("Deserialization failed: {}", e);
                return Err(e).context("Failed to deserialize STDIN input from Maelstrom");
            }
        };
        match node.next(input,tx.clone()) {
            Ok(_) => eprintln!("Message handled successfully"),
            Err(e) => eprintln!("Failed to handle message: {}", e),
        }
    }
    let _ = print_thread_handle.join();
    Ok(())
}
