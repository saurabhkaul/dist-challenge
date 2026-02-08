use anyhow::Context;
use node::{Message, Node, NodeTrait};
use serde_path_to_error::deserialize;
use std::{
    io::{stdin, stdout, BufRead, Lines, Write},
    sync::mpsc,
    thread,
};

fn main() -> anyhow::Result<()> {
    let mut node: Node<u32> = Node::default();
    let (tx, rx) = mpsc::channel::<Message>();

    let print_thread_handle = thread::spawn(move || {
        let mut stdout = stdout().lock();
        while let Ok(message) = rx.recv() {
            eprintln!("Sending: src={}, dest={}", message.src, message.dest);
            let _ = serde_json::to_writer(&mut stdout, &message).context("serializing response");
            let _ = stdout
                .write_all(b"\n")
                .context("write trailing newline")
                .context("Couldn't write to stdout");
        }
    });
    let lines = stdin().lock().lines();
    main_loop(lines, &mut node, &tx, |node, tx| {
        node.retry_messages(tx.clone())
    })?;

    //trigger one final resend
    node.retry_messages(tx.clone())?;

    let lines = stdin().lock().lines();
    main_loop(lines, &mut node, &tx, |_node, _tx| Ok(()))?;
    let _ = print_thread_handle.join();
    Ok(())
}

fn main_loop(
    stdin: Lines<std::io::StdinLock<'_>>,
    node: &mut Node<u32>,
    tx: &mpsc::Sender<Message>,
    periodic: impl Fn(&mut Node<u32>, &mpsc::Sender<Message>) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    for (msg_num, line) in stdin.enumerate() {
        // Trigger every 50th iteration
        if (msg_num + 1) % 50 == 0 {
            periodic(node, tx)?;
        };
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
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("Deserialization failed: {}", e);
                return Err(e).context("Failed to deserialize STDIN input from Maelstrom");
            }
        };
        match node.next(input, tx.clone()) {
            Ok(_) => eprintln!("Message handled successfully"),
            Err(e) => eprintln!("Failed to handle message: {}", e),
        }
    }
    Ok(())
}
