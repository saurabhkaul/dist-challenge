use anyhow::Context;
use node::{Message, Node, NodeTrait};
use serde_path_to_error::deserialize;
use std::{
    io::{stdin, stdout, BufRead, Lines, Write},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
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
    main_loop(lines, &mut node, &tx, true)?;

    //trigger one final resend
    node.fanout_messages(tx.clone())?;
    node.retry_messages(tx.clone())?;

    let lines = stdin().lock().lines();
    main_loop(lines, &mut node, &tx, false)?;
    let _ = print_thread_handle.join();
    Ok(())
}

fn main_loop(
    stdin: Lines<std::io::StdinLock<'_>>,
    node: &mut Node<u32>,
    tx: &mpsc::Sender<Message>,
    run_periodic: bool,
) -> anyhow::Result<()> {
    let fanout_interval = Duration::from_millis(25);
    let retry_interval = Duration::from_millis(50);
    let mut last_fanout = Instant::now();
    let mut last_retry = Instant::now();

    for (_msg_num, line) in stdin.enumerate() {
        if run_periodic {
            if last_fanout.elapsed() >= fanout_interval {
                node.fanout_messages(tx.clone())?;
                last_fanout = Instant::now();
            }

            if last_retry.elapsed() >= retry_interval {
                node.retry_messages(tx.clone())?;
                last_retry = Instant::now();
            }
        }

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
