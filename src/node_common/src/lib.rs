use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::time::Duration;
use std::{collections::HashMap, sync::mpsc::Sender, time::Instant};

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
    fn get_and_increment_msg_id(&self) -> u32;
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


//State keeper for tracking the messages a node has to gossip out
// with checks for messages we are currently waiting
#[derive(Debug, Clone)]
pub struct Outbox<
    NodeId,
    //ItemId here is identifying one unit of logical work. For retries and something else, where the same unit of logical work is coming in, the id needs to be the same.
    //Similarly, every unique unit of logical work should have its own unique ItemId. We leave it to the caller to decide this.
    ItemId,
    Payload, 
    MessageId> {
    queued: HashMap<NodeId, HashMap<ItemId, Payload>>,
    in_flight: HashMap<MessageId, InFlightBatch<NodeId, ItemId>>,
}

impl<P, I, V, M> Default for Outbox<P, I, V, M>
where
    P: Clone + Eq + Hash,
    I: Clone + Eq + Hash,
    V: Clone,
    M: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self {
            queued: Default::default(),
            in_flight: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InFlightBatch<NodeId, ItemId> {
    peer: NodeId,
    item_ids: Vec<ItemId>,
    sent_at: Instant,
}

pub struct OutgoingBatch<NodeId, ItemId, Payload, MessageId> {
    pub message_id: MessageId,
    pub peer: NodeId,
    pub items: Vec<(ItemId, Payload)>,
}

impl<P, I, V, M> Outbox<P, I, V, M>
where
    P: Clone + Eq + Hash,
    I: Clone + Eq + Hash,
    V: Clone,
    M: Clone + Eq + Hash,
{
    pub fn enqueue(&mut self, peer: P, item_id: I, payload: V) {
        self.queued
            .entry(peer)
            .or_default()
            .entry(item_id)
            .or_insert(payload);
    }

    /// Expire timed-out attempts and reserve batches ready to send.
    /// The caller must send each batch or release it with `mark_send_failed`.
    pub fn poll(
        &mut self,
        now: Instant,
        timeout: Duration,
        mut next_message_id: impl FnMut() -> M,
    ) -> Vec<OutgoingBatch<P, I, V, M>> {
        self.in_flight
            .retain(|_, batch| now.saturating_duration_since(batch.sent_at) < timeout);

        let eligible_peers: Vec<P> = self
            .queued
            .iter()
            .filter(|(_, items)| !items.is_empty())
            .filter(|(peer, _)| !self.has_in_flight_for(peer))
            .map(|(peer, _)| peer.clone())
            .collect();

        eligible_peers
            .into_iter()
            .filter_map(|peer| {
                let items: Vec<(I, V)> = self
                    .queued
                    .get(&peer)?
                    .iter()
                    .map(|(id, payload)| (id.clone(), payload.clone()))
                    .collect();

                let message_id = next_message_id();
                self.in_flight.insert(
                    message_id.clone(),
                    InFlightBatch {
                        peer: peer.clone(),
                        item_ids: items.iter().map(|(id, _)| id.clone()).collect(),
                        sent_at: now,
                    },
                );
                Some(OutgoingBatch {
                    message_id,
                    peer,
                    items,
                })
            })
            .collect()
    }

    pub fn acknowledge(&mut self, message_id: &M) -> bool {
        let Some(batch) = self.in_flight.remove(message_id) else {
            return false;
        };
        if let Some(peer_queue) = self.queued.get_mut(&batch.peer) {
            for item_id in batch.item_ids {
                peer_queue.remove(&item_id);
            }

            if peer_queue.is_empty() {
                self.queued.remove(&batch.peer);
            }
        }
        true
    }

    pub fn mark_send_failed(&mut self, message_id: &M) -> bool {
        // Items remain queued, so removing the reservation makes them
        // immediately eligible for another send.
        self.in_flight.remove(message_id).is_some()
    }

    fn has_in_flight_for(&self, peer: &P) -> bool {
        self.in_flight.values().any(|batch| &batch.peer == peer)
    }
}

#[cfg(test)]
mod tests {
    use super::Outbox;
    use std::time::{Duration, Instant};

    const TIMEOUT: Duration = Duration::from_millis(300);

    type TestOutbox = Outbox<&'static str, u32, &'static str, u32>;

    #[test]
    fn enqueue_deduplicates_items_by_peer_and_item_id() {
        let mut outbox = TestOutbox::default();
        outbox.enqueue("n2", 1, "first");
        outbox.enqueue("n2", 1, "replacement");

        let batches = outbox.poll(Instant::now(), TIMEOUT, || 10);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].items, vec![(1, "first")]);
    }

    #[test]
    fn poll_allows_only_one_in_flight_batch_per_peer() {
        let mut outbox = TestOutbox::default();
        outbox.enqueue("n2", 1, "one");

        assert_eq!(outbox.poll(Instant::now(), TIMEOUT, || 10).len(), 1);
        outbox.enqueue("n2", 2, "two");
        assert!(outbox.poll(Instant::now(), TIMEOUT, || 11).is_empty());
    }

    #[test]
    fn acknowledge_removes_only_items_in_the_acknowledged_batch() {
        let mut outbox = TestOutbox::default();
        outbox.enqueue("n2", 1, "one");
        outbox.poll(Instant::now(), TIMEOUT, || 10);
        outbox.enqueue("n2", 2, "two");

        assert!(outbox.acknowledge(&10));
        let batches = outbox.poll(Instant::now(), TIMEOUT, || 11);

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].items, vec![(2, "two")]);
    }

    #[test]
    fn acknowledge_unknown_message_id_does_nothing() {
        let mut outbox = TestOutbox::default();
        outbox.enqueue("n2", 1, "one");

        assert!(!outbox.acknowledge(&99));
        assert_eq!(outbox.poll(Instant::now(), TIMEOUT, || 10).len(), 1);
    }

    #[test]
    fn expired_batch_becomes_eligible_for_retry() {
        let mut outbox = TestOutbox::default();
        let sent_at = Instant::now();
        outbox.enqueue("n2", 1, "one");
        outbox.poll(sent_at, TIMEOUT, || 10);

        assert!(outbox
            .poll(sent_at + Duration::from_millis(299), TIMEOUT, || 11)
            .is_empty());
        let retried = outbox.poll(sent_at + TIMEOUT, TIMEOUT, || 11);
        // A late acknowledgement must not remove the new attempt's items.
        assert!(!outbox.acknowledge(&10));
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].message_id, 11);
        assert_eq!(retried[0].items, vec![(1, "one")]);
    }

    #[test]
    fn send_failure_releases_batch_for_retry() {
        let mut outbox = TestOutbox::default();
        outbox.enqueue("n2", 1, "one");
        outbox.poll(Instant::now(), TIMEOUT, || 10);

        assert!(outbox.mark_send_failed(&10));
        let retried = outbox.poll(Instant::now(), TIMEOUT, || 11);
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].message_id, 11);
    }
}
