use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use anyhow::Result;
use dashmap::DashMap;
use resp::Value;
use strum::Display;
use tokio::sync::mpsc;

use super::server::Connection;

const BROADCAST_CHANNEL_SIZE: usize = 16;

#[derive(Clone, PartialEq, Display)]
pub enum ReplicaType {
    #[strum(serialize = "master")]
    Leader,
    #[strum(serialize = "slave")]
    Follower,
}

#[derive(Default)]
pub struct ReplicationHub {
    next_id: AtomicUsize,
    senders: DashMap<usize, mpsc::Sender<Value>>,
    state: Arc<ReplicationState>,
}

impl ReplicationHub {
    pub fn publish(&self, msg: Value) {
        self.senders
            .retain(|_, sender| sender.try_send(msg.clone()).is_ok());
    }

    pub fn has_replicas(&self) -> bool {
        !self.senders.is_empty()
    }

    pub fn num_replicas(&self) -> usize {
        self.senders.len()
    }

    pub fn get_num_ack(&self) -> usize {
        self.state.get_num_ack()
    }
    pub fn get_num_commands(&self) -> usize {
        self.state.get_num_commands()
    }
    pub fn incr_num_commands(&self) {
        self.state.incr_num_commands()
    }
    pub fn reset(&self) {
        self.state.reset()
    }

    pub async fn run_replica(&self, conn: &mut Connection) -> Result<()> {
        let (id, mut rx) = self.register();
        loop {
            tokio::select! {
                maybe_msg = rx.recv() => {
                    if let Some(msg) = maybe_msg {
                        conn.write_value(&msg).await?
                    } else {
                        break;
                    }
                }
                ack = conn.read_value() => {
                    if let Err(e) = ack {
                        eprintln!("Error reading ACK: {e}");
                        break;
                    } else {
                        self.state.incr_num_ack();
                    }
                }
            }
        }
        self.unregister(id);
        Ok(())
    }

    fn register(&self) -> (usize, mpsc::Receiver<Value>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(BROADCAST_CHANNEL_SIZE);
        self.senders.insert(id, tx);
        (id, rx)
    }

    fn unregister(&self, id: usize) {
        self.senders.remove(&id);
    }
}

#[derive(Default)]
struct ReplicationState {
    num_ack: AtomicUsize,
    num_commands: AtomicUsize,
}

impl ReplicationState {
    fn get_num_ack(&self) -> usize {
        self.num_ack.load(Ordering::Relaxed)
    }

    fn get_num_commands(&self) -> usize {
        self.num_commands.load(Ordering::Relaxed)
    }

    fn incr_num_ack(&self) {
        self.num_ack.fetch_add(1, Ordering::Relaxed);
    }

    fn incr_num_commands(&self) {
        self.num_commands.fetch_add(1, Ordering::Relaxed);
    }

    fn reset(&self) {
        self.num_ack.store(0, Ordering::Relaxed);
        self.num_commands.store(0, Ordering::Relaxed);
    }
}
