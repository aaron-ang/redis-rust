use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use anyhow::Result;
use resp::Value;
use strum::Display;
use tokio::{sync::mpsc, time::Duration};

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
    senders: Mutex<HashMap<usize, mpsc::Sender<Value>>>,
    state: Arc<ReplicationState>,
}

impl ReplicationHub {
    pub fn publish(&self, msg: Value) {
        let mut to_remove = Vec::new();
        let mut senders = self.senders.lock().unwrap();

        for (&id, sender) in senders.iter() {
            if sender.try_send(msg.clone()).is_err() {
                to_remove.push(id);
            }
        }

        for id in to_remove {
            senders.remove(&id);
        }
    }

    pub fn num_replicas(&self) -> usize {
        self.senders.lock().unwrap().len()
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

    pub async fn run_replica(self: &Arc<Self>, conn: &mut Connection) -> Result<()> {
        let (id, mut rx) = self.register();
        loop {
            tokio::select! {
                maybe_msg = rx.recv() => {
                    match maybe_msg {
                        Some(msg) => {
                            conn.write_value(&msg).await?;
                        }
                        None => break,
                    }
                }
                ack = conn.read_value_with_timeout(Duration::from_millis(100)) => {
                    match ack {
                        Ok(Some(_)) => {
                            self.state.incr_num_ack()
                        }
                        Ok(None) => { /* timeout */ }
                        Err(e) => {
                            eprintln!("Error reading ACK: {e}");
                            break;
                        }
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
        self.senders.lock().unwrap().insert(id, tx);
        (id, rx)
    }

    fn unregister(&self, id: usize) {
        self.senders.lock().unwrap().remove(&id);
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
