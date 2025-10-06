use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use tokio::sync::broadcast;

const CHANNEL_CAPACITY: usize = 128;

#[derive(Debug, Default)]
pub struct PubSub {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<String>>>>,
}

impl PubSub {
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<String> {
        let mut channels = self.channels.write().unwrap();
        channels
            .entry(channel.to_string())
            .or_insert_with(|| broadcast::channel(CHANNEL_CAPACITY).0)
            .subscribe()
    }

    pub fn publish(&self, channel: &str, message: &str) -> usize {
        let Some(sender) = self.channels.read().unwrap().get(channel).cloned() else {
            return 0;
        };
        if sender.receiver_count() == 0 {
            self.channels.write().unwrap().remove(channel);
            return 0;
        }
        sender.send(message.to_string()).unwrap_or(0)
    }
}
