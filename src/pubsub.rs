use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;
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

    pub fn publish(&self, channel: &str, message: &str) -> Result<usize> {
        let channels = self.channels.read().unwrap();
        match channels.get(channel) {
            Some(sender) => sender.send(message.to_string()).map_err(Into::into),
            None => Ok(0),
        }
    }
}
