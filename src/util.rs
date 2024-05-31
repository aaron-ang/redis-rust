use std::fmt;

#[derive(Clone, Copy, PartialEq)]
pub enum ReplicaType {
    Leader,
    Follower,
}

impl fmt::Display for ReplicaType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ReplicaType::Leader => write!(f, "master"),
            ReplicaType::Follower => write!(f, "slave"),
        }
    }
}

pub struct ReplicationState {
    num_ack: usize,
    prev_client_cmd: String,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            num_ack: 0,
            prev_client_cmd: String::new(),
        }
    }

    pub fn get_num_ack(&self) -> usize {
        self.num_ack
    }

    pub fn incr_num_ack(&mut self) {
        self.num_ack += 1;
    }

    pub fn reset_num_ack(&mut self) {
        self.num_ack = 0;
    }

    pub fn get_prev_client_cmd(&self) -> String {
        self.prev_client_cmd.clone()
    }

    pub fn set_prev_client_cmd(&mut self, cmd: String) {
        self.prev_client_cmd = cmd.to_lowercase();
    }
}
