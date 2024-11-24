use strum::{Display, EnumString};

#[derive(Clone, Copy, PartialEq, Display, EnumString)]
pub enum Command {
    PING,
    ECHO,
    SET,
    GET,
    INFO,
    REPLCONF,
    PSYNC,
    WAIT,
    CONFIG,
}

#[derive(Clone, Copy, PartialEq, Display)]
pub enum ReplicaType {
    #[strum(serialize = "master")]
    Leader,
    #[strum(serialize = "slave")]
    Follower,
}

pub struct ReplicationState {
    num_ack: usize,
    prev_client_cmd: Option<Command>,
}

impl ReplicationState {
    pub fn new() -> Self {
        Self {
            num_ack: 0,
            prev_client_cmd: None,
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

    pub fn get_prev_client_cmd(&self) -> Option<Command> {
        self.prev_client_cmd
    }

    pub fn set_prev_client_cmd(&mut self, cmd: Option<Command>) {
        self.prev_client_cmd = cmd;
    }
}
