use std::fmt;

#[derive(Clone, Copy)]
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
