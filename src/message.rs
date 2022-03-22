use std::collections::HashSet;
use std::fmt::Display;
use std::net::SocketAddr;

use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};

const GOSSIP_LEN: usize = 15;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Gossip([u8; GOSSIP_LEN]);

impl Gossip {
    pub fn random() -> Self {
        let mut gossip = Self([0u8; GOSSIP_LEN]);
        thread_rng().fill(&mut gossip.0);
        gossip
    }
}

impl Display for Gossip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", base64::encode(self.0)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum Payload {
    Hi,
    RequestPeers,
    Gossip(Gossip),
    Peers(HashSet<SocketAddr>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Message {
    pub cannonical_addr: SocketAddr,
    pub payload: Payload,
}
