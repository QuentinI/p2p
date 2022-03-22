use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::handler::{Error, Handler};
use crate::{Gossip, OPTS};

macro_rules! upgrade {
    ($reader:ident) => {
        let mut $reader = RwLockUpgradableReadGuard::upgrade($reader).await;
    };
}

pub(crate) struct State {
    pub gossip_tx: broadcast::Sender<Gossip>,
    connections: RwLock<Vec<Connection>>,
}

enum Connection {
    Handler(Handler),
    Error {
        backoff: Duration,
        last_retry: Instant,
        addr: SocketAddr,
    },
}

impl Connection {
    fn handler(&self) -> Option<&Handler> {
        match self {
            Connection::Handler(h) => Some(h),
            _ => None,
        }
    }

    async fn peer(&self) -> Option<SocketAddr> {
        match self {
            Connection::Handler(h) => h.peer().await,
            Connection::Error { addr, .. } => Some(*addr),
        }
    }
}

impl State {
    pub fn new() -> Arc<Self> {
        let (gossip_tx, _) = broadcast::channel(128);
        Arc::new(Self {
            gossip_tx,
            connections: RwLock::new(Vec::new()),
        })
    }

    pub async fn known_peers(self: Arc<Self>) -> HashSet<SocketAddr> {
        futures::stream::iter(self.connections.read().await.iter())
            .filter_map(|conn| async { conn.peer().await })
            .collect()
            .await
    }

    pub async fn alive_peers(self: Arc<Self>) -> HashSet<SocketAddr> {
        futures::stream::iter(self.connections.read().await.iter())
            .filter_map(|conn| async { conn.handler() })
            .filter_map(|handler| async { handler.peer().await })
            .collect()
            .await
    }

    pub async fn connect(self: &Arc<Self>, peer: SocketAddr) -> Result<(), Error> {
        let handler = Handler::connect(peer, self.clone()).await?;
        handler.start()?;
        self.insert(handler).await;
        Ok(())
    }

    pub async fn insert(self: &Arc<Self>, handler: Handler) {
        self.connections
            .write()
            .await
            .push(Connection::Handler(handler));
    }

    pub async fn connect_many_if_unconnected(self: Arc<Self>, peers: &HashSet<SocketAddr>) {
        let conns = self.connections.upgradable_read().await;

        futures::stream::iter(self.connections.read().await.iter())
            .map(|conn| async move {
                if let Connection::Handler(handler) = conn {
                    handler.wait_determined().await
                }
            })
            .collect::<Vec<_>>()
            .await;

        let known_peers = self.clone().known_peers().await;

        upgrade!(conns);
        for peer in peers {
            if !known_peers.contains(peer) && *peer != OPTS.listen {
                match Handler::connect(*peer, self.clone()).await {
                    Ok(handler) if handler.start().is_ok() => {
                        conns.push(Connection::Handler(handler));
                    }
                    _ => println!("!   error connecting to new peer"),
                }
            }
        }
    }

    pub async fn check(self: Arc<Self>) {
        let conns = self.connections.upgradable_read().await;
        for (idx, conn) in conns.iter().enumerate() {
            match conn {
                Connection::Handler(handler) => {
                    if let Some(res) = handler.result().await {
                        match res {
                            Ok(_) => panic!("Handler finished without an error"),
                            Err(e) => {
                                println!(
                                    "!   [{}] handler error: {}",
                                    handler.peer().await
                                           .map(|addr| addr.to_string())
                                           .unwrap_or_else(|| "cannonical address unknown".to_string()),
                                    e
                                );
                                if let Ok(new_handler) = handler.restart().await {
                                    upgrade!(conns);
                                    conns[idx] = Connection::Handler(new_handler);
                                } else if let Some(addr) = handler.peer().await {
                                    upgrade!(conns);
                                    conns[idx] = Connection::Error {
                                        addr,
                                        backoff: Duration::from_millis(50),
                                        last_retry: Instant::now(),
                                    };
                                } else {
                                    upgrade!(conns);
                                    // No good trying to re-establish connection without cannonical address
                                    conns.remove(idx);
                                }
                                break;
                            }
                        }
                    }
                }
                Connection::Error {
                    backoff,
                    last_retry,
                    addr,
                    ..
                } => {
                    if self.clone().alive_peers().await.contains(addr) {
                        upgrade!(conns);
                        conns.remove(idx);
                        break;
                    } else if Instant::now().duration_since(*last_retry) >= *backoff {
                        match Handler::connect(*addr, self.clone()).await {
                            Ok(handler) if handler.start().is_ok() => {
                                upgrade!(conns);
                                conns[idx] = Connection::Handler(handler);
                                break;
                            }
                            _ => {
                                let backoff = backoff.saturating_mul(2);
                                let addr = *addr;
                                upgrade!(conns);
                                conns[idx] = Connection::Error {
                                    last_retry: Instant::now(),
                                    backoff,
                                    addr,
                                };
                                break;
                            }
                        };
                    }
                }
            }
        }
    }
}
