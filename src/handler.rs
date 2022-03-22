use std::convert;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
use std::time::Duration;

use futures::poll;
use thiserror::Error;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::{yield_now, JoinError, JoinHandle};
use tokio::time::error::Elapsed;
use tokio::time::{interval, timeout, Interval};
use tokio::{io::AsyncWriteExt, net::TcpStream, select, sync::broadcast};

use crate::message::Gossip;
use crate::message::Message;
use crate::message::Payload;
use crate::{State, OPTS};

pub(crate) struct Handler {
    inner: Arc<Mutex<Inner>>,
    cannonical_addr: Arc<OnceCell<SocketAddr>>,
    task: Mutex<OnceCell<JoinHandle<Result<()>>>>,
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Tcp error")]
    Socket(#[from] std::io::Error),
    #[error("Serialization error")]
    Serialization(#[from] bincode::Error),
    #[error("Dangling handler")]
    DanglingHandler,
    #[error("Error receiving broadcast")]
    Broadcast(#[from] broadcast::error::RecvError),
    #[error("Handler was already started")]
    Started,
    #[error("Connection closed")]
    Closed,
    #[error("Handler task failed to complete")]
    JoinError(#[from] JoinError),
    #[error("Tried to reconnect to peer without known address")]
    AddrUnknown,
    #[error("Timeout")]
    Timeout(#[from] Elapsed),
}

type Result<T> = std::result::Result<T, Error>;

impl Handler {
    pub fn start(&self) -> Result<()> {
        let inner = self.inner.clone();

        let task = tokio::spawn(async move {
            inner
                .lock()
                .await
                .send_message(&Message {
                    cannonical_addr: OPTS.listen,
                    payload: Payload::Hi,
                })
                .await
                .unwrap(); // TODO:

            loop {
                inner.lock().await.run().await?;
            }
        });

        self.task
            .try_lock()
            .unwrap()
            .set(task)
            .map_err(|_| Error::Started)
    }

    pub async fn result(&self) -> Option<Result<()>> {
        if let Some(fut) = self.task.lock().await.get_mut() {
            match poll!(fut) {
                std::task::Poll::Ready(res) => {
                    Some(res.map_err(Error::JoinError).and_then(convert::identity))
                }
                std::task::Poll::Pending => None,
            }
        } else {
            None
        }
    }

    pub async fn peer(&self) -> Option<SocketAddr> {
        self.cannonical_addr.get().cloned()
    }

    pub async fn wait_determined(&self) {
        while !self.cannonical_addr.initialized() {
            yield_now().await;
        }
    }

    pub async fn connect(addr: SocketAddr, state: Arc<State>) -> Result<Self> {
        let connection = TcpStream::connect(addr).await?;
        let inner = Inner::new(connection, state);

        // inner has just been created, so set() should alsways succeed
        inner.cannonical_addr.set(addr).unwrap();

        Ok(Self {
            cannonical_addr: inner.cannonical_addr.clone(),
            inner: Arc::new(Mutex::new(inner)),
            task: Mutex::new(OnceCell::new()),
        })
    }

    pub fn new(connection: TcpStream, state: Arc<State>) -> Self {
        let inner = Inner::new(connection, state);

        Self {
            cannonical_addr: inner.cannonical_addr.clone(),
            inner: Arc::new(Mutex::new(inner)),
            task: Mutex::new(OnceCell::new()),
        }
    }

    pub async fn restart(&self) -> Result<Self> {
        let addr = self
            .cannonical_addr
            .get()
            .cloned()
            .ok_or(Error::AddrUnknown)?;
        let inner = self.inner.lock().await;
        let state = inner
            .state
            .clone()
            .upgrade()
            .ok_or(Error::DanglingHandler)?;

        let connection = TcpStream::connect(addr).await?;
        let inner = Inner::new(connection, state);
        // inner has just been created, so set() should alsways succeed
        inner.cannonical_addr.set(addr).unwrap();

        Ok(Self {
            cannonical_addr: inner.cannonical_addr.clone(),
            inner: Arc::new(Mutex::new(inner)),
            task: Mutex::new(OnceCell::new()),
        })
    }
}

struct Inner {
    cannonical_addr: Arc<OnceCell<SocketAddr>>,
    connection: TcpStream,
    buffer: Vec<u8>,
    state: Weak<State>,
    gossip_rx: broadcast::Receiver<Gossip>,
    discovery_interval: Interval,
}

impl Inner {
    fn new(connection: TcpStream, state: Arc<State>) -> Self {
        let gossip_rx = state.gossip_tx.subscribe();
        Self {
            cannonical_addr: Arc::new(OnceCell::new()),
            connection,
            buffer: Vec::with_capacity(10),
            state: Arc::downgrade(&state),
            gossip_rx,
            discovery_interval: interval(Duration::from_secs(5)),
        }
    }

    async fn handle_message(&mut self) -> Result<()> {
        let message: Message = bincode::deserialize(&self.buffer[..])?;

        self.cannonical_addr
            .get_or_init(|| async { message.cannonical_addr })
            .await;

        match &message.payload {
            Payload::RequestPeers => {
                let peers = self
                    .state
                    .upgrade()
                    .ok_or(Error::DanglingHandler)?
                    .alive_peers()
                    .await;

                let message = Message {
                    cannonical_addr: OPTS.listen,
                    payload: Payload::Peers(peers),
                };

                self.send_message(&message).await?;
            }
            Payload::Gossip(gossip) => {
                println!("<-- {} from [{}]", gossip, message.cannonical_addr)
            }
            Payload::Peers(peerlist) => {
                println!(
                    "<-- peers {:?} from [{}]",
                    peerlist, message.cannonical_addr
                );
                self.state
                    .upgrade()
                    .ok_or(Error::DanglingHandler)?
                    .connect_many_if_unconnected(peerlist)
                    .await;
            }
            Payload::Hi => {}
        }

        Ok(())
    }

    async fn send_message(&mut self, message: &Message) -> Result<()> {
        let peer_addr = self
            .cannonical_addr
            .get()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "cannonical address unknown".to_string());

        match &message.payload {
            Payload::Hi => {}
            Payload::RequestPeers => {}
            Payload::Gossip(gossip) => {
                println!("--> {}  to  [{}]", gossip, peer_addr)
            }
            Payload::Peers(peerlist) => {
                println!("--> peers {:?}  to  [{}]", peerlist, peer_addr)
            }
        }

        let serialized = bincode::serialize(message)?;
        timeout(
            Duration::from_secs(5),
            self.connection.write_all(&serialized),
        )
        .await??;
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        select! {
            readable = self.connection.readable() => {
                readable?;

                loop {
                    match self.connection.try_read_buf(&mut self.buffer) {
                        Ok(0) => {
                            return Err(Error::Closed);
                        },
                        Ok(_) => {
                            if self.buffer.capacity() == self.buffer.len() {
                                self.buffer.reserve(self.buffer.len());
                            } else {
                                self.handle_message().await?;
                                self.buffer.clear();
                                break;
                            }
                        },
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            break;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }
            },
            gossip = self.gossip_rx.recv() => {
                let message = Message {
                    cannonical_addr: OPTS.listen,
                    payload: Payload::Gossip(gossip?),
                };
                self.send_message(&message).await?;
            },
            _ = self.discovery_interval.tick() => {
                let message = Message {
                    cannonical_addr: OPTS.listen,
                    payload: Payload::RequestPeers,
                };
                self.send_message(&message).await?;
            }
        }
        Ok(())
    }
}
