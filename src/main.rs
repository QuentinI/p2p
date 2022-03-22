use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;
use lazy_static::lazy_static;
use tokio::join;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::time::interval;

mod handler;
mod message;
mod state;

use handler::Handler;
use message::Gossip;
use state::State;

#[derive(Debug, Parser)]
/// Demo peer-to-peer gossip program with full-mesh topology.
struct Opts {
    #[clap(
        short,
        long,
        name = "listen address",
        long_help = "interface:port pair to bind to, e.g. 127.0.0.1:9990"
    )]
    listen: SocketAddr,
    #[clap(
        short,
        long,
        name = "gossip interval",
        long_help = "interval in seconds between generating gossip messages"
    )]
    interval: u64,
    #[clap(
        name = "seed peers",
        long_help = "list of interface:port pairs of peers to try to connect to"
    )]
    seed_peers: Vec<SocketAddr>,
}

lazy_static! {
    static ref OPTS: Opts = Opts::parse();
}

async fn gossip_producer(queue: broadcast::Sender<Gossip>) -> anyhow::Result<()> {
    let mut interval = interval(Duration::from_secs(OPTS.interval));
    loop {
        let (_, gossip) = join!(interval.tick(), tokio::task::spawn_blocking(Gossip::random));
        // It's OK for channel to be "dropped"
        // if we aren't connected to any peers at the moment
        drop(queue.send(gossip?));
    }
}

async fn server(listener: TcpListener, state: Arc<State>) -> ! {
    loop {
        match listener.accept().await {
            Ok((conn, _)) => {
                let handler = Handler::new(conn, state.clone());
                handler.start().unwrap();
                state.insert(handler).await;
            }
            Err(e) => {
                println!("!   error accepting connection: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    if OPTS.listen.ip().is_unspecified() {
        bail!("Listening on unspecified IP is not supported");
    }

    let listener = TcpListener::bind(OPTS.listen).await?;

    let state = State::new();

    tokio::spawn(gossip_producer(state.gossip_tx.clone()));

    for peer in &OPTS.seed_peers {
        state.connect(*peer).await.unwrap_or_else(|e| {
            println!("!   error connecting to seed peer: {}", e);
        });
    }

    tokio::spawn(server(listener, state.clone()));

    loop {
        state.clone().check().await;
    }
}
