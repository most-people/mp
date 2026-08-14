use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use mp_core::{Node, NodeOptions, ObjectStore, RelayConfig, ShareLink, parse_file_cid};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "mp",
    version,
    about = "Experimental content-addressed file propagation over Peeroxide"
)]
struct Cli {
    /// Persistent identity, objects, and holdings directory.
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    /// Force server connections through PUBKEY@IP:PORT blind relay.
    #[arg(long, global = true, value_name = "PUBKEY@IP:PORT")]
    force_relay: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Import a file, print its mp:// link, and keep seeding it.
    Publish {
        /// Regular file to publish.
        file: PathBuf,
    },

    /// Download a verified object by mp:// link and keep seeding it.
    Get {
        /// Canonical mp:// share link.
        link: String,

        /// Discovery and transfer deadline in seconds.
        #[arg(long, default_value_t = 90)]
        timeout: u64,
    },

    /// Start the node and seed every valid persisted holding.
    Node,

    /// Validate and list local holdings without starting the network.
    Holdings,

    /// Verify identity, storage, and public HyperDHT bootstrap access.
    Doctor {
        /// Bootstrap deadline in seconds.
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
}

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("ERROR {error:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let data_dir = match cli.data_dir {
        Some(path) => path,
        None => default_data_dir()?,
    };
    let node_options = parse_node_options(cli.force_relay.as_deref())?;

    match cli.command {
        Command::Publish { file } => publish(&data_dir, &file, node_options).await,
        Command::Get { link, timeout } => get(&data_dir, &link, timeout, node_options).await,
        Command::Node => run_node(&data_dir, node_options).await,
        Command::Holdings => list_holdings(&data_dir),
        Command::Doctor { timeout } => doctor(&data_dir, timeout, node_options).await,
    }
}

async fn publish(data_dir: &Path, file: &Path, options: NodeOptions) -> Result<()> {
    let store = open_store(data_dir)?;
    let store_for_import = store.clone();
    let file = file.to_path_buf();
    let imported = tokio::task::spawn_blocking(move || store_for_import.import_file(file))
        .await
        .context("file import task failed")??;
    let node = Node::start_with_options(store, options).await?;

    print_node_warnings(&node);
    println!("CID {}", imported.holding.cid);
    println!("LINK {}", imported.link);
    println!("SIZE {}", imported.holding.size);
    println!("OBJECT {}", imported.object_path.display());
    println!("PEER {}", node.status().public_key);
    println!("TOPIC announced");
    println!("SEEDING press Ctrl-C to stop");

    wait_and_shutdown(node).await
}

async fn get(
    data_dir: &Path,
    raw_link: &str,
    timeout_seconds: u64,
    options: NodeOptions,
) -> Result<()> {
    let link: ShareLink = raw_link.parse()?;
    let store = open_store(data_dir)?;
    let node = Node::start_with_options(store, options).await?;
    print_node_warnings(&node);
    println!("PEER {}", node.status().public_key);
    println!("LOOKUP {}", link.cid());

    let downloaded = node
        .download(&link, Duration::from_secs(timeout_seconds))
        .await?;
    println!("CID {}", downloaded.holding.cid);
    println!("SIZE {}", downloaded.holding.size);
    println!("OBJECT {}", downloaded.object_path.display());
    println!("SOURCE_PEER {}", downloaded.remote_public_key);
    println!("TOPIC announced");
    println!("SEEDING press Ctrl-C to stop");

    wait_and_shutdown(node).await
}

async fn run_node(data_dir: &Path, options: NodeOptions) -> Result<()> {
    let node = Node::start_with_options(open_store(data_dir)?, options).await?;
    print_node_warnings(&node);
    println!("PEER {}", node.status().public_key);
    for cid in node.announced_cids()? {
        println!("TOPIC {cid} announced");
    }
    println!("READY {} topics", node.announced_cids()?.len());
    println!("SEEDING press Ctrl-C to stop");
    wait_and_shutdown(node).await
}

fn list_holdings(data_dir: &Path) -> Result<()> {
    let store = open_store(data_dir)?;
    let report = store.validate_holdings()?;
    for holding in &report.valid {
        let cid = parse_file_cid(&holding.cid)?;
        println!(
            "HOLDING {} size={} topic=ready object={}",
            holding.cid,
            holding.size,
            store.object_path(&cid).display()
        );
    }
    for validation in &report.invalid {
        println!(
            "INVALID {} reason={}",
            validation.holding.cid,
            validation.error.as_deref().unwrap_or("unknown")
        );
    }
    println!(
        "SUMMARY valid={} invalid={}",
        report.valid.len(),
        report.invalid.len()
    );
    Ok(())
}

async fn doctor(data_dir: &Path, timeout_seconds: u64, options: NodeOptions) -> Result<()> {
    let store = open_store(data_dir)?;
    let node = tokio::time::timeout(
        Duration::from_secs(timeout_seconds),
        Node::start_with_options(store, options),
    )
    .await
    .map_err(|_| anyhow!("public HyperDHT bootstrap timed out after {timeout_seconds}s"))??;
    print_node_warnings(&node);
    println!("IDENTITY ok {}", node.status().public_key);
    println!("STORAGE ok {} topics", node.announced_cids()?.len());
    println!("HYPERDHT ok");
    node.shutdown().await?;
    Ok(())
}

async fn wait_and_shutdown(node: Node) -> Result<()> {
    tokio::signal::ctrl_c()
        .await
        .context("failed to install Ctrl-C handler")?;
    println!("STOPPING");
    node.shutdown()
        .await
        .context("failed to stop Peeroxide node")
}

fn print_node_warnings(node: &Node) {
    if let Some(relay) = &node.status().relay {
        println!("RELAY forced {relay}");
    }
    for validation in &node.status().invalid_holdings {
        eprintln!(
            "WARNING holding {} not announced: {}",
            validation.holding.cid,
            validation.error.as_deref().unwrap_or("unknown")
        );
    }
}

fn parse_node_options(value: Option<&str>) -> Result<NodeOptions> {
    let Some(value) = value else {
        return Ok(NodeOptions::default());
    };
    let (public_key, address) = value
        .split_once('@')
        .ok_or_else(|| anyhow!("force relay must use PUBKEY@IP:PORT"))?;
    let decoded = hex::decode(public_key).context("relay public key must be hexadecimal")?;
    let public_key: [u8; 32] = decoded.try_into().map_err(|decoded: Vec<u8>| {
        anyhow!("relay public key must be 32 bytes, got {}", decoded.len())
    })?;
    let address: SocketAddr = address
        .parse()
        .context("relay address must be a numeric IP:PORT")?;
    Ok(NodeOptions {
        force_relay: Some(RelayConfig {
            public_key,
            address,
        }),
    })
}

fn open_store(data_dir: &Path) -> Result<ObjectStore> {
    ObjectStore::open(data_dir)
        .with_context(|| format!("failed to open data directory {}", data_dir.display()))
}

fn default_data_dir() -> Result<PathBuf> {
    ProjectDirs::from("red.most", "Most", "mp")
        .map(|dirs| dirs.data_local_dir().to_path_buf())
        .ok_or_else(|| anyhow!("could not determine a platform data directory"))
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("mp_core=info,peeroxide=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_force_relay() {
        let options = parse_node_options(Some(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f@127.0.0.1:49742",
        ))
        .unwrap();
        let relay = options.force_relay.unwrap();
        assert_eq!(relay.public_key[0], 0);
        assert_eq!(relay.public_key[31], 31);
        assert_eq!(relay.address, "127.0.0.1:49742".parse().unwrap());
    }

    #[test]
    fn rejects_malformed_force_relay() {
        assert!(parse_node_options(Some("not-a-relay")).is_err());
        assert!(parse_node_options(Some("00@127.0.0.1:49742")).is_err());
    }
}
