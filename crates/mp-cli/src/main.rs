use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand};
use directories::ProjectDirs;
use mp_core::{
    ChannelInvite, ChannelNode, ChannelStore, Node, NodeOptions, ObjectStore, RelayConfig,
    ShareLink, parse_file_cid,
};
use tokio::io::{AsyncBufReadExt, BufReader};
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

        /// Maximum time to discover and connect to a seed, in seconds.
        #[arg(long, default_value_t = 90)]
        discovery_timeout: u64,
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

    /// Create, join, or open a live capability channel.
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ChannelCommand {
    /// Create and persist a channel capability without going online.
    Create {
        /// Advisory channel display name.
        name: String,
    },

    /// Persist an invite and enter its live interactive session.
    Join {
        /// Canonical mp-channel:// capability invite.
        invite: String,
    },

    /// Reopen a locally persisted channel by id.
    Open {
        /// Stable hexadecimal channel id.
        channel_id: String,
    },

    /// Explicitly print the capability invite for a local channel.
    Invite {
        /// Stable hexadecimal channel id.
        channel_id: String,
    },

    /// List locally persisted channels without revealing capabilities.
    List,
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
        Command::Get {
            link,
            discovery_timeout,
        } => get(&data_dir, &link, discovery_timeout, node_options).await,
        Command::Node => run_node(&data_dir, node_options).await,
        Command::Holdings => list_holdings(&data_dir),
        Command::Doctor { timeout } => doctor(&data_dir, timeout, node_options).await,
        Command::Channel { command } => channel(&data_dir, command, node_options).await,
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
    discovery_timeout_seconds: u64,
    options: NodeOptions,
) -> Result<()> {
    let link: ShareLink = raw_link.parse()?;
    let store = open_store(data_dir)?;
    let node = Node::start_with_options(store, options).await?;
    print_node_warnings(&node);
    println!("PEER {}", node.status().public_key);
    println!("LOOKUP {}", link.cid());

    let downloaded = node
        .download(&link, Duration::from_secs(discovery_timeout_seconds))
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

async fn channel(data_dir: &Path, command: ChannelCommand, options: NodeOptions) -> Result<()> {
    let store = ChannelStore::open(data_dir)
        .with_context(|| format!("failed to open data directory {}", data_dir.display()))?;
    match command {
        ChannelCommand::Create { name } => {
            let invite = store.create(name)?;
            print_channel_invite(&invite)?;
            Ok(())
        }
        ChannelCommand::Join { invite } => {
            let invite: ChannelInvite = invite.parse()?;
            let invite = store.add(&invite)?;
            run_channel_session(store, invite, options).await
        }
        ChannelCommand::Open { channel_id } => {
            let invite = store.invite(&channel_id)?;
            run_channel_session(store, invite, options).await
        }
        ChannelCommand::Invite { channel_id } => {
            print_channel_invite(&store.invite(&channel_id)?)?;
            Ok(())
        }
        ChannelCommand::List => {
            for channel in store.channels()? {
                println!(
                    "CHANNEL {} name={} messages={}",
                    channel.id,
                    serde_json::to_string(&channel.name)?,
                    channel.message_count
                );
            }
            Ok(())
        }
    }
}

fn print_channel_invite(invite: &ChannelInvite) -> Result<()> {
    print_channel_identity(invite)?;
    println!("INVITE {invite}");
    Ok(())
}

fn print_channel_identity(invite: &ChannelInvite) -> Result<()> {
    println!("CHANNEL {}", invite.id());
    println!("NAME {}", serde_json::to_string(invite.name())?);
    Ok(())
}

async fn run_channel_session(
    store: ChannelStore,
    invite: ChannelInvite,
    options: NodeOptions,
) -> Result<()> {
    let mut node = ChannelNode::start_with_options(store, invite.clone(), options).await?;
    print_channel_identity(&invite)?;
    println!("PEER {}", node.status().public_key);
    if let Some(relay) = &node.status().relay {
        println!("RELAY forced {relay}");
    }
    println!("TOPIC {} announced", hex::encode(invite.topic()));
    println!("READY live-only");

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    loop {
        tokio::select! {
            signal = &mut ctrl_c => {
                signal.context("failed to install Ctrl-C handler")?;
                break;
            }
            line = lines.next_line() => {
                let Some(line) = line.context("failed to read channel input")? else {
                    break;
                };
                match line.as_str() {
                    "/quit" => break,
                    "/typing on" => {
                        if let Err(error) = node.send_typing(true) {
                            eprintln!("WARNING {error}");
                        }
                    }
                    "/typing off" => {
                        if let Err(error) = node.send_typing(false) {
                            eprintln!("WARNING {error}");
                        }
                    }
                    _ if line.trim().is_empty() => {}
                    _ => {
                        if let Err(error) = node.send_text(line).await {
                            eprintln!("WARNING {error}");
                        }
                    }
                }
            }
            event = node.next_event() => {
                let Some(event) = event else { break };
                println!("EVENT {}", serde_json::to_string(&event)?);
            }
        }
    }

    println!("STOPPING");
    node.shutdown()
        .await
        .context("failed to stop Peeroxide channel node")
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

    #[test]
    fn parses_explicit_channel_invite_command() {
        let cli = Cli::try_parse_from(["mp", "channel", "invite", &"01".repeat(32)]).unwrap();
        assert!(matches!(
            cli.command,
            Command::Channel {
                command: ChannelCommand::Invite { .. }
            }
        ));
    }
}
