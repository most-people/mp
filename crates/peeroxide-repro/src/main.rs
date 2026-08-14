use std::fmt;
use std::future::Future;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use peeroxide::{JoinOpts, SwarmConfig, SwarmConnection, SwarmHandle, spawn};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until, timeout, timeout_at};
use tracing_subscriber::EnvFilter;

const PING: &[u8] = b"ping";
const PONG: &[u8] = b"pong";
const PEEROXIDE_VERSION: &str = "1.7.3";

#[derive(Debug, Parser)]
#[command(
    name = "peeroxide-repro",
    version,
    about = "Minimal cross-device ping/pong reproduction for Peeroxide 1.7.3"
)]
struct Cli {
    /// Shared 32-byte topic as exactly 64 hexadecimal characters.
    #[arg(long, value_name = "HEX")]
    topic: String,

    /// Deadline in seconds for each bootstrap, join, connection, read, and write stage.
    #[arg(long, global = true, default_value_t = 60)]
    timeout: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Announce the topic and answer each ping with pong.
    Server {
        /// Number of successful or failed connections to observe before exiting.
        #[arg(long, default_value_t = 1)]
        rounds: u32,

        /// Maximum total server runtime in seconds.
        #[arg(long, default_value_t = 2400)]
        overall_timeout: u64,
    },

    /// Create a fresh swarm identity for every ping/pong round.
    Client {
        /// Number of independent swarm connections to attempt.
        #[arg(long, default_value_t = 1)]
        rounds: u32,

        /// Delay between independent rounds in milliseconds.
        #[arg(long, default_value_t = 500)]
        delay_ms: u64,
    },
}

#[derive(Debug)]
struct StageFailure {
    stage: &'static str,
    error: String,
}

impl StageFailure {
    fn new(stage: &'static str, error: impl fmt::Display) -> Self {
        Self {
            stage,
            error: error.to_string(),
        }
    }
}

impl fmt::Display for StageFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.stage, self.error)
    }
}

impl std::error::Error for StageFailure {}

#[derive(Debug)]
struct ServerOutcome {
    connection: u32,
    remote: String,
    result: std::result::Result<(), StageFailure>,
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
    let topic = parse_topic(&cli.topic)?;
    let stage_timeout = positive_duration(cli.timeout, "timeout")?;

    match cli.command {
        Command::Server {
            rounds,
            overall_timeout,
        } => {
            ensure_positive_rounds(rounds)?;
            run_server(
                topic,
                rounds,
                stage_timeout,
                positive_duration(overall_timeout, "overall-timeout")?,
            )
            .await
        }
        Command::Client { rounds, delay_ms } => {
            ensure_positive_rounds(rounds)?;
            run_client(
                topic,
                rounds,
                stage_timeout,
                Duration::from_millis(delay_ms),
            )
            .await
        }
    }
}

async fn run_server(
    topic: [u8; 32],
    expected: u32,
    stage_timeout: Duration,
    overall_timeout: Duration,
) -> Result<()> {
    let (swarm_task, handle, mut connection_rx) = spawn_swarm(stage_timeout)
        .await
        .context("server bootstrap failed")?;
    let peer = hex::encode(handle.key_pair().public_key);

    run_stage("join", stage_timeout, handle.join(topic, server_only()))
        .await
        .map_err(|error| anyhow!(error.to_string()))?;
    run_stage("flush", stage_timeout, handle.flush())
        .await
        .map_err(|error| anyhow!(error.to_string()))?;

    println!(
        "READY role=server peer={peer} topic={} rounds={expected} peeroxide={PEEROXIDE_VERSION}",
        hex::encode(topic)
    );

    let deadline = Instant::now() + overall_timeout;
    let (outcome_tx, mut outcome_rx) = mpsc::channel(expected as usize);
    let mut accepted = 0;
    let mut passed = 0;
    let mut failed = 0;
    let mut deadline_elapsed = false;

    while passed + failed < expected {
        tokio::select! {
            connection = connection_rx.recv() => {
                let Some(connection) = connection else {
                    println!("FAIL role=server stage=accept error=connection-channel-closed");
                    failed += 1;
                    break;
                };
                accepted += 1;
                let connection_number = accepted;
                let remote = hex::encode(connection.remote_public_key());
                println!(
                    "CONNECTED role=server connection={connection_number} remote={remote} initiator={}",
                    connection.is_initiator
                );
                let outcome_tx = outcome_tx.clone();
                tokio::spawn(async move {
                    let result = server_exchange(connection, stage_timeout).await;
                    let _ = outcome_tx
                        .send(ServerOutcome {
                            connection: connection_number,
                            remote,
                            result,
                        })
                        .await;
                });
            }
            outcome = outcome_rx.recv() => {
                let Some(outcome) = outcome else {
                    break;
                };
                match outcome.result {
                    Ok(()) => {
                        passed += 1;
                        println!(
                            "PASS role=server connection={} remote={}",
                            outcome.connection, outcome.remote
                        );
                    }
                    Err(error) => {
                        failed += 1;
                        println!(
                            "FAIL role=server connection={} remote={} stage={} error={}",
                            outcome.connection,
                            outcome.remote,
                            error.stage,
                            sanitize(&error.error)
                        );
                    }
                }
            }
            () = sleep_until(deadline) => {
                deadline_elapsed = true;
                println!(
                    "FAIL role=server stage=overall-timeout error=deadline-elapsed"
                );
                break;
            }
        }
    }

    drop(outcome_tx);
    shutdown_swarm(handle, swarm_task, stage_timeout).await?;
    println!(
        "SUMMARY role=server expected={expected} accepted={accepted} passed={passed} failed={failed} deadline_elapsed={deadline_elapsed}"
    );

    if passed == expected && failed == 0 && !deadline_elapsed {
        Ok(())
    } else {
        bail!(
            "server acceptance failed: expected={expected} accepted={accepted} passed={passed} failed={failed}"
        )
    }
}

async fn run_client(
    topic: [u8; 32],
    rounds: u32,
    stage_timeout: Duration,
    delay: Duration,
) -> Result<()> {
    println!(
        "READY role=client topic={} rounds={rounds} peeroxide={PEEROXIDE_VERSION}",
        hex::encode(topic)
    );
    let mut passed = 0;
    let mut failed = 0;

    for round in 1..=rounds {
        match client_round(topic, round, stage_timeout).await {
            Ok(remote) => {
                passed += 1;
                println!("PASS role=client round={round} remote={remote}");
            }
            Err(error) => {
                failed += 1;
                println!(
                    "FAIL role=client round={round} stage={} error={}",
                    error.stage,
                    sanitize(&error.error)
                );
            }
        }
        if round < rounds {
            sleep(delay).await;
        }
    }

    println!("SUMMARY role=client expected={rounds} passed={passed} failed={failed}");
    if failed == 0 {
        Ok(())
    } else {
        bail!("client acceptance failed: passed={passed} failed={failed}")
    }
}

async fn client_round(
    topic: [u8; 32],
    round: u32,
    stage_timeout: Duration,
) -> std::result::Result<String, StageFailure> {
    let (swarm_task, handle, mut connection_rx) = spawn_swarm(stage_timeout).await?;
    let peer = hex::encode(handle.key_pair().public_key);
    println!("ROUND role=client round={round} peer={peer}");

    let exchange_result = async {
        run_stage("join", stage_timeout, handle.join(topic, client_only())).await?;
        run_stage("flush", stage_timeout, handle.flush()).await?;
        let mut connection = timeout(stage_timeout, connection_rx.recv())
            .await
            .map_err(|_| StageFailure::new("connect", "deadline elapsed"))?
            .ok_or_else(|| StageFailure::new("connect", "connection channel closed"))?;
        let remote = hex::encode(connection.remote_public_key());
        println!(
            "CONNECTED role=client round={round} remote={remote} initiator={}",
            connection.is_initiator
        );

        run_stage("write", stage_timeout, connection.peer.stream.write(PING)).await?;
        let reply = run_stage("read", stage_timeout, connection.peer.stream.read())
            .await?
            .ok_or_else(|| StageFailure::new("read", "connection closed before pong"))?;
        if reply != PONG {
            return Err(StageFailure::new(
                "validate",
                format!(
                    "expected={} received={}",
                    hex::encode(PONG),
                    hex::encode(reply)
                ),
            ));
        }
        Ok(remote)
    }
    .await;

    let shutdown_result = shutdown_swarm(handle, swarm_task, stage_timeout)
        .await
        .map_err(|error| StageFailure::new("shutdown", error));
    exchange_result.and_then(|remote| shutdown_result.map(|()| remote))
}

async fn server_exchange(
    mut connection: SwarmConnection,
    stage_timeout: Duration,
) -> std::result::Result<(), StageFailure> {
    let request = run_stage("read", stage_timeout, connection.peer.stream.read())
        .await?
        .ok_or_else(|| StageFailure::new("read", "connection closed before ping"))?;
    if request != PING {
        return Err(StageFailure::new(
            "validate",
            format!(
                "expected={} received={}",
                hex::encode(PING),
                hex::encode(request)
            ),
        ));
    }
    run_stage("write", stage_timeout, connection.peer.stream.write(PONG)).await
}

async fn spawn_swarm(
    stage_timeout: Duration,
) -> std::result::Result<(JoinHandle<()>, SwarmHandle, mpsc::Receiver<SwarmConnection>), StageFailure>
{
    run_stage(
        "bootstrap",
        stage_timeout,
        spawn(SwarmConfig::with_public_bootstrap()),
    )
    .await
}

async fn shutdown_swarm(
    handle: SwarmHandle,
    mut swarm_task: JoinHandle<()>,
    stage_timeout: Duration,
) -> Result<()> {
    timeout(stage_timeout, handle.destroy())
        .await
        .context("swarm destroy timed out")?
        .context("swarm destroy failed")?;
    if timeout_at(Instant::now() + stage_timeout, &mut swarm_task)
        .await
        .context("swarm task shutdown timed out")?
        .is_err()
    {
        bail!("swarm task failed during shutdown")
    }
    Ok(())
}

async fn run_stage<T, E>(
    stage: &'static str,
    duration: Duration,
    future: impl Future<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, StageFailure>
where
    E: fmt::Display,
{
    timeout(duration, future)
        .await
        .map_err(|_| StageFailure::new(stage, "deadline elapsed"))?
        .map_err(|error| StageFailure::new(stage, error))
}

fn server_only() -> JoinOpts {
    let mut options = JoinOpts::default();
    options.server = true;
    options.client = false;
    options
}

fn client_only() -> JoinOpts {
    let mut options = JoinOpts::default();
    options.server = false;
    options.client = true;
    options
}

fn parse_topic(value: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(value).context("topic must be hexadecimal")?;
    bytes
        .try_into()
        .map_err(|bytes: Vec<u8>| anyhow!("topic must be 32 bytes, got {}", bytes.len()))
}

fn positive_duration(seconds: u64, name: &str) -> Result<Duration> {
    if seconds == 0 {
        bail!("{name} must be greater than zero")
    }
    Ok(Duration::from_secs(seconds))
}

fn ensure_positive_rounds(rounds: u32) -> Result<()> {
    if rounds == 0 {
        bail!("rounds must be greater than zero")
    }
    Ok(())
}

fn sanitize(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join("-")
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_topic() {
        let raw = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        assert_eq!(hex::encode(parse_topic(raw).unwrap()), raw);
    }

    #[test]
    fn rejects_short_topic() {
        let error = parse_topic("00").unwrap_err();
        assert!(error.to_string().contains("32 bytes"));
    }

    #[test]
    fn rejects_non_hex_topic() {
        let error = parse_topic(&"z".repeat(64)).unwrap_err();
        assert!(error.to_string().contains("hexadecimal"));
    }
}
