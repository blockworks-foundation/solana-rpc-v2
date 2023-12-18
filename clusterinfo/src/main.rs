// RUST_LOG=info cargo run

use async_stream::stream;
use futures::{Stream, StreamExt};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_streamer::socket::SocketAddrSpace;
use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::pin;
use std::str::FromStr;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;
use yellowstone_grpc_proto::prelude::SubscribeUpdateClusterInfo;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots};
use yellowstone_grpc_proto::tonic::Status;

const RPC_URL: &str = "http://localhost:8899";
const GRPC_URL: &str = "http://localhost:10000";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ctrl_c_signal = tokio::signal::ctrl_c();

    //config
    let allow_private_addr = false;
    let addr_verifier = SocketAddrSpace::new(allow_private_addr);
    //let genesis_config_hash = solana_sdk::Hash();
    // let shred_version =
    // solana_sdk::shred_version::compute_shred_version(&genesis_config.hash(), Some(&hard_forks));

    let cluster_info_stream = create_geyser_clusterinfo_stream().await?;

    let jh = tokio::spawn({
        let shred_version = TESTNET_SHRED_VERSION;

        async move {
            run_loop(cluster_info_stream, shred_version).await;
        }
    });

    tokio::select! {
        res = jh => {
            log::error!("Process quit unexpectedly {res:?}");

        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");
        }
    }

    Ok(())
}

//let socket_addr_space = SocketAddrSpace::new(matches.is_present("allow_private_addr"));
//    pub shred_version: u16 = 0,

const TESTNET_SHRED_VERSION: u16 = 5106;
const TESTNET_GENESIS_HASH: &str = "4uhcVJyU9pJkvQyS88uRDiswHXSCkY3zQawwpjk2NsNY";

const MAINNET_SHRED_VERSION: u16 = 5106;
const MAINNET_GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";

fn verify_node(
    node: &SubscribeUpdateClusterInfo,
    addr_verifier: &SocketAddrSpace,
    shred_version: u16,
) -> bool {
    node.gossip
        .as_ref()
        .and_then(|addr| std::net::SocketAddr::from_str(addr).ok())
        .map(|addr| addr_verifier.check(&addr))
        .unwrap_or(false)
        && node.shred_version as u16 == shred_version
}

async fn run_loop<S>(cluster_info_stream: S, shred_version: u16)
where
    S: Stream<Item = SubscribeUpdateClusterInfo>,
{
    let mut node_list: HashMap<String, SubscribeUpdateClusterInfo> = HashMap::new();
    let mut cluster_info_stream = pin!(cluster_info_stream);

    //Log current size of the cluster node info list size.
    let mut log_interval = tokio::time::interval(tokio::time::Duration::from_secs(15));
    //spik first tick
    log_interval.tick().await;

    //TODO remove only to see how shred version change.
    let socket_addr_space = SocketAddrSpace::new(false);

    loop {
        tokio::select! {
            //log interval TODO remove
            _ = log_interval.tick() => {
                log::info!("Current cluster info list size:{}", node_list.len());
                //verify cluster_infos
                let geyzer_clluster = node_list.clone();
                tokio::task::spawn_blocking(move ||{
                    match get_rpc_cluster_info() {
                        Ok(rpc_cluster) => {
                            verify_clusters(geyzer_clluster, rpc_cluster);
                        },
                        Err(err) => log::warn!("Error during get RPC cluster nodes: {err}"),
                    }
                });
            }
            Some(update) = cluster_info_stream.next() => {
                match verify_node(&update, &socket_addr_space, shred_version) {
                    true => {node_list.insert(update.pubkey.clone(), update);},
                    false=> log::info!("verify_node fail for:{} for addr:{}", update.pubkey, update.gossip.unwrap_or("None".to_string())),
                }

            }
        }
    }
}

async fn create_geyser_clusterinfo_stream(
) -> anyhow::Result<impl Stream<Item = SubscribeUpdateClusterInfo>> {
    let geyzer_stream = subscribe_geyzer_clusterinfo(GRPC_URL.to_string()).await?;

    Ok(stream! {
            for await update_message in geyzer_stream {
                match update_message {
                    Ok(update_message) => {
                        match update_message.update_oneof {
                            Some(UpdateOneof::ClusterInfo(update_cluster_info)) =>
                            {
                                yield update_cluster_info;
                            }
                            _ => (),
                        }

                    }
                    Err(tonic_status) => {
                        // TODO identify non-recoverable errors and cancel stream
                        panic!("Receive error on geyzer stream {:?}", tonic_status);
                    }
                }
            } // -- production loop
    }) // -- stream!
}

//subscribe Geyser grpc
async fn subscribe_geyzer_clusterinfo(
    grpc_url: String,
) -> anyhow::Result<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
    let mut client = GeyserGrpcClient::connect(grpc_url, None::<&'static str>, None)?;

    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots::default());

    //account subscription
    let mut cluster_filter: HashSet<String> = HashSet::new();
    cluster_filter.insert("client".to_owned());

    let confirmed_stream = client
        .subscribe_once(
            slots.clone(),
            Default::default(), //accounts
            Default::default(), //tx
            Default::default(), //entry
            Default::default(), //full block
            Default::default(), //block meta
            Some(CommitmentLevel::Processed),
            vec![],
            None,
            Some(cluster_filter),
        )
        .await?;

    Ok(confirmed_stream)
}

fn get_rpc_cluster_info() -> anyhow::Result<Vec<solana_client::rpc_response::RpcContactInfo>> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        RPC_URL.to_string(),
        tokio::time::Duration::from_secs(600),
        CommitmentConfig::confirmed(),
    );

    let cluster_nodes = rpc_client.get_cluster_nodes()?;
    log::info!("RPC cluster nodes size:{}", cluster_nodes.len());
    Ok(cluster_nodes)
}

fn verify_clusters(
    mut geyzer_cluster: HashMap<String, SubscribeUpdateClusterInfo>,
    rpc_cluster: Vec<solana_client::rpc_response::RpcContactInfo>,
) {
    //verify if all rpc are in geyzer
    for rpc in &rpc_cluster {
        if let None = geyzer_cluster.remove(&rpc.pubkey) {
            log::info!("Rpc node not present in geyzer: {:?} ", rpc);
        }
    }
    geyzer_cluster.iter().for_each(|(key, node)| {
        log::info!(
            "Geyzer node not present in RPC: {} gossip:{} shred_version:{} node:{:?}",
            key,
            node.gossip
                .as_ref()
                .map(|addr| addr.to_string())
                .unwrap_or("None".to_string()),
            node.shred_version,
            node
        )
    });
}
