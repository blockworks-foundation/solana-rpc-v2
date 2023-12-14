// RUST_LOG=info cargo run

use async_stream::stream;
use futures::{Stream, StreamExt};
use std::collections::HashMap;
use std::collections::HashSet;
use std::pin::pin;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdate;
use yellowstone_grpc_proto::prelude::SubscribeUpdateClusterInfo;
use yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots};
use yellowstone_grpc_proto::tonic::Status;

const GRPC_URL: &str = "http://localhost:10000";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let ctrl_c_signal = tokio::signal::ctrl_c();

    let cluster_info_stream = create_geyser_clusterinfo_stream().await?;

    let jh = tokio::spawn(async move {
        run_loop(cluster_info_stream).await;
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

async fn run_loop<S>(cluster_info_stream: S)
where
    S: Stream<Item = SubscribeUpdateClusterInfo>,
{
    let mut node_list: HashMap<String, SubscribeUpdateClusterInfo> = HashMap::new();
    let mut cluster_info_stream = pin!(cluster_info_stream);

    //Log current size of the cluster node info list size.
    let mut log_interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

    loop {
        tokio::select! {
            //log interval TODO remove
            _ = log_interval.tick() => {
                log::info!("Current cluster info list size:{}", node_list.len());
            }
            Some(update) = cluster_info_stream.next() => {
                node_list.insert(update.pubkey.clone(), update);
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
