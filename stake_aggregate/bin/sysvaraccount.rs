use solana_sdk::commitment_config::CommitmentConfig;
use anyhow::bail;
use futures_util::StreamExt;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots},
    tonic::service::Interceptor,
};
use solana_client::rpc_response::Response;
use solana_client::client_error::ClientError;
use solana_client::rpc_client::RpcClient;

const GRPC_URL: &str = "http://localhost:10000";
const RPC_URL: &str = "http://localhost:8899";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut client = GeyserGrpcClient::connect(GRPC_URL, None::<&'static str>, None)?;

    let version = client.get_version().await?;
    println!("Version: {:?}", version);

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        res = run_loop(client) => {
            // This should never happen
            log::error!("Services quit unexpectedly {res:?}");
        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");
        }
    }

    Ok(())
}

async fn run_loop<F: Interceptor>(mut client: GeyserGrpcClient<F>) -> anyhow::Result<()> {
    //subscribe Geyser grpc
    //slot subscription
    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    //account subscription
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts.insert(
        "client".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![
                solana_sdk::sysvar::stake_history::ID.to_string(),
            ],
            filters: vec![],
        },
    );

    let mut confirmed_stream = client
        .subscribe_once(
            slots.clone(),
            accounts.clone(),   //accounts
            Default::default(), //tx
            Default::default(), //entry
            Default::default(),     //full block
            Default::default(), //block meta
            Some(CommitmentLevel::Confirmed),
            vec![],
        )
        .await?;

    while let Some(Ok(update)) = confirmed_stream.next().await {
        match update.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                if let Some(account) = account.account {
                    let owner = Pubkey::try_from(account.owner).expect("valid pubkey");
                    match owner {
                        solana_sdk::sysvar::stake_history::ID => {
                            log::info!("Geyser notif Stake History account:{:?}", owner);
                        }
                        _ => log::warn!("receive an account notification from a unknown owner:{owner:?}"),
                    }
                }
            }
            Some(UpdateOneof::Slot(_)) => (),
            Some(UpdateOneof::Ping(_)) => {
                log::trace!("GRPC Ping");
            }
            k => {
                bail!("Unexpected update: {k:?}");
            }
        };
    }
    Ok(())

}

fn load_account_from_rpc() {
    let rpc_client = RpcClient::new(RPC_URL);
    match rpc_client.get_account_with_commitment(&solana_sdk::sysvar::stake_history::id(), CommitmentConfig::confirmed()) {
        Ok(Response { context: Some(_), .. }) => log::info!("RPC get_account return the stake history account"),
        Ok(Response { context: None, .. }) => log::info!("RPC get_account doesn't find the stake history account"),
        Err(_) => log::error!("Error during RPC call:{err}")
    }

}