// cargo run --bin testdeleteacc

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

const GRPC_URL: &str = "http://localhost:10000";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let client = GeyserGrpcClient::connect(GRPC_URL, None::<&'static str>, None)?;

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
    slots.insert("client".to_string(), SubscribeRequestFilterSlots::default());

    //account subscription
    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts_filter.insert(
        "client".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![solana_sdk::stake::program::ID.to_string()],
            filters: vec![],
        },
    );

    let mut confirmed_stream = client
        .subscribe_once(
            slots.clone(),
            accounts_filter,    //accounts
            Default::default(), //tx
            Default::default(), //entry
            Default::default(), //full block
            Default::default(), //block meta
            Some(CommitmentLevel::Confirmed),
            vec![],
            None,
        )
        .await?;

    while let Some(Ok(update)) = confirmed_stream.next().await {
        match update.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                log::info!("Geyser receive account at slot:{}", account.slot);
                log::info!(
                    "Geyser notif with previous account owner:{:?}",
                    account
                        .previous_account_state
                        .map(|acc| Pubkey::try_from(acc.owner).expect("valid pubkey"))
                );
                if let Some(account) = account.account {
                    log::info!(
                        "Geyser notif for account account:{:?}",
                        Pubkey::try_from(account.pubkey).expect("valid pubkey")
                    );
                    log::info!(
                        "Geyser notif account owner:{:?}",
                        Pubkey::try_from(account.owner).expect("valid pubkey")
                    );
                }
            }
            Some(UpdateOneof::Slot(slot)) => log::trace!(
                "Geyser receive slot:{} commitment:{:?}",
                slot.slot,
                slot.status()
            ),
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
