use anyhow::bail;
use futures_util::StreamExt;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_response::Response;
use solana_sdk::commitment_config::CommitmentConfig;
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
const RPC_URL: &str = "http://localhost:8899";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let client = GeyserGrpcClient::connect(GRPC_URL, None::<&'static str>, None)?;

    let ctrl_c_signal = tokio::signal::ctrl_c();

    let sysvar_account_list = [
        (solana_sdk::sysvar::clock::ID, "Clock"),
        (solana_sdk::sysvar::epoch_schedule::ID, "Epoch Schedule"),
        (solana_sdk::sysvar::fees::ID, "Fee"),
        (solana_sdk::sysvar::instructions::ID, "Instructions"),
        (
            solana_sdk::sysvar::recent_blockhashes::ID,
            "Recent Blockhashes",
        ),
        (solana_sdk::sysvar::rent::ID, "Rent"),
        (solana_sdk::sysvar::rewards::ID, "Rewards"),
        (solana_sdk::sysvar::slot_hashes::ID, "Slot Hashes"),
        (solana_sdk::sysvar::slot_history::ID, "Slot  History"),
        (solana_sdk::sysvar::stake_history::ID, "Stake history"),
    ];

    load_accounts_from_rpc(&sysvar_account_list);

    tokio::select! {
        res = run_loop(client, &sysvar_account_list) => {
            // This should never happen
            log::error!("Services quit unexpectedly {res:?}");
        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");
        }
    }

    Ok(())
}

async fn run_loop<F: Interceptor>(
    mut client: GeyserGrpcClient<F>,
    accounts: &[(Pubkey, &str)],
) -> anyhow::Result<()> {
    //subscribe Geyser grpc
    //slot subscription
    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    //account subscription
    let mut accounts_filter: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    for (id, _) in accounts {
        accounts_filter.insert(
            "client".to_owned(),
            SubscribeRequestFilterAccounts {
                account: vec![id.to_string()],
                owner: vec![],
                filters: vec![],
            },
        );
    }

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
        )
        .await?;

    while let Some(Ok(update)) = confirmed_stream.next().await {
        match update.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                log::info!("Geyser receive account at slot:{}", account.slot);
                if let Some(account) = account.account {
                    let acc_id = Pubkey::try_from(account.pubkey).expect("valid pubkey");
                    log::info!("Geyser notif for account account:{:?}", acc_id);

                    match accounts.iter().filter(|(id, _)| *id == acc_id).next() {
                        Some((_, name)) => {
                            log::info!("Geyser receive notification for account:{name}")
                        }
                        None => log::warn!(
                            "Geyser receive a notification from a unknown account:{acc_id}"
                        ),
                    }
                }
            }
            Some(UpdateOneof::Slot(slot)) => log::info!(
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

fn load_accounts_from_rpc(accounts: &[(Pubkey, &str)]) {
    let rpc_client = RpcClient::new(RPC_URL);
    for (id, name) in accounts {
        load_account_from_rpc(*id, name, &rpc_client);
    }
}
fn load_account_from_rpc(id: Pubkey, name: &str, rpc_client: &RpcClient) {
    match rpc_client.get_account_with_commitment(&id, CommitmentConfig::confirmed()) {
        Ok(Response { value: Some(_), .. }) => {
            log::info!("RPC get_account return the {name} account")
        }
        Ok(Response { value: None, .. }) => {
            log::info!("RPC get_account doesn't find the {name} account")
        }
        Err(err) => log::error!("Error during RPC call:{err}"),
    }
}
