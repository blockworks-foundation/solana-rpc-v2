use solana_client::client_error::ClientError;
use solana_client::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::account::AccountSharedData;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::stake_history::StakeHistory;
use std::time::Duration;

const RPC_URL: &str = "http://147.28.169.13:8899";

pub fn get_stakehistory_account(rpc_url: String) -> Result<Account, ClientError> {
    log::info!("TaskToExec RpcGetStakeHistory start");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_stake = rpc_client.get_account(&solana_sdk::sysvar::stake_history::id());
    log::info!("TaskToExec RpcGetStakeHistory END",);
    res_stake
}

fn main() {
    let history = get_stakehistory_account(RPC_URL.to_string())
        .ok()
        .and_then(|account| {
            solana_sdk::account::from_account::<StakeHistory, _>(&AccountSharedData::from(account))
        });
    println!("RPC Stake_history: {:?}", history);
}
