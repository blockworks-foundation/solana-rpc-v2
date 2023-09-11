//cargo run --bin readsa HT9SLHiq4ARRUS7EsCK3EZh3MutRR73ts1fojzHwJYvh

use anyhow::bail;
use borsh::de::BorshDeserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake::state::StakeState;
use std::env;
use std::str::FromStr;

const RPC_URL: &str = "http://localhost:8899";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    println!("args:{args:?}");

    if args.len() <= 1 {
        eprintln!("Please provide account pubkey");
        std::process::exit(1);
    }

    let pk_str: String = args[1].parse().expect("First argument should be a String");

    let rpc_client =
        RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::confirmed());

    // Fetch current epoch
    let account = rpc_client
        .get_account(&Pubkey::from_str(&pk_str).expect("invalid pubkey"))
        .await?;

    let stake = read_stake_from_account_data(account.data.as_slice())?;

    println!("account:{pk_str} stake:{:?}", stake);

    Ok(())
}

fn read_stake_from_account_data(mut data: &[u8]) -> anyhow::Result<Option<Delegation>> {
    if data.is_empty() {
        log::warn!("Stake account with empty data. Can't read stake.");
        bail!("Error: read Stake account with empty data");
    }
    match StakeState::deserialize(&mut data)? {
        StakeState::Stake(_, stake) => Ok(Some(stake.delegation)),
        StakeState::Initialized(_) => Ok(None),
        other => {
            bail!("read stake from account not a stake account. read:{other:?}");
        }
    }
}
