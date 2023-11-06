//cargo run --bin readstakes ../temp/extract_stake_529_9h.json
//RUST_BACKTRACE=1 cargo run --bin readstakes ../../temp/extract_stake_529_9h.json &> ../../temp/aggregate_expor_stake_529_9h.txt
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::vote::state::VoteState;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::time::Duration;

//const RPC_URL: &str = "http://localhost:8899";
const RPC_URL: &str = "https://api.testnet.solana.com";
const MAX_EPOCH_VALUE: u64 = 18446744073709551615;

type Slot = u64;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredStake {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub stake: Delegation,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct FileResult {
    jsonrpc: String,
    result: (Slot, HashMap<String, StoredStake>),
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        eprintln!("Please provide the path to the stake file");
        std::process::exit(1);
    }

    let stake_file: String = args[1].parse().expect("First argument should be a String");
    let stakes_data =
        std::fs::read_to_string(stake_file).expect("Should have been able to read the file");

    let file_content: FileResult = serde_json::from_str(&stakes_data).unwrap();
    let votes_map = get_vote_accounts_map();

    let stakes = file_content
        .result
        .1
        .into_iter()
        .map(|(pk, st)| (Pubkey::from_str(&pk).unwrap(), st))
        .collect();

    let nodeid_stakes = build_nodeid_accounts_stakes(stakes, votes_map, 529);
    log::info!("{:?}", nodeid_stakes);
    Ok(())
}

fn get_vote_accounts_map() -> HashMap<Pubkey, Pubkey> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        RPC_URL.to_string(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_vote = rpc_client
        .get_program_accounts(&solana_sdk::vote::program::id())
        .unwrap();
    res_vote
        .into_iter()
        .filter_map(
            |(pk, account)| match VoteState::deserialize(&account.data) {
                Ok(vote) => Some((pk, vote.node_pubkey)),
                Err(err) => {
                    log::warn!("Error during vote account data deserialisation:{err}");
                    None
                }
            },
        )
        .collect()
}

fn build_nodeid_accounts_stakes(
    storestake: HashMap<Pubkey, StoredStake>,
    vote_map: HashMap<Pubkey, Pubkey>,
    epoch: u64,
) -> Vec<(Pubkey, u64)> {
    let mut final_stakes = HashMap::<Pubkey, u64>::new();

    //log::trace!("calculate_leader_schedule_from_stake_map stake_map:{stake_map:?} current_epoch_info:{current_epoch_info:?}");
    for storestake in storestake.values() {
        //log::info!("Program_accounts stake:{stake:#?}");
        if is_stake_to_add(storestake.pubkey, &storestake.stake, epoch) {
            // Add the stake in this stake account to the total for the delegated-to vote account
            //get nodeid for vote account
            let Some(nodeid) = vote_map.get(&storestake.stake.voter_pubkey) else {
                log::warn!(
                    "Vote account not found in vote map for stake vote account:{}",
                    &storestake.stake.voter_pubkey
                );
                continue;
            };
            *(final_stakes.entry(*nodeid).or_insert(0)) += storestake.stake.stake;
        }
    }
    final_stakes.into_iter().collect()
}

fn is_stake_to_add(stake_pubkey: Pubkey, stake: &Delegation, epoch: u64) -> bool {
    //On test validator all stakes are attributes to an account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE.
    //It's considered as activated stake.
    if stake.activation_epoch == MAX_EPOCH_VALUE {
        log::info!(
            "Found account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE use it: {}",
            stake_pubkey.to_string()
        );
    } else {
        // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
        // u64::MAX which indicates no activation ever happened)
        if stake.activation_epoch >= epoch {
            return false;
        }
        // Ignore stake accounts deactivated before this epoch
        if stake.deactivation_epoch < epoch {
            return false;
        }
    }
    true
}
