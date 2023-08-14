use borsh::BorshDeserialize;
use chrono::{Datelike, Local, Timelike};
use serde_derive::Serialize;
use solana_sdk::stake::state::StakeState;
use core::str::FromStr;
use serde_json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::env;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

//const RPC_URL: &str = "https://api.devnet.solana.com";

const SLOTS_IN_EPOCH: u64 = 432000;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = env::args().collect();

    match write_schedule(0).await {
        Ok(()) => {
        }
        Err(err) => {
            log::info!("error:{err}");
        }
    }
    Ok(())
}

#[derive(Serialize, Clone, Copy, Debug)]
pub struct NodeStakeData {
    pub identity: Pubkey,
    pub stakes_from_vote_account : u64,
    pub stakes_from_stake_accounts : u64,
    pub times_in_leader_schedule: u64,
    pub times_in_leader_schedule_calculated_by_vote_account: u64,
    pub times_in_leader_schedule_calculated_by_stake_accounts: u64,
}

async fn write_schedule(epoch_offset: u64) -> anyhow::Result<()> {
    let schedule = process_schedule(epoch_offset).await?;
    let serialized_map = serde_json::to_string(&schedule).unwrap();
    let now = Local::now();
    let date_string = format!(
        "{}_{}_{}-{}_{}_{}",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );

    // Create the file name
    let file_name = format!("output_{}.json", date_string);

    // Write to the file
    let mut file = File::create(file_name).await?;
    file.write_all(serialized_map.as_bytes()).await?;
    Ok(())
}

async fn process_schedule(epoch_offset: u64) -> anyhow::Result<Vec<NodeStakeData>> {
    let rpc_client =
        RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::finalized());

    let slot = rpc_client.get_slot().await?;

    // Fetch current epoch
    let epoch_info = rpc_client.get_epoch_info().await?;

    let current_epoch = epoch_info.epoch;

    // Fetch stakes in current epoch
    // let response = rpc_client
    //     .get_program_accounts(&solana_sdk::stake::program::id())
    //     .await?;

    log::info!("current_slot:{slot:?}");
    log::info!("epoch_info:{epoch_info:?}");

    let mut stakes = HashMap::<Pubkey, u64>::new();
    stakes.insert(Pubkey::new_unique(), 100);


    //get leader schedule from rpc
    let leader_schedule_finalized = rpc_client.get_leader_schedule(None).await?.unwrap(); //Some(slot)
    let mut nb_slots_in_ls =  0;
    for l in leader_schedule_finalized.iter().map(|x| x.1.len()) {
        nb_slots_in_ls += l;
    }
    assert!( nb_slots_in_ls == 432000 );

    //build vote account node key association table
    let vote_account = rpc_client.get_vote_accounts().await?;

    let identity_vote_pk_map : HashMap<_,_> = vote_account.current
    .iter()
    .chain(vote_account.delinquent.iter())
    .map(|x| (Pubkey::from_str(x.vote_pubkey.as_str()).unwrap(), Pubkey::from_str(x.node_pubkey.as_str()).unwrap())).collect();

    //build schedule from vote account.
    let vote_stakes: HashMap<Pubkey, u64> = vote_account
        .current
        .iter()
        .chain(vote_account.delinquent.iter())
        .filter(|x| x.epoch_vote_account && x.activated_stake > 0)
        .map(|va| {
            (
                Pubkey::from_str(&va.node_pubkey).unwrap(),
                va.activated_stake,
            )
        })
        .collect();

    let leader_schedule_va = calculate_leader_schedule(current_epoch + epoch_offset, vote_stakes.clone());
    let mut leader_times_va : HashMap<Pubkey, u64> = HashMap::new();
    leader_schedule_va.get_slot_leaders().iter().for_each(|l| {
        *leader_times_va
            .entry(l.clone())
            .or_insert(0) += 1;

    });

    // build schedule from stake accounts
    let response = rpc_client
        .get_program_accounts(&solana_sdk::stake::program::id())
        .await?;
    let mut stakes = HashMap::<Pubkey, u64>::new();
    for (_, account) in response {
        // Zero-length accounts owned by the stake program are system accounts that were re-assigned and are to be
        // ignored
        if account.data.len() == 0 || account.lamports == 0 {
            continue;
        }

        match StakeState::deserialize(&mut account.data.as_slice())? {
            StakeState::Stake(_, stake) => {
                // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
                // u64::MAX which indicates no activation ever happened)
                if stake.delegation.activation_epoch > current_epoch {
                    continue;
                }
                // Ignore stake accounts deactivated before this epoch
                if stake.delegation.deactivation_epoch <= current_epoch {
                    continue;
                }

                let node_pk = identity_vote_pk_map.get(&stake.delegation.voter_pubkey).unwrap().clone();
                // Add the stake in this stake account to the total for the delegated-to vote account
                *(stakes
                    .entry(node_pk)
                    .or_insert(0)) += stake.delegation.stake;
            }
            _ => (),
        }
    }

    let leader_schedule_by_stake_account = calculate_leader_schedule(current_epoch + epoch_offset, stakes.clone());
    let mut leader_times_sa : HashMap<Pubkey, u64> = HashMap::new();
    leader_schedule_by_stake_account.get_slot_leaders().iter().for_each(|l| {
        *leader_times_sa
            .entry(l.clone())
            .or_insert(0) += 1;

    });

    let data = leader_schedule_finalized.iter().map(
        |(key, schedule)| {
            let identity = Pubkey::from_str(key.as_str()).unwrap();
            NodeStakeData {
                identity: Pubkey::from_str(key.as_str()).unwrap(),
                times_in_leader_schedule: schedule.len() as u64,
                stakes_from_stake_accounts: *stakes.get(&identity).unwrap(),
                stakes_from_vote_account: *vote_stakes.get(&identity).unwrap(),
                times_in_leader_schedule_calculated_by_stake_accounts: *leader_times_va.get(&identity).unwrap(),
                times_in_leader_schedule_calculated_by_vote_account: *leader_times_sa.get(&identity).unwrap(),
            }
        }
    ).collect();
    Ok(data)
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
fn calculate_leader_schedule(epoch: u64, stakes: HashMap<Pubkey, u64>) -> LeaderSchedule {
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    let mut stakes: Vec<_> = stakes
        .iter()
        .map(|(pubkey, stake)| (*pubkey, *stake))
        .collect();
    sort_stakes(&mut stakes);
    LeaderSchedule::new(&stakes, seed, SLOTS_IN_EPOCH, NUM_CONSECUTIVE_LEADER_SLOTS)
}

// Cribbed from leader_schedule_utils
fn sort_stakes(stakes: &mut Vec<(Pubkey, u64)>) {
    // Sort first by stake. If stakes are the same, sort by pubkey to ensure a
    // deterministic result.
    // Note: Use unstable sort, because we dedup right after to remove the equal elements.
    stakes.sort_unstable_by(|(l_pubkey, l_stake), (r_pubkey, r_stake)| {
        if r_stake == l_stake {
            r_pubkey.cmp(l_pubkey)
        } else {
            r_stake.cmp(l_stake)
        }
    });

    // Now that it's sorted, we can do an O(n) dedup.
    stakes.dedup();
}