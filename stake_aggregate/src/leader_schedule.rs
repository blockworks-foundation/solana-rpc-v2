use anyhow::bail;
use solana_client::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;

const MAX_EPOCH_VALUE: u64 = 18446744073709551615;

pub fn calculate_leader_schedule_from_stake_map(
    stake_map: &crate::stakestore::StakeMap,
    current_epoch_info: &EpochInfo,
) -> anyhow::Result<LeaderSchedule> {
    let mut stakes = HashMap::<Pubkey, u64>::new();
    //log::trace!("calculate_leader_schedule_from_stake_map stake_map:{stake_map:?} current_epoch_info:{current_epoch_info:?}");
    for storestake in stake_map.values() {
        //log::info!("Program_accounts stake:{stake:#?}");
        //On test validator all stakes are attributes to an account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE.
        //It's considered as activated stake.
        if storestake.stake.activation_epoch == MAX_EPOCH_VALUE {
            log::info!("Found account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE use it: {}", storestake.pubkey.to_string());
        } else {
            // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
            // u64::MAX which indicates no activation ever happened)
            if storestake.stake.activation_epoch >= current_epoch_info.epoch {
                continue;
            }
            // Ignore stake accounts deactivated before this epoch
            if storestake.stake.deactivation_epoch < current_epoch_info.epoch {
                continue;
            }
        }

        // Add the stake in this stake account to the total for the delegated-to vote account
        *(stakes.entry(storestake.stake.voter_pubkey).or_insert(0)) += storestake.stake.stake;
    }
    calculate_leader_schedule(stakes, current_epoch_info)
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
fn calculate_leader_schedule(
    stakes: HashMap<Pubkey, u64>,
    current_epoch_info: &EpochInfo,
) -> anyhow::Result<LeaderSchedule> {
    if stakes.is_empty() {
        bail!("calculate_leader_schedule stakes list is empty. no schedule can be calculated.");
    }
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&current_epoch_info.epoch.to_le_bytes());
    let mut stakes: Vec<_> = stakes
        .iter()
        .map(|(pubkey, stake)| (*pubkey, *stake))
        .collect();
    sort_stakes(&mut stakes);
    log::trace!("calculate_leader_schedule stakes:{stakes:?}");
    Ok(LeaderSchedule::new(
        &stakes,
        seed,
        current_epoch_info.slots_in_epoch,
        NUM_CONSECUTIVE_LEADER_SLOTS,
    ))
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

pub fn verify_schedule(schedule: LeaderSchedule, rpc_url: String) -> anyhow::Result<()> {
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let Some(rpc_leader_schedule) = rpc_client.get_leader_schedule(None)? else {
        log::info!("verify_schedule RPC return no schedule. Try later.");
        return Ok(());
    };

    log::info!("");

    let vote_account = rpc_client.get_vote_accounts()?;
    let node_vote_table = vote_account
        .current
        .into_iter()
        .chain(vote_account.delinquent.into_iter())
        .map(|va| (va.node_pubkey, va.vote_pubkey))
        .collect::<HashMap<String, String>>();

    //log::info!("node_vote_table:{node_vote_table:?}");

    //map leaderscheudle to HashMap<PubKey, Vec<slot>>
    let mut input_leader_schedule: HashMap<String, Vec<usize>> = HashMap::new();
    for (slot, pubkey) in schedule.get_slot_leaders().iter().copied().enumerate() {
        input_leader_schedule
            .entry(pubkey.to_string())
            .or_insert(vec![])
            .push(slot);
    }

    if let Err(err) = save_schedule_on_file("generated", &input_leader_schedule) {
        log::error!("Error during saving generated schedule:{err}");
    }

    //map rpc leader schedule node pubkey to vote account
    let mut rpc_leader_schedule: HashMap<String, Vec<usize>> = rpc_leader_schedule.into_iter().filter_map(|(pk, slots)| match node_vote_table.get(&pk) {
            Some(vote_account) => Some((vote_account.clone(),slots)),
            None => {
                log::warn!("verify_schedule RPC get_leader_schedule return some Node account:{pk} that are not mapped by rpc get_vote_accounts");
                None
            },
        }).collect();

    if let Err(err) = save_schedule_on_file("rpc", &rpc_leader_schedule) {
        log::error!("Error during saving generated schedule:{err}");
    } //log::trace!("verify_schedule calculated_leader_schedule:{input_leader_schedule:?} RPC leader schedule:{rpc_leader_schedule:?}");

    let mut vote_account_in_error: Vec<String> = input_leader_schedule
        .into_iter()
        .filter_map(|(input_vote_key, mut input_slot_list)| {
            let Some(mut rpc_strake_list) = rpc_leader_schedule.remove(&input_vote_key) else {
            log::warn!("verify_schedule vote account not found in RPC:{input_vote_key}");
            return Some(input_vote_key);
        };
            input_slot_list.sort();
            rpc_strake_list.sort();
            if input_slot_list
                .into_iter()
                .zip(rpc_strake_list.into_iter())
                .any(|(in_v, rpc)| in_v != rpc)
            {
                log::warn!("verify_schedule bad slots for {input_vote_key}"); // Caluclated:{input_slot_list:?} rpc:{rpc_strake_list:?}
                Some(input_vote_key)
            } else {
                None
            }
        })
        .collect();

    if !rpc_leader_schedule.is_empty() {
        log::warn!(
            "verify_schedule RPC vote account not present in calculated schedule:{:?}",
            rpc_leader_schedule.keys()
        );
        vote_account_in_error
            .append(&mut rpc_leader_schedule.keys().cloned().collect::<Vec<String>>());
    }

    log::info!("verify_schedule these account are wrong:{vote_account_in_error:?}");
    Ok(())
}

use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

fn save_schedule_on_file(name: &str, map: &HashMap<String, Vec<usize>>) -> anyhow::Result<()> {
    let serialized_map = serde_json::to_string(map).unwrap();

    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .unwrap_or(std::time::Duration::from_secs(1234));

    // Format the timestamp as seconds and sub-seconds (nanoseconds)
    let filename = format!(
        "schedule_{name}_{}_{}.txt",
        since_the_epoch.as_secs(),
        since_the_epoch.subsec_nanos()
    );

    // Write to the file
    let mut file = File::create(filename)?;
    file.write_all(serialized_map.as_bytes())?;
    //log::info!("Files: {file_name}");
    //log::info!("{}", serialized_map);
    Ok(())
}
