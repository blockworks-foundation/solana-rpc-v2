use crate::stakestore::{extract_stakestore, merge_stakestore, StakeMap, StakeStore};
use crate::votestore::{extract_votestore, merge_votestore, VoteMap, VoteStore};
use anyhow::bail;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

const SCHEDULE_STAKE_BASE_FILE_NAME: &str = "aggregate_export_votestake_";

pub const MAX_EPOCH_VALUE: u64 = 18446744073709551615;

#[derive(Debug, Default)]
pub struct CalculatedSchedule {
    pub current: Option<LeaderScheduleData>,
    pub next: Option<LeaderScheduleData>,
}

#[derive(Debug)]
pub struct LeaderScheduleData {
    pub schedule: Arc<HashMap<String, Vec<usize>>>,
    pub vote_stakes: Vec<(Pubkey, u64)>,
    pub epoch: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EpochStake {
    epoch: u64,
    stakes: Vec<(Pubkey, u64)>,
}

impl From<SavedStake> for EpochStake {
    fn from(saved_stakes: SavedStake) -> Self {
        EpochStake {
            epoch: saved_stakes.epoch,
            stakes: saved_stakes
                .stakes
                .into_iter()
                .map(|(pk, st)| (Pubkey::from_str(&pk).unwrap(), st))
                .collect(),
        }
    }
}

pub fn bootstrap_leader_schedule(
    current_file_patch: &str,
    next_file_patch: &str,
    slots_in_epoch: u64,
) -> anyhow::Result<CalculatedSchedule> {
    let mut current_stakes = read_schedule_vote_stakes(current_file_patch)?;
    let mut next_stakes = read_schedule_vote_stakes(next_file_patch)?;

    //calcualte leader schedule for all vote stakes.
    let current_schedule = calculate_leader_schedule(
        &mut current_stakes.stakes,
        current_stakes.epoch,
        slots_in_epoch,
    )?;
    let next_schedule =
        calculate_leader_schedule(&mut next_stakes.stakes, next_stakes.epoch, slots_in_epoch)?;

    Ok(CalculatedSchedule {
        current: Some(LeaderScheduleData {
            schedule: Arc::new(current_schedule),
            vote_stakes: current_stakes.stakes,
            epoch: current_stakes.epoch,
        }),
        next: Some(LeaderScheduleData {
            schedule: Arc::new(next_schedule),
            vote_stakes: next_stakes.stakes,
            epoch: next_stakes.epoch,
        }),
    })
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SavedStake {
    epoch: u64,
    stakes: Vec<(String, u64)>,
}

fn read_schedule_vote_stakes(file_path: &str) -> anyhow::Result<EpochStake> {
    let content = std::fs::read_to_string(file_path)?;
    let stakes_str: SavedStake = serde_json::from_str(&content)?;
    //convert to EpochStake because json hashmap parser can only have String key.
    Ok(stakes_str.into())
}

fn save_schedule_vote_stakes(
    base_file_path: &str,
    stakes: &Vec<(Pubkey, u64)>,
    epoch: u64,
) -> anyhow::Result<()> {
    //save new schedule for restart.
    let filename = format!("{base_file_path}_{epoch}.json",);
    let save_stakes = SavedStake {
        epoch,
        stakes: stakes
            .iter()
            .map(|(pk, st)| (pk.to_string(), *st))
            .collect(),
    };
    let serialized_stakes = serde_json::to_string(&save_stakes).unwrap();
    let mut file = File::create(filename).unwrap();
    file.write_all(serialized_stakes.as_bytes()).unwrap();
    file.flush().unwrap();
    Ok(())
}

/*
Leader schedule calculus state diagram

InitLeaderschedule
       |
   |extract store stake and vote|
     |                   |
   Error          CalculateScedule(stakes, votes)
     |                   |
 | Wait(1s)|       |Calculate schedule|
     |                       |
InitLeaderscedule        MergeStore(stakes, votes, schedule)
                             |                      |
                           Error                   SaveSchedule(schedule)
                             |                              |
              |never occurs restart (wait 1s)|         |save schedule and verify (opt)|
                             |
                      InitLeaderscedule
*/

pub fn run_leader_schedule_events(
    event: LeaderScheduleEvent,
    bootstrap_tasks: &mut FuturesUnordered<JoinHandle<LeaderScheduleEvent>>,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
) -> Option<(
    anyhow::Result<(HashMap<String, Vec<usize>>, Vec<(Pubkey, u64)>)>,
    EpochInfo,
)> {
    let result = process_leadershedule_event(event, stakestore, votestore);
    match result {
        LeaderScheduleResult::TaskHandle(jh) => {
            bootstrap_tasks.push(jh);
            None
        }
        LeaderScheduleResult::Event(event) => {
            run_leader_schedule_events(event, bootstrap_tasks, stakestore, votestore)
        }
        LeaderScheduleResult::End(schedule, epoch) => Some((schedule, epoch)),
    }
}

pub enum LeaderScheduleEvent {
    InitLeaderschedule(EpochInfo),
    CalculateScedule(StakeMap, VoteMap, EpochInfo),
    MergeStoreAndSaveSchedule(
        StakeMap,
        VoteMap,
        anyhow::Result<(HashMap<String, Vec<usize>>, Vec<(Pubkey, u64)>)>,
        EpochInfo,
    ),
}

enum LeaderScheduleResult {
    TaskHandle(JoinHandle<LeaderScheduleEvent>),
    Event(LeaderScheduleEvent),
    End(
        anyhow::Result<(HashMap<String, Vec<usize>>, Vec<(Pubkey, u64)>)>,
        EpochInfo,
    ),
}

//TODO remove desactivated account after leader schedule calculus.
fn process_leadershedule_event(
    event: LeaderScheduleEvent,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
) -> LeaderScheduleResult {
    match event {
        LeaderScheduleEvent::InitLeaderschedule(schedule_epoch) => {
            log::info!("LeaderScheduleEvent::InitLeaderschedule RECV");
            match (extract_stakestore(stakestore), extract_votestore(votestore)) {
                (Ok(stake_map), Ok(vote_map)) => LeaderScheduleResult::Event(
                    LeaderScheduleEvent::CalculateScedule(stake_map, vote_map, schedule_epoch),
                ),
                _ => {
                    log::warn!("process_leadershedule_event error during extract store");
                    let jh = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        LeaderScheduleEvent::InitLeaderschedule(schedule_epoch)
                    });
                    LeaderScheduleResult::TaskHandle(jh)
                }
            }
        }
        LeaderScheduleEvent::CalculateScedule(stake_map, vote_map, schedule_epoch) => {
            log::info!("LeaderScheduleEvent::CalculateScedule RECV");
            let jh = tokio::task::spawn_blocking({
                move || {
                    let schedule_and_stakes =
                        crate::leader_schedule::calculate_leader_schedule_from_stake_map(
                            &stake_map,
                            &vote_map,
                            &schedule_epoch,
                        );
                    if let Ok((_, vote_stakes)) = &schedule_and_stakes {
                        if let Err(err) = save_schedule_vote_stakes(
                            SCHEDULE_STAKE_BASE_FILE_NAME,
                            vote_stakes,
                            schedule_epoch.epoch,
                        ) {
                            log::error!(
                                "Error during saving in file of the new schedule of epoch:{} error:{err}",
                                schedule_epoch.epoch
                            );
                        }
                    }
                    log::info!("End calculate leader schedule");
                    LeaderScheduleEvent::MergeStoreAndSaveSchedule(
                        stake_map,
                        vote_map,
                        schedule_and_stakes,
                        schedule_epoch,
                    )
                }
            });
            LeaderScheduleResult::TaskHandle(jh)
        }
        LeaderScheduleEvent::MergeStoreAndSaveSchedule(
            stake_map,
            vote_map,
            schedule,
            schedule_epoch,
        ) => {
            log::info!("LeaderScheduleEvent::MergeStoreAndSaveSchedule RECV");
            match (
                merge_stakestore(stakestore, stake_map, schedule_epoch.epoch),
                merge_votestore(votestore, vote_map),
            ) {
                (Ok(()), Ok(())) => LeaderScheduleResult::End(schedule, schedule_epoch),
                _ => {
                    //this shoud never arrive because the store has been extracted before.
                    //TODO remove this error using type state
                    log::warn!("LeaderScheduleEvent::MergeStoreAndSaveSchedule merge stake or vote fail, -restart Schedule");
                    LeaderScheduleResult::Event(LeaderScheduleEvent::InitLeaderschedule(
                        schedule_epoch,
                    ))
                }
            }
        }
    }
}

fn calculate_leader_schedule_from_stake_map(
    stake_map: &crate::stakestore::StakeMap,
    vote_map: &crate::votestore::VoteMap,
    current_epoch_info: &EpochInfo,
) -> anyhow::Result<(HashMap<String, Vec<usize>>, Vec<(Pubkey, u64)>)> {
    let mut stakes = HashMap::<Pubkey, u64>::new();
    log::trace!(
        "calculate_leader_schedule_from_stake_map vote map len:{} stake map len:{}",
        vote_map.len(),
        stake_map.len()
    );

    let ten_epoch_slot_long = 10 * current_epoch_info.slots_in_epoch;

    //log::trace!("calculate_leader_schedule_from_stake_map stake_map:{stake_map:?} current_epoch_info:{current_epoch_info:?}");
    for storestake in stake_map.values() {
        //log::info!("Program_accounts stake:{stake:#?}");
        if is_stake_to_add(storestake.pubkey, &storestake.stake, &current_epoch_info) {
            // Add the stake in this stake account to the total for the delegated-to vote account
            //get nodeid for vote account
            let Some(vote_account) = vote_map.get(&storestake.stake.voter_pubkey) else {
                log::warn!(
                    "Vote account not found in vote map for stake vote account:{}",
                    &storestake.stake.voter_pubkey
                );
                continue;
            };
            //remove vote account that hasn't vote since 10 epoch.
            //on testnet the vote account CY7gjryUPV6Pwbsn4aArkMBL7HSaRHB8sPZUvhw558Tm node_id:6YpwLjgXcMWAj29govWQr87kaAGKS7CnoqWsEDJE4h8P
            //hasn't vote since a long time but still return on RPC call get_voteaccounts.
            //the validator don't use it for leader schedule.
            if vote_account.vote_data.root_slot.unwrap_or(0)
                < current_epoch_info
                    .absolute_slot
                    .saturating_sub(ten_epoch_slot_long)
            {
                log::warn!("Vote account:{} nodeid:{} that hasn't vote since 10 epochs. Remove leader_schedule."
                    , storestake.stake.voter_pubkey
                    ,vote_account.vote_data.node_pubkey
                );
            } else {
                *(stakes
                    .entry(vote_account.vote_data.node_pubkey)
                    .or_insert(0)) += storestake.stake.stake;
            }
        }
    }

    let mut schedule_stakes: Vec<(Pubkey, u64)> = vec![];
    schedule_stakes.extend(stakes.drain());

    let leader_schedule = calculate_leader_schedule(
        &mut schedule_stakes,
        current_epoch_info.epoch,
        current_epoch_info.slots_in_epoch,
    )?;
    Ok((leader_schedule, schedule_stakes))
}

fn is_stake_to_add(
    stake_pubkey: Pubkey,
    stake: &Delegation,
    current_epoch_info: &EpochInfo,
) -> bool {
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
        if stake.activation_epoch >= current_epoch_info.epoch {
            return false;
        }
        // Ignore stake accounts deactivated before this epoch
        if stake.deactivation_epoch < current_epoch_info.epoch {
            return false;
        }
    }
    true
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
fn calculate_leader_schedule(
    stakes: &mut Vec<(Pubkey, u64)>,
    epoch: u64,
    slots_in_epoch: u64,
) -> anyhow::Result<HashMap<String, Vec<usize>>> {
    if stakes.is_empty() {
        bail!("calculate_leader_schedule stakes list is empty. no schedule can be calculated.");
    }
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    sort_stakes(stakes);
    log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{epoch:?}");
    let schedule = LeaderSchedule::new(&stakes, seed, slots_in_epoch, NUM_CONSECUTIVE_LEADER_SLOTS);

    let slot_schedule = schedule
        .get_slot_leaders()
        .iter()
        .enumerate()
        .map(|(i, pk)| (pk.to_string(), i))
        .into_group_map()
        .into_iter()
        .collect();
    Ok(slot_schedule)
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
    log::info!("verify_schedule Start.");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(600),
        CommitmentConfig::confirmed(),
    );
    let Some(mut rpc_leader_schedule) = rpc_client.get_leader_schedule(None)? else {
        log::info!("verify_schedule RPC return no schedule. Try later.");
        return Ok(());
    };

    log::info!("");

    // let vote_account = rpc_client.get_vote_accounts()?;
    // let node_vote_table = vote_account
    //     .current
    //     .into_iter()
    //     .chain(vote_account.delinquent.into_iter())
    //     .map(|va| (va.node_pubkey, va.vote_pubkey))
    //     .collect::<HashMap<String, String>>();

    //log::info!("node_vote_table:{node_vote_table:?}");

    //map leaderscheudle to HashMap<PubKey, Vec<slot>>
    let mut input_leader_schedule: HashMap<String, Vec<usize>> = HashMap::new();
    for (slot, pubkey) in schedule.get_slot_leaders().iter().copied().enumerate() {
        input_leader_schedule
            .entry(pubkey.to_string())
            .or_insert(vec![])
            .push(slot);
    }

    let mut vote_account_in_error: Vec<String> = input_leader_schedule
        .into_iter()
        .filter_map(|(input_vote_key, mut input_slot_list)| {
            let Some(mut rpc_strake_list) = rpc_leader_schedule.remove(&input_vote_key) else {
                log::trace!("verify_schedule vote account not found in RPC:{input_vote_key}");
                return Some(input_vote_key);
            };
            input_slot_list.sort();
            rpc_strake_list.sort();
            if input_slot_list
                .into_iter()
                .zip(rpc_strake_list.into_iter())
                .any(|(in_v, rpc)| in_v != rpc)
            {
                log::trace!("verify_schedule bad slots for {input_vote_key}");
                Some(input_vote_key)
            } else {
                None
            }
        })
        .collect();

    if !rpc_leader_schedule.is_empty() {
        log::trace!(
            "verify_schedule RPC vote account not present in calculated schedule:{:?}",
            rpc_leader_schedule.keys()
        );
        vote_account_in_error
            .append(&mut rpc_leader_schedule.keys().cloned().collect::<Vec<String>>());
    }

    log::info!("verify_schedule these account are wrong:{vote_account_in_error:?}");
    //print_current_program_account(&rpc_client);
    Ok(())
}

pub fn save_schedule_on_file<T: serde::Serialize>(
    name: &str,
    map: &BTreeMap<String, T>,
) -> anyhow::Result<()> {
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

use borsh::BorshDeserialize;
use solana_sdk::stake::state::StakeState;
pub fn build_current_stakes(
    stake_map: &crate::stakestore::StakeMap,
    current_epoch_info: &EpochInfo,
    rpc_url: String,
    commitment: CommitmentConfig,
) -> BTreeMap<String, (u64, u64)> {
    // Fetch stakes in current epoch
    let rpc_client =
        RpcClient::new_with_timeout_and_commitment(rpc_url, Duration::from_secs(600), commitment);
    let response = rpc_client
        .get_program_accounts(&solana_sdk::stake::program::id())
        .unwrap();

    //log::trace!("get_program_accounts:{:?}", response);
    //use btreemap to always sort the same way.
    let mut stakes_aggregated = BTreeMap::<String, (u64, u64)>::new();
    let mut rpc_stakes = BTreeMap::<String, Vec<(String, u64, u64, u64)>>::new();
    for (pubkey, account) in response {
        // Zero-length accounts owned by the stake program are system accounts that were re-assigned and are to be
        // ignored
        if account.data.len() == 0 {
            continue;
        }

        match BorshDeserialize::deserialize(&mut account.data.as_slice()).unwrap() {
            StakeState::Stake(_, stake) => {
                //vote account version
                if is_stake_to_add(pubkey, &stake.delegation, current_epoch_info) {
                    // Add the stake in this stake account to the total for the delegated-to vote account
                    log::trace!("RPC Stake {pubkey} account:{account:?} stake:{stake:?}");
                    (stakes_aggregated
                        .entry(stake.delegation.voter_pubkey.to_string())
                        .or_insert((0, 0)))
                    .0 += stake.delegation.stake;
                    let st_list = rpc_stakes
                        .entry(stake.delegation.voter_pubkey.to_string())
                        .or_insert(vec![]);
                    st_list.push((
                        pubkey.to_string(),
                        stake.delegation.stake,
                        stake.delegation.activation_epoch,
                        stake.delegation.deactivation_epoch,
                    ));
                }
            }
            _ => (),
        }
    }
    let mut local_stakes = BTreeMap::<String, Vec<(String, u64, u64, u64)>>::new();
    stake_map
        .iter()
        .filter(|(pubkey, stake)| is_stake_to_add(**pubkey, &stake.stake, current_epoch_info))
        .for_each(|(pubkey, stake)| {
            // log::trace!(
            //     "LCOAL Stake {pubkey} account:{:?} stake:{stake:?}",
            //     stake.stake.voter_pubkey
            // );
            (stakes_aggregated
                .entry(stake.stake.voter_pubkey.to_string())
                .or_insert((0, 0)))
            .1 += stake.stake.stake;
            let st_list = local_stakes
                .entry(stake.stake.voter_pubkey.to_string().to_string())
                .or_insert(vec![]);
            st_list.push((
                pubkey.to_string(),
                stake.stake.stake,
                stake.stake.activation_epoch,
                stake.stake.deactivation_epoch,
            ));
        });

    //verify the list
    let diff_list: BTreeMap<String, (u64, u64)> = stakes_aggregated
        .iter()
        .filter(|(_, (rpc, local))| rpc != local)
        .map(|(pk, vals)| (pk.clone(), vals.clone()))
        .collect();
    if diff_list.len() > 0 {
        log::warn!(
            "VERIF Aggregated stakes list differs for vote accounts:{:?}",
            diff_list
        );
        //Print all associated stakes
        let rpc_diff_list: Vec<(&String, u64, u64, &Vec<(String, u64, u64, u64)>)> = diff_list
            .iter()
            .filter_map(|(pk, (rpc, local))| {
                rpc_stakes
                    .get(pk)
                    .map(|acc_list| ((pk, *rpc, *local, acc_list)))
            })
            .collect();
        let local_diff_list: Vec<(&String, u64, u64, &Vec<(String, u64, u64, u64)>)> = diff_list
            .iter()
            .filter_map(|(pk, (rpc, local))| {
                local_stakes
                    .get(pk)
                    .map(|acc_list| ((pk, *rpc, *local, acc_list)))
            })
            .collect();
        log::warn!("VERIF RPC accounts:{rpc_diff_list:?}",);
        log::warn!("VERIF LOCAL accounts:{local_diff_list:?}",);
    }
    stakes_aggregated
}
