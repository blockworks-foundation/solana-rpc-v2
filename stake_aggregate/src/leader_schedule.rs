use crate::stakestore::{extract_stakestore, merge_stakestore, StakeMap, StakeStore};
use crate::votestore::{extract_votestore, merge_votestore, VoteMap, VoteStore};
use anyhow::bail;
use futures::stream::FuturesUnordered;
use solana_client::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::JoinHandle;

pub const MAX_EPOCH_VALUE: u64 = 18446744073709551615;

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
) -> Option<(Option<LeaderSchedule>, EpochInfo)> {
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

pub struct LeaderScheduleData {
    pub leader_schedule: Option<LeaderSchedule>,
    pub schedule_epoch: EpochInfo,
}

pub enum LeaderScheduleEvent {
    InitLeaderschedule(EpochInfo),
    CalculateScedule(StakeMap, VoteMap, EpochInfo),
    MergeStoreAndSaveSchedule(StakeMap, VoteMap, Option<LeaderSchedule>, EpochInfo),
}

enum LeaderScheduleResult {
    TaskHandle(JoinHandle<LeaderScheduleEvent>),
    Event(LeaderScheduleEvent),
    End(Option<LeaderSchedule>, EpochInfo),
}

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
                    let schedule = crate::leader_schedule::calculate_leader_schedule_from_stake_map(
                        &stake_map,
                        &vote_map,
                        &schedule_epoch,
                    );
                    log::info!("End calculate leader schedule");
                    LeaderScheduleEvent::MergeStoreAndSaveSchedule(
                        stake_map,
                        vote_map,
                        schedule.ok(),
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
                    log::warn!("LeaderScheduleEvent::MergeStoreAndSaveSchedule merge stake or vote fail,  non extracted stake/vote map err, restart Schedule");
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
) -> anyhow::Result<LeaderSchedule> {
    let mut stakes = HashMap::<Pubkey, u64>::new();
    log::trace!(
        "calculate_leader_schedule_from_stake_map vote map len:{} stake map len:{}",
        vote_map.len(),
        stake_map.len()
    );
    //log::trace!("calculate_leader_schedule_from_stake_map stake_map:{stake_map:?} current_epoch_info:{current_epoch_info:?}");
    for storestake in stake_map.values() {
        //log::info!("Program_accounts stake:{stake:#?}");
        if is_stake_to_add(storestake.pubkey, &storestake.stake, &current_epoch_info) {
            // Add the stake in this stake account to the total for the delegated-to vote account
            //get nodeid for vote account
            let Some(nodeid) = vote_map.get(&storestake.stake.voter_pubkey).map(|v| v.vote_data.node_pubkey) else {
                log::warn!("Vote account not found in vote map for stake vote account:{}", &storestake.stake.voter_pubkey);
                continue;
            };
            *(stakes.entry(nodeid).or_insert(0)) += storestake.stake.stake;
        }
    }
    calculate_leader_schedule(stakes, current_epoch_info)
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
    log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{current_epoch_info:?}");
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

    // if let Err(err) = save_schedule_on_file("generated", &input_leader_schedule) {
    //     log::error!("Error during saving generated schedule:{err}");
    // }

    //map rpc leader schedule node pubkey to vote account
    // let mut rpc_leader_schedule: HashMap<String, Vec<usize>> = rpc_leader_schedule.into_iter().filter_map(|(pk, slots)| match node_vote_table.get(&pk) {
    //         Some(vote_account) => Some((vote_account.clone(),slots)),
    //         None => {
    //             log::warn!("verify_schedule RPC get_leader_schedule return some Node account:{pk} that are not mapped by rpc get_vote_accounts");
    //             None
    //         },
    //     }).collect();

    // if let Err(err) = save_schedule_on_file("rpc", &rpc_leader_schedule) {
    //     log::error!("Error during saving generated schedule:{err}");
    // } //log::trace!("verify_schedule calculated_leader_schedule:{input_leader_schedule:?} RPC leader schedule:{rpc_leader_schedule:?}");

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
                log::trace!("verify_schedule bad slots for {input_vote_key}"); // Caluclated:{input_slot_list:?} rpc:{rpc_strake_list:?}
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

use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

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
        RpcClient::new_with_timeout_and_commitment(rpc_url, Duration::from_secs(600), commitment); //CommitmentConfig::confirmed());
    let response = rpc_client
        .get_program_accounts(&solana_sdk::stake::program::id())
        .unwrap();

    //log::trace!("get_program_accounts:{:?}", response);
    //use btreemap to always sort the same way.
    let mut stakes_aggregated = BTreeMap::<String, (u64, u64)>::new();
    let mut rpc_stakes = BTreeMap::<String, Vec<solana_sdk::stake::state::Delegation>>::new();
    for (pubkey, account) in response {
        // Zero-length accounts owned by the stake program are system accounts that were re-assigned and are to be
        // ignored
        if account.data.len() == 0 {
            continue;
        }

        match StakeState::deserialize(&mut account.data.as_slice()).unwrap() {
            StakeState::Stake(_, stake) => {
                //vote account version
                if is_stake_to_add(pubkey, &stake.delegation, current_epoch_info) {
                    // Add the stake in this stake account to the total for the delegated-to vote account
                    log::trace!("RPC Stake {pubkey} account:{account:?} stake:{stake:?}");
                    (stakes_aggregated
                        .entry(stake.delegation.voter_pubkey.to_string())
                        .or_insert((0, 0)))
                    .0 += stake.delegation.stake;
                }
                let st_list = rpc_stakes.entry(pubkey.to_string()).or_insert(vec![]);
                st_list.push(stake.delegation);
            }
            _ => (),
        }
    }
    let mut local_stakes = BTreeMap::<String, Vec<solana_sdk::stake::state::Delegation>>::new();
    stake_map
        .iter()
        .filter(|(pubkey, stake)| is_stake_to_add(**pubkey, &stake.stake, current_epoch_info))
        .for_each(|(_, stake)| {
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
            st_list.push(stake.stake);
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
        let rpc_diff_list: Vec<(
            &String,
            u64,
            u64,
            &Vec<solana_sdk::stake::state::Delegation>,
        )> = diff_list
            .iter()
            .filter_map(|(pk, (rpc, local))| {
                rpc_stakes
                    .get(pk)
                    .map(|acc_list| ((pk, *rpc, *local, acc_list)))
            })
            .collect();
        let local_diff_list: Vec<(
            &String,
            u64,
            u64,
            &Vec<solana_sdk::stake::state::Delegation>,
        )> = diff_list
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
