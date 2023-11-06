use crate::epoch::CurrentEpochSlotState;
use crate::epoch::Epoch;
use crate::stakestore::{extract_stakestore, merge_stakestore, StakeMap, StakeStore};
use crate::votestore::StoredVote;
use crate::votestore::{extract_votestore, merge_votestore, VoteMap, VoteStore};
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_program::sysvar::epoch_schedule::EpochSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::feature_set::FeatureSet;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::StakeActivationStatus;
use solana_sdk::stake_history::StakeHistory;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

const SCHEDULE_STAKE_BASE_FILE_NAME: &str = "aggregate_export_votestake";

//pub const MAX_EPOCH_VALUE: u64 = 18446744073709551615;

#[derive(Debug, Default)]
pub struct CalculatedSchedule {
    pub current: Option<LeaderScheduleData>,
    pub next: Option<LeaderScheduleData>,
}

#[derive(Debug)]
pub struct LeaderScheduleData {
    pub schedule: Arc<HashMap<String, Vec<usize>>>,
    pub vote_stakes: HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    pub epoch: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EpochStake {
    epoch: u64,
    stake_vote_map: HashMap<Pubkey, (u64, Arc<StoredVote>)>,
}

impl From<SavedStake> for EpochStake {
    fn from(saved_stakes: SavedStake) -> Self {
        EpochStake {
            epoch: saved_stakes.epoch,
            stake_vote_map: saved_stakes
                .stake_vote_map
                .into_iter()
                .map(|(pk, st)| (Pubkey::from_str(&pk).unwrap(), (st.0, st.1)))
                .collect(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct SavedStake {
    epoch: u64,
    stake_vote_map: HashMap<String, (u64, Arc<StoredVote>)>,
}

pub fn bootstrap_leader_schedule(
    current_file_patch: &str,
    next_file_patch: &str,
    epoch_state: &CurrentEpochSlotState,
) -> anyhow::Result<CalculatedSchedule> {
    let current_stakes = read_schedule_vote_stakes(current_file_patch)?;
    let next_stakes = read_schedule_vote_stakes(next_file_patch)?;

    //calcualte leader schedule for all vote stakes.
    let current_schedule = calculate_leader_schedule(
        &current_stakes.stake_vote_map,
        current_stakes.epoch,
        epoch_state.get_slots_in_epoch(current_stakes.epoch),
    );
    let next_schedule = calculate_leader_schedule(
        &next_stakes.stake_vote_map,
        next_stakes.epoch,
        epoch_state.get_slots_in_epoch(next_stakes.epoch),
    );

    Ok(CalculatedSchedule {
        current: Some(LeaderScheduleData {
            schedule: Arc::new(current_schedule),
            vote_stakes: current_stakes.stake_vote_map,
            epoch: epoch_state.get_slots_in_epoch(current_stakes.epoch),
        }),
        next: Some(LeaderScheduleData {
            schedule: Arc::new(next_schedule),
            vote_stakes: next_stakes.stake_vote_map,
            epoch: epoch_state.get_slots_in_epoch(next_stakes.epoch),
        }),
    })
}

fn read_schedule_vote_stakes(file_path: &str) -> anyhow::Result<EpochStake> {
    let content = std::fs::read_to_string(file_path)?;
    let stakes_str: SavedStake = serde_json::from_str(&content)?;
    //convert to EpochStake because json hashmap parser can only have String key.
    Ok(stakes_str.into())
}

fn save_schedule_vote_stakes(
    base_file_path: &str,
    stake_vote_map: &HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    epoch: u64,
) -> anyhow::Result<()> {
    //save new schedule for restart.
    let filename = format!("{base_file_path}_{}.json", epoch);
    let save_stakes = SavedStake {
        epoch,
        stake_vote_map: stake_vote_map
            .iter()
            .map(|(pk, st)| (pk.to_string(), (st.0, Arc::clone(&st.1))))
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
) -> Option<LeaderScheduleData> {
    let result = process_leadershedule_event(event, stakestore, votestore);
    match result {
        LeaderScheduleResult::TaskHandle(jh) => {
            bootstrap_tasks.push(jh);
            None
        }
        LeaderScheduleResult::Event(event) => {
            run_leader_schedule_events(event, bootstrap_tasks, stakestore, votestore)
        }
        LeaderScheduleResult::End(schedule) => Some(schedule),
    }
}

// struct LeaderScheduleEpochData {
//     current_epoch: u64,
//     next_epoch: u64,
//     slots_in_epoch: u64,
//     epoch_schedule: Arc<EpochSchedule>,
// }

pub enum LeaderScheduleEvent {
    Init(u64, u64, Arc<EpochSchedule>),
    MergeStoreAndSaveSchedule(
        StakeMap,
        VoteMap,
        LeaderScheduleData,
        (u64, u64, Arc<EpochSchedule>),
        Option<StakeHistory>,
    ),
}

enum LeaderScheduleResult {
    TaskHandle(JoinHandle<LeaderScheduleEvent>),
    Event(LeaderScheduleEvent),
    End(LeaderScheduleData),
}

pub fn create_schedule_init_event(
    current_epoch: u64,
    slots_in_epoch: u64,
    epoch_schedule: Arc<EpochSchedule>,
) -> LeaderScheduleEvent {
    log::info!("LeaderScheduleEvent::InitLeaderschedule START");
    //TODO get a way to be updated of stake history.
    //request the current state using RPC.
    //TODO remove the await from the scheduler task.
    // let before_stake_histo = stake_history.clone();
    // let stake_history = crate::bootstrap::get_stakehistory_account(rpc_url)
    //     .map(|account| crate::stakestore::read_historystake_from_account(account))
    //     .unwrap_or_else(|_| {
    //         log::error!("Error during stake history fetch. Use bootstrap one");
    //         stake_history.map(|st| st.as_ref().clone())
    //     });
    // log::info!(
    //     "stake_hsitorique epoch{} before:{before_stake_histo:?} after:{stake_history:?}",
    //     epoch_data.current_epoch
    // );
    LeaderScheduleEvent::Init(current_epoch, slots_in_epoch, epoch_schedule)
}

//TODO remove desactivated account after leader schedule calculus.
fn process_leadershedule_event(
    //    rpc_url: String,
    event: LeaderScheduleEvent,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
) -> LeaderScheduleResult {
    match event {
        LeaderScheduleEvent::Init(new_epoch, slots_in_epoch, epoch_schedule) => {
            match (extract_stakestore(stakestore), extract_votestore(votestore)) {
                (Ok((stake_map, mut stake_history)), Ok(vote_map)) => {
                    //For test TODO put in extract and restore process to avoid to clone.
                    log::info!("LeaderScheduleEvent::CalculateScedule");
                    let epoch_schedule = Arc::clone(&epoch_schedule);
                    let jh = tokio::task::spawn_blocking({
                        move || {
                            let epoch_vote_stakes = calculate_epoch_stakes(
                                &stake_map,
                                &vote_map,
                                new_epoch,
                                stake_history.as_mut(),
                                &epoch_schedule,
                            );

                            //calculate the schedule for next epoch using the current epoch start stake.
                            let next_epoch = new_epoch + 1;
                            let leader_schedule = calculate_leader_schedule(
                                &epoch_vote_stakes,
                                next_epoch,
                                slots_in_epoch,
                            );

                            // let epoch_vote_stakes_old = calculate_epoch_stakes_old_algo(
                            //     &stake_map,
                            //     &vote_map,
                            //     &epoch_state.next_epoch,
                            //     stake_history.as_ref(),
                            // );

                            // let leader_schedule_old =
                            //     calculate_leader_schedule(&epoch_vote_stakes_old, &next_epoch);
                            if let Err(err) = save_schedule_vote_stakes(
                                SCHEDULE_STAKE_BASE_FILE_NAME,
                                &epoch_vote_stakes,
                                next_epoch,
                            ) {
                                log::error!(
                                "Error during saving in file of the new schedule of epoch:{} error:{err}",
                                next_epoch
                            );
                            }
                            log::info!("End calculate leader schedule");
                            LeaderScheduleEvent::MergeStoreAndSaveSchedule(
                                stake_map,
                                vote_map,
                                LeaderScheduleData {
                                    schedule: Arc::new(leader_schedule),
                                    vote_stakes: epoch_vote_stakes,
                                    epoch: next_epoch,
                                },
                                (new_epoch, slots_in_epoch, epoch_schedule),
                                stake_history,
                            )
                        }
                    });
                    LeaderScheduleResult::TaskHandle(jh)
                }
                _ => {
                    log::error!("Create leadershedule init event error during extract store");
                    LeaderScheduleResult::Event(LeaderScheduleEvent::Init(
                        new_epoch,
                        slots_in_epoch,
                        epoch_schedule,
                    ))
                }
            }
        }
        LeaderScheduleEvent::MergeStoreAndSaveSchedule(
            stake_map,
            vote_map,
            schedule_data,
            (new_epoch, slots_in_epoch, epoch_schedule),
            stake_history,
        ) => {
            log::info!("LeaderScheduleEvent::MergeStoreAndSaveSchedule RECV");
            match (
                merge_stakestore(stakestore, stake_map, stake_history),
                merge_votestore(votestore, vote_map),
            ) {
                (Ok(()), Ok(())) => LeaderScheduleResult::End(schedule_data),
                _ => {
                    //this shoud never arrive because the store has been extracted before.
                    //TODO remove this error using type state
                    log::warn!("LeaderScheduleEvent::MergeStoreAndSaveSchedule merge stake or vote fail, -restart Schedule");
                    LeaderScheduleResult::Event(LeaderScheduleEvent::Init(
                        new_epoch,
                        slots_in_epoch,
                        epoch_schedule,
                    ))
                }
            }
        }
    }
}

//     /// get the epoch for which the given slot should save off
// ///  information about stakers
// pub fn get_leader_schedule_epoch(&self, slot: Slot) -> Epoch {
//     if slot < self.first_normal_slot {
//         // until we get to normal slots, behave as if leader_schedule_slot_offset == slots_per_epoch
//         self.get_epoch_and_slot_index(slot).0.saturating_add(1)
//     } else {
//         let new_slots_since_first_normal_slot = slot.saturating_sub(self.first_normal_slot);
//         let new_first_normal_leader_schedule_slot =
//             new_slots_since_first_normal_slot.saturating_add(self.leader_schedule_slot_offset);
//         let new_epochs_since_first_normal_leader_schedule =
//             new_first_normal_leader_schedule_slot
//                 .checked_div(self.slots_per_epoch)
//                 .unwrap_or(0);
//         self.first_normal_epoch
//             .saturating_add(new_epochs_since_first_normal_leader_schedule)
//     }
// }

fn calculate_epoch_stakes(
    stake_map: &StakeMap,
    vote_map: &VoteMap,
    new_epoch: u64,
    mut stake_history: Option<&mut StakeHistory>,
    epoch_schedule: &EpochSchedule,
) -> HashMap<Pubkey, (u64, Arc<StoredVote>)> {
    //update stake history with current end epoch stake values.
    let new_rate_activation_epoch =
        FeatureSet::default().new_warmup_cooldown_rate_epoch(epoch_schedule);

    log::info!("calculate_epoch_stakes new_epoch:{new_epoch}",);

    let ended_epoch = new_epoch - 1;
    //update stake history for the ended epoch.
    let stake_history_entry =
        stake_map
            .values()
            .fold(StakeActivationStatus::default(), |acc, stake_account| {
                let delegation = stake_account.stake;
                acc + delegation.stake_activating_and_deactivating(
                    ended_epoch,
                    stake_history.as_deref(),
                    new_rate_activation_epoch,
                )
            });
    match stake_history {
        Some(ref mut stake_history) => {
            log::info!(
                "Stake_history add epoch{ended_epoch} stake history:{stake_history_entry:?}"
            );
            stake_history.add(ended_epoch, stake_history_entry)
        }
        None => log::warn!("Vote stake calculus without Stake History"),
    };

    //Done for verification not in the algo
    //get current stake history
    match crate::bootstrap::get_stakehistory_account(crate::RPC_URL.to_string())
        .ok()
        .and_then(|rpc_history_account| {
            crate::stakestore::read_historystake_from_account(rpc_history_account)
        }) {
        Some(rpc_history) => {
            log::info!(
                "Stake_history ended epoch:{ended_epoch} C:{:?} RPC:{:?}",
                stake_history.as_ref().map(|h| h.get(ended_epoch)),
                rpc_history.get(ended_epoch)
            );
            log::info!(
                "Stake_history last epoch:{} C:{:?} RPC:{:?}",
                ended_epoch - 1,
                stake_history.as_ref().map(|h| h.get(ended_epoch - 1)),
                rpc_history.get(ended_epoch - 1)
            );
        }
        None => log::error!("can't get rpc history from RPC"),
    }

    //calculate schedule stakes.
    let delegated_stakes: HashMap<Pubkey, u64> =
        stake_map
            .values()
            .fold(HashMap::default(), |mut delegated_stakes, stake_account| {
                let delegation = stake_account.stake;
                let entry = delegated_stakes.entry(delegation.voter_pubkey).or_default();
                *entry += delegation.stake(
                    new_epoch,
                    stake_history.as_deref(),
                    new_rate_activation_epoch,
                );
                delegated_stakes
            });

    let staked_vote_map: HashMap<Pubkey, (u64, Arc<StoredVote>)> = vote_map
        .values()
        .map(|vote_account| {
            let delegated_stake = delegated_stakes
                .get(&vote_account.pubkey)
                .copied()
                .unwrap_or_else(|| {
                    log::info!(
                        "calculate_epoch_stakes stake with no vote account:{}",
                        vote_account.pubkey
                    );
                    Default::default()
                });
            (vote_account.pubkey, (delegated_stake, vote_account.clone()))
        })
        .collect();
    staked_vote_map
}

fn calculate_epoch_stakes_old_algo(
    stake_map: &StakeMap,
    vote_map: &VoteMap,
    next_epoch: &Epoch,
    stake_history: Option<&StakeHistory>,
) -> HashMap<Pubkey, (u64, Arc<StoredVote>)> {
    let mut stakes = HashMap::<Pubkey, (u64, Arc<StoredVote>)>::new();
    log::info!(
        "calculate_leader_schedule_from_stake_map vote map len:{} stake map len:{} history len:{:?}",
        vote_map.len(),
        stake_map.len(),
        stake_history.as_ref().map(|h| h.len())
    );

    let ten_epoch_slot_long = 10 * next_epoch.slots_in_epoch;

    //log::trace!("calculate_leader_schedule_from_stake_map stake_map:{stake_map:?} current_epoch_info:{current_epoch_info:?}");
    for storestake in stake_map.values() {
        //get nodeid for vote account
        let Some(vote_account) = vote_map.get(&storestake.stake.voter_pubkey) else {
            log::info!(
                "Vote account not found in vote map for stake vote account:{}",
                &storestake.stake.voter_pubkey
            );
            continue;
        };
        //TODO validate the number of epoch.
        //remove vote account that hasn't vote since 10 epoch.
        //on testnet the vote account CY7gjryUPV6Pwbsn4aArkMBL7HSaRHB8sPZUvhw558Tm node_id:6YpwLjgXcMWAj29govWQr87kaAGKS7CnoqWsEDJE4h8P
        //hasn't vote since a long time but still return on RPC call get_voteaccounts.
        //the validator don't use it for leader schedule.
        if vote_account.vote_data.root_slot.unwrap_or_else(|| {
            //vote with no root_slot are added. They have just been activated and can have active stake- TODO TO TEST.
            log::info!(
                "leader schedule vote:{} with None root_slot, add it",
                vote_account.pubkey
            );
            next_epoch.absolute_slot
        }) < next_epoch.absolute_slot.saturating_sub(ten_epoch_slot_long)
        {
            log::info!("Vote account:{} nodeid:{} that hasn't vote since 10 epochs. Stake for account:{:?}. Remove leader_schedule."
                    , storestake.stake.voter_pubkey
                    ,vote_account.vote_data.node_pubkey
                    //TODO us the right reduce_stake_warmup_cooldown_epoch value from validator feature.
                    , storestake.stake.stake(next_epoch.epoch, stake_history, Some(0)),
                );
        } else {
            let effective_stake = storestake
                .stake
                //TODO us the right reduce_stake_warmup_cooldown_epoch value from validator feature.
                .stake(next_epoch.epoch, stake_history, Some(0));

            stakes
                .entry(vote_account.pubkey)
                .or_insert((0, vote_account.clone()))
                .0 += effective_stake;

            // //only vote account with positive stake are use for the schedule.
            // if effective_stake > 0 {
            //     *(stakes
            //         .entry(vote_account.vote_data.node_pubkey)
            //         .or_insert(0)) += effective_stake;
            // } else {
            //     log::info!(
            //         "leader schedule vote:{} with 0 effective vote",
            //         storestake.stake.voter_pubkey
            //     );
            // }
        }
    }
    stakes
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
fn calculate_leader_schedule(
    stake_vote_map: &HashMap<Pubkey, (u64, Arc<StoredVote>)>,
    epoch: u64,
    slots_in_epoch: u64,
) -> HashMap<String, Vec<usize>> {
    let stakes_map: HashMap<Pubkey, u64> = stake_vote_map
        .iter()
        .filter_map(|(_, (stake, vote_account))| {
            (*stake != 0u64).then_some((vote_account.vote_data.node_pubkey, *stake))
        })
        .into_grouping_map()
        .aggregate(|acc, _node_pubkey, stake| Some(acc.unwrap_or_default() + stake));
    let mut stakes: Vec<(Pubkey, u64)> = stakes_map
        .into_iter()
        .map(|(key, stake)| (key, stake))
        .collect();

    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    sort_stakes(&mut stakes);
    log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{epoch}");
    let schedule = LeaderSchedule::new(&stakes, seed, slots_in_epoch, NUM_CONSECUTIVE_LEADER_SLOTS);

    let slot_schedule = schedule
        .get_slot_leaders()
        .iter()
        .enumerate()
        .map(|(i, pk)| (pk.to_string(), i))
        .into_group_map()
        .into_iter()
        .collect();
    slot_schedule
}

// fn calculate_leader_schedule_from_stake_map(
//     stake_map: &crate::stakestore::StakeMap,
//     vote_map: &crate::votestore::VoteMap,
//     current_epoch_info: &EpochInfo,
//     stake_history: Option<&StakeHistory>,
// ) -> anyhow::Result<(HashMap<String, Vec<usize>>, Vec<(Pubkey, u64)>)> {
//     let mut stakes = HashMap::<Pubkey, u64>::new();
//     log::info!(
//         "calculate_leader_schedule_from_stake_map vote map len:{} stake map len:{} history len:{:?}",
//         vote_map.len(),
//         stake_map.len(),
//         stake_history.as_ref().map(|h| h.len())
//     );

//     let ten_epoch_slot_long = 10 * current_epoch_info.slots_in_epoch;

//     //log::trace!("calculate_leader_schedule_from_stake_map stake_map:{stake_map:?} current_epoch_info:{current_epoch_info:?}");
//     for storestake in stake_map.values() {
//         //get nodeid for vote account
//         let Some(vote_account) = vote_map.get(&storestake.stake.voter_pubkey) else {
//             log::info!(
//                 "Vote account not found in vote map for stake vote account:{}",
//                 &storestake.stake.voter_pubkey
//             );
//             continue;
//         };
//         //TODO validate the number of epoch.
//         //remove vote account that hasn't vote since 10 epoch.
//         //on testnet the vote account CY7gjryUPV6Pwbsn4aArkMBL7HSaRHB8sPZUvhw558Tm node_id:6YpwLjgXcMWAj29govWQr87kaAGKS7CnoqWsEDJE4h8P
//         //hasn't vote since a long time but still return on RPC call get_voteaccounts.
//         //the validator don't use it for leader schedule.
//         if vote_account.vote_data.root_slot.unwrap_or_else(|| {
//             //vote with no root_slot are added. They have just been activated and can have active stake- TODO TO TEST.
//             log::info!(
//                 "leader schedule vote:{} with None root_slot, add it",
//                 vote_account.pubkey
//             );
//             current_epoch_info.absolute_slot
//         }) < current_epoch_info
//             .absolute_slot
//             .saturating_sub(ten_epoch_slot_long)
//         {
//             log::info!("Vote account:{} nodeid:{} that hasn't vote since 10 epochs. Stake for account:{:?}. Remove leader_schedule."
//                     , storestake.stake.voter_pubkey
//                     ,vote_account.vote_data.node_pubkey
//                     //TODO us the right reduce_stake_warmup_cooldown_epoch value from validator feature.
//                     , storestake.stake.stake(current_epoch_info.epoch, stake_history, Some(0)),
//                 );
//         } else {
//             let effective_stake = storestake
//                 .stake
//                 //TODO us the right reduce_stake_warmup_cooldown_epoch value from validator feature.
//                 .stake(current_epoch_info.epoch, stake_history, Some(0));
//             //only vote account with positive stake are use for the schedule.
//             if effective_stake > 0 {
//                 *(stakes
//                     .entry(vote_account.vote_data.node_pubkey)
//                     .or_insert(0)) += effective_stake;
//             } else {
//                 log::info!(
//                     "leader schedule vote:{} with 0 effective vote",
//                     storestake.stake.voter_pubkey
//                 );
//             }
//         }
//     }

//     let mut schedule_stakes: Vec<(Pubkey, u64)> = vec![];
//     schedule_stakes.extend(stakes.drain());

//     let leader_schedule = calculate_leader_schedule_old(
//         &mut schedule_stakes,
//         current_epoch_info.epoch,
//         current_epoch_info.slots_in_epoch,
//     )?;
//     Ok((leader_schedule, schedule_stakes))
// }

// fn is_stake_to_add(
//     stake_pubkey: Pubkey,
//     stake: &Delegation,
//     current_epoch_info: &EpochInfo,
// ) -> bool {
//     //On test validator all stakes are attributes to an account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE.
//     //It's considered as activated stake.
//     if stake.activation_epoch == MAX_EPOCH_VALUE {
//         log::info!(
//             "Found account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE use it: {}",
//             stake_pubkey.to_string()
//         );
//     } else {
//         // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
//         // u64::MAX which indicates no activation ever happened)
//         if stake.activation_epoch >= current_epoch_info.epoch {
//             return false;
//         }
//         // Ignore stake accounts deactivated before this epoch
//         if stake.deactivation_epoch < current_epoch_info.epoch {
//             return false;
//         }
//     }
//     true
// }

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
// fn calculate_leader_schedule_old(
//     stakes: &mut Vec<(Pubkey, u64)>,
//     epoch: u64,
//     slots_in_epoch: u64,
// ) -> anyhow::Result<HashMap<String, Vec<usize>>> {
//     if stakes.is_empty() {
//         bail!("calculate_leader_schedule stakes list is empty. no schedule can be calculated.");
//     }
//     let mut seed = [0u8; 32];
//     seed[0..8].copy_from_slice(&epoch.to_le_bytes());
//     sort_stakes(stakes);
//     log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{epoch:?}");
//     let schedule = LeaderSchedule::new(&stakes, seed, slots_in_epoch, NUM_CONSECUTIVE_LEADER_SLOTS);

//     let slot_schedule = schedule
//         .get_slot_leaders()
//         .iter()
//         .enumerate()
//         .map(|(i, pk)| (pk.to_string(), i))
//         .into_group_map()
//         .into_iter()
//         .collect();
//     Ok(slot_schedule)
// }

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
    stake_history: Option<&StakeHistory>,
    current_epoch: u64,
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
                let effective_stake = stake
                    .delegation
                    .stake(current_epoch, stake_history, Some(0));
                if effective_stake > 0 {
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
                        effective_stake,
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
        .filter_map(|(pubkey, stake)| {
            let effective_stake = stake.stake.stake(current_epoch, stake_history, Some(0));
            (effective_stake > 0).then(|| (pubkey, stake, effective_stake))
        })
        .for_each(|(pubkey, stake, effective_stake)| {
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
                effective_stake,
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
