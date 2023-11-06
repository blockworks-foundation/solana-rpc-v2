//cargo run 1 10 23 15 for 1d 10h 23mn 15s

use borsh::BorshDeserialize;
use chrono::{Datelike, Local, NaiveDate, NaiveTime, Timelike};
use core::str::FromStr;
use serde_json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::StakeState;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime, UtcOffset};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

//const RPC_URL: &str = "https://api.mainnet-beta.solana.com";
//const RPC_URL: &str = "https://api.testnet.solana.com";
//const RPC_URL: &str = "https://api.devnet.solana.com";
//const RPC_URL: &str = "http://localhost:8899";
const RPC_URL: &str = "http://192.168.88.31:8899";

//const SLOTS_IN_EPOCH: u64 = 432000;
const MAX_EPOCH_VALUE: u64 = 18446744073709551615;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!("Please provide 3 arguments: hour, minute and seconds");
        std::process::exit(1);
    }

    let day: u64 = args[1]
        .parse()
        .expect("First argument should be a number representing the hour");
    let hour: u64 = args[2]
        .parse()
        .expect("First argument should be a number representing the hour");
    let minute: u64 = args[3]
        .parse()
        .expect("Second argument should be a number representing the minute");
    let second: u64 = args[4]
        .parse()
        .expect("Third argument should be a number representing the seconds");

    let seconds_until_target = day * 24 * 3600 + hour * 3600 + minute * 60 + second;
    log::info!("seconds_until_target:{}", seconds_until_target);
    let to_wait = Duration::from_secs(seconds_until_target as u64);
    tokio::time::sleep(to_wait).await;

    let mut counter = 0;
    let mut schedule_counter = 0;
    let mut epoch_offset = 0;

    loop {
        match write_schedule(0).await {
            Ok(()) => {
                epoch_offset = 0;
                schedule_counter += 1;
                if schedule_counter == 3 {
                    break;
                }
                break;
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Err(err) => {
                log::error!("error:{err}");
                tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;
                counter += 1;
                if counter == 5 {
                    break;
                }
            }
        }
    }
    Ok(())
}

async fn save_map(file_name: &str, map: &BTreeMap<String, (u64, u64, u64)>) -> anyhow::Result<()> {
    let serialized_map = serde_json::to_string(map).unwrap();
    // Write to the file
    let mut file = File::create(file_name).await?;
    file.write_all(serialized_map.as_bytes()).await?;
    //log::info!("Files: {file_name}");
    //log::info!("{}", serialized_map);
    Ok(())
}

async fn write_schedule(epoch_offset: u64) -> anyhow::Result<()> {
    log::info!("start schedule calculus process");
    let (schedule, stakes_aggregated) = process_schedule(epoch_offset).await?;
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
    let file_name = format!("out_sc_{}.json", date_string);
    save_map(&file_name, &schedule).await?;
    let file_name = format!("out_st_{}.json", date_string);
    //filter with stake diff accont.
    let stakes_aggregated: BTreeMap<String, (u64, u64, u64)> = stakes_aggregated
        .into_iter()
        .filter_map(|(pk, (pa, va))| (pa != va).then_some((pk, (0u64, pa, va))))
        .collect();
    save_map(&file_name, &stakes_aggregated).await?;

    //show all schedule aggregated.
    // let mut print_finalized = schedule
    //     .into_iter()
    //     .map(|(key, values)| format!("{key}:{:?}", values))
    //     .collect::<Vec<String>>();
    // print_finalized.sort();
    //log::info!("leader_schedule_finalized:{:?}", print_finalized);
    Ok(())
}

async fn process_schedule(
    epoch_offset: u64,
) -> anyhow::Result<(
    BTreeMap<String, (u64, u64, u64)>,
    BTreeMap<String, (u64, u64)>,
)> {
    let rpc_client =
        RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::finalized());

    let slot = rpc_client.get_slot().await?;

    // Fetch current epoch
    let epoch_info = rpc_client.get_epoch_info().await?;
    log::info!("process_schedule current_epoch:{epoch_info:?}");

    let current_epoch = epoch_info.epoch;
    log::info!("current_slot:{slot:?}");
    log::info!("epoch_info:{epoch_info:?}");

    let call_program_account = true;
    let pa_stakes = if call_program_account {
        let mut stakes = HashMap::<Pubkey, u64>::new();
        // Fetch stakes in current epoch
        let response = rpc_client
            .get_program_accounts(&solana_sdk::stake::program::id())
            .await?;

        //log::info!("get_program_accounts:{:?}", response);

        for (pubkey, account) in response {
            // Zero-length accounts owned by the stake program are system accounts that were re-assigned and are to be
            // ignored
            if account.data.len() == 0 {
                continue;
            }

            match StakeState::deserialize(&mut account.data.as_slice())? {
                StakeState::Stake(_, stake) => {
                    //log::info!("Program_accounts stake:{stake:#?}");
                    //On test validator all stakes are attributes to an account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE.
                    //It's considered as activated stake.
                    if stake.delegation.activation_epoch == MAX_EPOCH_VALUE {
                        log::info!("Found account with stake.delegation.activation_epoch == MAX_EPOCH_VALUE use it: {}", pubkey.to_string());
                    } else {
                        // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
                        // u64::MAX which indicates no activation ever happened)
                        if stake.delegation.activation_epoch >= current_epoch {
                            continue;
                        }
                        // Ignore stake accounts deactivated before this epoch
                        if stake.delegation.deactivation_epoch < current_epoch {
                            continue;
                        }
                    }

                    // Add the stake in this stake account to the total for the delegated-to vote account
                    log::info!("Stake {pubkey} account:{account:?} stake:{stake:?} ");
                    *(stakes
                        .entry(stake.delegation.voter_pubkey.clone())
                        .or_insert(0)) += stake.delegation.stake;
                }
                _ => (),
            }
        }
        stakes
    } else {
        //Put a dummy value if no PA stake are available.
        let mut stakes = HashMap::<Pubkey, u64>::new();
        stakes.insert(
            Pubkey::from_str("9C4UgjzAch2ZTaBnjCyJ4oXLBnkyZZqdaB6fmRdPuHeD").unwrap(),
            10,
        );
        stakes
    };

    log::info!("PA Stakes:{pa_stakes:?}");

    let mut stakes_aggregated: BTreeMap<String, (u64, u64)> = pa_stakes
        .iter()
        .map(|(pk, stake)| (pk.to_string(), (*stake, 0u64)))
        .collect();

    let leader_schedule = calculate_leader_schedule(
        current_epoch + epoch_offset,
        pa_stakes,
        epoch_info.slots_in_epoch,
    );

    let mut leader_schedule_aggregated: BTreeMap<String, (u64, u64, u64)> = leader_schedule
        .get_slot_leaders()
        .iter()
        .fold(BTreeMap::new(), |mut sc, l| {
            sc.entry(l.to_string()).or_insert((0, 0, 0)).1 += 1;
            sc
        });
    // for (leader, nb) in leader_schedule_aggregated {
    //     println!("{leader}:{nb}");
    // }

    //let _ = verify_schedule(leader_schedule, RPC_URL.to_string()).await;

    //build vote account node key association table
    let vote_account = rpc_client.get_vote_accounts().await?;
    let note_vote_table = vote_account
        .current
        .iter()
        .chain(vote_account.delinquent.iter())
        .map(|va| (va.node_pubkey.clone(), va.vote_pubkey.clone()))
        .collect::<HashMap<String, String>>();

    log::info!("VOTE Stakes:{vote_account:#?}");

    //get leader schedule from rpc
    let leader_schedule_finalized = rpc_client.get_leader_schedule(Some(slot)).await?; //Some(slot)

    let binding = "Vote key not found".to_string();
    leader_schedule_finalized
        .unwrap()
        .into_iter()
        .for_each(|(key, slots)| {
            let vote_key = note_vote_table.get(&key.to_string()).unwrap_or(&binding);
            leader_schedule_aggregated
                .entry(vote_key.clone())
                .or_insert((0, 0, 0))
                .0 += slots.len() as u64
        });

    //build schedule from vote account.
    let vote_stackes: HashMap<Pubkey, u64> = vote_account
        .current
        .iter()
        .chain(vote_account.delinquent.iter())
        .map(|va| {
            (
                Pubkey::from_str(&va.vote_pubkey).unwrap(),
                va.activated_stake,
            )
        })
        .collect();

    vote_stackes.iter().for_each(|(pk, stake)| {
        stakes_aggregated.entry(pk.to_string()).or_insert((0, 0)).1 = *stake;
    });

    let leader_schedule_va = calculate_leader_schedule(
        current_epoch + epoch_offset,
        vote_stackes,
        epoch_info.slots_in_epoch,
    );
    leader_schedule_va.get_slot_leaders().iter().for_each(|l| {
        leader_schedule_aggregated
            .entry(l.to_string())
            .or_insert((0, 0, 0))
            .2 += 1;
    });

    //verify VA schedule
    let _ = verify_schedule(leader_schedule_va, &note_vote_table, RPC_URL.to_string()).await;

    // log::info!(
    //     "vote account current:{:?}",
    //     vote_account
    //         .current
    //         .iter()
    //         .map(|va| format!("{}/{}", va.vote_pubkey, va.node_pubkey))
    //         .collect::<Vec<String>>()
    // );
    // log::info!(
    //     "vote account delinquent:{:?}",
    //     vote_account
    //         .delinquent
    //         .iter()
    //         .map(|va| format!("{}/{}", va.vote_pubkey, va.node_pubkey))
    //         .collect::<Vec<String>>()
    // );
    log::info!("end process_schedule:{:?}", leader_schedule_aggregated);
    Ok((leader_schedule_aggregated, stakes_aggregated))
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
fn calculate_leader_schedule(
    epoch: u64,
    stakes: HashMap<Pubkey, u64>,
    slots_in_epoch: u64,
) -> LeaderSchedule {
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&epoch.to_le_bytes());
    let mut stakes: Vec<_> = stakes
        .iter()
        .map(|(pubkey, stake)| (*pubkey, *stake))
        .collect();
    sort_stakes(&mut stakes);
    LeaderSchedule::new(&stakes, seed, slots_in_epoch, NUM_CONSECUTIVE_LEADER_SLOTS)
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

fn seconds_until_target_time_with_time(
    target_hour: u8,
    target_minute: u8,
    target_second: u8,
) -> i64 {
    //let local_offset = UtcOffset::local_offset_at(OffsetDateTime::UNIX_EPOCH);
    //log::info!("{local_offset:?}");
    //set UTC+2
    let utcp2 = UtcOffset::from_hms(2, 0, 0).unwrap();
    let now = OffsetDateTime::now_utc().to_offset(utcp2);
    //let now = OffsetDateTime::now_utc();
    log::info!("now:{now:?}");
    let mut target_time = now
        .date()
        .with_hms(target_hour, target_minute, target_second)
        .unwrap()
        .assume_offset(utcp2);

    // If the target time has passed for today, calculate for next day
    if now > target_time {
        log::info!("add one day");
        target_time = target_time + TimeDuration::days(1);
    }
    log::info!("target_time:{target_time:?}");

    let duration_until_target = target_time - now;
    duration_until_target.whole_seconds()
}

pub async fn verify_schedule(
    schedule: LeaderSchedule,
    node_vote_account_map: &HashMap<String, String>,
    rpc_url: String,
) -> anyhow::Result<()> {
    let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    let Some(leader_schedule_finalized) = rpc_client.get_leader_schedule(None).await? else {
        log::info!("verify_schedule RPC return no schedule. Try later.");
        return Ok(());
    };

    //map leaderscheudle to HashMap<PubKey, Vec<slot>>
    let mut input_leader_schedule: HashMap<Pubkey, Vec<usize>> = HashMap::new();
    for (slot, pubkey) in schedule.get_slot_leaders().iter().copied().enumerate() {
        input_leader_schedule
            .entry(pubkey)
            .or_insert(vec![])
            .push(slot);
    }

    log::trace!("verify_schedule input_leader_schedule:{input_leader_schedule:?} leader_schedule_finalized:{leader_schedule_finalized:?}");

    //map rpc leader schedule node pubkey to vote account
    let mut leader_schedule_finalized: HashMap<&String, Vec<usize>> = leader_schedule_finalized.into_iter().filter_map(|(pk, slots)| match node_vote_account_map.get(&pk) {
            Some(vote_account) => Some((vote_account,slots)),
            None => {
                log::warn!("verify_schedule RPC get_leader_schedule return some Node account:{pk} that are not mapped by rpc get_vote_accounts");
                None
            },
        }).collect();

    let mut vote_account_in_error: Vec<Pubkey> = input_leader_schedule.into_iter().filter_map(|(input_vote_key, mut input_slot_list)| {
        let Some(mut rpc_strake_list) = leader_schedule_finalized.remove(&input_vote_key.to_string()) else {
            log::warn!("verify_schedule vote account not found in RPC:{input_vote_key}");
            return Some(input_vote_key);
        };
        input_slot_list.sort();
        rpc_strake_list.sort();
        if input_slot_list.into_iter().zip(rpc_strake_list.into_iter()).any(|(in_v, rpc)| in_v != rpc) {
            log::warn!("verify_schedule bad slots for {input_vote_key}"); // Caluclated:{input_slot_list:?} rpc:{rpc_strake_list:?}
            Some(input_vote_key)
        } else {
            None
        }
    }).collect();

    if !leader_schedule_finalized.is_empty() {
        log::warn!(
            "verify_schedule RPC vote account not present in calculated schedule:{:?}",
            leader_schedule_finalized.keys()
        );
        vote_account_in_error.append(
            &mut leader_schedule_finalized
                .keys()
                .map(|sk| Pubkey::from_str(sk).unwrap())
                .collect::<Vec<Pubkey>>(),
        );
    }

    log::info!("verify_schedule these account are wrong:{vote_account_in_error:?}");
    Ok(())
}
