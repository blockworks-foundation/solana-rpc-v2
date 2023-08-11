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
use std::collections::HashMap;
use std::env;
use std::time::Duration;
use time::{Duration as TimeDuration, OffsetDateTime, UtcOffset};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

const RPC_URL: &str = "https://api.mainnet-beta.solana.com";
//const RPC_URL: &str = "https://api.testnet.solana.com";
//const RPC_URL: &str = "https://api.devnet.solana.com";

const SLOTS_IN_EPOCH: u64 = 432000;

#[tokio::main(flavor = "multi_thread", worker_threads = 16)]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = env::args().collect();

    if args.len() != 4 {
        eprintln!("Please provide 3 arguments: hour, minute and seconds");
        std::process::exit(1);
    }

    let target_hour: u32 = args[1]
        .parse()
        .expect("First argument should be a number representing the hour");
    let target_minute: u32 = args[2]
        .parse()
        .expect("Second argument should be a number representing the minute");
    let target_second: u32 = args[3]
        .parse()
        .expect("Third argument should be a number representing the seconds");

    let seconds_until_target = seconds_until_target_time(target_hour, target_minute, target_second);
    log::info!("seconds_until_target:{}", seconds_until_target);
    let to_wait = Duration::from_secs(seconds_until_target as u64 - 30);
    tokio::time::sleep(to_wait).await;

    let mut counter = 0;
    let mut schedule_counter = 0;
    let mut epoch_offset = 1;

    loop {
        match write_schedule(0).await {
            Ok(()) => {
                epoch_offset = 0;
                schedule_counter += 1;
                if schedule_counter == 3 {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
            Err(err) => {
                log::info!("error:{err}");
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
    //show all schedule aggregated.
    let mut print_finalized = schedule
        .into_iter()
        .map(|(key, values)| format!("{key}:{:?}", values))
        .collect::<Vec<String>>();
    print_finalized.sort();
    log::info!("leader_schedule_finalized:{:?}", print_finalized);
    Ok(())
}

async fn process_schedule(epoch_offset: u64) -> anyhow::Result<HashMap<String, (u64, u64, u64)>> {
    let rpc_client =
        RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::finalized());

    let slot = rpc_client.get_slot().await?;

    // Fetch current epoch
    let epoch_info = rpc_client.get_epoch_info().await?;

    let current_epoch = epoch_info.epoch;

    // Fetch stakes in current epoch
    let response = rpc_client
        .get_program_accounts(&solana_sdk::stake::program::id())
        .await?;

    log::info!("current_slot:{slot:?}");
    log::info!("epoch_info:{epoch_info:?}");
    log::info!("get_program_accounts:{:?}", response.len());

    let mut stakes = HashMap::<Pubkey, u64>::new();

    for (pubkey, account) in response {
        // Zero-length accounts owned by the stake program are system accounts that were re-assigned and are to be
        // ignored
        if account.data.len() == 0 {
            continue;
        }

        match StakeState::deserialize(&mut account.data.as_slice())? {
            StakeState::Stake(_, stake) => {
                // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
                // u64::MAX which indicates no activation ever happened)
                if stake.delegation.activation_epoch >= current_epoch {
                    continue;
                }
                // Ignore stake accounts deactivated before this epoch
                if stake.delegation.deactivation_epoch < current_epoch {
                    continue;
                }
                // Add the stake in this stake account to the total for the delegated-to vote account
                *(stakes
                    .entry(stake.delegation.voter_pubkey.clone())
                    .or_insert(0)) += stake.delegation.stake;
            }
            _ => (),
        }
    }

    let leader_schedule = calculate_leader_schedule(current_epoch + epoch_offset, stakes);

    let mut leader_schedule_aggregated: HashMap<String, (u64, u64, u64)> = leader_schedule
        .get_slot_leaders()
        .iter()
        .fold(HashMap::new(), |mut sc, l| {
            sc.entry(l.to_string()).or_insert((0, 0, 0)).1 += 1;
            sc
        });
    // for (leader, nb) in leader_schedule_aggregated {
    //     println!("{leader}:{nb}");
    // }

    //build vote account node key association table
    let vote_account = rpc_client.get_vote_accounts().await?;
    let note_vote_table = vote_account
        .current
        .iter()
        .chain(vote_account.delinquent.iter())
        .map(|va| (va.node_pubkey.clone(), va.vote_pubkey.clone()))
        .collect::<HashMap<String, String>>();

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
    let leader_schedule_va = calculate_leader_schedule(current_epoch + epoch_offset, vote_stackes);
    leader_schedule_va.get_slot_leaders().iter().for_each(|l| {
        leader_schedule_aggregated
            .entry(l.to_string())
            .or_insert((0, 0, 0))
            .2 += 1;
    });

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

    Ok(leader_schedule_aggregated)
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

fn seconds_until_target_time(target_hour: u32, target_minute: u32, target_second: u32) -> u64 {
    let now = Local::now();
    log::info!("now:{now:?}");
    let today = now.date_naive();
    let target_naive_time =
        NaiveTime::from_hms_opt(target_hour, target_minute, target_second).unwrap();
    let mut target_time = NaiveDate::and_time(&today, target_naive_time);

    // If the target time has passed for today, calculate for next day
    if target_time < now.naive_local() {
        target_time = NaiveDate::and_time(&(today + chrono::Duration::days(1)), target_naive_time);
    }

    log::info!("target_time:{target_time:?}");
    let duration_until_target = target_time
        .signed_duration_since(now.naive_local())
        .num_seconds() as u64;
    duration_until_target
}
