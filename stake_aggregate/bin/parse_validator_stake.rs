//cargo run --bin parsestake validatorfilepath aggregatefilepath epoch(u64)

use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;
use std::time::Duration;

//const STAKE_FILE: &str = "epoch528_leader_schedule_stakes.txt";
//const RPC_URL: &str = "http://localhost:8899";
const RPC_URL: &str = "https://api.testnet.solana.com";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    println!("args:{args:?}");

    if args.len() <= 3 {
        eprintln!("Error missing parameter");
        std::process::exit(1);
    }

    let validator_stake_file_path: String =
        args[1].parse().expect("First argument should be a String");
    let contents = std::fs::read_to_string(validator_stake_file_path)
        .expect("Should have been able to read the file");
    let validator_stakes = parse(contents);
    log::info!("validator_stakes len:{}", validator_stakes.len());

    let aggregate_stake_file_path: String =
        args[2].parse().expect("Second argument should be a String");
    let contents = std::fs::read_to_string(&aggregate_stake_file_path)
        .expect("Should have been able to read the file");
    let mut aggregate_stakes = parse(contents);
    log::info!("aggregate_stakes len:{}", aggregate_stakes.len());

    let epoch: u64 = args[3].parse().expect("Third argument should be a u64");

    //save aggregate stake in a file as a Vec to be loader by the aggregator
    {
        #[derive(Debug, Default, Clone, Serialize, Deserialize)]
        struct SavedStake {
            epoch: u64,
            stakes: Vec<(String, u64)>,
        }

        let tosave: Vec<(String, u64)> = aggregate_stakes
            .iter()
            .map(|(pk, st)| (pk.to_string(), *st))
            .collect();
        let save_stakes = SavedStake {
            epoch,
            stakes: tosave,
        };
        let serialized_stakes = serde_json::to_string(&save_stakes).unwrap();
        let filename = format!("{aggregate_stake_file_path}.json",);
        // Write to the file
        let mut file = File::create(filename).unwrap();
        file.write_all(serialized_stakes.as_bytes()).unwrap();
        file.flush().unwrap();
    }

    //let test_aggregate_stakes = aggregate_stakes.clone();

    //compare to map.
    for (pk, vstake) in validator_stakes.iter() {
        match aggregate_stakes.remove(pk) {
            Some(st) => {
                if st != *vstake {
                    println!("Stake diff detected {pk} v:{vstake} agg:{st}");
                }
            }
            None => println!("Found nodeid in validator not present:{pk}"),
        }
    }

    println!("Aggregate nodeid not in validator:{:?}", aggregate_stakes);

    let epoch = EpochInfo {
        epoch,
        slot_index: 48586,
        slots_in_epoch: 432000,
        absolute_slot: 223052842,
        block_height: 189421502,
        transaction_count: None,
    };

    let leader_schedule = calculate_leader_schedule(validator_stakes, &epoch).unwrap();
    verify_schedule(leader_schedule, &aggregate_stake_file_path).unwrap();

    Ok(())
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
    //log::info!("calculate_leader_schedule stakes:{stakes:?} epoch:{current_epoch_info:?}");
    Ok(LeaderSchedule::new(
        &stakes,
        seed,
        current_epoch_info.slots_in_epoch,
        NUM_CONSECUTIVE_LEADER_SLOTS,
    ))
}

fn verify_schedule(schedule: LeaderSchedule, file_name: &str) -> anyhow::Result<()> {
    log::info!("verify_schedule Start.");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        RPC_URL.to_string(),
        Duration::from_secs(600),
        CommitmentConfig::confirmed(),
    );
    let Some(mut rpc_leader_schedule) = rpc_client.get_leader_schedule(None).unwrap() else {
        log::info!("verify_schedule RPC return no schedule. Try later.");
        return Ok(());
    };
    //HashMap<String, Vec<usize, Global>
    log::info!("rpc_leader_schedule len:{}", rpc_leader_schedule.len());

    // let vote_account = rpc_client.get_vote_accounts()?;
    // let node_vote_table = vote_account
    //     .current
    //     .into_iter()
    //     .chain(vote_account.delinquent.into_iter())
    //     .map(|va| (va.node_pubkey, va.vote_pubkey))
    //     .collect::<HashMap<String, String>>();

    //log::info!("node_vote_table:{node_vote_table:?}");

    //map leaderscheudle to HashMap<PubKey, Vec<slot>>
    let slot_leaders = schedule.get_slot_leaders();

    //save_slot_leaders
    let string_slot_leader: Vec<String> = slot_leaders.iter().map(|pk| pk.to_string()).collect();
    let serialized_schedule = serde_json::to_string(&string_slot_leader).unwrap();
    let filename = format!("{file_name}.schedule.json",);
    // Write to the file
    let mut file = File::create(filename).unwrap();
    file.write_all(serialized_schedule.as_bytes()).unwrap();
    file.flush().unwrap();

    log::info!("aggregate_leader_schedule len:{}", slot_leaders.len());
    let mut input_leader_schedule: HashMap<String, Vec<usize>> = HashMap::new();
    for (slot, pubkey) in slot_leaders.iter().copied().enumerate() {
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

            // log::info!(
            //     "slot diff input:{:?} rpc:{:?}",
            //     input_slot_list,
            //     rpc_strake_list
            // );

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

    log::info!(
        "verify_schedule these accounts are wrong:{:?}",
        vote_account_in_error.len()
    );
    //print_current_program_account(&rpc_client);
    Ok(())
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

fn parse(data: String) -> HashMap<Pubkey, u64> {
    //let res = read_all_entry("(5D1fNXzvv5NjV1ysLjirC4WY92RNsVH18vjmcszZd8on, 35100122586019840), (J7v9ndmcoBuo9to2MnHegLnBkC9x3SAVbQBJo5MMJrN1, 6174680975486678), ");
    let (_, res) = read_all_entry(&data).expect("Unable to parse the file content");
    let stake_map: HashMap<Pubkey, u64> = res
        .into_iter()
        .map(|(pk, val)| {
            (
                Pubkey::from_str(pk).expect("not a public key"),
                val.parse::<u64>().expect("not a stake"),
            )
        })
        .collect();
    stake_map
}

//entry pattern (5D1fNXzvv5NjV1ysLjirC4WY92RNsVH18vjmcszZd8on, 35100122586019840), (J7v9ndmcoBuo9to2MnHegLnBkC9x3SAVbQBJo5MMJrN1, 6174680975486678),
use nom::{
    bytes::complete::{is_not, tag, take_until},
    // see the "streaming/complete" paragraph lower for an explanation of these submodules
    character::complete::char,
    multi::many1,
    sequence::delimited,
    IResult,
};

fn read_parenthesis(input: &str) -> IResult<&str, &str> {
    delimited(char('('), is_not(")"), char(')'))(input)
}

fn read_nodeid(input: &str) -> IResult<&str, &str> {
    take_until(",")(input)
}

fn read_one_entry(input: &str) -> IResult<&str, (&str, &str)> {
    let (res, base) = read_parenthesis(input)?;
    let (stake, nodeid) = read_nodeid(base)?;
    let (res, _) = tag(", ")(res)?;
    let (stake, _) = tag(", ")(stake)?;
    Ok((res, (nodeid, stake)))
}

fn read_all_entry(i: &str) -> IResult<&str, Vec<(&str, &str)>> {
    many1(read_one_entry)(i)
}
