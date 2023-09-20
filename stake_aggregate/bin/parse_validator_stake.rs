//cargo run --bin parsestake validatorfilepath aggregatefilepath

use anyhow::bail;
use borsh::de::BorshDeserialize;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake::state::StakeState;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

const STAKE_FILE: &str = "epoch528_leader_schedule_stakes.txt";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = env::args().collect();

    println!("args:{args:?}");

    if args.len() <= 2 {
        eprintln!("Please provide validator stake file path");
        std::process::exit(1);
    }

    let validator_stake_file_path: String =
        args[1].parse().expect("First argument should be a String");
    let contents = std::fs::read_to_string(validator_stake_file_path)
        .expect("Should have been able to read the file");
    let validator_stakes = parse(contents);

    let aggregate_stake_file_path: String =
        args[2].parse().expect("First argument should be a String");
    let contents = std::fs::read_to_string(aggregate_stake_file_path)
        .expect("Should have been able to read the file");
    let mut aggregate_stakes = parse(contents);

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

    Ok(())
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
