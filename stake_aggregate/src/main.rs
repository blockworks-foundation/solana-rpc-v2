//RUST_BACKTRACE=1 RUST_LOG=stake_aggregate=trace cargo run --release
/*
 RPC calls;
 curl http://localhost:3000 -X POST -H "Content-Type: application/json" -d '
  {
    "jsonrpc": "2.0",
    "id" : 1,
    "method": "save_stakes",
    "params": []
  }
'

 curl http://localhost:3000 -X POST -H "Content-Type: application/json" -d '
  {
    "jsonrpc": "2.0",
    "id" : 1,
    "method": "bootstrap_accounts",
    "params": []
  }
'
*/

//TODO: add stake verify that it' not already desactivated.

use crate::stakestore::extract_stakestore;
use crate::stakestore::merge_stakestore;
use crate::stakestore::StakeStore;
use crate::votestore::extract_votestore;
use crate::votestore::merge_votestore;
use crate::votestore::VoteStore;
use anyhow::bail;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::state::Delegation;
use solana_sdk::vote::state::VoteState;
use std::collections::HashMap;
use tokio::time::Duration;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccount;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocks;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterBlocksMeta;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots, SubscribeUpdateSlot},
    tonic::service::Interceptor,
};

mod leader_schedule;
mod rpc;
mod stakestore;
mod votestore;

type Slot = u64;

//WebSocket URL: ws://localhost:8900/ (computed)

const GRPC_URL: &str = "http://localhost:10000";
const RPC_URL: &str = "http://localhost:8899";

//const RPC_URL: &str = "https://api.mainnet-beta.solana.com";
//const RPC_URL: &str = "https://api.testnet.solana.com";
//const RPC_URL: &str = "https://api.devnet.solana.com";

const STAKESTORE_INITIAL_CAPACITY: usize = 600000;
const VOTESTORE_INITIAL_CAPACITY: usize = 600000;

pub fn log_end_epoch(
    current_slot: Slot,
    end_epoch_slot: Slot,
    epoch_slot_index: Slot,
    msg: String,
) {
    //log 50 end slot.
    if current_slot != 0 && current_slot + 20 > end_epoch_slot {
        log::info!("{current_slot}/{end_epoch_slot} {}", msg);
    }
    if epoch_slot_index < 20 {
        log::info!("{current_slot}/{end_epoch_slot} {}", msg);
    }
}

/// Connect to yellow stone plugin using yellow stone gRpc Client
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut client = GeyserGrpcClient::connect(GRPC_URL, None::<&'static str>, None)?;

    let version = client.get_version().await?;
    println!("Version: {:?}", version);

    let ctrl_c_signal = tokio::signal::ctrl_c();

    tokio::select! {
        res = run_loop(client) => {
            // This should never happen
            log::error!("Services quit unexpectedly {res:?}");
        }
        _ = ctrl_c_signal => {
            log::info!("Received ctrl+c signal");
        }
    }

    Ok(())
}

async fn run_loop<F: Interceptor>(mut client: GeyserGrpcClient<F>) -> anyhow::Result<()> {
    //local vars
    let mut current_slot: CurrentSlot = Default::default();
    let mut stakestore = StakeStore::new(STAKESTORE_INITIAL_CAPACITY);
    let mut current_epoch = {
        let rpc_client =
            RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::finalized());

        // Fetch current epoch
        rpc_client.get_epoch_info().await?
    };
    let mut next_epoch_start_slot =
        current_epoch.slots_in_epoch - current_epoch.slot_index + current_epoch.absolute_slot;
    log::info!("Run_loop init {next_epoch_start_slot} current_epoch:{current_epoch:?}");

    let mut spawned_task_toexec = FuturesUnordered::new();
    let mut spawned_task_result = FuturesUnordered::new();

    //use to set an initial state of all PA
    spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetStakeAccount(
        current_epoch.absolute_slot - current_epoch.slot_index,
        0,
    )));
    spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetVoteAccount(
        current_epoch.absolute_slot - current_epoch.slot_index,
        0,
    )));

    //subscribe Geyser grpc
    //slot subscription
    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    //account subscription
    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();
    accounts.insert(
        "client".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![
                solana_sdk::stake::program::ID.to_string(),
                solana_sdk::vote::program::ID.to_string(),
            ],
            filters: vec![],
        },
    );

    //block subscription
    let mut blocks = HashMap::new();
    blocks.insert(
        "client".to_string(),
        SubscribeRequestFilterBlocks {
            account_include: Default::default(),
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: None,
        },
    );

    //block Meta subscription filter
    let mut blocks_meta = HashMap::new();
    blocks_meta.insert("client".to_string(), SubscribeRequestFilterBlocksMeta {});

    let mut confirmed_stream = client
        .subscribe_once(
            slots.clone(),
            accounts.clone(),   //accounts
            Default::default(), //tx
            Default::default(), //entry
            //blocks,             //full block
            Default::default(), //full block
            //blocks_meta,        //block meta
            Default::default(), //block meta
            Some(CommitmentLevel::Confirmed),
            vec![],
        )
        .await?;

    //log current data at interval
    let mut log_interval = tokio::time::interval(Duration::from_millis(600000));

    //start local rpc access to execute command.
    let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(100);
    let rpc_handle = crate::rpc::run_server(request_tx).await?;
    //make it run forever
    tokio::spawn(rpc_handle.stopped());

    //Vote account management struct
    let mut votestore = VoteStore::new(VOTESTORE_INITIAL_CAPACITY);

    loop {
        tokio::select! {
            Some(req) = request_rx.recv() => {
                match req {
                    crate::rpc::Requests::SaveStakes => {
                        tokio::task::spawn_blocking({
                            log::info!("RPC start save_stakes");
                            let current_stakes = stakestore.get_cloned_stake_map();
                            let move_epoch = current_epoch.clone();
                            move || {
                                let current_stake = crate::leader_schedule::build_current_stakes(&current_stakes, &move_epoch, RPC_URL.to_string(), CommitmentConfig::confirmed());
                                log::info!("RPC save_stakes generation done");
                                if let Err(err) = crate::leader_schedule::save_schedule_on_file("stakes", &current_stake) {
                                    log::error!("Error during current stakes saving:{err}");
                                }
                                log::info!("RPC save_stakes END");

                            }
                        });
                    }
                    crate::rpc::Requests::BootstrapAccounts(tx) => {
                        log::info!("RPC start save_stakes");
                        let current_stakes = stakestore.get_cloned_stake_map();
                        if let Err(err) = tx.send((current_stakes, current_slot.confirmed_slot)){
                            println!("Channel error during sending bacl request status error:{err:?}");
                        }
                        log::info!("RPC bootstrap account send");
                    }
                }

            },
            //log interval
            _ = log_interval.tick() => {
                log::info!("Run_loop update new epoch:{current_epoch:?} current slot:{current_slot:?} next epoch start slot:{next_epoch_start_slot}");
                log::info!("Change epoch equality {} >= {}", current_slot.confirmed_slot, next_epoch_start_slot-1);
                log::info!("number of stake accounts:{}", stakestore.nb_stake_account());
            }
            //Execute RPC call in another task
            Some(to_exec) = spawned_task_toexec.next() =>  {
                let jh = tokio::spawn(async move {
                    match to_exec {
                        TaskToExec::RpcGetStakeAccount(epoch_start_slot, sleep_time) => {
                            if sleep_time > 0 {
                                tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                            }
                            log::info!("TaskToExec RpcGetStakeAccount start");
                            let rpc_client = RpcClient::new_with_timeout_and_commitment(RPC_URL.to_string(), Duration::from_secs(600), CommitmentConfig::finalized());
                            let res_stake = rpc_client.get_program_accounts(&solana_sdk::stake::program::id()).await;
                            log::info!("TaskToExec RpcGetStakeAccount END");
                            TaskResult::RpcGetStakeAccount(res_stake, epoch_start_slot)
                        },
                        TaskToExec::RpcGetVoteAccount(epoch_start_slot, sleep_time) => {
                            if sleep_time > 0 {
                                tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                            }
                            log::info!("TaskToExec RpcGetVoteAccount start");
                            let rpc_client = RpcClient::new_with_timeout_and_commitment(RPC_URL.to_string(), Duration::from_secs(600), CommitmentConfig::finalized());
                            let res_vote = rpc_client.get_program_accounts(&solana_sdk::vote::program::id()).await;
                            log::info!("TaskToExec RpcGetVoteAccount END");
                            TaskResult::RpcGetVoteAccount(res_vote, epoch_start_slot)
                        },
                        TaskToExec::RpcGetCurrentEpoch => {
                            //TODO remove no need epoch is calculated.
                            log::info!("TaskToExec RpcGetCurrentEpoch start");
                            //wait 1 sec to be sure RPC change epoch
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            let rpc_client = RpcClient::new_with_timeout_and_commitment(RPC_URL.to_string(), Duration::from_secs(600), CommitmentConfig::finalized());
                            let res = rpc_client.get_epoch_info().await;
                            TaskResult::CurrentEpoch(res)
                        }
                    }
                });
                spawned_task_result.push(jh);
            }
            //Manage RPC call result execution
            Some(some_res) = spawned_task_result.next() =>  {
                match some_res {
                    Ok(TaskResult::RpcGetStakeAccount(Ok(stake_list), epoch_start_slot)) => {
                        let Ok(mut stake_map) = extract_stakestore(&mut stakestore) else {
                            //retry later, epoch schedule is currently processed
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetStakeAccount(epoch_start_slot, 0)));
                            continue;
                        };
                        //merge new PA with stake map in a specific thread
                        log::trace!("Run_loop before Program account stake merge START");

                        let jh = tokio::task::spawn_blocking({
                            let move_epoch = current_epoch.clone();
                            move || {
                                //update pa_list to set slot update to start epoq one.
                                crate::stakestore::merge_program_account_in_strake_map(&mut stake_map, stake_list, epoch_start_slot, &move_epoch);
                                TaskResult::MergeStakeList(stake_map)
                            }
                        });
                        spawned_task_result.push(jh);
                    }
                    //getPA can fail should be retarted.
                    Ok(TaskResult::RpcGetStakeAccount(Err(err), epoch_start_slot)) => {
                        log::warn!("RPC call get Stake Account return invalid result: {err:?}");
                        //get pa can fail should be retarted.
                        spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetStakeAccount(epoch_start_slot, 0)));
                    }
                    Ok(TaskResult::MergeStakeList(stake_map)) => {
                        if let Err(err) = merge_stakestore(&mut stakestore, stake_map, &current_epoch) {
                            //should never occurs because only one extract can occurs at time.
                            // during PA no epoch schedule can be done.
                            log::warn!("merge stake on a non extract stake map err:{err}");
                            //restart the getPA.
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetStakeAccount(0, 0)));
                            continue;
                        };
                        log::info!("Run_loop Program account stake merge END");

                        //TODO REMOVE
                        //To test verify the schedule
                        // let Ok(stake_map) = extract_stakestore(&mut stakestore) else {
                        //     log::info!("Epoch schedule aborted because a getPA is currently running.");
                        //     continue;
                        // };
                        // let jh = tokio::task::spawn_blocking({
                        //     let move_epoch = current_epoch.clone();
                        //     move || {
                        //         let schedule = crate::leader_schedule::calculate_leader_schedule_from_stake_map(&stake_map, &move_epoch);
                        //         TaskResult::ScheduleResult(schedule.ok(), stake_map)
                        //     }
                        // });
                        // spawned_task_result.push(jh);
                        //end test

                    }
                    Ok(TaskResult::RpcGetVoteAccount(Ok(vote_list), epoch_start_slot)) => {
                        let Ok(mut vote_map) = extract_votestore(&mut votestore) else {
                            //retry later, epoch schedule is currently processed
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetVoteAccount(epoch_start_slot, 0)));
                            continue;
                        };
                        //merge new PA with stake map in a specific thread
                        log::trace!("Run_loop before Program account VOTE merge START");

                        let jh = tokio::task::spawn_blocking({
                            move || {
                                //update pa_list to set slot update to start epoq one.
                                crate::votestore::merge_program_account_in_vote_map(&mut vote_map, vote_list, epoch_start_slot);
                                TaskResult::MergeVoteList(vote_map)
                            }
                        });
                        spawned_task_result.push(jh);
                    }
                    //getPA can fail should be retarted.
                    Ok(TaskResult::RpcGetVoteAccount(Err(err), epoch_start_slot)) => {
                        log::warn!("RPC call getVote account return invalid result: {err:?}");
                        //get pa can fail should be retarted.
                        spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetVoteAccount(epoch_start_slot, 0)));
                    }

                    Ok(TaskResult::MergeVoteList(vote_map)) => {
                        if let Err(err) = merge_votestore(&mut votestore, vote_map) {
                            //should never occurs because only one extract can occurs at time.
                            // during PA no epoch schedule can be done.
                            log::warn!("merge vote on a non extract stake map err:{err}");
                            //restart the getPA.
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetVoteAccount(0, 0)));
                            continue;
                        };
                        log::info!("Run_loop Program Vote account merge END");
                    }
                    Ok(TaskResult::CurrentEpoch(Ok(epoch_info))) => {
                        //TODO remove no need epoch is calculated.
                        //only update new epoch slot if the RPC call return the next epoch. Some time it still return the current epoch.
                        if current_epoch.epoch <= epoch_info.epoch {
                            current_epoch = epoch_info;
                            //calcualte slotindex with current slot. getEpichInfo doesn't return data  on current slot.
                            if current_slot.confirmed_slot > current_epoch.absolute_slot {
                                let diff =  current_slot.confirmed_slot - current_epoch.absolute_slot;
                                current_epoch.absolute_slot += diff;
                                current_epoch.slot_index += diff;
                                log::trace!("Update current epoch, diff:{diff}");
                            }
                            next_epoch_start_slot = current_epoch.slots_in_epoch - current_epoch.slot_index + current_epoch.absolute_slot;
                            log::info!("Run_loop update new epoch:{current_epoch:?} current slot:{current_slot:?} next_epoch_start_slot:{next_epoch_start_slot}");
                        } else {
                            //RPC epoch hasn't changed retry
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetCurrentEpoch));
                        }
                    }
                    Ok(TaskResult::ScheduleResult(schedule_opt, stake_map, vote_map)) => {
                        //merge stake
                        let merge_error = match merge_stakestore(&mut stakestore, stake_map, &current_epoch) {
                            Ok(()) => false,
                            Err(err) => {
                                //should never occurs because only one extract can occurs at time.
                                // during PA no epoch schedule can be done.
                                log::warn!("merge stake on a non extract stake map err:{err}");
                                //restart the getPA.
                                spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetStakeAccount(0, 0)));
                                true
                            }
                        };
                        //merge vote
                        if let Err(err) = merge_votestore(&mut votestore, vote_map) {
                            //should never occurs because only one extract can occurs at time.
                            // during PA no epoch schedule can be done.
                            log::warn!("merge stake on a non extract stake map err:{err}");
                            //restart the getPA.
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetVoteAccount(0, 0)));
                            continue;
                        };

                        if merge_error {
                            continue;
                        }

                        //verify calculated shedule with the one the RPC return.
                        if let Some(schedule) = schedule_opt {
                            tokio::task::spawn_blocking(|| {
                                //10 second that the schedule has been calculated on the validator
                                std::thread::sleep(std::time::Duration::from_secs(20));
                                log::info!("Start Verify schedule");
                                if let Err(err) = crate::leader_schedule::verify_schedule(schedule,RPC_URL.to_string()) {
                                    log::warn!("Error during schedule verification:{err}");
                                }
                                log::info!("End Verify schedule");
                            });
                        }
                    }
                    _ => log::warn!("RPC call return invalid result: {some_res:?}"),
                }

            }

            //get confirmed slot or account
            ret = confirmed_stream.next() => {
                match ret {
                     Some(message) => {
                        //process the message
                        match message {
                            Ok(msg) => {
                                //log::info!("new message: {msg:?}");
                                match msg.update_oneof {
                                    Some(UpdateOneof::Account(account)) => {
                                        //store new account stake.
                                        if let Some(account) = read_account(account, current_slot.confirmed_slot) {
                                            //log::trace!("Geyser receive new account");
                                            match account.owner {
                                                solana_sdk::stake::program::ID => {
                                                    if let Err(err) = stakestore.add_stake(account, next_epoch_start_slot-1, &current_epoch) {
                                                        log::warn!("Can't add new stake from account data err:{}", err);
                                                        continue;
                                                    }
                                                }
                                                solana_sdk::vote::program::ID => {
                                                    //process vote accout notification
                                                    if let Err(err) = votestore.add_vote(account, next_epoch_start_slot-1) {
                                                        log::warn!("Can't add new stake from account data err:{}", err);
                                                        continue;
                                                    }
                                                }
                                                _ => log::warn!("receive an account notification from a unknown owner:{account:?}"),
                                            }
                                        }
                                    }
                                    Some(UpdateOneof::Slot(slot)) => {
                                        //for the first update of slot correct epoch info data.
                                        if let CommitmentLevel::Confirmed = slot.status(){
                                            if current_slot.confirmed_slot == 0 {
                                                let diff =  slot.slot - current_epoch.absolute_slot;
                                                current_epoch.absolute_slot += diff;
                                                current_epoch.slot_index += diff;
                                                log::trace!("Set current epoch with diff:{diff} slot:{} current:{}", slot.slot, current_epoch.absolute_slot);
                                            }
                                        }


                                        //update current slot
                                        //log::info!("Processing slot: {:?} current slot:{:?}", slot, current_slot);
                                        log_end_epoch(current_slot.confirmed_slot, next_epoch_start_slot, current_epoch.slot_index, format!("Receive slot: {:?} at commitment:{:?}", slot.slot, slot.status()));

                                        //update epoch info
                                        if let CommitmentLevel::Confirmed = slot.status(){
                                            if current_slot.confirmed_slot != 0 && slot.slot > current_slot.confirmed_slot {
                                                let diff =  slot.slot - current_slot.confirmed_slot;
                                                current_epoch.slot_index += diff;
                                                current_epoch.absolute_slot += diff;
                                                log::trace!("Update epoch with slot, diff:{diff}");
                                            }
                                        }

                                        current_slot.update_slot(&slot);

                                        if current_slot.confirmed_slot >= next_epoch_start_slot-1 { //slot can be non consecutif.

                                            log::info!("End epoch slot, change epoch. Calculate schedule at current slot:{}", current_slot.confirmed_slot);
                                            let Ok(stake_map) = extract_stakestore(&mut stakestore) else {
                                                log::info!("Epoch schedule aborted because a extract_stakestore faild.");
                                                continue;
                                            };

                                            let Ok(vote_map) = extract_votestore(&mut votestore) else {
                                                log::info!("Epoch schedule aborted because extract_votestore faild.");
                                                //cancel stake extraction
                                                merge_stakestore(&mut stakestore, stake_map, &current_epoch).unwrap(); //just extracted.
                                                continue;
                                            };

                                            //reload PA account for new epoch start. TODO replace with bootstrap.
                                            //spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetStakeAccount(next_epoch_start_slot, 30))); //Wait 30s to get new PA.

                                            //change epoch. Change manually then update using RPC.
                                            current_epoch.epoch +=1;
                                            current_epoch.slot_index = 1;
                                            next_epoch_start_slot = next_epoch_start_slot + current_epoch.slots_in_epoch; //set to next epochs.

                                            log::info!("End slot epoch update calculated next epoch:{current_epoch:?}");

                                            //calculate schedule in a dedicated thread.
                                            let jh = tokio::task::spawn_blocking({
                                                let move_epoch = current_epoch.clone();
                                                move || {
                                                    let schedule = crate::leader_schedule::calculate_leader_schedule_from_stake_map(&stake_map, &vote_map, &move_epoch);
                                                    log::info!("End calculate leader schedule at slot:{}", current_slot.confirmed_slot);
                                                    TaskResult::ScheduleResult(schedule.ok(), stake_map, vote_map)
                                                }
                                            });
                                            spawned_task_result.push(jh);

                                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetCurrentEpoch));
                                            //reload current Stake account a epoch change to synchronize.

                                        }
                                    }
                                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                                        log::info!("Receive Block Meta at slot: {}", block_meta.slot);
                                    }
                                    Some(UpdateOneof::Block(block)) => {
                                        println!("Receive Block at slot: {}", block.slot);
                                    }
                                    Some(UpdateOneof::Ping(_)) => log::trace!("UpdateOneof::Ping"),
                                    bad_msg => {
                                        log::info!("Geyser stream unexpected message received:{:?}",bad_msg);
                                    }
                                }
                            }
                            Err(error) => {
                                log::error!("Geyser stream receive an error has message: {error:?}, try to reconnect and resynchronize.");
                                //todo reconnect and resynchronize.
                                //break;
                            }
                        }
                     }
                     None => {
                        log::warn!("The geyser stream close try to reconnect and resynchronize.");
                        let new_confirmed_stream = client
                        .subscribe_once(
                            slots.clone(),
                            accounts.clone(),           //accounts
                            Default::default(), //tx
                            Default::default(), //entry
                            Default::default(), //full block
                            Default::default(), //block meta
                            Some(CommitmentLevel::Confirmed),
                            vec![],
                        )
                        .await?;

                        confirmed_stream = new_confirmed_stream;
                        log::info!("reconnection done");

                        //TODO resynchronize.
                     }
                }
            }
        }
    }

    //Ok(())
}

#[derive(Default, Debug, Clone)]
struct CurrentSlot {
    processed_slot: u64,
    confirmed_slot: u64,
    finalized_slot: u64,
}

impl CurrentSlot {
    fn update_slot(&mut self, slot: &SubscribeUpdateSlot) {
        let updade = |commitment: &str, current_slot: &mut u64, new_slot: u64| {
            //verify that the slot is consecutif
            if *current_slot != 0 && new_slot != *current_slot + 1 {
                log::trace!(
                    "At {commitment} not consecutif slot send: current_slot:{} new_slot{}",
                    current_slot,
                    new_slot
                );
            }
            *current_slot = new_slot
        };

        match slot.status() {
            CommitmentLevel::Processed => updade("Processed", &mut self.processed_slot, slot.slot),

            CommitmentLevel::Confirmed => updade("Confirmed", &mut self.confirmed_slot, slot.slot),

            CommitmentLevel::Finalized => updade("Finalized", &mut self.finalized_slot, slot.slot),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    is_startup: bool,
    slot: u64,
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
    write_version: u64,
    txn_signature: String,
}

impl AccountPretty {
    fn read_stake(&self) -> anyhow::Result<Option<Delegation>> {
        if self.data.is_empty() {
            log::warn!("Stake account with empty data. Can't read vote.");
            bail!("Error: read Stake account with empty data");
        }
        crate::stakestore::read_stake_from_account_data(self.data.as_slice())
    }
    fn read_vote(&self) -> anyhow::Result<VoteState> {
        if self.data.is_empty() {
            log::warn!("Vote account with empty data. Can't read vote.");
            bail!("Error: read Vote account with empty data");
        }
        Ok(VoteState::deserialize(&self.data)?)
    }
}

fn read_account(
    geyser_account: SubscribeUpdateAccount,
    current_slot: u64,
) -> Option<AccountPretty> {
    let Some(inner_account) = geyser_account.account else {
            log::warn!("Receive a SubscribeUpdateAccount without account.");
            return None;
        };

    if geyser_account.slot != current_slot {
        log::trace!(
            "Get geyser account on a different slot:{} of the current:{current_slot}",
            geyser_account.slot
        );
    }

    Some(AccountPretty {
        is_startup: geyser_account.is_startup,
        slot: geyser_account.slot,
        pubkey: Pubkey::try_from(inner_account.pubkey).expect("valid pubkey"),
        lamports: inner_account.lamports,
        owner: Pubkey::try_from(inner_account.owner).expect("valid pubkey"),
        executable: inner_account.executable,
        rent_epoch: inner_account.rent_epoch,
        data: inner_account.data,
        write_version: inner_account.write_version,
        txn_signature: bs58::encode(inner_account.txn_signature.unwrap_or_default()).into_string(),
    })
}

#[derive(Debug)]
enum TaskToExec {
    RpcGetStakeAccount(u64, u64), //epoch_start_slot, sleept time
    RpcGetVoteAccount(u64, u64),  //epoch_start_slot, sleept time
    RpcGetCurrentEpoch,
}

#[derive(Debug)]
enum TaskResult {
    RpcGetStakeAccount(Result<Vec<(Pubkey, Account)>, ClientError>, u64), //stake_list, vote_list
    RpcGetVoteAccount(Result<Vec<(Pubkey, Account)>, ClientError>, u64),  //stake_list, vote_list
    CurrentEpoch(Result<EpochInfo, ClientError>),
    MergeStakeList(crate::stakestore::StakeMap),
    MergeVoteList(crate::votestore::VoteMap),
    ScheduleResult(
        Option<LeaderSchedule>,
        crate::stakestore::StakeMap,
        crate::votestore::VoteMap,
    ),
}
