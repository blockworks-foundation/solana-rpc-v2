//RUST_BACKTRACE=1 RUST_LOG=stake_aggregate=trace cargo run
use crate::stakestore::StakeStore;
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
use std::collections::HashMap;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::geyser::SubscribeUpdateAccount;
use yellowstone_grpc_proto::prelude::SubscribeRequestFilterAccounts;
use yellowstone_grpc_proto::{
    prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots, SubscribeUpdateSlot},
    tonic::service::Interceptor,
};

mod leader_schedule;
mod stakestore;

type Slot = u64;

//WebSocket URL: ws://localhost:8900/ (computed)

//const GRPC_URL: &str = "http://127.0.0.0:10000";
const GRPC_URL: &str = "http://192.168.88.31:10000";
//const RPC_URL: &str = "http://localhost:8899";
const RPC_URL: &str = "http://192.168.88.31:8899";

//const RPC_URL: &str = "https://api.mainnet-beta.solana.com";
//const RPC_URL: &str = "https://api.testnet.solana.com";
//const RPC_URL: &str = "https://api.devnet.solana.com";

const STAKESTORE_INITIAL_CAPACITY: usize = 600000;

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

fn extract_stakestore(stakestore: &mut StakeStore) -> anyhow::Result<crate::stakestore::StakeMap> {
    let new_store = std::mem::take(stakestore);
    let (new_store, stake_map) = new_store.extract()?;
    *stakestore = new_store;
    Ok(stake_map)
}

fn merge_stakestore(
    stakestore: &mut StakeStore,
    stake_map: crate::stakestore::StakeMap,
) -> anyhow::Result<()> {
    let new_store = std::mem::take(stakestore);
    let new_store = new_store.merge_stake(stake_map)?;
    *stakestore = new_store;
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
    log::trace!("Run_loop init current_epoch:{current_epoch:?}");

    let mut spawned_task_toexec = FuturesUnordered::new();
    let mut spawned_task_result = FuturesUnordered::new();

    spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetPa));

    let mut slots = HashMap::new();
    slots.insert("client".to_string(), SubscribeRequestFilterSlots {});

    let mut accounts: HashMap<String, SubscribeRequestFilterAccounts> = HashMap::new();

    accounts.insert(
        "client".to_owned(),
        SubscribeRequestFilterAccounts {
            account: vec![],
            owner: vec![solana_sdk::stake::program::id().to_string()],
            filters: vec![],
        },
    );

    let mut confirmed_stream = client
        .subscribe_once(
            slots,
            accounts,           //accounts
            Default::default(), //tx
            Default::default(), //entry
            Default::default(), //full block
            Default::default(), //block meta
            Some(CommitmentLevel::Confirmed),
            vec![],
        )
        .await?;

    loop {
        tokio::select! {
            //Execute RPC call in another task
           Some(to_exec) = spawned_task_toexec.next() =>  {
                let jh = tokio::spawn(async move {
                    match to_exec {
                        TaskToExec::RpcGetPa => {
                            log::trace!("TaskToExec RpcGetPa start");
                            let rpc_client = RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::finalized());
                            let res = rpc_client.get_program_accounts(&solana_sdk::stake::program::id()).await;
                            TaskResult::RpcGetPa(res)
                        },
                        TaskToExec::RpcGetCurrentEpoch => {
                            log::trace!("TaskToExec RpcGetCurrentEpoch start");
                            let rpc_client = RpcClient::new_with_commitment(RPC_URL.to_string(), CommitmentConfig::finalized());
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
                    Ok(TaskResult::RpcGetPa(Ok(pa_list))) => {
                        let Ok(mut stake_map) = extract_stakestore(&mut stakestore) else {
                            //retry later, epoch schedule is currently processed
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetPa));
                            continue;
                        };
                        //merge new PA with stake map in a specific thread
                        log::trace!("Run_loop before Program account stake merge START");

                        let jh = tokio::task::spawn_blocking(|| {
                            crate::stakestore::merge_program_account_in_strake_map(&mut stake_map, pa_list);
                            TaskResult::MergePAList(stake_map)
                        });
                        spawned_task_result.push(jh);
                    }
                    Ok(TaskResult::CurrentEpoch(Ok(epoch_info))) => {
                        log::trace!("Run_loop update new epoch:{epoch_info:?}");
                        current_epoch = epoch_info;
                    }
                    Ok(TaskResult::MergePAList(stake_map)) => {
                        if let Err(err) = merge_stakestore(&mut stakestore, stake_map) {
                            //should never occurs because only one extract can occurs at time.
                            // during PA no epoch schedule can be done.
                            log::warn!("merge stake on a non extract stake map err:{err}");
                            //restart the getPA.
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetPa));
                            continue;
                        };
                        log::trace!("Run_loop Program account stake merge END");

                        //TODO REMOVE
                        //To test verify the schedule
                        let Ok(stake_map) = extract_stakestore(&mut stakestore) else {
                            log::info!("Epoch schedule aborted because a getPA is currently running.");
                            continue;
                        };
                        let move_epoch = current_epoch.clone();
                        let jh = tokio::task::spawn_blocking(move || {
                            let schedule = crate::leader_schedule::calculate_leader_schedule_from_stake_map(&stake_map, &move_epoch);
                            TaskResult::ScheduleResult(schedule.ok(), stake_map)
                        });
                        spawned_task_result.push(jh);
                        //end test

                    }
                    Ok(TaskResult::ScheduleResult(schedule_opt, stake_map)) => {
                        //merge stake
                        if let Err(err) = merge_stakestore(&mut stakestore, stake_map) {
                            //should never occurs because only one extract can occurs at time.
                            // during PA no epoch schedule can be done.
                            log::warn!("merge stake on a non extract stake map err:{err}");
                            //restart the getPA.
                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetPa));
                            continue;
                        };

                        //verify calculated shedule with the one the RPC return.
                        if let Some(schedule) = schedule_opt {
                            tokio::task::spawn_blocking(|| {
                                if let Err(err) = crate::leader_schedule::verify_schedule(schedule,RPC_URL.to_string()) {
                                    log::warn!("Error during schedule verification:{err}");
                                }
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
                                log::info!("new message: {msg:?}");
                                match msg.update_oneof {
                                    Some(UpdateOneof::Account(account)) => {
                                        //store new account stake.
                                        if let Some(account) = read_account(account, current_slot.confirmed_slot) {
                                            log::trace!("Geyser receive new account");
                                            if let Err(err) = stakestore.add_stake(account) {
                                                log::warn!("Can't add new stake from account data err:{}", err);
                                                continue;
                                            }
                                        }
                                    }
                                    Some(UpdateOneof::Slot(slot)) => {
                                        //update current slot
                                        //log::info!("Processing slot: {:?} current slot:{:?}", slot, current_slot);
                                        current_slot.update_slot(&slot);

                                        if current_slot.confirmed_slot == current_epoch.absolute_slot + current_epoch.slots_in_epoch {
                                            log::info!("End epoch slot. Calculate schedule.");
                                            let Ok(stake_map) = extract_stakestore(&mut stakestore) else {
                                                log::info!("Epoch schedule aborted because a getPA is currently running.");
                                                continue;
                                            };

                                            //calculate schedule in a dedicated thread.
                                            let move_epoch = current_epoch.clone();
                                            let jh = tokio::task::spawn_blocking(move || {
                                                let schedule = crate::leader_schedule::calculate_leader_schedule_from_stake_map(&stake_map, &move_epoch);
                                                TaskResult::ScheduleResult(schedule.ok(), stake_map)
                                            });
                                            spawned_task_result.push(jh);

                                            //change epoch. Change manually then update using RPC.
                                            current_epoch.epoch +=1;
                                            current_epoch.slot_index += current_epoch.slots_in_epoch + 1;
                                            spawned_task_toexec.push(futures::future::ready(TaskToExec::RpcGetCurrentEpoch));


                                        }
                                    }
                                    Some(UpdateOneof::Ping(_)) => (),
                                    bad_msg => {
                                        log::info!("Geyser stream unexpected message received:{:?}",bad_msg);
                                    }
                                }
                            }
                            Err(error) => {
                                log::error!("Geyser stream receive an error has message: {error:?}, try to reconnect and resynchronize.");
                                break;
                            }
                        }
                     }
                     None => {
                        log::warn!("The geyser stream close try to reconnect and resynchronize.");
                        break; //TODO reconnect.
                     }
                }
            }
        }
    }

    Ok(())
}

#[derive(Default, Debug)]
struct CurrentSlot {
    processed_slot: u64,
    confirmed_slot: u64,
    finalized_slot: u64,
}

impl CurrentSlot {
    fn update_slot(&mut self, slot: &SubscribeUpdateSlot) {
        match slot.status() {
            CommitmentLevel::Processed => self.processed_slot = slot.slot,
            CommitmentLevel::Confirmed => self.confirmed_slot = slot.slot,
            CommitmentLevel::Finalized => self.finalized_slot = slot.slot,
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
        crate::stakestore::read_stake_from_account_data(self.data.as_slice())
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
        log::info!(
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
    RpcGetPa,
    RpcGetCurrentEpoch,
}

#[derive(Debug)]
enum TaskResult {
    RpcGetPa(Result<Vec<(Pubkey, Account)>, ClientError>),
    CurrentEpoch(Result<EpochInfo, ClientError>),
    MergePAList(crate::stakestore::StakeMap),
    ScheduleResult(Option<LeaderSchedule>, crate::stakestore::StakeMap),
}
