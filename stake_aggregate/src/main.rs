//RUST_BACKTRACE=1 RUST_LOG=stake_aggregate=trace cargo run --release --bin stake_aggregate
//RUST_BACKTRACE=1 RUST_LOG=stake_aggregate=info cargo run --release --bin stake_aggregate &> stake_logs.txt &
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
' -o extract_stake2.json
*/

//TODO: add stake verify that it' not already desactivated.

use crate::bootstrap::BootstrapData;
use crate::bootstrap::BootstrapEvent;
use crate::leader_schedule::LeaderScheduleData;
use crate::stakestore::StakeStore;
use crate::votestore::VoteStore;
use anyhow::bail;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use solana_sdk::commitment_config::CommitmentConfig;
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
    prelude::{subscribe_update::UpdateOneof, SubscribeRequestFilterSlots},
    tonic::service::Interceptor,
};

mod bootstrap;
mod epoch;
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
    if current_slot != 0 && current_slot + 10 > end_epoch_slot {
        log::info!("{current_slot}/{end_epoch_slot} {}", msg);
    }
    if epoch_slot_index < 10 {
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
    //slot and epoch
    let mut current_epoch_state =
        epoch::CurrentEpochSlotState::bootstrap(RPC_URL.to_string()).await?;

    //Stake account management struct
    let mut stakestore = StakeStore::new(STAKESTORE_INITIAL_CAPACITY);

    //Vote account management struct
    let mut votestore = VoteStore::new(VOTESTORE_INITIAL_CAPACITY);

    //leader schedule
    let mut current_leader_schedule = LeaderScheduleData {
        leader_schedule: None,
        schedule_epoch: current_epoch_state.current_epoch.clone(),
    };

    //future execution collection.
    let mut spawned_bootstrap_task = FuturesUnordered::new();
    let mut spawned_leader_schedule_task = FuturesUnordered::new();

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
                //                solana_sdk::system_program::ID.to_string(),
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
            blocks.clone(),     //full block
            //Default::default(), //full block
            //blocks_meta,        //block meta
            Default::default(), //block meta
            Some(CommitmentLevel::Confirmed),
            vec![],
        )
        .await?;

    //log current data at interval TODO to be removed.  only for test.
    let mut log_interval = tokio::time::interval(Duration::from_millis(600000));

    //start local rpc access to get RPC request.
    let (request_tx, mut request_rx) = tokio::sync::mpsc::channel(100);
    let rpc_handle = crate::rpc::run_server(request_tx).await?;
    //make it run forever
    tokio::spawn(rpc_handle.stopped());

    //Init bootstrap process
    let bootstrap_data = BootstrapData {
        current_epoch: current_epoch_state.current_epoch.epoch,
        next_epoch_start_slot: current_epoch_state.next_epoch_start_slot,
        sleep_time: 1,
        rpc_url: RPC_URL.to_string(),
    };
    let jh = tokio::spawn(async move { BootstrapEvent::InitBootstrap });
    spawned_bootstrap_task.push(jh);

    loop {
        tokio::select! {
            Some(req) = request_rx.recv() => {
                match req {
                    crate::rpc::Requests::SaveStakes => {
                        tokio::task::spawn_blocking({
                            log::info!("RPC start save_stakes");
                            let current_stakes = stakestore.get_cloned_stake_map();
                            let move_epoch = current_epoch_state.current_epoch.clone();
                            move || {
                                let current_stake = crate::leader_schedule::build_current_stakes(
                                    &current_stakes,
                                    &move_epoch,
                                    RPC_URL.to_string(),
                                    CommitmentConfig::confirmed(),
                                );
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
                        if let Err(err) = tx.send((current_stakes, current_epoch_state.current_slot.confirmed_slot)){
                            println!("Channel error during sending bacl request status error:{err:?}");
                        }
                        log::info!("RPC bootstrap account send");
                    }
                }

            },
            //log interval TODO remove
            _ = log_interval.tick() => {
                log::info!("Run_loop update new epoch:{current_epoch_state:?}");
                log::info!("Change epoch equality {} >= {}", current_epoch_state.current_slot.confirmed_slot, current_epoch_state.current_epoch_end_slot());
                log::info!("number of stake accounts:{}", stakestore.nb_stake_account());
            }

            //exec bootstrap task
            Some(Ok(event)) = spawned_bootstrap_task.next() =>  {
                crate::bootstrap::run_bootstrap_events(event, &mut spawned_bootstrap_task, &mut stakestore, &mut votestore, &bootstrap_data);
            }
            //Manage RPC call result execution
            Some(Ok(event)) = spawned_leader_schedule_task.next() =>  {
                if let Some((new_schedule, epoch)) = crate::leader_schedule::run_leader_schedule_events(
                    event,
                    &mut spawned_leader_schedule_task,
                    &mut stakestore,
                    &mut votestore,
                ) {
                    //current_leader_schedule.leader_schedule = new_schedule;
                    current_leader_schedule.schedule_epoch = epoch;

                    //TODO remove verification when schedule ok.
                    //verify calculated shedule with the one the RPC return.
                    if let Some(schedule) = new_schedule {
                        tokio::task::spawn_blocking(|| {
                            //10 second that the schedule has been calculated on the validator
                            std::thread::sleep(std::time::Duration::from_secs(5));
                            log::info!("Start Verify schedule");
                            if let Err(err) = crate::leader_schedule::verify_schedule(schedule,RPC_URL.to_string()) {
                                log::warn!("Error during schedule verification:{err}");
                            }
                            log::info!("End Verify schedule");
                        });
                    }

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
                                        if let Some(account) = read_account(account, current_epoch_state.current_slot.confirmed_slot) {
                                            //log::trace!("Geyser receive new account");
                                            match account.owner {
                                                solana_sdk::stake::program::ID => {
                                                    log::trace!("Geyser receive new stake account:{account:?}");
                                                    if let Err(err) = stakestore.add_stake(
                                                        account,
                                                        current_epoch_state.current_epoch_end_slot(),
                                                        current_epoch_state.current_epoch.epoch,
                                                    ) {
                                                        log::warn!("Can't add new stake from account data err:{}", err);
                                                        continue;
                                                    }
                                                }
                                                solana_sdk::vote::program::ID => {
                                                    //process vote accout notification
                                                    if let Err(err) = votestore.add_vote(account, current_epoch_state.current_epoch_end_slot()) {
                                                        log::warn!("Can't add new stake from account data err:{}", err);
                                                        continue;
                                                    }
                                                }
                                                solana_sdk::system_program::ID => {
                                                    log::info!("system_program account:{}",account.pubkey);
                                                }
                                                _ => log::warn!("receive an account notification from a unknown owner:{account:?}"),
                                            }
                                        }
                                    }
                                    Some(UpdateOneof::Slot(slot)) => {
                                        log::trace!("Receive slot slot: {slot:?}");
                                        //TODO remove log
                                        //log::info!("Processing slot: {:?} current slot:{:?}", slot, current_slot);
                                        // log_end_epoch(
                                        //     current_epoch_state.current_slot.confirmed_slot,
                                        //     current_epoch_state.next_epoch_start_slot,
                                        //     current_epoch_state.current_epoch.slot_index,
                                        //     format!(
                                        //         "Receive slot: {:?} at commitment:{:?}",
                                        //         slot.slot,
                                        //         slot.status()
                                        //     ),
                                        // );

                                        let schedule_event = current_epoch_state.process_new_slot(&slot);
                                        if let Some(init_event) = schedule_event {
                                            crate::leader_schedule::run_leader_schedule_events(
                                                init_event,
                                                &mut spawned_leader_schedule_task,
                                                &mut stakestore,
                                                &mut votestore,
                                            );
                                        }

                                    }
                                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                                        log::info!("Receive Block Meta at slot: {}", block_meta.slot);
                                    }
                                    Some(UpdateOneof::Block(block)) => {
                                        log::trace!("Receive Block at slot: {}", block.slot);
                                        //parse to detect stake merge tx.
                                        //first in the main thread then in a specific thread.
                                        let stake_public_key: Vec<u8> = solana_sdk::stake::program::id().to_bytes().to_vec();
                                        for notif_tx in block.transactions {
                                            if !notif_tx.is_vote {
                                                if let Some(message) = notif_tx.transaction.and_then(|tx| tx.message) {
                                                    for instruction in message.instructions {
                                                        //filter stake tx
                                                        if message.account_keys[instruction.program_id_index as usize] ==  stake_public_key {
                                                            let source_bytes: [u8; 64] = notif_tx.signature[..solana_sdk::signature::SIGNATURE_BYTES]
                                                                .try_into()
                                                                .unwrap();
                                                            log::info!("New stake Tx sign:{} at block slot:{:?} current_slot:{}"
                                                                , solana_sdk::signature::Signature::from(source_bytes).to_string()
                                                                , block.slot
                                                                , current_epoch_state.current_slot.confirmed_slot
                                                            );
                                                            let program_index = instruction.program_id_index;
                                                            crate::stakestore::process_stake_tx_message(
                                                                &mut stakestore
                                                                , &message.account_keys
                                                                , instruction
                                                                , program_index
                                                            );
                                                        }
                                                    }
                                                }
                                            }
                                        }

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
                        //TODO call same initial code.
                        let new_confirmed_stream = client
                        .subscribe_once(
                            slots.clone(),
                            accounts.clone(),           //accounts
                            Default::default(), //tx
                            Default::default(), //entry
                            blocks.clone(), //full block
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
