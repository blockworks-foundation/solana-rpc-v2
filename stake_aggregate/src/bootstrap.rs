use crate::stakestore::{extract_stakestore, merge_stakestore, StakeMap, StakeStore};
use crate::votestore::{extract_votestore, merge_votestore, VoteMap, VoteStore};
use crate::Slot;
use futures_util::stream::FuturesUnordered;
use futures_util::TryFutureExt;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinHandle;
use tokio::time::Duration;
/*

Bootstrap state changes

  InitBootstrap
       |
  |Fetch accounts|

    |          |
  Error   BootstrapAccountsFetched(account list)
    |          |
 |Exit|     |Extract stores|
               |         |
            Error     StoreExtracted(account list, stores)
               |                       |
            | Wait(1s)|           |Merge accounts in store|
               |                            |          |
   BootstrapAccountsFetched(account list)  Error    AccountsMerged(stores)
                                            |                        |
                                      |Log and skip|          |Merges store|
                                      |Account     |            |         |
                                                               Error     End
                                                                |
                                                              |never occurs restart|
                                                                 |
                                                          InitBootstrap
*/

pub fn run_bootstrap_events(
    event: BootstrapEvent,
    bootstrap_tasks: &mut FuturesUnordered<JoinHandle<BootstrapEvent>>,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
    data: &BootstrapData,
) {
    let result = process_bootstrap_event(event, stakestore, votestore, data);
    match result {
        BootsrapProcessResult::TaskHandle(jh) => bootstrap_tasks.push(jh),
        BootsrapProcessResult::Event(event) => {
            run_bootstrap_events(event, bootstrap_tasks, stakestore, votestore, data)
        }
        BootsrapProcessResult::End => (),
    }
}

pub enum BootstrapEvent {
    InitBootstrap,
    BootstrapAccountsFetched(Vec<(Pubkey, Account)>, Vec<(Pubkey, Account)>),
    StoreExtracted(
        StakeMap,
        VoteMap,
        Vec<(Pubkey, Account)>,
        Vec<(Pubkey, Account)>,
    ),
    AccountsMerged(StakeMap, VoteMap),
    Exit,
}

enum BootsrapProcessResult {
    TaskHandle(JoinHandle<BootstrapEvent>),
    Event(BootstrapEvent),
    End,
}

pub struct BootstrapData {
    pub current_epoch: u64,
    pub next_epoch_start_slot: Slot,
    pub sleep_time: u64,
    pub rpc_url: String,
}

fn process_bootstrap_event(
    event: BootstrapEvent,
    stakestore: &mut StakeStore,
    votestore: &mut VoteStore,
    data: &BootstrapData,
) -> BootsrapProcessResult {
    match event {
        BootstrapEvent::InitBootstrap => {
            let jh = tokio::spawn({
                let rpc_url = data.rpc_url.clone();
                let sleep_time = data.sleep_time;
                async move {
                    log::info!("BootstrapEvent::InitBootstrap RECV");
                    if sleep_time > 0 {
                        tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                    }
                    match crate::bootstrap::bootstrap_accounts(rpc_url).await {
                        Ok((stakes, votes)) => {
                            BootstrapEvent::BootstrapAccountsFetched(stakes, votes)
                        }
                        Err(err) => {
                            log::warn!(
                                "Bootstrap account error during fetching accounts err:{err}. Exit"
                            );
                            BootstrapEvent::Exit
                        }
                    }
                }
            });
            BootsrapProcessResult::TaskHandle(jh)
        }
        BootstrapEvent::BootstrapAccountsFetched(stakes, votes) => {
            log::info!("BootstrapEvent::BootstrapAccountsFetched RECV");
            match (extract_stakestore(stakestore), extract_votestore(votestore)) {
                (Ok(stake_map), Ok(vote_map)) => BootsrapProcessResult::Event(
                    BootstrapEvent::StoreExtracted(stake_map, vote_map, stakes, votes),
                ),
                _ => {
                    let jh = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        BootstrapEvent::BootstrapAccountsFetched(stakes, votes)
                    });
                    BootsrapProcessResult::TaskHandle(jh)
                }
            }
        }
        BootstrapEvent::StoreExtracted(mut stake_map, mut vote_map, stakes, votes) => {
            log::trace!("BootstrapEvent::StoreExtracted RECV");
            //merge new PA with stake map and vote map in a specific task
            let jh = tokio::task::spawn_blocking({
                let next_epoch_start_slot = data.next_epoch_start_slot;
                let current_epoch = data.current_epoch;
                move || {
                    //update pa_list to set slot update to start epoq one.
                    crate::stakestore::merge_program_account_in_strake_map(
                        &mut stake_map,
                        stakes,
                        next_epoch_start_slot,
                        current_epoch,
                    );
                    crate::votestore::merge_program_account_in_vote_map(
                        &mut vote_map,
                        votes,
                        next_epoch_start_slot,
                    );
                    BootstrapEvent::AccountsMerged(stake_map, vote_map)
                }
            });
            BootsrapProcessResult::TaskHandle(jh)
        }
        BootstrapEvent::AccountsMerged(stake_map, vote_map) => {
            log::trace!("BootstrapEvent::AccountsMerged RECV");
            match (
                merge_stakestore(stakestore, stake_map, data.current_epoch),
                merge_votestore(votestore, vote_map),
            ) {
                (Ok(()), Ok(())) => BootsrapProcessResult::End,
                _ => {
                    //TODO remove this error using type state
                    log::warn!("BootstrapEvent::AccountsMerged merge stake or vote fail,  non extracted stake/vote map err, restart bootstrap");
                    BootsrapProcessResult::Event(BootstrapEvent::InitBootstrap)
                }
            }
        }
        BootstrapEvent::Exit => panic!("Bootstrap account can't be done exit"),
    }
}

async fn bootstrap_accounts(
    rpc_url: String,
) -> Result<(Vec<(Pubkey, Account)>, Vec<(Pubkey, Account)>), ClientError> {
    let furure = get_stake_account(rpc_url.clone()).and_then(|stakes| async move {
        get_vote_account(rpc_url).await.map(|votes| (stakes, votes))
    });
    furure.await
}

async fn get_stake_account(rpc_url: String) -> Result<Vec<(Pubkey, Account)>, ClientError> {
    log::info!("TaskToExec RpcGetStakeAccount start");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_stake = rpc_client
        .get_program_accounts(&solana_sdk::stake::program::id())
        .await;
    log::info!("TaskToExec RpcGetStakeAccount END");
    res_stake
}

async fn get_vote_account(rpc_url: String) -> Result<Vec<(Pubkey, Account)>, ClientError> {
    log::info!("TaskToExec RpcGetVoteAccount start");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_vote = rpc_client
        .get_program_accounts(&solana_sdk::vote::program::id())
        .await;
    log::info!("TaskToExec RpcGetVoteAccount END");
    res_vote
}
