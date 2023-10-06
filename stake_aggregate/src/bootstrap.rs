use crate::stakestore::{extract_stakestore, merge_stakestore, StakeMap, StakeStore};
use crate::votestore::{extract_votestore, merge_votestore, VoteMap, VoteStore};
use futures_util::stream::FuturesUnordered;
use solana_client::client_error::ClientError;
use solana_client::rpc_client::RpcClient;
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake_history::StakeHistory;
use std::time::Duration;
use tokio::task::JoinHandle;
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
    data: &mut BootstrapData,
) {
    let result = process_bootstrap_event(event, stakestore, votestore, data);
    match result {
        BootsrapProcessResult::TaskHandle(jh) => bootstrap_tasks.push(jh),
        BootsrapProcessResult::Event(event) => {
            run_bootstrap_events(event, bootstrap_tasks, stakestore, votestore, data)
        }
        BootsrapProcessResult::End => data.done = true,
    }
}

pub enum BootstrapEvent {
    InitBootstrap,
    BootstrapAccountsFetched(Vec<(Pubkey, Account)>, Vec<(Pubkey, Account)>, Account),
    StoreExtracted(
        StakeMap,
        VoteMap,
        Vec<(Pubkey, Account)>,
        Vec<(Pubkey, Account)>,
        Account,
    ),
    AccountsMerged(StakeMap, Option<StakeHistory>, VoteMap),
    Exit,
}

enum BootsrapProcessResult {
    TaskHandle(JoinHandle<BootstrapEvent>),
    Event(BootstrapEvent),
    End,
}

pub struct BootstrapData {
    pub done: bool,
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
            let jh = tokio::task::spawn_blocking({
                let rpc_url = data.rpc_url.clone();
                let sleep_time = data.sleep_time;
                move || {
                    log::info!("BootstrapEvent::InitBootstrap RECV");
                    if sleep_time > 0 {
                        std::thread::sleep(Duration::from_secs(sleep_time));
                    }
                    match crate::bootstrap::bootstrap_accounts(rpc_url) {
                        Ok((stakes, votes, history)) => {
                            BootstrapEvent::BootstrapAccountsFetched(stakes, votes, history)
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
        BootstrapEvent::BootstrapAccountsFetched(stakes, votes, history) => {
            log::info!("BootstrapEvent::BootstrapAccountsFetched RECV");
            match (extract_stakestore(stakestore), extract_votestore(votestore)) {
                (Ok((stake_map, _)), Ok(vote_map)) => BootsrapProcessResult::Event(
                    BootstrapEvent::StoreExtracted(stake_map, vote_map, stakes, votes, history),
                ),
                _ => {
                    let jh = tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        BootstrapEvent::BootstrapAccountsFetched(stakes, votes, history)
                    });
                    BootsrapProcessResult::TaskHandle(jh)
                }
            }
        }
        BootstrapEvent::StoreExtracted(mut stake_map, mut vote_map, stakes, votes, history) => {
            log::info!("BootstrapEvent::StoreExtracted RECV");

            let stake_history = crate::stakestore::read_historystake_from_account(history);
            if stake_history.is_none() {
                //TODO return error.
                log::error!("Bootstrap error, can't read stake history.");
            }

            //merge new PA with stake map and vote map in a specific task
            let jh = tokio::task::spawn_blocking({
                move || {
                    //update pa_list to set slot update to start epoq one.
                    crate::stakestore::merge_program_account_in_strake_map(
                        &mut stake_map,
                        stakes,
                        0, //with RPC no way to know the slot of the account update. Set to 0.
                    );
                    crate::votestore::merge_program_account_in_vote_map(
                        &mut vote_map,
                        votes,
                        0, //with RPC no way to know the slot of the account update. Set to 0.
                    );

                    BootstrapEvent::AccountsMerged(stake_map, stake_history, vote_map)
                }
            });
            BootsrapProcessResult::TaskHandle(jh)
        }
        BootstrapEvent::AccountsMerged(stake_map, stake_history, vote_map) => {
            log::info!("BootstrapEvent::AccountsMerged RECV");
            match (
                merge_stakestore(stakestore, stake_map, stake_history),
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

fn bootstrap_accounts(
    rpc_url: String,
) -> Result<(Vec<(Pubkey, Account)>, Vec<(Pubkey, Account)>, Account), ClientError> {
    get_stake_account(rpc_url)
        .and_then(|(stakes, rpc_url)| {
            get_vote_account(rpc_url).map(|(votes, rpc_url)| (stakes, votes, rpc_url))
        })
        .and_then(|(stakes, votes, rpc_url)| {
            get_stakehistory_account(rpc_url).map(|history| (stakes, votes, history))
        })
}

fn get_stake_account(rpc_url: String) -> Result<(Vec<(Pubkey, Account)>, String), ClientError> {
    log::info!("TaskToExec RpcGetStakeAccount start");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_stake = rpc_client.get_program_accounts(&solana_sdk::stake::program::id());
    log::info!("TaskToExec RpcGetStakeAccount END");
    res_stake.map(|stake| (stake, rpc_url))
}

fn get_vote_account(rpc_url: String) -> Result<(Vec<(Pubkey, Account)>, String), ClientError> {
    log::info!("TaskToExec RpcGetVoteAccount start");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_vote = rpc_client.get_program_accounts(&solana_sdk::vote::program::id());
    log::info!("TaskToExec RpcGetVoteAccount END");
    res_vote.map(|votes| (votes, rpc_url))
}

pub fn get_stakehistory_account(rpc_url: String) -> Result<Account, ClientError> {
    log::info!("TaskToExec RpcGetStakeHistory start");
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url,
        Duration::from_secs(600),
        CommitmentConfig::finalized(),
    );
    let res_stake = rpc_client.get_account(&solana_sdk::sysvar::stake_history::id());
    log::info!("TaskToExec RpcGetStakeHistory END",);
    res_stake
}
