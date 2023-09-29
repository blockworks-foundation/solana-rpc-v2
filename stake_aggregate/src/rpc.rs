use crate::epoch::CurrentEpochSlotState;
use crate::leader_schedule::CalculatedSchedule;
use crate::stakestore::StakeMap;
use crate::stakestore::StoredStake;
use crate::votestore::{StoredVote, VoteMap};
use crate::LeaderScheduleData;
use crate::Slot;
use jsonrpsee::core::Error as JsonRpcError;
use jsonrpsee::proc_macros::rpc;
use jsonrpsee::server::{RpcModule, Server, ServerHandle};
use jsonrpsee::types::error::ErrorObjectOwned as JsonRpcErrorOwned;
use solana_client::rpc_config::RpcContextConfig;
use solana_client::rpc_response::RpcBlockhash;
use solana_client::rpc_response::RpcVoteAccountStatus;
use solana_sdk::commitment_config::CommitmentConfig;
use std::sync::Arc;
//use solana_rpc_client_api::response::Response as RpcResponse;
use solana_sdk::epoch_info::EpochInfo;
use std::collections::HashMap;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

const PRIVATE_RPC_ADDRESS: &str = "0.0.0.0:3000";
const SERVER_ERROR_MSG: &str = "Internal server error";

//internal RPC access
#[derive(Debug, Error)]
pub enum ConsensusRpcError {
    #[error("Error during during json RPC request receive '{0}'")]
    JsonRpcError(#[from] JsonRpcError),

    #[error("Error during channel send '{0}'")]
    SendError(String),

    #[error("Error during channel receive '{0}'")]
    RcvError(#[from] tokio::sync::oneshot::error::RecvError),

    #[error("Bad RPC service address '{0}'")]
    AddressParseError(#[from] std::net::AddrParseError),

    #[error("Custom Error '{0}'")]
    Custom(String),
}

// Conversions for errors which occur in the context of a JSON-RPC method call.
// Crate-local error variants are converted to JSON-RPC errors which are
// then return to the caller.
impl From<ConsensusRpcError> for JsonRpcErrorOwned {
    fn from(err: ConsensusRpcError) -> Self {
        match &err {
            ConsensusRpcError::JsonRpcError(err_msg) => {
                JsonRpcErrorOwned::owned(-32000, SERVER_ERROR_MSG, Some(err_msg.to_string()))
            }
            ConsensusRpcError::AddressParseError(err_msg) => {
                JsonRpcErrorOwned::owned(-32001, SERVER_ERROR_MSG, Some(err_msg.to_string()))
            }
            ConsensusRpcError::RcvError(err_msg) => {
                JsonRpcErrorOwned::owned(-32002, SERVER_ERROR_MSG, Some(err_msg.to_string()))
            }
            _ => todo!(),
        }
    }
}

pub type Result<T> = std::result::Result<T, ConsensusRpcError>;

//public RPC access
#[rpc(server)]
pub trait ConsensusRpc {
    #[method(name = "getLatestBlockhash")]
    async fn get_latest_blockhash(&self, config: Option<RpcContextConfig>) -> Result<RpcBlockhash>;

    #[method(name = "getSlot")]
    async fn get_slot(&self, config: Option<RpcContextConfig>) -> Result<Slot>;

    #[method(name = "getEpochInfo")]
    async fn get_epoch_info(&self) -> Result<EpochInfo>;

    #[method(name = "getVoteAccounts")]
    async fn get_vote_accounts(&self) -> Result<RpcVoteAccountStatus>;
}

pub struct RPCServer {
    request_tx: Sender<Requests>,
}

#[jsonrpsee::core::async_trait]
impl ConsensusRpcServer for RPCServer {
    async fn get_latest_blockhash(&self, config: Option<RpcContextConfig>) -> Result<RpcBlockhash> {
        todo!()
    }

    async fn get_slot(&self, config: Option<RpcContextConfig>) -> Result<Slot> {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.request_tx.send(Requests::Slot(tx, config)).await {
            return Err(ConsensusRpcError::SendError(err.to_string()));
        }
        Ok(rx.await?.ok_or(ConsensusRpcError::Custom(
            "No slot after min slot".to_string(),
        ))?)
    }

    async fn get_epoch_info(&self) -> Result<EpochInfo> {
        let (tx, rx) = oneshot::channel();
        if let Err(err) = self.request_tx.send(Requests::EpochInfo(tx)).await {
            return Err(ConsensusRpcError::SendError(err.to_string()));
        }
        Ok(rx.await?)
    }

    async fn get_vote_accounts(&self) -> Result<RpcVoteAccountStatus> {
        todo!()
    }
}

pub enum Requests {
    SaveStakes,
    GetStakestore(tokio::sync::oneshot::Sender<(StakeMap, Slot)>),
    GetVotestore(tokio::sync::oneshot::Sender<(VoteMap, Slot)>),
    EpochInfo(tokio::sync::oneshot::Sender<EpochInfo>),
    Slot(
        tokio::sync::oneshot::Sender<Option<Slot>>,
        Option<RpcContextConfig>,
    ),
    LeaderSchedule(
        tokio::sync::oneshot::Sender<Option<Arc<HashMap<String, Vec<usize>>>>>,
        Option<u64>,
    ),
}

pub fn server_rpc_request(
    request: Requests,
    current_epoch_state: &CurrentEpochSlotState,
    leader_schedules: &CalculatedSchedule,
) {
    match request {
        crate::rpc::Requests::EpochInfo(tx) => {
            if let Err(err) = tx.send(current_epoch_state.current_epoch.clone()) {
                log::warn!("Channel error during sending back request status error:{err:?}");
            }
        }
        crate::rpc::Requests::Slot(tx, config) => {
            let slot = config.and_then(|conf| {
                let slot = current_epoch_state.current_slot.get_slot_with_commitment(
                    conf.commitment.unwrap_or(CommitmentConfig::confirmed()),
                );
                (slot >= conf.min_context_slot.unwrap_or(0)).then_some(slot)
            });
            if let Err(err) = tx.send(slot) {
                log::warn!("Channel error during sending back request status error:{err:?}");
            }
        }
        crate::rpc::Requests::LeaderSchedule(tx, slot) => {
            let slot = slot.unwrap_or_else(|| current_epoch_state.current_slot.confirmed_slot);
            let epoch = crate::epoch::get_epoch_for_slot(slot, current_epoch_state);

            log::info!(
                "Requests::LeaderSchedule slot:{slot} epoch:{epoch} current epoch:{}",
                current_epoch_state.current_epoch.epoch
            );
            //currently only return schedule for current of next epoch.
            let get_schedule_fn = |schedule: &LeaderScheduleData| {
                (schedule.epoch == epoch).then(|| schedule.schedule.clone()) //Arc clone.
            };
            let schedule = leader_schedules
                .current
                .as_ref()
                .and_then(get_schedule_fn)
                .or_else(|| leader_schedules.next.as_ref().and_then(get_schedule_fn));

            if let Err(err) = tx.send(schedule) {
                log::warn!("Channel error during sending back request status error:{err:?}");
            }
        }
        _ => unreachable!(),
    }
}

//start private RPC access

pub(crate) async fn run_server(request_tx: Sender<Requests>) -> Result<ServerHandle> {
    let server = Server::builder()
        .max_response_body_size(1048576000)
        .build(PRIVATE_RPC_ADDRESS.parse::<SocketAddr>()?)
        .await?;
    let mut module = RpcModule::new(request_tx);

    //Public RPC method
    //use this way to process the RPC call to avoid to clone the content of the schedule.
    //in the trait version, we have to return the full data that are serialized in json after.
    module.register_async_method("getLeaderSchedule", |params, request_tx| async move {
        log::info!("RPC getLeaderSchedule");
        let slot: Option<u64> = params.one().unwrap_or(None);
        let (tx, rx) = oneshot::channel();
        if let Err(err) = request_tx.send(Requests::LeaderSchedule(tx, slot)).await {
            return serde_json::Value::String(format!("error during query processing:{err}",));
        }
        rx.await
            .map_err(|err| format!("error during bootstrap query processing:{err}"))
            .and_then(|schedule| {
                serde_json::to_value(&schedule).map_err(|err| {
                    format!(
                        "error during json serialisation:{}",
                        JsonRpcError::ParseError(err)
                    )
                })
            })
            .unwrap_or_else(|err| serde_json::Value::String(err.to_string()))
    })?;

    //Test RPC method
    module.register_async_method("save_stakes", |_params, request_tx| async move {
        log::trace!("RPC save_stakes");
        request_tx
            .send(Requests::SaveStakes)
            .await
            .map(|_| "Get save_stakes status successfully".to_string())
            .unwrap_or_else(|_| "error during save_stakes request execution".to_string())
    })?;
    module.register_async_method("bootstrap_accounts", |_params, request_tx| async move {
        log::trace!("RPC bootstrap_accounts");
        let (tx, rx) = oneshot::channel();
        if let Err(err) = request_tx.send(Requests::GetStakestore(tx)).await {
            return serde_json::Value::String(format!("error during query processing:{err}",));
        }
        rx.await
            .map_err(|err| format!("error during bootstrap query processing:{err}"))
            .and_then(|(accounts, slot)| {
                println!("RPC end request status");
                //replace pubkey with String. Json only allow distionary key with string.
                let ret: HashMap<String, StoredStake> = accounts
                    .into_iter()
                    .map(|(pk, acc)| (pk.to_string(), acc))
                    .collect();
                serde_json::to_value((slot, ret)).map_err(|err| {
                    format!(
                        "error during json serialisation:{}",
                        JsonRpcError::ParseError(err)
                    )
                })
            })
            .unwrap_or_else(|err| serde_json::Value::String(err.to_string()))
    })?;
    module.register_async_method("stake_accounts", |_params, request_tx| async move {
        log::trace!("RPC bootstrap_accounts");
        let (tx, rx) = oneshot::channel();
        if let Err(err) = request_tx.send(Requests::GetVotestore(tx)).await {
            return serde_json::Value::String(format!("error during query processing:{err}",));
        }
        rx.await
            .map_err(|err| format!("error during bootstrap query processing:{err}"))
            .and_then(|(accounts, slot)| {
                println!("RPC end request status");
                if slot == 0 {
                    Err("Error stake store extracted".to_string())
                } else {
                    //replace pubkey with String. Json only allow distionary key with string.
                    let ret: HashMap<String, StoredVote> = accounts
                        .into_iter()
                        .map(|(pk, acc)| (pk.to_string(), acc))
                        .collect();
                    serde_json::to_value((slot, ret)).map_err(|err| {
                        format!(
                            "error during json serialisation:{}",
                            JsonRpcError::ParseError(err)
                        )
                    })
                }
            })
            .unwrap_or_else(|err| serde_json::Value::String(err.to_string()))
    })?;
    let server_handle = server.start(module);
    Ok(server_handle)
}
