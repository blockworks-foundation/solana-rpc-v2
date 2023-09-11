use crate::stakestore::StakeMap;
use crate::stakestore::StoredStake;
use crate::Slot;
use jsonrpsee_core::error::Error as JsonRpcError;
use std::collections::HashMap;
//use jsonrpsee_http_server::{HttpServerBuilder, HttpServerHandle, RpcModule};
use jsonrpsee_server::{RpcModule, Server, ServerHandle};
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

const RPC_ADDRESS: &str = "0.0.0.0:3000";

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("Error during during json RPC request receive '{0}'")]
    JsonRpcError(#[from] JsonRpcError),

    #[error("Bad RPC service address '{0}'")]
    AddressParseError(#[from] std::net::AddrParseError),
}

//start RPC access

pub enum Requests {
    SaveStakes,
    BootstrapAccounts(tokio::sync::oneshot::Sender<(StakeMap, Slot)>),
}

pub(crate) async fn run_server(request_tx: Sender<Requests>) -> Result<ServerHandle, RpcError> {
    let server = Server::builder()
        .build(RPC_ADDRESS.parse::<SocketAddr>()?)
        .await?;
    let mut module = RpcModule::new(request_tx);

    //register start Batch Tx send entry point
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
        if let Err(err) = request_tx.send(Requests::BootstrapAccounts(tx)).await {
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
    let server_handle = server.start(module);
    Ok(server_handle)
}
