use jsonrpsee_core::error::Error as JsonRpcError;
//use jsonrpsee_http_server::{HttpServerBuilder, HttpServerHandle, RpcModule};
use jsonrpsee_server::{RpcModule, Server, ServerHandle};
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::mpsc::Sender;

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
            .unwrap_or_else(|_| "error during request execution".to_string())
    })?;
    let server_handle = server.start(module);
    Ok(server_handle)
}
