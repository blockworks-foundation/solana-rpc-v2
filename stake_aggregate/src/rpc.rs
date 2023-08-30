use reqwest;
use serde::Deserialize;
use serde_json::json;
use solana_rpc_client_api::{
    config::RpcProgramAccountsConfig,
    response::{OptionalContext, RpcKeyedAccount},
};
use solana_sdk::account::Account;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use tokio::time::Duration;

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    jsonrpc: String,
    result: T,
    id: i32,
}

// #[derive(Debug, Deserialize)]
// struct ProgramAccount {
//     pubkey: String,
//     account: UiAccount,
// }

pub async fn get_program_accounts(
    url: &str,
    program_id: &Pubkey,
) -> Result<Vec<(Pubkey, Account)>, String> {
    let mut default_headers = reqwest::header::HeaderMap::new();
    default_headers.append(
        reqwest::header::HeaderName::from_static("solana-client"),
        reqwest::header::HeaderValue::from_str(
            format!("rust/{}", solana_version::Version::default()).as_str(),
        )
        .unwrap(),
    );
    let client = reqwest::Client::builder()
        .default_headers(default_headers)
        .timeout(Duration::from_secs(600)) // 10 minutes in seconds
        .build()
        .map_err(|err| format!("{err}"))?;

    let mut config = RpcProgramAccountsConfig::default();
    let commitment = CommitmentConfig::confirmed();
    config.account_config.commitment = Some(commitment);

    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            program_id.to_string(),
            config
        ]
    });

    log::info!("{payload}");

    // let payload = json!([pubkey.to_string(), config])

    let resp = client
        .post(url)
        .json(&payload)
        .send()
        .await
        .map_err(|err| format!("{err}"))?;

    log::info!("{resp:?}");
    log::info!("{:?}", resp.text().await);

    // let resp: RpcResponse<Vec<RpcKeyedAccount>> =
    //     resp.json().await.map_err(|err| format!("{err}"))?;

    // let accounts = parse_keyed_accounts(resp.result)?;
    // Ok(accounts)
    Ok(vec![])
}

fn parse_keyed_accounts(accounts: Vec<RpcKeyedAccount>) -> Result<Vec<(Pubkey, Account)>, String> {
    let mut pubkey_accounts: Vec<(Pubkey, Account)> = Vec::with_capacity(accounts.len());
    for RpcKeyedAccount { pubkey, account } in accounts.into_iter() {
        let pubkey = pubkey.parse().map_err(|err| format!("{err}"))?;
        pubkey_accounts.push((
            pubkey,
            account
                .decode()
                .ok_or_else(|| "Parse error for accour from rpc".to_string())?,
        ));
    }
    Ok(pubkey_accounts)
}

// pub async fn send<T>(&self, request: RpcRequest, params: Value) -> ClientResult<T>
// where
//     T: serde::de::DeserializeOwned,
// {
//     assert!(params.is_array() || params.is_null());

//     let response = self
//         .sender
//         .send(request, params)
//         .await
//         .map_err(|err| err.into_with_request(request))?;
//     serde_json::from_value(response)
//         .map_err(|err| ClientError::new_with_request(err.into(), request))
// }

// pub async fn get_program_accounts_with_config(
//     &self,
//     pubkey: &Pubkey,
//     mut config: RpcProgramAccountsConfig,
// ) -> ClientResult<Vec<(Pubkey, Account)>> {
//     let commitment = config
//         .account_config
//         .commitment
//         .unwrap_or_else(|| self.commitment());
//     let commitment = self.maybe_map_commitment(commitment).await?;
//     config.account_config.commitment = Some(commitment);
//     if let Some(filters) = config.filters {
//         config.filters = Some(self.maybe_map_filters(filters).await?);
//     }

//     let accounts = self
//         .send::<OptionalContext<Vec<RpcKeyedAccount>>>(
//             RpcRequest::GetProgramAccounts,
//             json!([pubkey.to_string(), config]),
//         )
//         .await?
//         .parse_value();
//     parse_keyed_accounts(accounts, RpcRequest::GetProgramAccounts)
// }
