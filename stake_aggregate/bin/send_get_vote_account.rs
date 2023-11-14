use futures_util::future::join_all;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

const LITE_RPC_URL: &str = "http://127.0.0.1:8890";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    log::info!("Start send get vote account");
    let mut handles = vec![];

    for _ in 0..10 {
        let h = tokio::task::spawn(async move {
            call_get_vote_account().await;
        });
        handles.push(h);
    }
    join_all(handles).await;
    //std::thread::sleep(std::time::Duration::from_millis(1000000));
    Ok(())
}

async fn call_get_vote_account() {
    let rpc_client =
        RpcClient::new_with_commitment(LITE_RPC_URL.to_string(), CommitmentConfig::confirmed());
    loop {
        match tokio::time::timeout(
            std::time::Duration::from_millis(1000),
            rpc_client.get_vote_accounts(),
        )
        .await
        {
            Ok(Ok(accounts)) => {
                if accounts.current.len() == 0 {
                    log::info!("get vote account len:0",);
                }
            }
            Err(_) => panic!("get_vote_account timeout"),
            Ok(Err(err)) => panic!("get_vote_account rpc error:{err}"),
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
