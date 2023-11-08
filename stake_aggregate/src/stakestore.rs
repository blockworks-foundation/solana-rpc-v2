use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::account::Account;
use solana_sdk::account::AccountSharedData;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::instruction::StakeInstruction;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake::state::StakeState;
use solana_sdk::stake_history::StakeHistory;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use yellowstone_grpc_proto::solana::storage::confirmed_block::CompiledInstruction;

pub type StakeMap = HashMap<Pubkey, StoredStake>;

pub fn extract_stakestore(
    stakestore: &mut StakeStore,
) -> anyhow::Result<(StakeMap, Option<StakeHistory>)> {
    let new_store = std::mem::take(stakestore);
    let (new_store, stake_map) = new_store.extract()?;
    *stakestore = new_store;
    Ok(stake_map)
}

pub fn merge_stakestore(
    stakestore: &mut StakeStore,
    stake_map: StakeMap,
    stake_history: Option<StakeHistory>,
) -> anyhow::Result<()> {
    let new_store = std::mem::take(stakestore);
    let new_store = new_store.merge_stakes(stake_map, stake_history)?;
    *stakestore = new_store;
    Ok(())
}

fn stake_map_notify_stake(map: &mut StakeMap, stake: StoredStake) {
    //log::info!("stake_map_notify_stake stake:{stake:?}");
    match map.entry(stake.pubkey) {
        // If value already exists, then increment it by one
        std::collections::hash_map::Entry::Occupied(occupied) => {
            let strstake = occupied.into_mut(); // <-- get mut reference to existing value
                                                //doesn't erase new state with an old one. Can arrive during bootstrapping.
                                                //several instructions can be done in the same slot.
            if strstake.last_update_slot <= stake.last_update_slot {
                log::trace!("stake_map_notify_stake Stake store updated stake: {} old_stake:{strstake:?} stake:{stake:?}", stake.pubkey);
                *strstake = stake;
            }
        }
        // If value doesn't exist yet, then insert a new value of 1
        std::collections::hash_map::Entry::Vacant(vacant) => {
            log::trace!(
                "stake_map_notify_stake Stake store insert stake: {} stake:{stake:?}",
                stake.pubkey
            );
            vacant.insert(stake);
        }
    };
}

#[derive(Debug, Default)]
pub enum ExtractedAction {
    Notify {
        stake: StoredStake,
    },
    Remove(Pubkey, Slot),
    Merge {
        source_account: Pubkey,
        destination_account: Pubkey,
        update_slot: Slot,
    },
    #[default]
    None,
}

impl ExtractedAction {
    fn get_update_slot(&self) -> u64 {
        match self {
            ExtractedAction::Notify { stake } => stake.last_update_slot,
            ExtractedAction::Remove(_, slot) => *slot,
            ExtractedAction::Merge { update_slot, .. } => *update_slot,
            ExtractedAction::None => 0,
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredStake {
    pub pubkey: Pubkey,
    pub lamports: u64,
    pub stake: Delegation,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Debug, Default)]
pub struct StakeStore {
    stakes: StakeMap,
    stake_history: Option<StakeHistory>,
    pub updates: Vec<ExtractedAction>,
    pub extracted: bool,
}

impl StakeStore {
    pub fn new(capacity: usize) -> Self {
        StakeStore {
            stakes: HashMap::with_capacity(capacity),
            stake_history: None,
            updates: vec![],
            extracted: false,
        }
    }

    // pub fn set_stake_history(&mut self, stake_history: StakeHistory) {
    //     self.stake_history = Some(Arc::new(stake_history));
    // }

    pub fn get_stake_history(&self) -> Option<StakeHistory> {
        self.stake_history.clone()
    }

    pub fn nb_stake_account(&self) -> usize {
        self.stakes.len()
    }

    pub fn get_cloned_stake_map(&self) -> StakeMap {
        self.stakes.clone()
    }

    pub fn notify_stake_change(
        &mut self,
        account: AccountPretty,
        current_end_epoch_slot: Slot,
    ) -> anyhow::Result<()> {
        //if lamport == 0 the account has been removed.
        if account.lamports == 0 {
            self.notify_stake_action(
                ExtractedAction::Remove(account.pubkey, account.slot),
                current_end_epoch_slot,
            );
        } else {
            let Ok(delegated_stake_opt) = account.read_stake() else {
                bail!("Can't read stake from account data");
            };

            if let Some(delegated_stake) = delegated_stake_opt {
                let stake = StoredStake {
                    pubkey: account.pubkey,
                    lamports: account.lamports,
                    stake: delegated_stake,
                    last_update_slot: account.slot,
                    write_version: account.write_version,
                };

                self.notify_stake_action(ExtractedAction::Notify { stake }, current_end_epoch_slot);
            }
        }

        Ok(())
    }
    pub fn notify_stake_action(&mut self, action: ExtractedAction, current_end_epoch_slot: Slot) {
        //during extract push the new update or
        //don't insertnow account change that has been done in next epoch.
        //put in update pool to be merged next epoch change.
        let insert_stake = !self.extracted || action.get_update_slot() > current_end_epoch_slot;
        match insert_stake {
            false => self.updates.push(action),
            true => self.process_stake_action(action),
        }
    }

    fn process_stake_action(&mut self, action: ExtractedAction) {
        match action {
            ExtractedAction::Notify { stake } => {
                stake_map_notify_stake(&mut self.stakes, stake);
            }
            ExtractedAction::Remove(account_pk, slot) => self.remove_from_store(&account_pk, slot),
            //not use currently. TODO remove.
            ExtractedAction::Merge {
                source_account: _,
                destination_account: _,
                update_slot: _,
            } => {
                //TODO
                ()
            }
            ExtractedAction::None => (),
        }
    }

    //return the contained stake map to do an external update.
    // During extract period (between extract and merge) added stake a stored to be processed later.
    //if the store is already extracted return an error.
    fn extract(self) -> anyhow::Result<(Self, (StakeMap, Option<StakeHistory>))> {
        if self.extracted {
            bail!("StakeStore already extracted. Try later");
        }
        let stakestore = StakeStore {
            stakes: HashMap::new(),
            stake_history: None,
            updates: self.updates,
            extracted: true,
        };
        Ok((stakestore, (self.stakes, self.stake_history)))
    }

    //Merge the stake map an,d replace the current one.
    fn merge_stakes(
        mut self,
        stakes: StakeMap,
        stake_history: Option<StakeHistory>,
    ) -> anyhow::Result<Self> {
        if !self.extracted {
            bail!("StakeStore merge of non extracted map. Try later");
        }
        let mut stakestore = StakeStore {
            stakes,
            stake_history,
            updates: vec![],
            extracted: false,
        };

        self.updates
            .sort_unstable_by(|a, b| a.get_update_slot().cmp(&b.get_update_slot()));
        //apply stake added during extraction.
        for action in self.updates {
            stakestore.process_stake_action(action);
        }
        Ok(stakestore)
    }

    fn remove_from_store(&mut self, account_pk: &Pubkey, update_slot: Slot) {
        if self
            .stakes
            .get(account_pk)
            .map(|stake| stake.last_update_slot <= update_slot)
            .unwrap_or(true)
        {
            log::info!("Stake remove_from_store for {}", account_pk.to_string());
            self.stakes.remove(account_pk);
        }
    }
}

pub fn merge_program_account_in_strake_map(
    stake_map: &mut StakeMap,
    stakes_list: Vec<(Pubkey, Account)>,
    last_update_slot: Slot,
) {
    stakes_list
        .into_iter()
        .filter_map(
            |(pk, account)| match read_stake_from_account_data(&account.data) {
                Ok(opt_stake) => opt_stake.map(|stake| (pk, stake, account.lamports)),
                Err(err) => {
                    log::warn!("Error during pa account data deserialisation:{err}");
                    None
                }
            },
        )
        .for_each(|(pk, delegated_stake, lamports)| {
            //log::info!("RPC merge {pk} stake:{delegated_stake:?}");
            let stake = StoredStake {
                pubkey: pk,
                lamports,
                stake: delegated_stake,
                last_update_slot,
                write_version: 0,
            };

            stake_map_notify_stake(stake_map, stake);
        });
}

pub fn read_stake_from_account_data(mut data: &[u8]) -> anyhow::Result<Option<Delegation>> {
    if data.is_empty() {
        log::warn!("Stake account with empty data. Can't read stake.");
        bail!("Error: read Stake account with empty data");
    }
    match BorshDeserialize::deserialize(&mut data)? {
        StakeState::Stake(_, stake) => Ok(Some(stake.delegation)),
        StakeState::Initialized(_) => Ok(None),
        StakeState::Uninitialized => Ok(None),
        StakeState::RewardsPool => Ok(None),
    }
}

pub fn read_historystake_from_account(account: Account) -> Option<StakeHistory> {
    solana_sdk::account::from_account::<StakeHistory, _>(&AccountSharedData::from(account))
}

pub async fn start_stake_verification_loop(
    rpc_url: String,
) -> Sender<(String, Pubkey, Option<StoredStake>)> {
    let (request_tx, mut request_rx) =
        tokio::sync::mpsc::channel::<(String, Pubkey, Option<StoredStake>)>(100);
    tokio::spawn(async move {
        while let Some((instruction, stake_pk, stake)) = request_rx.recv().await {
            tokio::spawn({
                let rpc_url = rpc_url.clone();
                async move {
                    tokio::time::sleep(tokio::time::Duration::from_millis(5000)).await;
                    //get stake from RPC
                    let rpc_client =
                        solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment(
                            rpc_url,
                            solana_sdk::commitment_config::CommitmentConfig::finalized(),
                        );
                    match rpc_client.get_account(&stake_pk).await {
                        Ok(account) => {
                            let stake_data = crate::stakestore::read_stake_from_account_data(
                                account.data.as_slice(),
                            );
                            log::info!(
                                    "VERIFICATION Instruction:{instruction} Account:{} verification current stake:{stake:?} Rpc stake:{stake_data:?}",
                                    stake_pk
                                );
                        }
                        Err(solana_client::client_error::ClientError {
                            kind:
                                solana_client::client_error::ClientErrorKind::RpcError(
                                    solana_client::rpc_request::RpcError::ForUser(msg),
                                ),
                            ..
                        }) => {
                            log::info!(
                                "VERIFICATION Instruction:{instruction} Account:{} verification RPC not found:{}",
                                stake_pk,
                                msg
                            );
                        }
                        Err(err) => {
                            log::info!(
                                "VERIFICATION Instruction:{instruction} Account:{} verification RPC error:{err}",
                                stake_pk
                            );
                        }
                    }
                }
            });
        }
    });
    request_tx
}

async fn send_verification(
    stake_sender: &mut Sender<(String, Pubkey, Option<StoredStake>)>,
    stakestore: &mut StakeStore,
    instr: &str,
    stake_pybkey: Pubkey,
) {
    let current_stake = stakestore.stakes.get(&stake_pybkey).cloned();
    stake_sender
        .send((instr.to_string(), stake_pybkey, current_stake))
        .await
        .unwrap();
}

fn verify_account_len(account_keys: &[Pubkey], instr_accounts: &[u8], indexes: Vec<usize>) -> bool {
    !indexes
        .into_iter()
        .filter(|index| (instr_accounts[*index] as usize) >= account_keys.len())
        .next()
        .is_some()
}

pub async fn process_stake_tx_message(
    stake_sender: &mut Sender<(String, Pubkey, Option<StoredStake>)>,
    stakestore: &mut StakeStore,
    account_keys_vec: &[Vec<u8>],
    instruction: CompiledInstruction,
    //for debug and trace purpose.
    program_id_index: u32,
    tx_slot: Slot,
    current_end_epoch_slot: u64,
) {
    //for tracing purpose
    let account_keys: Vec<Pubkey> = account_keys_vec
        .iter()
        .map(|acc_bytes| {
            let source_bytes: [u8; 32] = acc_bytes[..solana_sdk::pubkey::PUBKEY_BYTES]
                .try_into()
                .unwrap();
            Pubkey::new_from_array(source_bytes)
        })
        .collect();

    //for debug get stake account index
    log::info!(
        "Found tx with stake account accounts:{:?} stake account index:{program_id_index:?}",
        account_keys,
    );

    //merge and delegate has 1 instruction. Create as 2 instructions and the first is not a StakeInstruction.
    log::info!(
        "Before read instruction of program_id_index:{}",
        instruction.program_id_index
    );
    let Ok(stake_inst) = bincode::deserialize::<StakeInstruction>(&instruction.data) else {
        log::info!(
            "Error during stake instruction decoding  :{:?}",
            &instruction.data
        );
        return;
    };
    match stake_inst {
        StakeInstruction::Initialize(authorized, lockup) => {
            let authorized = json!({
                "staker": authorized.staker.to_string(),
                "withdrawer": authorized.withdrawer.to_string(),
            });
            let lockup = json!({
                "unixTimestamp": lockup.unix_timestamp,
                "epoch": lockup.epoch,
                "custodian": lockup.custodian.to_string(),
            });

            if verify_account_len(&account_keys, &instruction.accounts, vec![0, 1]) {
                log::info!("StakeInstruction::Initialize authorized:{authorized} lockup:{lockup} stakeAccount:{} rentSysvar:{}"
                    , account_keys[instruction.accounts[0] as usize].to_string()
                    , account_keys[instruction.accounts[1] as usize].to_string()
                );
            } else {
                log::warn!("StakeInstruction::Initialize authorized:{authorized} lockup:{lockup} Index error in instruction:{:?}",  instruction.accounts);
            }

            send_verification(
                stake_sender,
                stakestore,
                "Initialize",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::Authorize(new_authorized, authority_type) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2]) {
                log::warn!(
                    "StakeInstruction::Authorize authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let value = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                "authority": account_keys[instruction.accounts[2] as usize].to_string(),
                "newAuthority": new_authorized.to_string(),
                "authorityType": authority_type,
            });
            let custodian = (instruction.accounts.len() >= 4).then(|| {
                format!(
                    "custodian:{}",
                    json!(account_keys[instruction.accounts[3] as usize].to_string())
                )
            });
            log::info!("StakeInstruction::Authorize value:{value} custodian:{custodian:?}");

            send_verification(
                stake_sender,
                stakestore,
                "Authorize",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::DelegateStake => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3, 4, 5]) {
                log::warn!(
                    "StakeInstruction::DelegateStake authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "voteAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                "stakeHistorySysvar": account_keys[instruction.accounts[3] as usize].to_string(),
                "stakeConfigAccount": account_keys[instruction.accounts[4] as usize].to_string(),
                "stakeAuthority": account_keys[instruction.accounts[5] as usize].to_string(),
            });
            log::info!("StakeInstruction::DelegateStake infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "DelegateStake",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::Split(lamports) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2]) {
                log::warn!(
                    "StakeInstruction::Split authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "newSplitAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                "stakeAuthority": account_keys[instruction.accounts[2] as usize].to_string(),
                "lamports": lamports,
            });
            log::info!("StakeInstruction::Split infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "Split",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::Withdraw(lamports) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3, 4]) {
                log::warn!(
                    "StakeInstruction::Withdraw authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "destination": account_keys[instruction.accounts[1] as usize].to_string(),
                "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                "stakeHistorySysvar": account_keys[instruction.accounts[3] as usize].to_string(),
                "withdrawAuthority": account_keys[instruction.accounts[4] as usize].to_string(),
                "lamports": lamports,
            });
            let custodian = (instruction.accounts.len() >= 6)
                .then(|| json!(account_keys[instruction.accounts[5] as usize].to_string()));
            log::info!("StakeInstruction::Withdraw custodian:{custodian:?}infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "Withdraw",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::Deactivate => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2]) {
                log::warn!(
                    "StakeInstruction::Deactivate authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                "stakeAuthority": account_keys[instruction.accounts[2] as usize].to_string(),
            });
            log::info!("StakeInstruction::Deactivate infos:{info}");
        }
        StakeInstruction::SetLockup(lockup_args) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1]) {
                log::warn!(
                    "StakeInstruction::SetLockup authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let unix_timestamp = lockup_args
                .unix_timestamp
                .map(|timestamp| json!(timestamp))
                .unwrap_or_else(|| json!("none"));
            let epoch = lockup_args
                .epoch
                .map(|epoch| json!(epoch))
                .unwrap_or_else(|| json!("none"));
            let custodian = lockup_args
                .custodian
                .map(|custodian| json!(custodian))
                .unwrap_or_else(|| json!("none"));
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "custodian": account_keys[instruction.accounts[1] as usize].to_string(),
            });
            log::info!("StakeInstruction::SetLockup unixTimestamp:{unix_timestamp} epoch:{epoch} custodian:{custodian} infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "SetLockup",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::AuthorizeWithSeed(args) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1]) {
                log::warn!(
                    "StakeInstruction::AuthorizeWithSeed authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "authorityBase": account_keys[instruction.accounts[1] as usize].to_string(),
                    "newAuthorized": args.new_authorized_pubkey.to_string(),
                    "authorityType": args.stake_authorize,
                    "authoritySeed": args.authority_seed,
                    "authorityOwner": args.authority_owner.to_string(),
            });
            let clock_sysvar = (instruction.accounts.len() >= 3)
                .then(|| json!(account_keys[instruction.accounts[2] as usize].to_string()));
            let custodian = (instruction.accounts.len() >= 4)
                .then(|| json!(account_keys[instruction.accounts[3] as usize].to_string()));
            log::info!("StakeInstruction::AuthorizeWithSeed clockSysvar:{clock_sysvar:?} custodian:{custodian:?} infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "AuthorizeWithSeed",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::InitializeChecked => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3]) {
                log::warn!(
                    "StakeInstruction::InitializeChecked authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "rentSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                "staker": account_keys[instruction.accounts[2] as usize].to_string(),
                "withdrawer": account_keys[instruction.accounts[3] as usize].to_string(),
            });
            log::info!("StakeInstruction::InitializeChecked infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "InitializeChecked",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::AuthorizeChecked(authority_type) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3]) {
                log::warn!(
                    "StakeInstruction::AuthorizeChecked authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                "authority": account_keys[instruction.accounts[2] as usize].to_string(),
                "newAuthority": account_keys[instruction.accounts[3] as usize].to_string(),
                "authorityType": authority_type,
            });
            let custodian = (instruction.accounts.len() >= 5)
                .then(|| json!(account_keys[instruction.accounts[4] as usize].to_string()));
            log::info!("StakeInstruction::AuthorizeChecked custodian:{custodian:?} infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "AuthorizeChecked",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::AuthorizeCheckedWithSeed(args) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3]) {
                log::warn!(
                    "StakeInstruction::AuthorizeCheckedWithSeed authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "authorityBase": account_keys[instruction.accounts[1] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                    "newAuthorized": account_keys[instruction.accounts[3] as usize].to_string(),
                    "authorityType": args.stake_authorize,
                    "authoritySeed": args.authority_seed,
                    "authorityOwner": args.authority_owner.to_string(),
            });
            let custodian = (instruction.accounts.len() >= 5)
                .then(|| json!(account_keys[instruction.accounts[4] as usize].to_string()));
            log::info!(
                "StakeInstruction::AuthorizeCheckedWithSeed custodian:{custodian:?} infos:{info}"
            );

            send_verification(
                stake_sender,
                stakestore,
                "AuthorizeCheckedWithSeed",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::SetLockupChecked(lockup_args) => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2]) {
                log::warn!(
                    "StakeInstruction::SetLockupChecked authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let unix_timestamp = lockup_args
                .unix_timestamp
                .map(|timestamp| json!(timestamp))
                .unwrap_or_else(|| json!("none"));
            let epoch = lockup_args
                .epoch
                .map(|epoch| json!(epoch))
                .unwrap_or_else(|| json!("none"));
            let custodian = (instruction.accounts.len() >= 3)
                .then(|| json!(account_keys[instruction.accounts[2] as usize].to_string()));
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "custodian": account_keys[instruction.accounts[1] as usize].to_string(),
            });
            log::info!("StakeInstruction::SetLockupChecked unixTimestamp:{unix_timestamp} epoch:{epoch} custodian:{custodian:?} infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "SetLockupChecked",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::GetMinimumDelegation => {
            log::info!("StakeInstruction::GetMinimumDelegation");
        }
        StakeInstruction::DeactivateDelinquent => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2]) {
                log::warn!(
                    "StakeInstruction::DeactivateDelinquent authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "voteAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                "referenceVoteAccount": account_keys[instruction.accounts[2] as usize].to_string(),
            });
            log::info!("StakeInstruction::DeactivateDelinquent infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "DeactivateDelinquent",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::Redelegate => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3, 4]) {
                log::warn!(
                    "StakeInstruction::Redelegate authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                "newStakeAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                "voteAccount": account_keys[instruction.accounts[2] as usize].to_string(),
                "stakeConfigAccount": account_keys[instruction.accounts[3] as usize].to_string(),
                "stakeAuthority": account_keys[instruction.accounts[4] as usize].to_string(),
            });
            log::info!("StakeInstruction::Redelegate infos:{info}");

            send_verification(
                stake_sender,
                stakestore,
                "Redelegate",
                account_keys[instruction.accounts[0] as usize],
            )
            .await;
        }
        StakeInstruction::Merge => {
            if !verify_account_len(&account_keys, &instruction.accounts, vec![0, 1, 2, 3, 4]) {
                log::warn!(
                    "StakeInstruction::Merge authorized Index error in instruction:{:?}",
                    instruction.accounts
                );
                return;
            }
            let info = json!({
                "destination": account_keys[instruction.accounts[0] as usize].to_string(),
                "source": account_keys[instruction.accounts[1] as usize].to_string(),
                "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                "stakeHistorySysvar": account_keys[instruction.accounts[3] as usize].to_string(),
                "stakeAuthority": account_keys[instruction.accounts[4] as usize].to_string(),
            });
            log::info!("StakeInstruction::Merge infos:{info}");
            let source_account = &account_keys_vec[instruction.accounts[1] as usize];
            let source_bytes: [u8; 32] = source_account[..solana_sdk::pubkey::PUBKEY_BYTES]
                .try_into()
                .unwrap();
            let source_pubkey = Pubkey::new_from_array(source_bytes);
            log::info!(
                "DETECT MERGE for source account:{}",
                source_pubkey.to_string()
            );

            stakestore.notify_stake_action(
                ExtractedAction::Remove(source_pubkey, tx_slot),
                current_end_epoch_slot,
            );
            stakestore.notify_stake_action(
                ExtractedAction::Merge {
                    source_account: account_keys[instruction.accounts[1] as usize],
                    destination_account: account_keys[instruction.accounts[0] as usize],
                    update_slot: tx_slot,
                },
                current_end_epoch_slot,
            );

            // send_verification(
            //     stake_sender,
            //     stakestore,
            //     "Merge Destination",
            //     account_keys[instruction.accounts[0] as usize],
            // )
            // .await;

            // send_verification(
            //     stake_sender,
            //     stakestore,
            //     "Merge Source",
            //     account_keys[instruction.accounts[1] as usize],
            // )
            // .await;
        }
    }
}
