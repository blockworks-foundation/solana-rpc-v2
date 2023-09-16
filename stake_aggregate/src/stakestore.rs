use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::stake::instruction::StakeInstruction;
use solana_sdk::stake::state::Delegation;
use solana_sdk::stake::state::StakeState;
use std::collections::HashMap;

pub type StakeMap = HashMap<Pubkey, StoredStake>;

pub fn extract_stakestore(stakestore: &mut StakeStore) -> anyhow::Result<StakeMap> {
    let new_store = std::mem::take(stakestore);
    let (new_store, stake_map) = new_store.extract()?;
    *stakestore = new_store;
    Ok(stake_map)
}

pub fn merge_stakestore(
    stakestore: &mut StakeStore,
    stake_map: StakeMap,
    current_epoch: u64,
) -> anyhow::Result<()> {
    let new_store = std::mem::take(stakestore);
    let new_store = new_store.merge_stakes(stake_map, current_epoch)?;
    *stakestore = new_store;
    Ok(())
}

fn stake_map_insert_stake(
    map: &mut StakeMap,
    stake_account: Pubkey,
    stake: StoredStake,
    //TODO manage already deractivated stake.
    current_epoch: u64,
) {
    //don't add stake that are already desactivated.
    //there's some stran,ge stake that has activeate_epock = max epoch and desactivate ecpock 90 that are taken into account.
    //Must be better defined.
    // if stake.stake.deactivation_epoch < current_epoch {
    //     return;
    // }
    match map.entry(stake_account) {
        // If value already exists, then increment it by one
        std::collections::hash_map::Entry::Occupied(occupied) => {
            let strstake = occupied.into_mut(); // <-- get mut reference to existing value
            if strstake.last_update_slot < stake.last_update_slot {
                if strstake.stake.stake != stake.stake.stake {
                    log::info!(
                        "Stake updated for: {stake_account} old_stake:{} stake:{stake:?}",
                        strstake.stake.stake
                    );
                }
                *strstake = stake;
            }
        }
        // If value doesn't exist yet, then insert a new value of 1
        std::collections::hash_map::Entry::Vacant(vacant) => {
            log::info!("New stake added for: {stake_account} stake:{stake:?}");
            vacant.insert(stake);
        }
    };
}

#[derive(Debug, Default)]
enum ExtractedAction {
    Insert {
        stake_account: Pubkey,
        stake: StoredStake,
    },
    Remove(Pubkey),
    #[default]
    None,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredStake {
    pub pubkey: Pubkey,
    pub stake: Delegation,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Debug, Default)]
pub struct StakeStore {
    stakes: StakeMap,
    updates: Vec<ExtractedAction>,
    extracted: bool,
}

impl StakeStore {
    pub fn new(capacity: usize) -> Self {
        StakeStore {
            stakes: HashMap::with_capacity(capacity),
            updates: vec![],
            extracted: false,
        }
    }

    pub fn nb_stake_account(&self) -> usize {
        self.stakes.len()
    }

    pub fn get_cloned_stake_map(&self) -> StakeMap {
        self.stakes.clone()
    }

    //return the contained stake map to do an external update.
    // During extract period (between extract and merge) added stake a stored to be processed later.
    //if the store is already extracted return an error.
    pub fn extract(self) -> anyhow::Result<(Self, StakeMap)> {
        if self.extracted {
            bail!("StakeStore already extracted. Try later");
        }
        let stakestore = StakeStore {
            stakes: HashMap::new(),
            updates: self.updates,
            extracted: true,
        };
        Ok((stakestore, self.stakes))
    }

    pub fn merge_stakes(self, stakes: StakeMap, current_epoch: u64) -> anyhow::Result<Self> {
        if !self.extracted {
            bail!("StakeStore merge of non extracted map. Try later");
        }
        let mut stakestore = StakeStore {
            stakes,
            updates: vec![],
            extracted: false,
        };

        //apply stake added during extraction.
        for action in self.updates {
            log::info!("merge_stakes update action:{action:?}");
            match action {
                ExtractedAction::Insert {
                    stake_account,
                    stake,
                } => {
                    stakestore.insert_stake(stake_account, stake, current_epoch);
                }
                ExtractedAction::Remove(account_pk) => stakestore.remove_from_store(&account_pk),

                ExtractedAction::None => (),
            };
        }
        Ok(stakestore)
    }

    pub fn add_stake(
        &mut self,
        new_account: AccountPretty,
        current_end_epoch_slot: Slot,
        current_epoch: u64,
    ) -> anyhow::Result<()> {
        let Ok(delegated_stake_opt) = new_account.read_stake() else {
            bail!("Can't read stake from account data");
        };

        if let Some(delegated_stake) = delegated_stake_opt {
            let ststake = StoredStake {
                pubkey: new_account.pubkey,
                stake: delegated_stake,
                last_update_slot: new_account.slot,
                write_version: new_account.write_version,
            };
            //during extract push the new update or
            //don't insertnow account change that has been done in next epoch.
            //put in update pool to be merged next epoch change.
            let insert_stake = !self.extracted || ststake.last_update_slot > current_end_epoch_slot;
            match insert_stake {
                false => self.updates.push(ExtractedAction::Insert {
                    stake_account: new_account.pubkey,
                    stake: ststake,
                }),
                true => self.insert_stake(new_account.pubkey, ststake, current_epoch),
            }
        }

        Ok(())
    }

    pub fn remove_stake(&mut self, account_pk: Pubkey) {
        match self.extracted {
            false => self.remove_from_store(&account_pk),

            true => self.updates.push(ExtractedAction::Remove(account_pk)),
        }
    }

    fn remove_from_store(&mut self, account_pk: &Pubkey) {
        log::info!("remove_from_store for {}", account_pk.to_string());
        self.stakes.remove(account_pk);
    }
    fn insert_stake(&mut self, stake_account: Pubkey, stake: StoredStake, current_epoch: u64) {
        stake_map_insert_stake(&mut self.stakes, stake_account, stake, current_epoch);
    }
}

pub fn merge_program_account_in_strake_map(
    stake_map: &mut StakeMap,
    pa_list: Vec<(Pubkey, Account)>,
    last_update_slot: Slot,
    current_epoch: u64,
) {
    pa_list
        .into_iter()
        .filter_map(
            |(pk, account)| match read_stake_from_account_data(&account.data) {
                Ok(opt_stake) => opt_stake.map(|stake| (pk, stake)),
                Err(err) => {
                    log::warn!("Error during pa account data deserialisation:{err}");
                    None
                }
            },
        )
        .for_each(|(pk, delegated_stake)| {
            let stake = StoredStake {
                pubkey: pk,
                stake: delegated_stake,
                last_update_slot,
                write_version: 0,
            };
            stake_map_insert_stake(stake_map, pk, stake, current_epoch);
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
        other => {
            bail!("read stake from account not a stake account. read:{other:?}");
        }
    }
}

pub fn process_stake_tx_message(
    stakestore: &mut StakeStore,
    message: yellowstone_grpc_proto::solana::storage::confirmed_block::Message,
    //for debug and trace purpose.
    stake_public_key: &[u8],
) {
    let account_keys: Vec<Pubkey> = message
        .account_keys
        .iter()
        .map(|acc_bytes| {
            let source_bytes: [u8; 32] = acc_bytes[..solana_sdk::pubkey::PUBKEY_BYTES]
                .try_into()
                .unwrap();
            Pubkey::new_from_array(source_bytes)
        })
        .collect();

    //for debug get stake account index
    let index = message
        .account_keys
        .iter()
        .enumerate()
        .filter_map(|(index, acc)| (acc == stake_public_key).then_some(index))
        .next();
    log::info!("Found tx with stake account accounts:{:?} stake account index:{index:?} with instructions:{}", account_keys, message.instructions.len());

    //merge and delegate has 1 instruction. Create as 2 instructions and the first is not a StakeInstruction.
    for instruction in &message.instructions {
        log::info!(
            "Before read instruction of program_id_index:{}",
            instruction.program_id_index
        );
        let Ok(stake_inst)  = bincode::deserialize::<StakeInstruction>(&instruction.data) else {
            log::info!("Error during stake instruction decoding  :{:?}", &message.instructions[0].data);
            continue;
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

                log::info!("StakeInstruction::Initialize authorized:{authorized} lockup:{lockup} stakeAccount:{} rentSysvar:{}"
                    , account_keys[instruction.accounts[0] as usize].to_string()
                    , account_keys[instruction.accounts[1] as usize].to_string()
                );
            }
            StakeInstruction::Authorize(new_authorized, authority_type) => {
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
            }
            StakeInstruction::DelegateStake => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "voteAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                    "stakeHistorySysvar": account_keys[instruction.accounts[3] as usize].to_string(),
                    "stakeConfigAccount": account_keys[instruction.accounts[4] as usize].to_string(),
                    "stakeAuthority": account_keys[instruction.accounts[5] as usize].to_string(),
                });
                log::info!("StakeInstruction::DelegateStake infos:{info}");
            }
            StakeInstruction::Split(lamports) => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "newSplitAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                    "stakeAuthority": account_keys[instruction.accounts[2] as usize].to_string(),
                    "lamports": lamports,
                });
                log::info!("StakeInstruction::Split infos:{info}");
            }
            StakeInstruction::Withdraw(lamports) => {
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
            }
            StakeInstruction::Deactivate => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "stakeAuthority": account_keys[instruction.accounts[2] as usize].to_string(),
                });
                log::info!("StakeInstruction::Deactivate infos:{info}");
            }
            StakeInstruction::SetLockup(lockup_args) => {
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
            }
            StakeInstruction::AuthorizeWithSeed(args) => {
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
            }
            StakeInstruction::InitializeChecked => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "rentSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "staker": account_keys[instruction.accounts[2] as usize].to_string(),
                    "withdrawer": account_keys[instruction.accounts[3] as usize].to_string(),
                });
                log::info!("StakeInstruction::InitializeChecked infos:{info}");
            }
            StakeInstruction::AuthorizeChecked(authority_type) => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[1] as usize].to_string(),
                    "authority": account_keys[instruction.accounts[2] as usize].to_string(),
                    "newAuthority": account_keys[instruction.accounts[3] as usize].to_string(),
                    "authorityType": authority_type,
                });
                let custodian = (instruction.accounts.len() >= 5)
                    .then(|| json!(account_keys[instruction.accounts[4] as usize].to_string()));
                log::info!(
                    "StakeInstruction::AuthorizeChecked custodian:{custodian:?} infos:{info}"
                );
            }
            StakeInstruction::AuthorizeCheckedWithSeed(args) => {
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
            }
            StakeInstruction::SetLockupChecked(lockup_args) => {
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
            }
            StakeInstruction::GetMinimumDelegation => {
                log::info!("StakeInstruction::GetMinimumDelegation");
            }
            StakeInstruction::DeactivateDelinquent => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "voteAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                    "referenceVoteAccount": account_keys[instruction.accounts[2] as usize].to_string(),
                });
                log::info!("StakeInstruction::DeactivateDelinquent infos:{info}");
            }
            StakeInstruction::Redelegate => {
                let info = json!({
                    "stakeAccount": account_keys[instruction.accounts[0] as usize].to_string(),
                    "newStakeAccount": account_keys[instruction.accounts[1] as usize].to_string(),
                    "voteAccount": account_keys[instruction.accounts[2] as usize].to_string(),
                    "stakeConfigAccount": account_keys[instruction.accounts[3] as usize].to_string(),
                    "stakeAuthority": account_keys[instruction.accounts[4] as usize].to_string(),
                });
                log::info!("StakeInstruction::Redelegate infos:{info}");
            }
            StakeInstruction::Merge => {
                let info = json!({
                    "destination": account_keys[instruction.accounts[0] as usize].to_string(),
                    "source": account_keys[instruction.accounts[1] as usize].to_string(),
                    "clockSysvar": account_keys[instruction.accounts[2] as usize].to_string(),
                    "stakeHistorySysvar": account_keys[instruction.accounts[3] as usize].to_string(),
                    "stakeAuthority": account_keys[instruction.accounts[4] as usize].to_string(),
                });
                log::info!("StakeInstruction::Merge infos:{info}");
                let source_account = &message.account_keys[instruction.accounts[1] as usize];
                let source_bytes: [u8; 32] = source_account[..solana_sdk::pubkey::PUBKEY_BYTES]
                    .try_into()
                    .unwrap();
                let source_pubkey = Pubkey::new_from_array(source_bytes);
                log::info!(
                    "DETECT MERGE for source account:{}",
                    source_pubkey.to_string()
                );
                stakestore.remove_stake(source_pubkey);
            }
        }
    }
}
