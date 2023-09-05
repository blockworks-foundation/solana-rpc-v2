use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use borsh::BorshDeserialize;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
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

pub fn merge_stakestore(stakestore: &mut StakeStore, stake_map: StakeMap) -> anyhow::Result<()> {
    let new_store = std::mem::take(stakestore);
    let new_store = new_store.merge_stake(stake_map)?;
    *stakestore = new_store;
    Ok(())
}

fn stake_map_insert_stake(map: &mut StakeMap, stake_account: Pubkey, stake: StoredStake) {
    match map.entry(stake_account) {
        // If value already exists, then increment it by one
        std::collections::hash_map::Entry::Occupied(occupied) => {
            let strstake = occupied.into_mut(); // <-- get mut reference to existing value
            if strstake.last_update_slot < stake.last_update_slot {
                log::info!("Stake updated for: {stake_account} stake:{stake:?}");
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

#[derive(Debug, Default, Clone)]
pub struct StoredStake {
    pub pubkey: Pubkey,
    pub stake: Delegation,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Debug, Default)]
pub struct StakeStore {
    stakes: StakeMap,
    updates: Vec<(Pubkey, StoredStake)>,
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

    pub fn merge_stake(self, stakes: StakeMap) -> anyhow::Result<Self> {
        if !self.extracted {
            bail!("StakeStore merge of non extracted map. Try later");
        }
        let mut stakestore = StakeStore {
            stakes,
            updates: vec![],
            extracted: false,
        };

        //apply stake added during extraction.
        for (stake_account, stake) in self.updates {
            stakestore.insert_stake(stake_account, stake);
        }
        Ok(stakestore)
    }

    pub fn add_stake(
        &mut self,
        new_account: AccountPretty,
        current_end_epoch_slot: Slot,
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
            //don't add account change that has been done in next epoch.
            let insert_stake = !self.extracted || ststake.last_update_slot > current_end_epoch_slot;
            match insert_stake {
                false => self.updates.push((new_account.pubkey, ststake)),
                true => self.insert_stake(new_account.pubkey, ststake),
            }
        }

        Ok(())
    }

    fn insert_stake(&mut self, stake_account: Pubkey, stake: StoredStake) {
        stake_map_insert_stake(&mut self.stakes, stake_account, stake);
    }
}

pub fn merge_program_account_in_strake_map(
    stake_map: &mut StakeMap,
    pa_list: Vec<(Pubkey, Account)>,
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
                last_update_slot: 0,
                write_version: 0,
            };
            stake_map_insert_stake(stake_map, pk, stake);
        });
}

pub fn read_stake_from_account_data(mut data: &[u8]) -> anyhow::Result<Option<Delegation>> {
    if data.is_empty() {
        log::warn!("Stake account with empty data. Can't read stake.");
        bail!("Error: read VA account with empty data");
    }
    match StakeState::deserialize(&mut data)? {
        StakeState::Stake(_, stake) => Ok(Some(stake.delegation)),
        StakeState::Initialized(_) => Ok(None),
        other => {
            bail!("read stake from account not a stake account. read:{other:?}");
        }
    }
}
