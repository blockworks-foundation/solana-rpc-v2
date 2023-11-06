pub struct ExtractableMap<T> {
    stakes: HashMap<Pubkey, T>,
    updates: Vec<(Pubkey, T)>,
    extracted: bool,
}

impl ExtractableMap {
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
}

fn extract_stakestore(stakestore: &mut StakeStore) -> anyhow::Result<crate::stakestore::StakeMap> {
    let new_store = std::mem::take(stakestore);
    let (new_store, stake_map) = new_store.extract()?;
    *stakestore = new_store;
    Ok(stake_map)
}

fn merge_stakestore(
    stakestore: &mut StakeStore,
    stake_map: crate::stakestore::StakeMap,
) -> anyhow::Result<()> {
    let new_store = std::mem::take(stakestore);
    let new_store = new_store.merge_stake(stake_map)?;
    *stakestore = new_store;
    Ok(())
}
