use crate::AccountPretty;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::vote::state::VoteState;
use std::collections::HashMap;
use std::sync::Arc;

pub type VoteMap = HashMap<Pubkey, Arc<StoredVote>>;

//Copy the solana_rpc_client_api::response::RpcVoteAccountInfo struct
//to avoid to add a dep to solana_rpc_client that create
//dep issues with Tokio version.
//solana_rpc_client is only use for API struct conformity
//and the RPC API of the new PRC can change from the current one.
// pub struct RpcVoteAccountInfo {
//     pub vote_pubkey: String,
//     pub node_pubkey: String,
//     pub activated_stake: u64,
//     pub commission: u8,
//     pub epoch_vote_account: bool,
//     pub epoch_credits: Vec<(Epoch, u64, u64)>,
//     pub last_vote: u64,
//     pub root_slot: Slot,
// }

pub fn extract_votestore(votestore: &mut VoteStore) -> anyhow::Result<VoteMap> {
    let new_store = std::mem::take(votestore);
    let (new_store, vote_map) = new_store.extract_votes()?;
    *votestore = new_store;
    Ok(vote_map)
}

pub fn merge_votestore(votestore: &mut VoteStore, vote_map: VoteMap) -> anyhow::Result<()> {
    let new_store = std::mem::take(votestore);
    let new_store = new_store.merge_votes(vote_map)?;
    *votestore = new_store;
    Ok(())
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct StoredVote {
    pub pubkey: Pubkey,
    pub vote_data: VoteState,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

#[derive(Default)]
pub struct VoteStore {
    votes: VoteMap,
    updates: Vec<(Pubkey, StoredVote)>,
    extracted: bool,
}

impl VoteStore {
    pub fn new(capacity: usize) -> Self {
        VoteStore {
            votes: HashMap::with_capacity(capacity),
            updates: vec![],
            extracted: false,
        }
    }

    pub fn get_cloned_vote_map(&self) -> VoteMap {
        self.votes.clone()
    }

    //return the contained stake map to do an external update.
    // During extract period (between extract and merge) added stake a stored to be processed later.
    //if the store is already extracted return an error.
    pub fn extract_votes(self) -> anyhow::Result<(Self, VoteMap)> {
        if self.extracted {
            bail!("VoteStore already extracted. Try later");
        }
        let votestore = VoteStore {
            votes: HashMap::new(),
            updates: self.updates,
            extracted: true,
        };
        Ok((votestore, self.votes))
    }

    pub fn merge_votes(self, votes: VoteMap) -> anyhow::Result<Self> {
        if !self.extracted {
            bail!("StakeStore merge of non extracted map. Try later");
        }
        let mut votestore = VoteStore {
            votes,
            updates: vec![],
            extracted: false,
        };

        //apply stake added during extraction.
        for (pubkey, vote) in self.updates {
            votestore.insert_vote(pubkey, vote);
        }
        Ok(votestore)
    }

    pub fn add_vote(
        &mut self,
        new_account: AccountPretty,
        current_end_epoch_slot: Slot,
    ) -> anyhow::Result<()> {
        let Ok(vote_data) = new_account.read_vote() else {
            bail!("Can't read Vote from account data");
        };

        //log::info!("add_vote {} :{vote_data:?}", new_account.pubkey);

        let new_voteacc = StoredVote {
            pubkey: new_account.pubkey,
            vote_data,
            last_update_slot: new_account.slot,
            write_version: new_account.write_version,
        };

        //during extract push the new update or
        //don't insertnow account change that has been done in next epoch.
        //put in update pool to be merged next epoch change.
        let insert_stake = !self.extracted || new_voteacc.last_update_slot > current_end_epoch_slot;
        match insert_stake {
            false => self.updates.push((new_account.pubkey, new_voteacc)),
            true => self.insert_vote(new_account.pubkey, new_voteacc),
        }
        Ok(())
    }
    fn insert_vote(&mut self, vote_account: Pubkey, vote_data: StoredVote) {
        vote_map_insert_vote(&mut self.votes, vote_account, vote_data);
    }
}

pub fn merge_program_account_in_vote_map(
    vote_map: &mut VoteMap,
    pa_list: Vec<(Pubkey, Account)>,
    last_update_slot: Slot,
) {
    pa_list
        .into_iter()
        .filter_map(
            |(pk, account)| match VoteState::deserialize(&account.data) {
                Ok(vote) => Some((pk, vote)),
                Err(err) => {
                    log::warn!("Error during vote account data deserialisation:{err}");
                    None
                }
            },
        )
        .for_each(|(pk, vote)| {
            //log::info!("Vote init {pk} :{vote:?}");
            let vote = StoredVote {
                pubkey: pk,
                vote_data: vote,
                last_update_slot,
                write_version: 0,
            };
            vote_map_insert_vote(vote_map, pk, vote);
        });
}

fn vote_map_insert_vote(map: &mut VoteMap, vote_account_pk: Pubkey, vote_data: StoredVote) {
    match map.entry(vote_account_pk) {
        std::collections::hash_map::Entry::Occupied(occupied) => {
            let voteacc = occupied.into_mut(); // <-- get mut reference to existing value
            if voteacc.last_update_slot <= vote_data.last_update_slot {
                // generate a lot of trace log::trace!(
                //     "Vote updated for: {vote_account_pk} node_id:{} root_slot:{:?}",
                //     vote_data.vote_data.node_pubkey,
                //     vote_data.vote_data.root_slot,
                // );
                if vote_data.vote_data.root_slot.is_none() {
                    log::info!("Update vote account:{vote_account_pk} with None root slot.");
                }

                if voteacc.vote_data.root_slot.is_none() {
                    log::info!(
                        "Update vote account:{vote_account_pk} that were having None root slot."
                    );
                }

                *voteacc = Arc::new(vote_data);
            }
        }
        // If value doesn't exist yet, then insert a new value of 1
        std::collections::hash_map::Entry::Vacant(vacant) => {
            log::info!(
                "New Vote added for: {vote_account_pk} node_id:{}, root slot:{:?}",
                vote_data.vote_data.node_pubkey,
                vote_data.vote_data.root_slot,
            );
            vacant.insert(Arc::new(vote_data));
        }
    };
}
