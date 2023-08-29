use solana_sdk::vote::state::VoteState;

pub type VoteMap = HashMap<Pubkey, StoredVote>;

//Copy the solana_rpc_client_api::response::RpcVoteAccountInfo struct
//to avoid to add a dep to solana_rpc_client that create
//dep issues with Tokio version.
//solana_rpc_client is only use for API struct conformity
//and the RPC API of the new PRC can change from the current one.
pub struct RpcVoteAccountInfo {
    pub vote_pubkey: String,
    pub node_pubkey: String,
    pub activated_stake: u64,
    pub commission: u8,
    pub epoch_vote_account: bool,
    pub epoch_credits: Vec<(Epoch, u64, u64)>,
    pub last_vote: u64,
    pub root_slot: Slot,
}

#[derive(Debug, Default)]
pub struct StoredVote {
    pub pubkey: Pubkey,
    pub vote: RpcVoteAccountInfo,
    pub last_update_slot: Slot,
    pub write_version: u64,
}

pub struct VoteStore {
    stakes: VoteMap,
    updates: Vec<(Pubkey, StoredVote)>,
    extracted: bool,
}

impl VoteStore {
    pub fn new(capacity: usize) -> Self {
        StakeStore {
            votes: HashMap::with_capacity(capacity),
            updates: vec![],
            extracted: false,
        }
    }

    //return the contained stake map to do an external update.
    // During extract period (between extract and merge) added stake a stored to be processed later.
    //if the store is already extracted return an error.
    pub fn extract(self) -> anyhow::Result<(Self, VoteMap)> {
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

    pub fn merge(self, votes: VoteMap) -> anyhow::Result<Self> {
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
}
