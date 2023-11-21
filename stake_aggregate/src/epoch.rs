use crate::leader_schedule::LeaderScheduleEvent;
use crate::Slot;
use anyhow::bail;
use serde::{Deserialize, Serialize};
use solana_account_decoder::parse_sysvar::SysvarAccountType;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::sysvar::epoch_schedule::EpochSchedule;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;
use yellowstone_grpc_proto::geyser::CommitmentLevel as GeyserCommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdateBlock;
use yellowstone_grpc_proto::prelude::SubscribeUpdateSlot;

#[derive(Debug)]
pub struct BlockSlotVerifier {
    block_cache: BTreeMap<u64, SubscribeUpdateBlock>,
    slot_cache: BTreeSet<u64>,
}

impl BlockSlotVerifier {
    pub fn new() -> Self {
        BlockSlotVerifier {
            block_cache: BTreeMap::new(),
            slot_cache: BTreeSet::new(),
        }
    }
    pub fn process_slot(&mut self, slot: u64) -> Option<(u64, SubscribeUpdateBlock)> {
        match self.block_cache.remove(&slot) {
            //the block is already seen. Return slot/block.
            Some(block) => Some((slot, block)),
            None => {
                self.slot_cache.insert(slot);
                self.verify(slot);
                None
            }
        }
    }
    pub fn process_block(
        &mut self,
        block: SubscribeUpdateBlock,
    ) -> Option<(u64, SubscribeUpdateBlock)> {
        let slot = block.slot;
        if self.slot_cache.remove(&slot) {
            //the slot is already seen. Return slot/block.
            Some((slot, block))
        } else {
            //Cache block and  wait  for the slot
            let old = self.block_cache.insert(slot, block);
            if old.is_some() {
                log::warn!("Receive 2 blocks for the same slot:{slot}");
            }
            None
        }
    }

    fn verify(&mut self, current_slot: u64) {
        //do some verification on cached block and slot
        let old_slot: Vec<_> = self
            .slot_cache
            .range((
                Bound::Unbounded,
                Bound::Included(current_slot.saturating_sub(2)),
            ))
            .copied()
            .collect();
        if old_slot.len() > 0 {
            log::error!("Missing block for slots:{:?}", old_slot);
            for slot in &old_slot {
                self.slot_cache.remove(&slot);
            }
        }

        //verify that there's no too old block.
        let old_block_slots: Vec<_> = self
            .block_cache
            .range((
                Bound::Unbounded,
                Bound::Included(current_slot.saturating_sub(2)),
            ))
            .map(|(slot, _)| slot)
            .copied()
            .collect();
        if old_block_slots.len() > 0 {
            log::error!("Missing slot for block  slot:{:?}", old_slot);
            for slot in old_block_slots {
                self.block_cache.remove(&slot);
            }
        }
    }
}

#[derive(Debug, Default, Copy, Clone, PartialOrd, PartialEq, Eq, Ord, Serialize, Deserialize)]
pub struct Epoch {
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub absolute_slot: Slot,
}

impl Epoch {
    pub fn get_next_epoch(&self, current_epoch: &CurrentEpochSlotState) -> Epoch {
        let last_slot = current_epoch.epoch_cache.get_last_slot_in_epoch(self.epoch);
        current_epoch.epoch_cache.get_epoch_at_slot(last_slot + 1)
    }

    pub fn into_epoch_info(&self, block_height: u64, transaction_count: Option<u64>) -> EpochInfo {
        EpochInfo {
            epoch: self.epoch,
            slot_index: self.slot_index,
            slots_in_epoch: self.slots_in_epoch,
            absolute_slot: self.absolute_slot,
            block_height,
            transaction_count,
        }
    }
}

pub fn get_epoch_for_slot(slot: Slot, current_epoch: &CurrentEpochSlotState) -> u64 {
    current_epoch.epoch_cache.get_epoch_at_slot(slot).epoch
}

#[derive(Clone, Debug)]
pub struct EpochCache {
    epoch_schedule: Arc<EpochSchedule>,
}

impl EpochCache {
    pub fn get_epoch_at_slot(&self, slot: Slot) -> Epoch {
        let (epoch, slot_index) = self.epoch_schedule.get_epoch_and_slot_index(slot);
        let slots_in_epoch = self.epoch_schedule.get_slots_in_epoch(epoch);
        Epoch {
            epoch,
            slot_index,
            slots_in_epoch,
            absolute_slot: slot,
        }
    }

    // pub fn get_epoch_shedule(&self) -> EpochSchedule {
    //     self.epoch_schedule.as_ref().clone()
    // }

    pub fn get_slots_in_epoch(&self, epoch: u64) -> u64 {
        self.epoch_schedule.get_slots_in_epoch(epoch)
    }

    // pub fn get_first_slot_in_epoch(&self, epoch: u64) -> u64 {
    //     self.epoch_schedule.get_first_slot_in_epoch(epoch)
    // }

    pub fn get_last_slot_in_epoch(&self, epoch: u64) -> u64 {
        self.epoch_schedule.get_last_slot_in_epoch(epoch)
    }

    pub async fn bootstrap_epoch(rpc_client: &RpcClient) -> anyhow::Result<EpochCache> {
        let res_epoch = rpc_client
            .get_account(&solana_sdk::sysvar::epoch_schedule::id())
            .await?;
        let Some(SysvarAccountType::EpochSchedule(epoch_schedule)) =
            bincode::deserialize(&res_epoch.data[..])
                .ok()
                .map(SysvarAccountType::EpochSchedule)
        else {
            bail!("Error during bootstrap epoch. SysvarAccountType::EpochSchedule can't be deserilized. Epoch can't be calculated.");
        };

        Ok(EpochCache {
            epoch_schedule: Arc::new(epoch_schedule),
        })
    }
}

#[derive(Debug)]
pub struct CurrentEpochSlotState {
    pub current_slot: CurrentSlot,
    epoch_cache: EpochCache,
    current_epoch_value: Epoch,
}

impl CurrentEpochSlotState {
    // pub fn get_epoch_shedule(&self) -> EpochSchedule {
    //     self.epoch_cache.get_epoch_shedule()
    // }

    pub fn get_current_epoch(&self) -> Epoch {
        self.current_epoch_value
    }

    pub fn get_slots_in_epoch(&self, epoch: u64) -> u64 {
        self.epoch_cache.get_slots_in_epoch(epoch)
    }

    pub async fn bootstrap(rpc_url: String) -> anyhow::Result<CurrentEpochSlotState> {
        let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

        let epoch_cache = EpochCache::bootstrap_epoch(&rpc_client).await?;
        let confirmed_slot = rpc_client.get_slot().await.unwrap_or(0);

        let mut state = CurrentEpochSlotState {
            current_slot: CurrentSlot::default(),
            epoch_cache,
            current_epoch_value: Epoch::default(),
        };
        state.current_slot.confirmed_slot = confirmed_slot;
        state.current_epoch_value = state.epoch_cache.get_epoch_at_slot(confirmed_slot);
        Ok(state)
    }

    // pub fn current_epoch_start_slot(&self) -> Slot {
    //     self.epoch_cache
    //         .get_first_slot_in_epoch(self.current_slot.confirmed_slot)
    // }

    pub fn current_epoch_end_slot(&self) -> Slot {
        self.epoch_cache
            .get_last_slot_in_epoch(self.current_epoch_value.epoch)
    }

    pub fn process_new_slot(
        &mut self,
        new_slot: &SubscribeUpdateSlot,
    ) -> Option<LeaderScheduleEvent> {
        self.current_slot.update_slot(&new_slot);

        if let GeyserCommitmentLevel::Confirmed = new_slot.status() {
            if self.current_epoch_value.epoch == 0 {
                //init first epoch
                self.current_epoch_value = self.epoch_cache.get_epoch_at_slot(new_slot.slot);
            }
            self.manage_change_epoch()
        } else {
            None
        }
    }

    fn manage_change_epoch(&mut self) -> Option<LeaderScheduleEvent> {
        //execute leaderschedule calculus at the last slot of the current epoch.
        //account change of the slot has been send.
        //first epoch slot send all stake change and during this send no slot is send.
        //to avoid to delay too much the schdule, start the calculus at the end of the epoch.
        if self.current_slot.confirmed_slot >= self.current_epoch_end_slot() {
            log::info!("Change epoch at slot:{}", self.current_slot.confirmed_slot);

            self.current_epoch_value = self.get_current_epoch().get_next_epoch(&self);
            //start leader schedule calculus
            //at current epoch change the schedule is calculated for the next epoch.
            Some(crate::leader_schedule::create_schedule_init_event(
                self.current_epoch_value.epoch,
                self.get_slots_in_epoch(self.current_epoch_value.epoch),
                Arc::clone(&self.epoch_cache.epoch_schedule),
            ))
        } else {
            None
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct CurrentSlot {
    pub processed_slot: u64,
    pub confirmed_slot: u64,
    pub finalized_slot: u64,
}

impl CurrentSlot {
    pub fn get_slot_with_commitment(&self, commitment: CommitmentConfig) -> Slot {
        match commitment.commitment {
            CommitmentLevel::Processed => self.processed_slot,
            CommitmentLevel::Confirmed => self.confirmed_slot,
            CommitmentLevel::Finalized => self.finalized_slot,
            _ => {
                log::error!(
                    "get_slot_with_commitment, Bad commitment specified:{}",
                    commitment.commitment
                );
                0
            }
        }
    }

    fn update_slot(&mut self, slot: &SubscribeUpdateSlot) {
        let updade = |commitment: &str, current_slot: &mut u64, new_slot: u64| {
            //verify that the slot is consecutif
            if *current_slot != 0 && new_slot != *current_slot + 1 {
                log::trace!(
                    "At {commitment} not consecutif slot send: current_slot:{} new_slot{}",
                    current_slot,
                    new_slot
                );
            }
            *current_slot = new_slot
        };

        match slot.status() {
            GeyserCommitmentLevel::Processed => {
                updade("Processed", &mut self.processed_slot, slot.slot)
            }
            GeyserCommitmentLevel::Confirmed => {
                updade("Confirmed", &mut self.confirmed_slot, slot.slot)
            }
            GeyserCommitmentLevel::Finalized => {
                updade("Finalized", &mut self.finalized_slot, slot.slot)
            }
        }
    }
}
