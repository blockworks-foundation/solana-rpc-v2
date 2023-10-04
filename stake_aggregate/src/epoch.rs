use crate::leader_schedule::LeaderScheduleEvent;
use crate::Slot;
use solana_account_decoder::parse_sysvar::SysvarAccountType;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::epoch_info::EpochInfo;
use yellowstone_grpc_proto::geyser::CommitmentLevel as GeyserCommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdateSlot;

pub fn get_epoch_for_slot(slot: Slot, current_epoch: &CurrentEpochSlotState) -> u64 {
    let slots_in_epoch = current_epoch.current_epoch.slots_in_epoch;
    let epoch_start_slot = current_epoch.current_epoch_start_slot();
    if slot >= epoch_start_slot {
        let slot_distance = (slot - epoch_start_slot) / slots_in_epoch;
        current_epoch.current_epoch.epoch + slot_distance
    } else {
        let slot_distance = (epoch_start_slot - slot) / slots_in_epoch;
        current_epoch.current_epoch.epoch - slot_distance
    }
}

#[derive(Debug)]
pub struct CurrentEpochSlotState {
    pub current_slot: CurrentSlot,
    pub current_epoch: EpochInfo,
    pub next_epoch_start_slot: Slot,
    pub first_epoch_slot: bool,
}

impl CurrentEpochSlotState {
    pub async fn bootstrap(rpc_url: String) -> Result<CurrentEpochSlotState, ClientError> {
        let rpc_client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::finalized());

        //get reduce_stake_warmup_cooldown feature info.
        //NOT AND ACCOUNT. Get from config.
        // let reduce_stake_warmup_cooldown_epoch = rpc_client
        //     .get_account(&feature_set::reduce_stake_warmup_cooldown::id())
        //     .await?;

        // let reduce_stake_warmup_cooldown_epoch = bincode::deserialize(&reduce_stake_warmup_cooldown_epoch.data[..])
        //     .ok()
        //     .map(SysvarAccountType::EpochSchedule);
        // log::info!("reduce_stake_warmup_cooldown_epoch {reduce_stake_warmup_cooldown_epoch:?}");

        //get epoch sysvar account to init epoch. More compatible with a snapshot  bootstrap.
        //sysvar_epoch_schedule Some(EpochSchedule(EpochSchedule { slots_per_epoch: 100, leader_schedule_slot_offset: 100, warmup: false, first_normal_epoch: 0, first_normal_slot: 0 }))
        let res_epoch = rpc_client
            .get_account(&solana_sdk::sysvar::epoch_schedule::id())
            .await?;
        let sysvar_epoch_schedule = bincode::deserialize(&res_epoch.data[..])
            .ok()
            .map(SysvarAccountType::EpochSchedule);
        log::info!("sysvar_epoch_schedule {sysvar_epoch_schedule:?}");

        // Fetch current epoch
        let current_epoch = rpc_client.get_epoch_info().await?;
        let next_epoch_start_slot =
            current_epoch.slots_in_epoch - current_epoch.slot_index + current_epoch.absolute_slot;
        log::info!("Run_loop init {next_epoch_start_slot} current_epoch:{current_epoch:?}");
        Ok(CurrentEpochSlotState {
            current_slot: Default::default(),
            current_epoch,
            next_epoch_start_slot,
            first_epoch_slot: false,
        })
    }

    pub fn current_epoch_start_slot(&self) -> Slot {
        self.current_epoch.absolute_slot - self.current_epoch.slot_index
    }

    pub fn current_epoch_end_slot(&self) -> Slot {
        self.next_epoch_start_slot - 1
    }

    pub fn process_new_slot(
        &mut self,
        new_slot: &SubscribeUpdateSlot,
    ) -> Option<LeaderScheduleEvent> {
        if let GeyserCommitmentLevel::Confirmed = new_slot.status() {
            //for the first update of slot correct epoch info data.
            if self.current_slot.confirmed_slot == 0 {
                let diff = new_slot.slot - self.current_epoch.absolute_slot;
                self.current_epoch.slot_index += diff;
                self.current_epoch.absolute_slot = new_slot.slot;
                log::trace!(
                    "Set current epoch with diff:{diff} slot:{} current:{:?}",
                    new_slot.slot,
                    self.current_epoch,
                );
            //if new slot update slot related state data.
            } else if new_slot.slot > self.current_slot.confirmed_slot {
                //update epoch info
                let mut diff = new_slot.slot - self.current_slot.confirmed_slot;
                //First epoch slot, index is 0 so remove 1 from diff.
                if self.first_epoch_slot {
                    //calculate next epoch data
                    self.current_epoch.epoch += 1;
                    //slot can be non consecutif, use diff.
                    self.current_epoch.slot_index = 0;
                    self.current_epoch.absolute_slot = new_slot.slot;
                    log::info!(
                        "change_epoch calculated next epoch:{:?} at slot:{}",
                        self.current_epoch,
                        new_slot.slot,
                    );

                    self.first_epoch_slot = false;
                    //slot_index start at 0.
                    diff -= 1; //diff is always >= 1
                    self.next_epoch_start_slot =
                        self.next_epoch_start_slot + self.current_epoch.slots_in_epoch;
                //set to next epochs.
                } else {
                    self.current_epoch.absolute_slot = new_slot.slot;
                }
                self.current_epoch.slot_index += diff;

                log::trace!(
                    "Slot epoch with slot:{} , diff:{diff} current epoch state:{self:?}",
                    new_slot.slot
                );
            }
        }
        //update slot state for all commitment.
        self.current_slot.update_slot(&new_slot);

        if let GeyserCommitmentLevel::Confirmed = new_slot.status() {
            self.manage_change_epoch()
        } else {
            None
        }
    }

    fn manage_change_epoch(&mut self) -> Option<LeaderScheduleEvent> {
        //we change the epoch at the last slot of the current epoch.
        if self.current_slot.confirmed_slot >= self.current_epoch_end_slot() {
            log::info!(
                "End epoch slot detected:{}",
                self.current_slot.confirmed_slot
            );

            //set epoch change effectif at next slot.
            self.first_epoch_slot = true;

            //start leader schedule calculus
            //switch to 2 next epoch to calculate schedule at next epoch.
            //at current epoch change the schedule is calculated for the next epoch.
            let schedule_epoch = crate::leader_schedule::next_schedule_epoch(&self.current_epoch);
            let schedule_epoch = crate::leader_schedule::next_schedule_epoch(&schedule_epoch);
            Some(LeaderScheduleEvent::InitLeaderschedule(schedule_epoch))
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
