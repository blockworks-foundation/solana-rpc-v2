use crate::leader_schedule::LeaderScheduleEvent;
use crate::Slot;
use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::epoch_info::EpochInfo;
use yellowstone_grpc_proto::geyser::CommitmentLevel;
use yellowstone_grpc_proto::prelude::SubscribeUpdateSlot;

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

    pub fn current_epoch_end_slot(&self) -> Slot {
        self.next_epoch_start_slot - 1
    }
    pub fn process_new_slot(
        &mut self,
        new_slot: &SubscribeUpdateSlot,
    ) -> Option<LeaderScheduleEvent> {
        if let CommitmentLevel::Confirmed = new_slot.status() {
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
                    change_epoch(&mut self.current_epoch, new_slot.slot);
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

        if let CommitmentLevel::Confirmed = new_slot.status() {
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
            //switch to next epoch to calculate schedule at next epoch.
            let mut schedule_epoch = self.current_epoch.clone();
            change_epoch(&mut schedule_epoch, self.current_slot.confirmed_slot);
            Some(LeaderScheduleEvent::InitLeaderschedule(schedule_epoch))
        } else {
            None
        }
    }
}

fn change_epoch(current_epoch: &mut EpochInfo, current_slot: Slot) {
    current_epoch.epoch += 1;
    //slot can be non consecutif, use diff.
    current_epoch.slot_index = current_epoch
        .slot_index
        .saturating_sub(current_epoch.slots_in_epoch);
    current_epoch.absolute_slot = current_slot;
}

#[derive(Default, Debug, Clone)]
pub struct CurrentSlot {
    pub processed_slot: u64,
    pub confirmed_slot: u64,
    pub finalized_slot: u64,
}

impl CurrentSlot {
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
            CommitmentLevel::Processed => updade("Processed", &mut self.processed_slot, slot.slot),
            CommitmentLevel::Confirmed => updade("Confirmed", &mut self.confirmed_slot, slot.slot),
            CommitmentLevel::Finalized => updade("Finalized", &mut self.finalized_slot, slot.slot),
        }
    }
}