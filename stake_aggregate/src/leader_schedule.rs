use solana_ledger::leader_schedule::LeaderSchedule;
use solana_sdk::clock::NUM_CONSECUTIVE_LEADER_SLOTS;
use solana_sdk::epoch_info::EpochInfo;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;

pub fn calculate_leader_schedule_from_stake_map(
    stake_map: &crate::stakestore::StakeMap,
    current_epoch_info: &EpochInfo,
) -> LeaderSchedule {
    let mut stakes = HashMap::<Pubkey, u64>::new();
    for storestake in stake_map.values() {
        // Ignore stake accounts activated in this epoch (or later, to include activation_epoch of
        // u64::MAX which indicates no activation ever happened)
        if storestake.stake.activation_epoch >= current_epoch_info.epoch {
            continue;
        }
        // Ignore stake accounts deactivated before this epoch
        if storestake.stake.deactivation_epoch < current_epoch_info.epoch {
            continue;
        }
        // Add the stake in this stake account to the total for the delegated-to vote account
        *(stakes.entry(storestake.stake.voter_pubkey).or_insert(0)) += storestake.stake.stake;
    }
    calculate_leader_schedule(stakes, current_epoch_info)
}

//Copied from leader_schedule_utils.rs
// Mostly cribbed from leader_schedule_utils
fn calculate_leader_schedule(
    stakes: HashMap<Pubkey, u64>,
    current_epoch_info: &EpochInfo,
) -> LeaderSchedule {
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&current_epoch_info.epoch.to_le_bytes());
    let mut stakes: Vec<_> = stakes
        .iter()
        .map(|(pubkey, stake)| (*pubkey, *stake))
        .collect();
    sort_stakes(&mut stakes);
    LeaderSchedule::new(
        &stakes,
        seed,
        current_epoch_info.slots_in_epoch,
        NUM_CONSECUTIVE_LEADER_SLOTS,
    )
}

// Cribbed from leader_schedule_utils
fn sort_stakes(stakes: &mut Vec<(Pubkey, u64)>) {
    // Sort first by stake. If stakes are the same, sort by pubkey to ensure a
    // deterministic result.
    // Note: Use unstable sort, because we dedup right after to remove the equal elements.
    stakes.sort_unstable_by(|(l_pubkey, l_stake), (r_pubkey, r_stake)| {
        if r_stake == l_stake {
            r_pubkey.cmp(l_pubkey)
        } else {
            r_stake.cmp(l_stake)
        }
    });

    // Now that it's sorted, we can do an O(n) dedup.
    stakes.dedup();
}
