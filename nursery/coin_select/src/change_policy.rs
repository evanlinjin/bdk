//! This module contains a collection of change policies.
//!
//! A change policy determines whether a given coin selection (presented by [`CoinSelector`]) should
//! construct a transaction with a change output. A change policy is represented as a function of
//! type `Fn(&CoinSelector) -> Option<u64>`.

#[allow(unused)] // some bug in <= 1.48.0 sees this as unused when it isn't
use crate::float::FloatExt;
use crate::{CoinSelector, FeeRate};
use core::convert::TryInto;

/// Construct a change policy that creates change when the change value is greater than `min_value`.
pub fn min_value(min_value: u64) -> impl Fn(&CoinSelector) -> Option<u64> {
    let min_value: i64 = min_value
        .try_into()
        .expect("min_value is ridiculously large");

    move |cs| {
        let excess = cs.excess(Some(0));
        if excess < min_value {
            return None;
        }

        let drain_value: u64 = excess
            .try_into()
            .expect("must be positive since it is greater than min_value (which is positive)");
        Some(drain_value)
    }
}

/// Construct a change policy that creates change when it would reduce the transaction waste.
///
/// **WARNING:** This may result in a change value that is below dust limit. [`min_value_and_waste`]
/// is a more sensible default.
pub fn min_waste(long_term_feerate: FeeRate) -> impl Fn(&CoinSelector) -> Option<u64> {
    move |cs| {
        // The output waste of a changeless solution is the excess.
        let waste_changeless = cs.excess(None);
        let waste_with_change = cs
            .target
            .drain_weights
            .waste(cs.target.feerate, long_term_feerate)
            .ceil() as i64;

        if waste_changeless <= waste_with_change {
            return None;
        }

        let drain_value = cs
            .excess(Some(0))
            .try_into()
            .expect("the excess must be positive because drain free excess was > waste");
        Some(drain_value)
    }
}

/// Construct a change policy that creates change when it would reduce the transaction waste given
/// that `min_value` is respected.
///
/// This is equivalent to combining [`min_value`] with [`min_waste`], and including change when both
/// policies have change.
pub fn min_value_and_waste(
    min_value: u64,
    long_term_feerate: FeeRate,
) -> impl Fn(&CoinSelector) -> Option<u64> {
    let min_waste_policy = crate::change_policy::min_waste(long_term_feerate);

    move |cs| {
        let drain_value = min_waste_policy(cs);
        if drain_value < Some(min_value) {
            None
        } else {
            drain_value
        }
    }
}
