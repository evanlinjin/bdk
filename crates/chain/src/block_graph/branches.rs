//! The [`Branches`] index used by [`crate::block_graph::ChangeSet`].

use core::fmt;

use crate::collections::{BTreeMap, BTreeSet};
use crate::{BlockId, Merge};

/// Per-tip branch index for [`crate::block_graph::ChangeSet`].
///
/// Wraps the canonical `BTreeMap<BlockId, BTreeSet<BlockId>>` (tip → BlockIds in that
/// branch's sparse chain) with a reverse index `BlockId → tips_containing` so
/// "which branches contain this BlockId as a member?" runs in `O(log N)` rather than
/// `O(N · M)`.
///
/// The reverse index is **derived** from the forward map and rebuilt during
/// [`Deserialize`](serde::Deserialize). The wire / serde format is just the forward map,
/// so [`Branches`] is on-disk compatible with a `BTreeMap<BlockId, BTreeSet<BlockId>>`.
///
/// Mutation is funnelled through [`Branches::insert`] / [`Branches::extend_branch`] so the
/// two maps cannot drift out of sync.
#[derive(Clone, Default)]
pub struct Branches {
    by_tip: BTreeMap<BlockId, BTreeSet<BlockId>>,
    /// `bid → set of tip BlockIds whose branch set contains `bid`. Derived; never
    /// serialized.
    by_member: BTreeMap<BlockId, BTreeSet<BlockId>>,
}

impl fmt::Debug for Branches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Skip the derived `by_member` index — it's noisy and redundant.
        f.debug_struct("Branches")
            .field("by_tip", &self.by_tip)
            .finish()
    }
}

impl PartialEq for Branches {
    fn eq(&self, other: &Self) -> bool {
        // Equality is defined by the canonical (forward) map only — the index is derived.
        self.by_tip == other.by_tip
    }
}

impl Branches {
    /// Insert a single [`BlockId`] into the branch identified by `tip`.
    ///
    /// Returns `true` if `bid` was newly added (i.e. wasn't already in that branch).
    pub fn insert(&mut self, tip: BlockId, bid: BlockId) -> bool {
        if self.by_tip.entry(tip).or_default().insert(bid) {
            self.by_member.entry(bid).or_default().insert(tip);
            true
        } else {
            false
        }
    }

    /// Insert many [`BlockId`]s into the branch identified by `tip`.
    pub fn extend_branch<I>(&mut self, tip: BlockId, bids: I)
    where
        I: IntoIterator<Item = BlockId>,
    {
        for bid in bids {
            self.insert(tip, bid);
        }
    }

    /// Get the set of [`BlockId`]s that constitute the branch ending at `tip`.
    pub fn get(&self, tip: &BlockId) -> Option<&BTreeSet<BlockId>> {
        self.by_tip.get(tip)
    }

    /// Branch tips whose chain contains `bid` as a member. Empty iterator if `bid` is
    /// not in any branch.
    pub fn containing<'a>(&'a self, bid: &BlockId) -> impl Iterator<Item = BlockId> + 'a {
        self.by_member
            .get(bid)
            .into_iter()
            .flat_map(|s| s.iter().copied())
    }

    /// Iterate `(tip, BlockId-set)` in tip `(height, hash)` order.
    pub fn iter(&self) -> impl Iterator<Item = (&BlockId, &BTreeSet<BlockId>)> {
        self.by_tip.iter()
    }

    /// Iterate tip BlockIds in `(height, hash)` order.
    pub fn keys(&self) -> impl Iterator<Item = &BlockId> {
        self.by_tip.keys()
    }

    /// Iterate BlockId-sets in tip-order.
    pub fn values(&self) -> impl Iterator<Item = &BTreeSet<BlockId>> {
        self.by_tip.values()
    }

    /// Whether the canonical forward map is empty.
    pub fn is_empty(&self) -> bool {
        self.by_tip.is_empty()
    }

    /// Number of distinct tips with a branch entry.
    pub fn len(&self) -> usize {
        self.by_tip.len()
    }

    /// Whether `tip` has a branch entry.
    pub fn contains_key(&self, tip: &BlockId) -> bool {
        self.by_tip.contains_key(tip)
    }
}

impl Merge for Branches {
    fn merge(&mut self, other: Self) {
        for (tip, set) in other.by_tip {
            for bid in set {
                self.insert(tip, bid);
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.by_tip.is_empty()
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Branches {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Wire format == the canonical forward map.
        self.by_tip.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Branches {
    fn deserialize<De: serde::Deserializer<'de>>(deserializer: De) -> Result<Self, De::Error> {
        let by_tip = BTreeMap::<BlockId, BTreeSet<BlockId>>::deserialize(deserializer)?;
        let mut by_member = BTreeMap::<BlockId, BTreeSet<BlockId>>::new();
        for (tip, set) in &by_tip {
            for bid in set {
                by_member.entry(*bid).or_default().insert(*tip);
            }
        }
        Ok(Self { by_tip, by_member })
    }
}
