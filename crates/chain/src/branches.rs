//! The [`Branches`] index used by [`crate::block_graph::ChangeSet`].
//!
//! Re-exported as [`crate::block_graph::Branches`].

use core::fmt;

use crate::collections::{BTreeMap, BTreeSet};
use crate::{BlockId, Merge};

/// Per-tip branch index for [`crate::block_graph::ChangeSet`].
///
/// Wraps a `BTreeMap<BlockId, BTreeSet<BlockId>>` (tip → BlockIds in that branch's sparse
/// chain) with a reverse index for `O(log N)` "which branches contain this BlockId?"
/// queries. The reverse index is derived — rebuilt on serde deserialize — and the wire
/// format is just the forward map, so [`Branches`] is on-disk compatible with
/// `BTreeMap<BlockId, BTreeSet<BlockId>>`.
///
/// Mutation goes through [`insert`](Self::insert) / [`extend_branch`](Self::extend_branch)
/// to keep the two views in sync.
#[derive(Clone, Default)]
pub struct Branches {
    by_tip: BTreeMap<BlockId, BTreeSet<BlockId>>,
    /// Reverse index. Derived; never serialized.
    by_member: BTreeMap<BlockId, BTreeSet<BlockId>>,
}

impl fmt::Debug for Branches {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Hide derived `by_member`.
        f.debug_struct("Branches")
            .field("by_tip", &self.by_tip)
            .finish()
    }
}

impl PartialEq for Branches {
    fn eq(&self, other: &Self) -> bool {
        // Forward map only — the reverse index is derived.
        self.by_tip == other.by_tip
    }
}

impl Branches {
    /// Insert `bid` into the branch at `tip`. Returns `true` if newly added.
    pub fn insert(&mut self, tip: BlockId, bid: BlockId) -> bool {
        if self.by_tip.entry(tip).or_default().insert(bid) {
            self.by_member.entry(bid).or_default().insert(tip);
            true
        } else {
            false
        }
    }

    /// Insert many [`BlockId`]s into the branch at `tip`.
    pub fn extend_branch<I>(&mut self, tip: BlockId, bids: I)
    where
        I: IntoIterator<Item = BlockId>,
    {
        for bid in bids {
            self.insert(tip, bid);
        }
    }

    /// BlockIds constituting the branch ending at `tip`.
    pub fn get(&self, tip: &BlockId) -> Option<&BTreeSet<BlockId>> {
        self.by_tip.get(tip)
    }

    /// Tips whose chain contains `bid`.
    pub fn containing<'a>(&'a self, bid: &BlockId) -> impl Iterator<Item = BlockId> + 'a {
        self.by_member
            .get(bid)
            .into_iter()
            .flat_map(|s| s.iter().copied())
    }

    /// `(tip, BlockId-set)` entries in tip order.
    pub fn iter(&self) -> impl Iterator<Item = (&BlockId, &BTreeSet<BlockId>)> {
        self.by_tip.iter()
    }

    /// Tip BlockIds in `(height, hash)` order.
    pub fn keys(&self) -> impl Iterator<Item = &BlockId> {
        self.by_tip.keys()
    }

    /// BlockId-sets in tip order.
    pub fn values(&self) -> impl Iterator<Item = &BTreeSet<BlockId>> {
        self.by_tip.values()
    }

    /// Whether the forward map is empty.
    pub fn is_empty(&self) -> bool {
        self.by_tip.is_empty()
    }

    /// Number of tips with a branch entry.
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
