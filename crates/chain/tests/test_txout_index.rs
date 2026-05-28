#![cfg(feature = "miniscript")]

use bdk_chain::{
    collections::BTreeMap,
    indexer::txout_index::{ChangeSet, ExistingAssignment, InsertError, TxOutIndex},
    DescriptorExt, Indexer, Merge, SpkIterator,
};
use bdk_testenv::{
    hash,
    utils::{new_tx, DESCRIPTORS},
};
use bitcoin::{secp256k1::Secp256k1, Amount, OutPoint, ScriptBuf, Transaction, TxOut};
use miniscript::{Descriptor, DescriptorPublicKey};

#[derive(Clone, Debug, PartialEq, Eq, Ord, PartialOrd)]
enum TestKeychain {
    External,
    Internal,
    /// A keychain backed by a raw script pubkey, no descriptor.
    RawWatched,
    /// Another raw-spk keychain, used to test multiple raw entries.
    RawSweep,
}

impl core::fmt::Display for TestKeychain {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            TestKeychain::External => write!(f, "External"),
            TestKeychain::Internal => write!(f, "Internal"),
            TestKeychain::RawWatched => write!(f, "RawWatched"),
            TestKeychain::RawSweep => write!(f, "RawSweep"),
        }
    }
}

fn parse_descriptor(descriptor: &str) -> Descriptor<DescriptorPublicKey> {
    let secp = bdk_chain::bitcoin::secp256k1::Secp256k1::signing_only();
    Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, descriptor)
        .unwrap()
        .0
}

fn spk_at_index(descriptor: &Descriptor<DescriptorPublicKey>, index: u32) -> ScriptBuf {
    descriptor
        .derived_descriptor(&Secp256k1::verification_only(), index)
        .expect("must derive")
        .script_pubkey()
}

fn init_descriptor_index(
    external_descriptor: Descriptor<DescriptorPublicKey>,
    internal_descriptor: Descriptor<DescriptorPublicKey>,
    lookahead: u32,
    persist_spks: bool,
) -> TxOutIndex<TestKeychain> {
    let mut idx = TxOutIndex::<TestKeychain>::new(lookahead, persist_spks);
    idx.insert_descriptor(TestKeychain::External, external_descriptor)
        .unwrap();
    idx.insert_descriptor(TestKeychain::Internal, internal_descriptor)
        .unwrap();
    idx
}

// ----------------------------------------------------------------------------
// Descriptor-backed keychains (wildcard + non-wildcard)
// ----------------------------------------------------------------------------

#[test]
fn reveals_for_wildcard_descriptor() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal, 5, false);

    assert_eq!(idx.next_index(TestKeychain::External), Some((0, true)));

    let (spk0, cs0) = idx.reveal_next_spk(TestKeychain::External).unwrap();
    assert_eq!(spk0, (0, spk_at_index(&external, 0)));
    assert_eq!(
        cs0.last_revealed.get(&external.descriptor_id()),
        Some(&0),
        "first reveal records the new last-revealed index"
    );

    let (spk1, _) = idx.reveal_next_spk(TestKeychain::External).unwrap();
    assert_eq!(spk1, (1, spk_at_index(&external, 1)));
    assert_eq!(idx.last_revealed_index(TestKeychain::External), Some(1));
}

#[test]
fn reveals_for_non_wildcard_descriptor() {
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    let descriptor = parse_descriptor(DESCRIPTORS[6]); // non-wildcard
    let only_spk = spk_at_index(&descriptor, 0);

    idx.insert_descriptor(TestKeychain::External, descriptor.clone())
        .unwrap();

    // First reveal returns the single spk; the changeset records last_revealed=0.
    let (spk, cs) = idx.reveal_next_spk(TestKeychain::External).unwrap();
    assert_eq!(spk, (0, only_spk.clone()));
    assert_eq!(cs.last_revealed.get(&descriptor.descriptor_id()), Some(&0));

    // Second reveal returns the same spk; no new index is advanced.
    let (spk, cs) = idx.reveal_next_spk(TestKeychain::External).unwrap();
    assert_eq!(spk, (0, only_spk));
    assert!(cs.is_empty(), "no advancement => empty changeset");
    assert_eq!(idx.next_index(TestKeychain::External), Some((0, false)));
}

#[test]
fn reveal_to_target_is_inclusive_and_idempotent() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal, 0, true);

    let (revealed, cs) = idx
        .reveal_to_target(TestKeychain::External, 3)
        .expect("keychain exists");
    assert_eq!(
        revealed.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(cs.last_revealed.get(&external.descriptor_id()), Some(&3));

    // Calling again with the same target yields no new indices.
    let (revealed, cs) = idx.reveal_to_target(TestKeychain::External, 3).unwrap();
    assert!(revealed.is_empty());
    assert!(cs.last_revealed.is_empty());
}

// ----------------------------------------------------------------------------
// Raw-spk keychains
// ----------------------------------------------------------------------------

fn fake_spk(byte: u8) -> ScriptBuf {
    let mut bytes = vec![0x00, 0x14];
    bytes.extend(core::iter::repeat_n(byte, 20));
    ScriptBuf::from_bytes(bytes)
}

#[test]
fn insert_spk_registers_a_raw_keychain() {
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    let spk = fake_spk(0xAA);

    assert!(idx
        .insert_spk(TestKeychain::RawWatched, spk.clone())
        .unwrap());
    // Re-inserting the same (k, spk) is a no-op.
    assert!(!idx
        .insert_spk(TestKeychain::RawWatched, spk.clone())
        .unwrap());

    assert!(idx.is_raw_keychain(&TestKeychain::RawWatched));
    assert_eq!(
        idx.spk_at_index(TestKeychain::RawWatched, 0),
        Some(spk.clone())
    );
    assert_eq!(idx.index_of_spk(&spk), Some(&(TestKeychain::RawWatched, 0)));

    // reveal_next_spk on a raw keychain always yields the single registered spk and never
    // produces a changeset.
    let (revealed, cs) = idx.reveal_next_spk(TestKeychain::RawWatched).unwrap();
    assert_eq!(revealed, (0, spk.clone()));
    assert!(cs.is_empty());

    // Same with next_unused: spk is unused, return it.
    let (unused, cs) = idx.next_unused_spk(TestKeychain::RawWatched).unwrap();
    assert_eq!(unused, (0, spk.clone()));
    assert!(cs.is_empty());

    // Marking the spk used does not invalidate the keychain; next_unused falls back to the
    // single registered spk (matching the existing non-wildcard descriptor behavior). The
    // caller checks `is_used` / `next_index`'s `new` flag to detect this.
    assert!(idx.mark_used(TestKeychain::RawWatched, 0));
    let (fallback, cs) = idx.next_unused_spk(TestKeychain::RawWatched).unwrap();
    assert_eq!(fallback, (0, spk));
    assert!(cs.is_empty());
    assert_eq!(
        idx.next_index(TestKeychain::RawWatched),
        Some((0, false)),
        "`new = false` signals there is no fresh spk to reveal"
    );
}

#[test]
fn raw_keychains_show_up_in_revealed_spks_iter() {
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    let spk_a = fake_spk(0x01);
    let spk_b = fake_spk(0x02);

    idx.insert_spk(TestKeychain::RawWatched, spk_a.clone())
        .unwrap();
    idx.insert_spk(TestKeychain::RawSweep, spk_b.clone())
        .unwrap();

    let revealed: Vec<_> = idx.revealed_spks(..).collect();
    assert_eq!(revealed.len(), 2);
    assert!(revealed.contains(&((TestKeychain::RawWatched, 0), spk_a)));
    assert!(revealed.contains(&((TestKeychain::RawSweep, 0), spk_b)));
}

#[test]
fn raw_and_descriptor_keychains_coexist() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal.clone(), 5, false);

    let raw = fake_spk(0xFF);
    idx.insert_spk(TestKeychain::RawWatched, raw.clone())
        .unwrap();

    // Wildcard reveal still works for the descriptor-backed keychain.
    let (spk, _) = idx.reveal_next_spk(TestKeychain::External).unwrap();
    assert_eq!(spk, (0, spk_at_index(&external, 0)));

    // The raw keychain is queryable by spk.
    assert_eq!(idx.index_of_spk(&raw), Some(&(TestKeychain::RawWatched, 0)));

    // index_tx picks up an output to the raw spk.
    let tx = Transaction {
        output: vec![TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: raw.clone(),
        }],
        ..new_tx(42)
    };
    let cs = idx.index_tx(&tx);
    // Raw-spk hits don't generate descriptor-related changeset entries.
    assert!(
        cs.is_empty(),
        "raw-spk receipts must not produce a descriptor changeset"
    );
    assert!(idx.is_used(TestKeychain::RawWatched, 0));
    assert_eq!(idx.last_used_index(TestKeychain::RawWatched), Some(0));
}

// ----------------------------------------------------------------------------
// Overlap policy
// ----------------------------------------------------------------------------

#[test]
fn raw_spk_overlapping_descriptor_is_rejected() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let lookahead = 10;
    let mut idx = init_descriptor_index(external.clone(), internal, lookahead, false);

    // An spk derived from the external descriptor at index 3 is already in the lookahead window.
    let in_lookahead = spk_at_index(&external, 3);

    let err = idx
        .insert_spk(TestKeychain::RawWatched, in_lookahead.clone())
        .unwrap_err();
    match err {
        InsertError::SpkAlreadyAssigned {
            attempted_keychain,
            existing_keychain,
            existing_index,
            ..
        } => {
            assert_eq!(attempted_keychain, TestKeychain::RawWatched);
            assert_eq!(existing_keychain, TestKeychain::External);
            assert_eq!(existing_index, 3);
        }
        other => panic!("expected SpkAlreadyAssigned, got {other:?}"),
    }
}

#[test]
fn descriptor_overlapping_raw_spk_is_rejected() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let lookahead = 10;

    let mut idx = TxOutIndex::<TestKeychain>::new(lookahead, false);
    // Steal index-3's spk before inserting the descriptor.
    let stolen = spk_at_index(&external, 3);
    idx.insert_spk(TestKeychain::RawWatched, stolen.clone())
        .unwrap();

    let err = idx
        .insert_descriptor(TestKeychain::External, external)
        .unwrap_err();
    match err {
        InsertError::SpkAlreadyAssigned {
            attempted_keychain,
            existing_keychain,
            attempted_index,
            ..
        } => {
            assert_eq!(attempted_keychain, TestKeychain::External);
            assert_eq!(existing_keychain, TestKeychain::RawWatched);
            assert_eq!(attempted_index, 3);
        }
        other => panic!("expected SpkAlreadyAssigned, got {other:?}"),
    }
}

#[test]
fn reassigning_raw_keychain_to_descriptor_is_rejected() {
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    idx.insert_spk(TestKeychain::External, fake_spk(0x10))
        .unwrap();
    let desc = parse_descriptor(DESCRIPTORS[6]);
    let err = idx
        .insert_descriptor(TestKeychain::External, desc)
        .unwrap_err();
    match err {
        InsertError::KeychainAlreadyAssigned {
            keychain,
            existing_assignment,
        } => {
            assert_eq!(keychain, TestKeychain::External);
            assert_eq!(existing_assignment, ExistingAssignment::RawSpk);
        }
        other => panic!("expected KeychainAlreadyAssigned (RawSpk), got {other:?}"),
    }
}

#[test]
fn reassigning_descriptor_keychain_to_raw_spk_is_rejected() {
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    let desc = parse_descriptor(DESCRIPTORS[6]);
    idx.insert_descriptor(TestKeychain::External, desc.clone())
        .unwrap();
    let err = idx
        .insert_spk(TestKeychain::External, fake_spk(0x10))
        .unwrap_err();
    match err {
        InsertError::KeychainAlreadyAssigned {
            keychain,
            existing_assignment,
        } => {
            assert_eq!(keychain, TestKeychain::External);
            assert!(matches!(
                existing_assignment,
                ExistingAssignment::Descriptor(_)
            ));
        }
        other => panic!("expected KeychainAlreadyAssigned (Descriptor), got {other:?}"),
    }
}

#[test]
fn reveal_to_target_does_not_extend_past_raw_spk_collision() {
    // The wildcard's index-5 spk is already claimed by a raw-spk keychain. We expect
    // `insert_descriptor` itself to reject — confirming the overlap policy fires before any
    // wildcard reveal would.
    let external = parse_descriptor(DESCRIPTORS[0]);
    let lookahead = 10;

    let mut idx = TxOutIndex::<TestKeychain>::new(lookahead, false);
    idx.insert_spk(TestKeychain::RawWatched, spk_at_index(&external, 5))
        .unwrap();

    let result = idx.insert_descriptor(TestKeychain::External, external);
    assert!(matches!(
        result,
        Err(InsertError::SpkAlreadyAssigned { .. })
    ));
}

// ----------------------------------------------------------------------------
// ChangeSet round-trip & Indexer impl
// ----------------------------------------------------------------------------

#[test]
fn changeset_round_trip_with_persisted_spks() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let lookahead = 5;

    let mut idx = init_descriptor_index(external.clone(), internal.clone(), lookahead, true);
    let _ = idx.reveal_to_target(TestKeychain::External, 3).unwrap();
    let _ = idx.reveal_to_target(TestKeychain::Internal, 1).unwrap();

    let initial = idx.initial_changeset();
    assert_eq!(
        initial.last_revealed.get(&external.descriptor_id()),
        Some(&3)
    );
    assert_eq!(
        initial.last_revealed.get(&internal.descriptor_id()),
        Some(&1)
    );
    assert!(
        initial
            .spk_cache
            .get(&external.descriptor_id())
            .unwrap()
            .len() as u32
            >= 3 + lookahead,
        "spk_cache covers up to last revealed + lookahead",
    );

    // Restore: build a fresh index from the changeset; re-insert descriptors; verify state.
    let mut restored = TxOutIndex::<TestKeychain>::from_changeset(lookahead, true, initial.clone());
    restored
        .insert_descriptor(TestKeychain::External, external.clone())
        .unwrap();
    restored
        .insert_descriptor(TestKeychain::Internal, internal.clone())
        .unwrap();

    assert_eq!(
        restored.last_revealed_index(TestKeychain::External),
        Some(3)
    );
    assert_eq!(
        restored.last_revealed_index(TestKeychain::Internal),
        Some(1)
    );
    // Every revealed spk should be derivable post-restore.
    for i in 0..=3 {
        assert_eq!(
            restored.spk_at_index(TestKeychain::External, i),
            Some(spk_at_index(&external, i))
        );
    }
}

#[test]
fn applying_changesets_one_by_one_vs_aggregate_matches() {
    let desc = parse_descriptor(DESCRIPTORS[0]);
    let changesets: &[ChangeSet] = &[
        ChangeSet {
            last_revealed: [(desc.descriptor_id(), 10)].into(),
            ..Default::default()
        },
        ChangeSet {
            last_revealed: [(desc.descriptor_id(), 12)].into(),
            ..Default::default()
        },
    ];

    let mut a = TxOutIndex::<TestKeychain>::new(0, true);
    a.insert_descriptor(TestKeychain::External, desc.clone())
        .unwrap();
    for cs in changesets {
        a.apply_changeset(cs.clone());
    }

    let mut b = TxOutIndex::<TestKeychain>::new(0, true);
    b.insert_descriptor(TestKeychain::External, desc.clone())
        .unwrap();
    let aggregate = changesets
        .iter()
        .cloned()
        .reduce(|mut agg, cs| {
            agg.merge(cs);
            agg
        })
        .unwrap();
    b.apply_changeset(aggregate);

    assert_eq!(
        a.last_revealed_index(TestKeychain::External),
        b.last_revealed_index(TestKeychain::External)
    );
    assert_eq!(
        a.spk_at_index(TestKeychain::External, 0),
        b.spk_at_index(TestKeychain::External, 0)
    );
}

#[test]
fn scan_with_lookahead_advances_last_revealed() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal, 10, true);

    let spks: BTreeMap<u32, ScriptBuf> = [0, 5, 10]
        .into_iter()
        .map(|i| (i, spk_at_index(&external, i)))
        .collect();

    for (i, spk) in &spks {
        let op = OutPoint::new(hash!("fake tx"), *i);
        let txout = TxOut {
            value: Amount::ZERO,
            script_pubkey: spk.clone(),
        };
        let cs = idx.index_txout(op, &txout);
        assert_eq!(cs.last_revealed.get(&external.descriptor_id()), Some(i));
    }

    // Lookahead surpassed => no indexing.
    let too_far = spk_at_index(&external, 50);
    let op = OutPoint::new(hash!("fake tx"), 50);
    let cs = idx.index_txout(
        op,
        &TxOut {
            value: Amount::ZERO,
            script_pubkey: too_far,
        },
    );
    assert!(cs.is_empty());
}

// ----------------------------------------------------------------------------
// Range / iteration semantics
// ----------------------------------------------------------------------------

#[test]
fn unused_spks_iterates_descriptors_and_raw_keychains() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal.clone(), 0, false);

    let _ = idx.reveal_next_spk(TestKeychain::External).unwrap();
    let raw = fake_spk(0xAB);
    idx.insert_spk(TestKeychain::RawWatched, raw.clone())
        .unwrap();

    let all_unused: Vec<_> = idx.unused_spks().collect();
    assert!(
        all_unused.iter().any(|((k, i), s)| {
            k == &TestKeychain::External && *i == 0 && s == &spk_at_index(&external, 0)
        }),
        "External(0) should appear as unused"
    );
    assert!(
        all_unused
            .iter()
            .any(|((k, i), s)| { k == &TestKeychain::RawWatched && *i == 0 && s == &raw }),
        "RawWatched(0) should appear as unused"
    );
}

#[test]
fn keychain_outpoints_groups_by_keychain() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal.clone(), 5, false);

    // Reveal and pay to External index 0.
    let (_, _) = idx.reveal_next_spk(TestKeychain::External).unwrap();
    let tx_ext = Transaction {
        output: vec![TxOut {
            value: Amount::from_sat(1_000),
            script_pubkey: spk_at_index(&external, 0),
        }],
        ..new_tx(0)
    };
    let _ = idx.index_tx(&tx_ext);

    // Pay to a raw keychain.
    let raw_spk = fake_spk(0x77);
    idx.insert_spk(TestKeychain::RawWatched, raw_spk.clone())
        .unwrap();
    let tx_raw = Transaction {
        output: vec![TxOut {
            value: Amount::from_sat(2_000),
            script_pubkey: raw_spk,
        }],
        ..new_tx(1)
    };
    let _ = idx.index_tx(&tx_raw);

    let ext_outs: Vec<_> = idx.keychain_outpoints(TestKeychain::External).collect();
    assert_eq!(ext_outs.len(), 1);
    assert_eq!(ext_outs[0].0, 0);

    let raw_outs: Vec<_> = idx.keychain_outpoints(TestKeychain::RawWatched).collect();
    assert_eq!(raw_outs.len(), 1);
    assert_eq!(raw_outs[0].0, 0);
}

#[test]
fn descriptor_collision_across_keychains_is_rejected() {
    let desc = parse_descriptor(DESCRIPTORS[0]);
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    idx.insert_descriptor(TestKeychain::External, desc.clone())
        .unwrap();
    let err = idx
        .insert_descriptor(TestKeychain::Internal, desc)
        .unwrap_err();
    assert!(matches!(err, InsertError::DescriptorAlreadyAssigned { .. }));
}

#[test]
fn raw_keychains_iterator_lists_only_raw_keys() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external, internal, 0, false);

    idx.insert_spk(TestKeychain::RawWatched, fake_spk(1))
        .unwrap();
    idx.insert_spk(TestKeychain::RawSweep, fake_spk(2)).unwrap();

    let raw: Vec<_> = idx.raw_keychains().cloned().collect();
    assert_eq!(raw.len(), 2);
    assert!(raw.contains(&TestKeychain::RawWatched));
    assert!(raw.contains(&TestKeychain::RawSweep));

    // The descriptor-backed iterator should still show the two descriptor keychains.
    assert_eq!(idx.keychains().count(), 2);
}

#[test]
fn unbounded_spk_iter_for_raw_keychain_returns_none() {
    let mut idx = TxOutIndex::<TestKeychain>::new(0, false);
    idx.insert_spk(TestKeychain::RawWatched, fake_spk(7))
        .unwrap();
    assert!(idx.unbounded_spk_iter(TestKeychain::RawWatched).is_none());
}

#[test]
fn revealed_keychain_spks_for_wildcard_matches_spk_iterator() {
    let external = parse_descriptor(DESCRIPTORS[0]);
    let internal = parse_descriptor(DESCRIPTORS[1]);
    let mut idx = init_descriptor_index(external.clone(), internal, 0, false);
    let _ = idx.reveal_to_target(TestKeychain::External, 4).unwrap();

    let revealed: Vec<_> = idx.revealed_keychain_spks(TestKeychain::External).collect();
    let expected: Vec<_> = SpkIterator::new_with_range(&external, 0..=4).collect();
    assert_eq!(revealed, expected);
}
