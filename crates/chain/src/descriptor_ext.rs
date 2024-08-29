use crate::miniscript::{Descriptor, DescriptorPublicKey};
use bitcoin::hashes::{hash_newtype, sha256, Hash};

hash_newtype! {
    /// Represents the unique ID of a descriptor.
    ///
    /// This is useful for having a fixed-length unique representation of a descriptor,
    /// in particular, we use it to persist application state changes related to the
    /// descriptor without having to re-write the whole descriptor each time.
    ///
    pub struct DescriptorId(pub sha256::Hash);
}

/// A trait to extend the functionality of a miniscript descriptor.
pub trait DescriptorExt {
    /// Returns the minimum value (in satoshis) at which an output is broadcastable.
    /// Panics if the descriptor wildcard is hardened.
    fn dust_value(&self) -> u64;

    /// Returns the descriptor ID, calculated as the sha256 hash of the spk derived from the
    /// descriptor at index 0.
    fn descriptor_id(&self) -> DescriptorId;

    /// Returns the weight of script pubkeys derived from this descriptor.
    fn spk_weight(&self) -> bitcoin::Weight;

    /// Returns the weight of a [`TxOut`] created with a script pubkey derived from this descriptor.
    fn txout_weight(&self) -> bitcoin::Weight {
        let spk_weight = self.spk_weight();
        let spk_len_weight = bitcoin::Weight::from_vb_unwrap(
            bitcoin::VarInt(spk_weight.to_vbytes_ceil()).size() as u64,
        );
        let value_weight = bitcoin::Weight::from_vb_unchecked(8);
        spk_weight + spk_len_weight + value_weight
    }
}

impl DescriptorExt for Descriptor<DescriptorPublicKey> {
    fn dust_value(&self) -> u64 {
        self.at_derivation_index(0)
            .expect("descriptor can't have hardened derivation")
            .script_pubkey()
            .minimal_non_dust()
            .to_sat()
    }

    fn descriptor_id(&self) -> DescriptorId {
        let spk = self.at_derivation_index(0).unwrap().script_pubkey();
        DescriptorId(sha256::Hash::hash(spk.as_bytes()))
    }

    fn spk_weight(&self) -> bitcoin::Weight {
        bitcoin::Weight::from_vb_unwrap(
            self.at_derivation_index(0)
                .expect("descriptor can't have hardened derivation")
                .script_pubkey()
                .len() as u64,
        )
    }
}

#[cfg(test)]
mod tests {
    use bitcoin::{key::Secp256k1, TxOut};
    use miniscript::{Descriptor, DescriptorPublicKey};

    use crate::DescriptorExt;

    #[test]
    fn txout_weight_default_impl() {
        let secp = Secp256k1::new();
        let descriptor_strs: &[&str] = &[
            "tr([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/0/*)",
            "tr([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/1/*)",
            "wpkh([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/1/0/*)",
            "tr(tprv8ZgxMBicQKsPd3krDUsBAmtnRsK3rb8u5yi1zhQgMhF1tR8MW7xfE4rnrbbsrbPR52e7rKapu6ztw1jXveJSCGHEriUGZV7mCe88duLp5pj/86'/1'/0'/0/*)",
            "tr(tprv8ZgxMBicQKsPd3krDUsBAmtnRsK3rb8u5yi1zhQgMhF1tR8MW7xfE4rnrbbsrbPR52e7rKapu6ztw1jXveJSCGHEriUGZV7mCe88duLp5pj/86'/1'/0'/1/*)",
            "wpkh(xprv9s21ZrQH143K4EXURwMHuLS469fFzZyXk7UUpdKfQwhoHcAiYTakpe8pMU2RiEdvrU9McyuE7YDoKcXkoAwEGoK53WBDnKKv2zZbb9BzttX/1/0/*)",
            // non-wildcard
            "wpkh([73c5da0a/86'/0'/0']xprv9xgqHN7yz9MwCkxsBPN5qetuNdQSUttZNKw1dcYTV4mkaAFiBVGQziHs3NRSWMkCzvgjEe3n9xV8oYywvM8at9yRqyaZVz6TYYhX98VjsUk/1/0)",
        ];
        for &desc_str in descriptor_strs {
            let (desc, _) =
                Descriptor::<DescriptorPublicKey>::parse_descriptor(&secp, desc_str).unwrap();
            let txout_weight = desc.txout_weight();
            let txout_weight_manual = {
                let txo = TxOut {
                    value: bitcoin::Amount::ZERO,
                    script_pubkey: desc.at_derivation_index(0).unwrap().script_pubkey(),
                };
                txo.weight()
            };
            assert_eq!(txout_weight, txout_weight_manual);
        }
    }
}
