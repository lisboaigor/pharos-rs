//! Smart-contract interaction vocabulary for UTXO/eUTXO chains.
//!
//! In the eUTXO model a "smart contract" is a script that locks an output.
//! Interacting with it means one of two things:
//!
//! - **Observing** its state — reading the [`Datum`] attached to the UTXOs held
//!   at the script's address.
//! - **Invoking** it — spending a script-locked UTXO by supplying a
//!   [`Redeemer`] (the argument the script validates against), or minting/burning
//!   assets under a policy script with a [`Mint`].
//!
//! Everything here is payload-agnostic: a [`Datum`], [`Redeemer`], and inline
//! script are opaque byte blobs, exactly like [`RawTx`](crate::RawTx). The core
//! defines the shape of an interaction; encoding it (Plutus data as CBOR on
//! Cardano, script bytes on Bitcoin) is the adapter's job.
//!
//! # Bitcoin ↔ Cardano
//!
//! | concept          | Bitcoin                     | Cardano (Plutus)          |
//! |------------------|-----------------------------|---------------------------|
//! | [`ScriptHash`]   | P2SH / P2WSH script hash    | script / policy hash      |
//! | [`Datum`]        | (rarely used)               | inline or hashed datum    |
//! | [`Redeemer`]     | `scriptSig` / witness stack | Plutus redeemer           |
//! | [`ScriptSource`] | inline redeem/witness script| inline or reference script|

use std::collections::BTreeMap;

use crate::identity::chain_id_type;
use crate::outpoint::OutPoint;
use crate::value::AssetId;

chain_id_type! {
    /// Hash that identifies a script (a validator or a minting policy).
    ///
    /// On Cardano an [`AssetId::policy_id`](crate::AssetId) is a `ScriptHash` of
    /// a minting policy. On Bitcoin it is the hash embedded in a P2SH/P2WSH
    /// address.
    ScriptHash
}

/// Contract state attached to a UTXO, as an opaque blob.
///
/// The bytes are the datum exactly as the chain represents it (Plutus data
/// encoded as CBOR on Cardano). The core does not interpret them.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Datum(pub Vec<u8>);

/// The argument that invokes a script when spending a script-locked UTXO.
///
/// The bytes are the redeemer as the chain represents it. Supplying a redeemer
/// is what "calling" a smart contract looks like in the eUTXO model.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Redeemer(pub Vec<u8>);

/// Where the script code for an interaction comes from.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScriptSource {
    /// The script lives at an existing on-chain output (a Cardano reference
    /// input), addressed by its outpoint. The spending transaction references
    /// it instead of carrying the bytes.
    ByReference(OutPoint),
    /// The script bytes are supplied inline with the spending transaction.
    Inline(Vec<u8>),
}

/// Everything needed to spend one script-locked UTXO: the interaction itself.
///
/// Pairs the `input` being spent with the `redeemer` that invokes its script,
/// the `datum` when the chain requires it to be provided alongside, and where
/// the script code comes from.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ScriptWitness {
    /// The script-locked output being spent.
    pub input: OutPoint,
    /// The argument that unlocks/invokes the script.
    pub redeemer: Redeemer,
    /// The datum, when the chain requires it to be provided at spend time.
    pub datum: Option<Datum>,
    /// The script code, inline or referenced.
    pub script: ScriptSource,
}

/// A minting or burning interaction under a policy script.
///
/// Positive quantities mint; negative quantities burn. The `redeemer` invokes
/// the minting policy identified by `policy`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Mint {
    /// The minting policy script.
    pub policy: ScriptHash,
    /// Assets to mint (positive) or burn (negative) under the policy.
    #[serde(with = "crate::value::assets_as_seq")]
    pub assets: BTreeMap<AssetId, i128>,
    /// The argument that invokes the minting policy.
    pub redeemer: Redeemer,
}

/// Ready-made [`Utxo::ext`](crate::Utxo) payload for eUTXO chains.
///
/// Bitcoin outputs carry no extension (`Utxo<()>`); Cardano outputs can carry a
/// datum and/or a reference script, which is what makes them eUTXO. Use
/// `Utxo<EutxoExt>` on chains that need it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EutxoExt {
    /// Datum attached to the output, when present.
    pub datum: Option<Datum>,
    /// Reference script published at the output, when present.
    pub script_ref: Option<ScriptSource>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_witness_round_trips() -> Result<(), serde_json::Error> {
        let witness = ScriptWitness {
            input: OutPoint::new("tx-1", 0),
            redeemer: Redeemer(vec![1, 2, 3]),
            datum: Some(Datum(vec![9])),
            script: ScriptSource::ByReference(OutPoint::new("script-tx", 1)),
        };
        let json = serde_json::to_string(&witness)?;
        let back: ScriptWitness = serde_json::from_str(&json)?;
        assert_eq!(back, witness);
        Ok(())
    }

    #[test]
    fn mint_expresses_mint_and_burn() -> Result<(), serde_json::Error> {
        let mint = Mint {
            policy: ScriptHash::from("policy-1"),
            assets: BTreeMap::from([
                (AssetId::new("policy-1", "UP"), 100),
                (AssetId::new("policy-1", "DOWN"), -50),
            ]),
            redeemer: Redeemer(vec![0xff]),
        };
        let json = serde_json::to_string(&mint)?;
        let back: Mint = serde_json::from_str(&json)?;
        assert_eq!(back, mint);
        assert_eq!(
            back.assets.get(&AssetId::new("policy-1", "DOWN")),
            Some(&-50)
        );
        Ok(())
    }

    #[test]
    fn eutxo_ext_defaults_to_empty() {
        let ext = EutxoExt::default();
        assert!(ext.datum.is_none());
        assert!(ext.script_ref.is_none());
    }
}
