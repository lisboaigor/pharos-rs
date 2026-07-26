//! An unspent transaction output.

use crate::identity::Address;
use crate::outpoint::OutPoint;
use crate::value::LedgerValue;

/// An unspent output: where it lives, who owns it, what it holds, and any
/// chain-specific extension.
///
/// The `Ext` type parameter keeps the model honest for both chains without
/// imposing one on the other. Bitcoin uses `Utxo<()>` — an output is just an
/// address and a value. Cardano uses `Utxo<`[`EutxoExt`](crate::EutxoExt)`>` so
/// the output can also carry a datum and a reference script, which is what
/// makes it eUTXO. The default `Ext = ()` means plain `Utxo` is the Bitcoin
/// shape.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Utxo<Ext = ()> {
    /// The output's location on the chain.
    pub outpoint: OutPoint,
    /// The address that owns the output.
    pub address: Address,
    /// The value held by the output.
    pub value: LedgerValue,
    /// Chain-specific extension (`()` on Bitcoin; datum/script on Cardano).
    pub ext: Ext,
}

impl<Ext> Utxo<Ext> {
    /// Creates a UTXO with an explicit extension.
    pub fn new(
        outpoint: OutPoint,
        address: impl Into<Address>,
        value: LedgerValue,
        ext: Ext,
    ) -> Self {
        Self {
            outpoint,
            address: address.into(),
            value,
            ext,
        }
    }
}

impl Utxo<()> {
    /// Creates a plain (Bitcoin-shaped) UTXO with no extension.
    pub fn plain(outpoint: OutPoint, address: impl Into<Address>, value: LedgerValue) -> Self {
        Self::new(outpoint, address, value, ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{Datum, EutxoExt};
    use pharos_core::{Currency, Money};

    #[test]
    fn bitcoin_utxo_needs_no_extension() {
        let utxo = Utxo::plain(
            OutPoint::new("tx-1", 0),
            "bc1qexample",
            LedgerValue::coin_only(Money::new(50_000, Currency::btc())),
        );
        assert_eq!(utxo.ext, ());
    }

    #[test]
    fn cardano_utxo_carries_a_datum() -> Result<(), serde_json::Error> {
        let utxo = Utxo::new(
            OutPoint::new("tx-2", 1),
            "addr1_script",
            LedgerValue::coin_only(Money::new(
                2_000_000,
                Currency::new("ADA", 6).unwrap_or_else(|_| panic!("valid")),
            )),
            EutxoExt {
                datum: Some(Datum(vec![1, 2, 3])),
                script_ref: None,
            },
        );
        assert_eq!(utxo.ext.datum, Some(Datum(vec![1, 2, 3])));

        // Round-trips with the extension serialized inline.
        let json = serde_json::to_string(&utxo)?;
        let back: Utxo<EutxoExt> = serde_json::from_str(&json)?;
        assert_eq!(back, utxo);
        Ok(())
    }
}
