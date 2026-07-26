//! Universal UTXO addressing.

use crate::identity::TxId;

/// A reference to one specific output of a transaction: the universal way to
/// address a UTXO on any UTXO/eUTXO chain.
///
/// Bitcoin calls the `index` field `vout`; Cardano calls it the output index.
/// The pair `(tx, index)` uniquely identifies a spendable output before it is
/// consumed.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct OutPoint {
    /// Transaction that created the output.
    pub tx: TxId,
    /// Zero-based index of the output within that transaction.
    pub index: u32,
}

impl OutPoint {
    /// Creates an outpoint from a transaction id and output index.
    pub fn new(tx: impl Into<TxId>, index: u32) -> Self {
        Self {
            tx: tx.into(),
            index,
        }
    }
}

impl std::fmt::Display for OutPoint {
    /// Formats as `txid#index`, the common human notation for an outpoint.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}#{}", self.tx, self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_and_round_trips() -> Result<(), serde_json::Error> {
        let outpoint = OutPoint::new("deadbeef", 2);
        assert_eq!(outpoint.to_string(), "deadbeef#2");

        let json = serde_json::to_string(&outpoint)?;
        let back: OutPoint = serde_json::from_str(&json)?;
        assert_eq!(back, outpoint);
        Ok(())
    }
}
