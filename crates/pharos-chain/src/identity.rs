//! Strong string newtypes for chain identifiers.
//!
//! The chain layer never passes bare `String`s around: a transaction hash, a
//! block hash, and an address are semantically different even though all three
//! are strings on the wire. Each gets a distinct newtype so the compiler
//! rejects mixing them. The types are format-agnostic — a Bitcoin txid and a
//! Cardano txid are both just [`TxId`] here; validating the concrete encoding
//! (hex length, bech32 prefix, …) is an adapter's job, not the core's.

/// Generates a `#[serde(transparent)]` string newtype with the ergonomics the
/// chain layer expects: `Deref<str>`, `Display`, `new`, `as_str`, and `From`
/// conversions from `&str`, `String`, and `Uuid`.
///
/// This mirrors `pharos-app`'s private `flow_id_type!`; that macro is not
/// exported, so the chain crate keeps its own copy rather than depending on an
/// internal detail of another crate.
macro_rules! chain_id_type {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        ///
        /// Serializes transparently as its inner string, so the wire format is
        /// identical to a plain string field. Derefs to `str` for ergonomic
        /// comparisons.
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Wraps an identifier value.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }

        impl From<::uuid::Uuid> for $name {
            fn from(value: ::uuid::Uuid) -> Self {
                Self(value.to_string())
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

pub(crate) use chain_id_type;

chain_id_type! {
    /// Identifier of a transaction on the chain (a Bitcoin txid, a Cardano
    /// transaction hash). Encoding is chain-specific and validated by adapters.
    TxId
}

chain_id_type! {
    /// Identifier of a block on the chain (a Bitcoin block hash, a Cardano
    /// block header hash). The parent link between blocks is what makes a reorg
    /// detectable; see [`crate::ChainBlock`].
    BlockId
}

chain_id_type! {
    /// A payment address that can own UTXOs. Format-agnostic: a Bitcoin
    /// address, a Cardano base/enterprise address, or a script address.
    Address
}

/// Height of a block, or the slot number on chains that use slots.
///
/// A plain, comparable counter used to order blocks and to anchor a
/// [`crate::ChainCursor`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct BlockHeight(pub u64);

impl std::fmt::Display for BlockHeight {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newtypes_are_distinct_and_transparent() -> Result<(), serde_json::Error> {
        let tx = TxId::from("abc123");
        assert_eq!(tx.as_str(), "abc123");
        assert_eq!(&*tx, "abc123");
        assert_eq!(tx.to_string(), "abc123");

        // Transparent serialization: identical to a bare string.
        assert_eq!(serde_json::to_string(&tx)?, "\"abc123\"");
        let back: TxId = serde_json::from_str("\"abc123\"")?;
        assert_eq!(back, tx);
        Ok(())
    }

    #[test]
    fn block_height_orders_and_serializes_as_number() -> Result<(), serde_json::Error> {
        assert!(BlockHeight(10) < BlockHeight(11));
        assert_eq!(serde_json::to_string(&BlockHeight(42))?, "42");
        let back: BlockHeight = serde_json::from_str("42")?;
        assert_eq!(back, BlockHeight(42));
        Ok(())
    }
}
