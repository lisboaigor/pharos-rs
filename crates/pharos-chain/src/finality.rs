//! Confirmation depth, finality, and the reorg signal.
//!
//! This is the hard part of talking to a chain: a block that looks applied can
//! later be rolled back. The types here let an application reason about *how
//! sure* it is that something happened, and be told explicitly when the chain
//! retracts history.

use crate::block::ChainBlock;
use crate::identity::{BlockHeight, BlockId};
use crate::outpoint::OutPoint;

/// How many blocks have been built on top of the block that contains a
/// transaction, itself included as one confirmation.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct Confirmations(pub u64);

/// The rule that decides when a transaction is final enough to act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FinalityPolicy {
    /// Treat a transaction as final once it has at least this many
    /// confirmations. Bitcoin conventionally uses 6; Cardano uses a depth in
    /// blocks as well.
    Depth(u64),
    /// The chain finalizes instantly (a BFT chain with deterministic finality).
    /// Reserved for future adapters; a UTXO chain today uses [`Depth`](Self::Depth).
    Instant,
}

impl FinalityPolicy {
    /// Returns whether `confirmations` satisfies this policy.
    pub fn is_final(&self, confirmations: Confirmations) -> bool {
        match self {
            FinalityPolicy::Depth(required) => confirmations.0 >= *required,
            FinalityPolicy::Instant => true,
        }
    }
}

/// A resumable position in the chain for the indexer.
///
/// A cursor is more than a height: it carries the `block` id observed at that
/// height, which is exactly what makes a reorg detectable. When the indexer
/// asks for what comes after a cursor and the successor's `parent` does not
/// match the cursor's `block`, the chain forked below the cursor.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainCursor {
    /// Height/slot of the last processed block.
    pub height: BlockHeight,
    /// Id of the last processed block.
    pub block: BlockId,
}

impl ChainCursor {
    /// Creates a cursor at a height and block.
    pub fn new(height: BlockHeight, block: impl Into<BlockId>) -> Self {
        Self {
            height,
            block: block.into(),
        }
    }

    /// Returns whether `next` builds directly on this cursor.
    ///
    /// `true` when `next.parent == self.block`; `false` signals that the chain
    /// reorganized at or below this cursor and the caller should reconcile.
    pub fn continues_into<Ext>(&self, next: &ChainBlock<Ext>) -> bool {
        next.parent == self.block
    }
}

/// The chain retracted a suffix of history and adopted a different one.
///
/// This is the signal that idempotent consumption alone cannot express: an
/// inbox deduplicates *re-delivery* of the same event, but a reorg is the chain
/// saying "the blocks I told you about between `from` and `to` are gone."
/// `rolled_back` lists the block ids that were undone so a projection can
/// compensate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReorgEvent {
    /// Cursor the chain rolled back from (the abandoned tip).
    pub from: ChainCursor,
    /// Cursor the chain rolled back to (the last common ancestor).
    pub to: ChainCursor,
    /// Blocks that were undone, newest first.
    pub rolled_back: Vec<BlockId>,
}

/// An inbound event produced by observing the chain.
///
/// The `Ext` parameter flows through to the block's outputs so eUTXO datums are
/// preserved end-to-end.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChainEvent<Ext = ()> {
    /// A new block was applied to the tip.
    BlockApplied(ChainBlock<Ext>),
    /// The chain reorganized.
    Reorg(ReorgEvent),
    /// An output gained confirmations (used to drive finality/confirmation).
    Confirmed {
        /// The output whose confirmation depth changed.
        outpoint: OutPoint,
        /// The new confirmation depth.
        depth: Confirmations,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn depth_policy_decides_finality() {
        let policy = FinalityPolicy::Depth(6);
        assert!(!policy.is_final(Confirmations(5)));
        assert!(policy.is_final(Confirmations(6)));
        assert!(policy.is_final(Confirmations(7)));
        assert!(FinalityPolicy::Instant.is_final(Confirmations(0)));
    }

    #[test]
    fn cursor_detects_continuation_versus_reorg() {
        let cursor = ChainCursor::new(BlockHeight(10), "block-A");

        let continuation: ChainBlock = ChainBlock {
            id: BlockId::from("block-B"),
            height: BlockHeight(11),
            parent: BlockId::from("block-A"),
            txs: vec![],
            timestamp: Utc::now(),
        };
        assert!(cursor.continues_into(&continuation));

        let forked: ChainBlock = ChainBlock {
            parent: BlockId::from("block-A-prime"),
            ..continuation
        };
        assert!(!cursor.continues_into(&forked));
    }
}
