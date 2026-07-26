//! Transactions and blocks as the indexer sees them.

use chrono::{DateTime, Utc};

use crate::identity::{BlockHeight, BlockId, TxId};
use crate::outpoint::OutPoint;
use crate::utxo::Utxo;

/// A transaction: the outputs it consumes and the outputs it creates.
///
/// A UTXO transaction is defined by what it spends (`inputs`, referenced by
/// outpoint) and what it produces (`outputs`, full [`Utxo`]s). The `Ext` type
/// parameter flows through to the outputs so eUTXO datums ride along.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainTx<Ext = ()> {
    /// Transaction id.
    pub id: TxId,
    /// Outputs consumed by this transaction.
    pub inputs: Vec<OutPoint>,
    /// Outputs created by this transaction.
    pub outputs: Vec<Utxo<Ext>>,
}

/// A block of transactions, anchored to its parent.
///
/// The `parent` link is the crux of reorg detection: when the indexer sees a
/// new block whose `parent` is not the block it last recorded at that height,
/// the chain has reorganized. Height and parent together let a
/// [`ChainCursor`](crate::ChainCursor) recognize a fork.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChainBlock<Ext = ()> {
    /// Block id (hash).
    pub id: BlockId,
    /// Block height or slot.
    pub height: BlockHeight,
    /// Id of the parent block this one builds on.
    pub parent: BlockId,
    /// Transactions contained in the block.
    pub txs: Vec<ChainTx<Ext>>,
    /// Block timestamp.
    pub timestamp: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::BlockId;

    #[test]
    fn a_reorg_shows_up_as_a_broken_parent_link() {
        // The indexer recorded block A at height 10.
        let recorded_a = BlockId::from("block-A");

        // A competing block B' arrives at height 11 whose parent is NOT A: it
        // builds on a fork, so the chain reorganized.
        let block_b_prime: ChainBlock = ChainBlock {
            id: BlockId::from("block-B-prime"),
            height: BlockHeight(11),
            parent: BlockId::from("block-A-prime"),
            txs: vec![],
            timestamp: Utc::now(),
        };
        assert_ne!(block_b_prime.parent, recorded_a);

        // A well-formed continuation would have parent == recorded tip.
        let block_b: ChainBlock = ChainBlock {
            parent: recorded_a.clone(),
            ..block_b_prime.clone()
        };
        assert_eq!(block_b.parent, recorded_a);
    }
}
