//! Ports for **observing** the chain (the inbound direction).
//!
//! An application does not embed a chain node; it observes one through these
//! ports. [`ChainSource`] streams the block/reorg/confirmation events an indexer
//! produces, and [`UtxoQuery`] answers point questions about the current UTXO
//! set — including the script-locked UTXOs that hold smart-contract state.
//!
//! Every port follows the framework's async style: a `Send + Sync + 'static`
//! trait whose methods return `impl Future<...> + Send` (RPITIT, no
//! `async_trait`), mirroring `pharos-messaging`.

use std::future::Future;

use crate::ScriptHash;
use crate::finality::{ChainCursor, ChainEvent};
use crate::identity::Address;
use crate::outpoint::OutPoint;
use crate::utxo::Utxo;

/// Streams inbound events from an indexed view of the chain.
///
/// Implementors drive an indexer: given where the consumer last was
/// ([`ChainCursor`]), they yield the next [`ChainEvent`] — a new block, a reorg,
/// or a confirmation update. Returning `Ok(None)` means the caller is caught up
/// to the tip and should poll again later.
pub trait ChainSource: Send + Sync + 'static {
    /// Chain-specific output extension (`()` on Bitcoin, datum/script on Cardano).
    type Ext;
    /// Concrete source error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns the current chain tip as a resumable cursor.
    fn tip(&self) -> impl Future<Output = Result<ChainCursor, Self::Error>> + Send;

    /// Returns the next event after `cursor`, or `None` when caught up.
    fn next_from(
        &self,
        cursor: &ChainCursor,
    ) -> impl Future<Output = Result<Option<ChainEvent<Self::Ext>>, Self::Error>> + Send;
}

/// Answers point queries against the current UTXO set.
///
/// This is the read side an application uses to resolve inputs, list what an
/// address owns, or read the state of a contract by listing the UTXOs held at a
/// script.
pub trait UtxoQuery: Send + Sync + 'static {
    /// Chain-specific output extension.
    type Ext;
    /// Concrete query error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Lists the UTXOs currently held at `address`.
    fn utxos_at(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<Vec<Utxo<Self::Ext>>, Self::Error>> + Send;

    /// Resolves a single outpoint to its UTXO, when still unspent.
    fn resolve(
        &self,
        outpoint: &OutPoint,
    ) -> impl Future<Output = Result<Option<Utxo<Self::Ext>>, Self::Error>> + Send;

    /// Lists the UTXOs locked at a script, to observe contract state.
    ///
    /// Each returned UTXO carries the contract's datum in its
    /// [`ext`](Utxo::ext) (for eUTXO chains, `Utxo<`[`EutxoExt`](crate::EutxoExt)`>`).
    fn utxos_at_script(
        &self,
        script: &ScriptHash,
    ) -> impl Future<Output = Result<Vec<Utxo<Self::Ext>>, Self::Error>> + Send;
}
