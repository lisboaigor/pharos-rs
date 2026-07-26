//! Ports for **submitting** to the chain (the outbound direction).
//!
//! Outbound work has three stages, each its own port so an adapter can
//! implement them independently:
//!
//! 1. [`CoinSelection`] — pick inputs to fund a target value.
//! 2. [`TxBuilder`] — assemble a [`TxIntent`] (payments and/or contract
//!    interactions) into a signed [`RawTx`].
//! 3. [`TxSubmitter`] — broadcast the [`RawTx`] and track its [`TxStatus`].
//!
//! A [`TxIntent`] is where smart-contract interaction becomes explicit: its
//! `spends` carry [`ScriptWitness`]es (redeemers that invoke validators) and its
//! `mints` carry [`Mint`]s (policy-script invocations). A plain Bitcoin payment
//! leaves both empty.

use std::future::Future;

use crate::contract::{Mint, ScriptWitness};
use crate::finality::Confirmations;
use crate::identity::{Address, TxId};
use crate::utxo::Utxo;
use crate::value::LedgerValue;

/// A signed, serialized transaction ready to broadcast.
///
/// The bytes are the chain's own encoding (CBOR on Cardano, raw transaction
/// bytes on Bitcoin). Like a broker message payload, the core stays agnostic to
/// what is inside.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RawTx(pub Vec<u8>);

/// The outcome of selecting inputs to fund a target.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Selection<Ext = ()> {
    /// The UTXOs chosen to fund the target.
    pub inputs: Vec<Utxo<Ext>>,
    /// The change left over after funding the target and fees.
    pub change: LedgerValue,
}

/// Selects inputs to cover a target value.
///
/// The algorithm (largest-first, branch-and-bound, …) lives in the adapter; the
/// core only fixes the contract: given a `target` and a set of `candidates`,
/// return the chosen inputs and the resulting change.
pub trait CoinSelection: Send + Sync + 'static {
    /// Chain-specific output extension.
    type Ext;
    /// Concrete selection error (e.g. insufficient funds).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Selects inputs from `candidates` to cover `target`.
    fn select(
        &self,
        target: &LedgerValue,
        candidates: &[Utxo<Self::Ext>],
    ) -> impl Future<Output = Result<Selection<Self::Ext>, Self::Error>> + Send;
}

/// A declared transaction: what to pay, which contracts to invoke, and where
/// change goes.
///
/// `outputs` are the outputs to create. `spends` invoke script-locked UTXOs
/// (each a [`ScriptWitness`] carrying a redeemer). `mints` invoke minting
/// policies. A pure payment has empty `spends` and `mints`; a contract
/// interaction populates them.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TxIntent<Ext = ()> {
    /// Outputs the transaction should create.
    pub outputs: Vec<Utxo<Ext>>,
    /// Script-locked inputs to spend, each with its invoking redeemer.
    pub spends: Vec<ScriptWitness>,
    /// Minting/burning interactions under policy scripts.
    pub mints: Vec<Mint>,
    /// Address that receives the change.
    pub change: Address,
}

impl<Ext> TxIntent<Ext> {
    /// Creates a pure-payment intent (no contract interaction).
    pub fn payment(outputs: Vec<Utxo<Ext>>, change: impl Into<Address>) -> Self {
        Self {
            outputs,
            spends: Vec::new(),
            mints: Vec::new(),
            change: change.into(),
        }
    }

    /// Returns whether this intent invokes any smart contract.
    pub fn invokes_contract(&self) -> bool {
        !self.spends.is_empty() || !self.mints.is_empty()
    }
}

/// Assembles a [`TxIntent`] into a signed [`RawTx`].
///
/// Balancing, fee calculation, script-witness attachment, and serialization are
/// all the adapter's job; the core only fixes that an intent goes in and a
/// broadcastable transaction comes out.
pub trait TxBuilder: Send + Sync + 'static {
    /// Chain-specific output extension.
    type Ext;
    /// Concrete build error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Builds a raw transaction from an intent.
    fn build(
        &self,
        intent: &TxIntent<Self::Ext>,
    ) -> impl Future<Output = Result<RawTx, Self::Error>> + Send;
}

/// The state of a submitted transaction as the chain sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TxStatus {
    /// The chain has never heard of the transaction.
    Unknown,
    /// The transaction is in the mempool, not yet in a block.
    Mempool,
    /// The transaction is in a block with the given confirmation depth.
    Confirmed {
        /// How deep the containing block is.
        depth: Confirmations,
    },
    /// The transaction was rejected or dropped.
    Failed,
}

/// Broadcasts transactions and reports their status.
pub trait TxSubmitter: Send + Sync + 'static {
    /// Concrete submission error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Submits a raw transaction and returns its id.
    fn submit(&self, tx: RawTx) -> impl Future<Output = Result<TxId, Self::Error>> + Send;

    /// Returns the current status of a transaction.
    fn status(&self, tx: &TxId) -> impl Future<Output = Result<TxStatus, Self::Error>> + Send;
}
