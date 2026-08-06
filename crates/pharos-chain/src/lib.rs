//! Chain-agnostic UTXO/eUTXO core for Pharos.
//!
//! Pharos applications model business facts as aggregates persisted through a
//! [`Repository`](pharos_core::Repository) — a database is the system of record,
//! guarded by optimistic concurrency. A blockchain is the opposite: **the chain
//! is the source of truth, and the aggregates are projections of it.** Those two
//! ownership models do not compose, so `pharos-chain` does *not* implement
//! `Repository`. It treats the chain as an **external integration system** and
//! offers ports in two directions, plus the reorg/finality-aware types and the
//! bridge that wires them into Pharos's existing seams.
//!
//! ```text
//!            observe (inbound)                         submit (outbound)
//!   chain ──▶ ChainSource ──▶ ChainEvent ──▶ bridge     TxIntent ──▶ TxBuilder
//!            UtxoQuery                       │            │            │
//!                                            ▼            ▼            ▼
//!                              process_idempotent   CoinSelection   RawTx ──▶ TxSubmitter
//!                              confirmation saga
//! ```
//!
//! # Scope
//!
//! This crate is the **chain-agnostic core: traits and types only.** It carries
//! no chain SDK and no network code. Concrete adapters (Bitcoin, Cardano) live
//! in separate crates and implement these ports. The core is validated
//! conceptually against both Bitcoin (UTXO) and Cardano (eUTXO), including
//! smart-contract interaction (Plutus / Script).
//!
//! # Values without bigint
//!
//! Unlike account-based chains (an ETH balance in wei overflows `u64`), UTXO
//! coin amounts fit `i128` with room to spare — Bitcoin's whole supply in
//! satoshi is ~2.1e15, Cardano's in lovelace ~4.5e16. So values reuse
//! [`Money`](pharos_core::Money) directly; [`LedgerValue`] adds Cardano's native
//! assets as a map alongside the coin.
//!
//! # Bitcoin ↔ Cardano mapping
//!
//! | type                    | Bitcoin (UTXO)              | Cardano (eUTXO)                 |
//! |-------------------------|-----------------------------|---------------------------------|
//! | [`LedgerValue`]         | `coin` only, `assets` empty | `coin` + native `assets`        |
//! | [`Utxo`] `Ext`          | `()`                        | [`EutxoExt`] (datum, script ref)|
//! | [`OutPoint`]            | `txid:vout`                 | tx hash + output index          |
//! | [`FinalityPolicy`]      | `Depth(6)` conventionally   | `Depth(n)`                      |
//! | [`Datum`]               | (rarely used)               | inline/hashed datum             |
//! | [`Redeemer`]            | `scriptSig` / witness       | Plutus redeemer                 |
//! | [`ScriptHash`]          | P2SH / P2WSH hash           | script / policy hash            |
//! | [`Mint`]                | —                           | minting policy invocation       |
//!
//! # Reusing existing seams
//!
//! The chain layer does not reinvent messaging. [`chain_event_to_message`] emits
//! a [`Message`](pharos_app::Message) with a deterministic id so the standard
//! idempotent consumer deduplicates re-observed transactions, and
//! [`ConfirmationState`] documents the confirmation saga that defers domain
//! facts until finality — so most reorgs never become facts at all.

mod block;
mod bridge;
mod contract;
mod errors;
mod finality;
mod identity;
mod inbound;
mod outbound;
mod outpoint;
mod utxo;
mod value;

pub use block::{ChainBlock, ChainTx};
pub use bridge::{ConfirmationState, chain_event_to_message};
pub use contract::{Datum, EutxoExt, Mint, Redeemer, ScriptHash, ScriptSource, ScriptWitness};
pub use errors::ChainError;
pub use finality::{ChainCursor, ChainEvent, Confirmations, FinalityPolicy, ReorgEvent};
pub use identity::{Address, BlockHeight, BlockId, TxId};
pub use inbound::{ChainSource, UtxoQuery};
pub use outbound::{CoinSelection, RawTx, Selection, TxBuilder, TxIntent, TxStatus, TxSubmitter};
pub use outpoint::OutPoint;
pub use utxo::Utxo;
pub use value::{AssetId, LedgerValue};
