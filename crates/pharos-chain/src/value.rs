//! Values as they exist on a UTXO/eUTXO ledger.
//!
//! A Bitcoin output holds a single coin amount. A Cardano output holds a coin
//! amount **plus** a bundle of native assets. [`LedgerValue`] models both: the
//! `coin` reuses [`Money`] (satoshi at exponent 8, lovelace at exponent 6 — both
//! fit `i128` with room to spare, so no bigint is needed), while `assets` maps
//! each [`AssetId`] to its integer quantity. Bitcoin simply leaves `assets`
//! empty.

use std::collections::BTreeMap;

use pharos_core::{Money, MoneyError};

/// Identifies a native (non-coin) asset by its minting policy and asset name.
///
/// On Cardano the `policy_id` is the hash of the minting script (see
/// [`ScriptHash`](crate::ScriptHash)) and `name` is the asset name under that
/// policy. The pair is globally unique. `Ord` is derived so an `AssetId` can key
/// the [`LedgerValue`] asset map deterministically.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AssetId {
    /// Minting policy id (a script hash on Cardano).
    pub policy_id: String,
    /// Asset name under the policy.
    pub name: String,
}

impl AssetId {
    /// Creates an asset id from a policy id and asset name.
    pub fn new(policy_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            policy_id: policy_id.into(),
            name: name.into(),
        }
    }
}

impl std::fmt::Display for AssetId {
    /// Formats as `policy_id.name`, the common notation for a Cardano asset.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.policy_id, self.name)
    }
}

/// The value carried by a UTXO: a coin amount plus optional native assets.
///
/// This is the value object suited to UTXO/eUTXO ledgers. Arithmetic is checked
/// and total: [`checked_add`](Self::checked_add) and
/// [`checked_sub`](Self::checked_sub) enforce the coin's currency invariant
/// (through [`Money`]) and guard every asset quantity against `i128` overflow.
/// An asset absent from a value counts as zero, and an asset whose quantity
/// reaches zero after an operation is dropped, so equal bundles compare equal
/// regardless of history.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LedgerValue {
    /// The coin amount (satoshi on Bitcoin, lovelace on Cardano).
    pub coin: Money,
    /// Native asset quantities keyed by asset id. Empty on Bitcoin.
    #[serde(with = "assets_as_seq")]
    pub assets: BTreeMap<AssetId, i128>,
}

/// Serializes an `AssetId`-keyed quantity map as a sequence of entries.
///
/// JSON object keys must be strings, so a `BTreeMap<AssetId, _>` (a struct key)
/// cannot serialize as a map. Encoding it as a list of `{asset, quantity}`
/// entries keeps the strong `AssetId` type. Quantities are strings for the same
/// reason [`Money`] amounts are: a JSON number loses precision above 2^53, and a
/// large token supply can exceed it. Shared with [`Mint`](crate::Mint).
pub(crate) mod assets_as_seq {
    use std::collections::BTreeMap;

    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::AssetId;

    #[derive(Serialize, Deserialize)]
    struct Entry {
        asset: AssetId,
        quantity: String,
    }

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<AssetId, i128>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<Entry> = map
            .iter()
            .map(|(asset, quantity)| Entry {
                asset: asset.clone(),
                quantity: quantity.to_string(),
            })
            .collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<AssetId, i128>, D::Error> {
        let entries = Vec::<Entry>::deserialize(deserializer)?;
        entries
            .into_iter()
            .map(|entry| {
                let quantity = entry.quantity.parse().map_err(|_| {
                    D::Error::custom(format!("invalid asset quantity {:?}", entry.quantity))
                })?;
                Ok((entry.asset, quantity))
            })
            .collect()
    }
}

impl LedgerValue {
    /// Creates a value with only a coin amount and no native assets.
    pub fn coin_only(coin: Money) -> Self {
        Self {
            coin,
            assets: BTreeMap::new(),
        }
    }

    /// Creates a value with a coin amount and a bundle of native assets.
    ///
    /// Assets with a zero quantity are dropped so the value stays canonical.
    pub fn new(coin: Money, assets: BTreeMap<AssetId, i128>) -> Self {
        let mut value = Self { coin, assets };
        value.assets.retain(|_, quantity| *quantity != 0);
        value
    }

    /// Adds another value, combining coins and merging asset bundles.
    ///
    /// The coin add goes through [`Money::checked_add`], so a currency mismatch
    /// or coin overflow returns the corresponding [`MoneyError`]. Each asset
    /// quantity is summed with `i128::checked_add`; an overflow returns
    /// [`MoneyError::Overflow`]. Assets that cancel to zero are removed.
    pub fn checked_add(&self, other: &Self) -> Result<Self, MoneyError> {
        let coin = self.coin.checked_add(&other.coin)?;
        let mut assets = self.assets.clone();
        for (asset, quantity) in &other.assets {
            let entry = assets.entry(asset.clone()).or_insert(0);
            *entry = entry.checked_add(*quantity).ok_or(MoneyError::Overflow)?;
        }
        assets.retain(|_, quantity| *quantity != 0);
        Ok(Self { coin, assets })
    }

    /// Subtracts another value, netting coins and asset quantities.
    ///
    /// Mirrors [`checked_add`](Self::checked_add): the coin sub enforces the
    /// currency invariant and guards overflow, and each asset is netted with
    /// `i128::checked_sub`. Quantities may go negative (a deliberate deficit,
    /// e.g. a burn), matching [`Money`]'s own signed semantics; only `i128`
    /// overflow is rejected.
    pub fn checked_sub(&self, other: &Self) -> Result<Self, MoneyError> {
        let coin = self.coin.checked_sub(&other.coin)?;
        let mut assets = self.assets.clone();
        for (asset, quantity) in &other.assets {
            let entry = assets.entry(asset.clone()).or_insert(0);
            *entry = entry.checked_sub(*quantity).ok_or(MoneyError::Overflow)?;
        }
        assets.retain(|_, quantity| *quantity != 0);
        Ok(Self { coin, assets })
    }

    /// Returns the quantity of one asset (zero when absent).
    pub fn asset_quantity(&self, asset: &AssetId) -> i128 {
        self.assets.get(asset).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pharos_core::Currency;

    fn sats(amount: i128) -> Money {
        Money::new(amount, Currency::btc())
    }

    fn ada(amount: i128) -> Money {
        // Lovelace: ADA at exponent 6.
        Money::new(
            amount,
            Currency::new("ADA", 6).unwrap_or_else(|_| panic!("valid currency")),
        )
    }

    #[test]
    fn coin_add_and_sub_respect_currency_and_overflow() -> Result<(), MoneyError> {
        let a = LedgerValue::coin_only(sats(1_000));
        let b = LedgerValue::coin_only(sats(400));
        assert_eq!(a.checked_add(&b)?.coin, sats(1_400));
        assert_eq!(a.checked_sub(&b)?.coin, sats(600));

        // Mixing coin currencies is rejected by Money's invariant.
        let mismatch = LedgerValue::coin_only(ada(400));
        assert!(matches!(
            a.checked_add(&mismatch),
            Err(MoneyError::CurrencyMismatch { .. })
        ));

        // Coin overflow surfaces as Overflow.
        let max = LedgerValue::coin_only(sats(i128::MAX));
        let one = LedgerValue::coin_only(sats(1));
        assert_eq!(max.checked_add(&one), Err(MoneyError::Overflow));
        Ok(())
    }

    #[test]
    fn assets_merge_net_and_drop_zero() -> Result<(), MoneyError> {
        let token = AssetId::new("policy1", "TOKEN");
        let other = AssetId::new("policy2", "OTHER");

        let a = LedgerValue::new(
            sats(10),
            BTreeMap::from([(token.clone(), 5), (other.clone(), 3)]),
        );
        let b = LedgerValue::new(sats(10), BTreeMap::from([(token.clone(), 2)]));

        let sum = a.checked_add(&b)?;
        assert_eq!(sum.asset_quantity(&token), 7);
        assert_eq!(sum.asset_quantity(&other), 3);

        // Subtracting an asset down to zero removes it from the bundle.
        let netted = a.checked_sub(&LedgerValue::new(
            sats(0),
            BTreeMap::from([(token.clone(), 5)]),
        ))?;
        assert_eq!(netted.asset_quantity(&token), 0);
        assert!(!netted.assets.contains_key(&token));
        // Assets can go negative (a burn deficit), like a negative Money.
        let deficit = a.checked_sub(&LedgerValue::new(
            sats(0),
            BTreeMap::from([(token.clone(), 8)]),
        ))?;
        assert_eq!(deficit.asset_quantity(&token), -3);
        Ok(())
    }

    #[test]
    fn i128_covers_satoshi_and_lovelace_supplies() {
        // Bitcoin's 21M BTC in satoshi (2.1e15) and Cardano's 45B ADA in
        // lovelace (4.5e16) both fit comfortably in i128 — no bigint needed.
        let max_satoshi: i128 = 21_000_000 * 100_000_000;
        let max_lovelace: i128 = 45_000_000_000 * 1_000_000;
        assert!(max_satoshi < i128::MAX);
        assert!(max_lovelace < i128::MAX);
        assert!(max_satoshi > i128::from(u32::MAX));
        assert!(max_lovelace > i128::from(u64::MAX) / 1_000);
    }
}
