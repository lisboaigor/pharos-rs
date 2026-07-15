//! Currency-aware monetary amounts in minor units.
//!
//! [`Money`] stores an [`i128`] amount of minor units (cents, satoshi, wei)
//! together with its [`Currency`]. `i128` covers crypto magnitudes — an ETH
//! balance in wei overflows `u64` — and every operation is checked: mixing
//! currencies or overflowing returns a [`MoneyError`] instead of corrupting a
//! balance. There are no floating-point conversions anywhere; money as float
//! is a bug, not a value object.

use std::fmt;

use thiserror::Error;

/// Error produced by [`Currency`] and [`Money`] operations.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum MoneyError {
    /// Two amounts of different currencies were combined.
    #[error("currency mismatch: {left} vs {right}")]
    CurrencyMismatch {
        /// Currency code on the left-hand side.
        left: String,
        /// Currency code on the right-hand side.
        right: String,
    },
    /// The operation overflowed `i128`.
    #[error("money arithmetic overflowed")]
    Overflow,
    /// The currency code or exponent is invalid.
    #[error("invalid currency: {0}")]
    InvalidCurrency(String),
    /// An allocation was requested over zero parts.
    #[error("cannot allocate money over zero parts")]
    InvalidAllocation,
}

/// A currency identified by an uppercase code and a minor-unit exponent.
///
/// The exponent is the number of decimal digits between the minor unit and
/// the display unit: `2` for BRL/USD cents, `8` for BTC satoshi, `18` for
/// ETH wei. Codes are not restricted to ISO 4217 so crypto assets and
/// exchange-specific tokens can be represented.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Currency {
    code: String,
    exponent: u8,
}

impl Currency {
    /// Maximum accepted minor-unit exponent.
    pub const MAX_EXPONENT: u8 = 38;

    /// Creates a currency from an uppercase alphanumeric code (2–12 chars)
    /// and a minor-unit exponent.
    pub fn new(code: impl Into<String>, exponent: u8) -> Result<Self, MoneyError> {
        let code = code.into();
        let valid_length = (2..=12).contains(&code.len());
        let valid_chars = code
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
        if !valid_length || !valid_chars {
            return Err(MoneyError::InvalidCurrency(format!(
                "code must be 2-12 uppercase alphanumeric characters, got {code:?}"
            )));
        }
        if exponent > Self::MAX_EXPONENT {
            return Err(MoneyError::InvalidCurrency(format!(
                "exponent must be at most {}, got {exponent}",
                Self::MAX_EXPONENT
            )));
        }
        Ok(Self { code, exponent })
    }

    /// Brazilian real, minor unit centavo (exponent 2).
    pub fn brl() -> Self {
        Self {
            code: "BRL".to_string(),
            exponent: 2,
        }
    }

    /// US dollar, minor unit cent (exponent 2).
    pub fn usd() -> Self {
        Self {
            code: "USD".to_string(),
            exponent: 2,
        }
    }

    /// Euro, minor unit cent (exponent 2).
    pub fn eur() -> Self {
        Self {
            code: "EUR".to_string(),
            exponent: 2,
        }
    }

    /// Bitcoin, minor unit satoshi (exponent 8).
    pub fn btc() -> Self {
        Self {
            code: "BTC".to_string(),
            exponent: 8,
        }
    }

    /// Ether, minor unit wei (exponent 18).
    pub fn eth() -> Self {
        Self {
            code: "ETH".to_string(),
            exponent: 18,
        }
    }

    /// Returns the currency code.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the minor-unit exponent.
    pub fn exponent(&self) -> u8 {
        self.exponent
    }
}

impl fmt::Display for Currency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.code)
    }
}

/// A monetary amount in minor units of a specific [`Currency`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Money {
    /// Minor units serialize as a decimal string: JSON numbers silently lose
    /// precision above 2^53 in non-Rust consumers, and wei amounts live there.
    #[cfg_attr(feature = "serde", serde(with = "amount_as_string"))]
    amount: i128,
    currency: Currency,
}

impl Money {
    /// Creates an amount of `amount` minor units of `currency`.
    pub fn new(amount: i128, currency: Currency) -> Self {
        Self { amount, currency }
    }

    /// Creates a zero amount of `currency`.
    pub fn zero(currency: Currency) -> Self {
        Self::new(0, currency)
    }

    /// Returns the amount in minor units.
    pub fn amount(&self) -> i128 {
        self.amount
    }

    /// Returns the currency.
    pub fn currency(&self) -> &Currency {
        &self.currency
    }

    /// Returns `true` when the amount is negative.
    pub fn is_negative(&self) -> bool {
        self.amount < 0
    }

    /// Returns `true` when the amount is zero.
    pub fn is_zero(&self) -> bool {
        self.amount == 0
    }

    /// Returns `true` when the amount is positive.
    pub fn is_positive(&self) -> bool {
        self.amount > 0
    }

    fn ensure_same_currency(&self, other: &Self) -> Result<(), MoneyError> {
        if self.currency == other.currency {
            Ok(())
        } else {
            Err(MoneyError::CurrencyMismatch {
                left: self.currency.code.clone(),
                right: other.currency.code.clone(),
            })
        }
    }

    /// Adds two amounts of the same currency.
    pub fn checked_add(&self, other: &Self) -> Result<Self, MoneyError> {
        self.ensure_same_currency(other)?;
        let amount = self
            .amount
            .checked_add(other.amount)
            .ok_or(MoneyError::Overflow)?;
        Ok(Self::new(amount, self.currency.clone()))
    }

    /// Subtracts an amount of the same currency.
    pub fn checked_sub(&self, other: &Self) -> Result<Self, MoneyError> {
        self.ensure_same_currency(other)?;
        let amount = self
            .amount
            .checked_sub(other.amount)
            .ok_or(MoneyError::Overflow)?;
        Ok(Self::new(amount, self.currency.clone()))
    }

    /// Multiplies the amount by an integer factor.
    pub fn checked_mul(&self, factor: i128) -> Result<Self, MoneyError> {
        let amount = self
            .amount
            .checked_mul(factor)
            .ok_or(MoneyError::Overflow)?;
        Ok(Self::new(amount, self.currency.clone()))
    }

    /// Negates the amount.
    pub fn neg(&self) -> Result<Self, MoneyError> {
        let amount = self.amount.checked_neg().ok_or(MoneyError::Overflow)?;
        Ok(Self::new(amount, self.currency.clone()))
    }

    /// Returns the absolute amount.
    pub fn abs(&self) -> Result<Self, MoneyError> {
        let amount = self.amount.checked_abs().ok_or(MoneyError::Overflow)?;
        Ok(Self::new(amount, self.currency.clone()))
    }

    /// Splits the amount into `parts` shares that sum exactly to the
    /// original: no minor unit is created or lost. The remainder is spread
    /// one unit at a time over the first shares, so shares differ by at most
    /// one minor unit.
    pub fn allocate(&self, parts: usize) -> Result<Vec<Self>, MoneyError> {
        if parts == 0 {
            return Err(MoneyError::InvalidAllocation);
        }
        let parts_i128 = parts as i128;
        let base = self.amount.div_euclid(parts_i128);
        let remainder = self.amount.rem_euclid(parts_i128);
        Ok((0..parts_i128)
            .map(|index| {
                let extra = i128::from(index < remainder);
                Self::new(base + extra, self.currency.clone())
            })
            .collect())
    }
}

impl crate::ValueObject for Money {}
impl crate::ValueObject for Currency {}

impl fmt::Display for Money {
    /// Formats as a plain decimal with the currency code, e.g. `-3.50 BRL`,
    /// using the currency exponent. Purely decimal string math; no floats.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exponent = usize::from(self.currency.exponent);
        let sign = if self.amount < 0 { "-" } else { "" };
        let digits = self.amount.unsigned_abs().to_string();
        if exponent == 0 {
            return write!(f, "{sign}{digits} {}", self.currency);
        }
        let padded = format!("{digits:0>width$}", width = exponent + 1);
        let (whole, frac) = padded.split_at(padded.len() - exponent);
        write!(f, "{sign}{whole}.{frac} {}", self.currency)
    }
}

#[cfg(feature = "serde")]
mod amount_as_string {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(amount: &i128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&amount.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i128, D::Error> {
        let raw = String::deserialize(deserializer)?;
        raw.parse()
            .map_err(|_| D::Error::custom(format!("invalid money amount {raw:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn currency_codes_are_validated() {
        assert!(Currency::new("BRL", 2).is_ok());
        assert!(Currency::new("USDC0", 6).is_ok());
        assert!(Currency::new("b", 2).is_err()); // lowercase and too short
        assert!(Currency::new("TOOLONGCURRENCY", 2).is_err());
        assert!(Currency::new("BRL", 39).is_err());
    }

    #[test]
    fn checked_arithmetic_rejects_mismatch_and_overflow() -> Result<(), MoneyError> {
        let a = Money::new(1_000, Currency::brl());
        let b = Money::new(500, Currency::brl());
        assert_eq!(a.checked_add(&b)?.amount(), 1_500);
        assert_eq!(a.checked_sub(&b)?.amount(), 500);
        assert_eq!(b.checked_mul(3)?.amount(), 1_500);

        let usd = Money::new(500, Currency::usd());
        assert_eq!(
            a.checked_add(&usd),
            Err(MoneyError::CurrencyMismatch {
                left: "BRL".to_string(),
                right: "USD".to_string(),
            })
        );

        let max = Money::new(i128::MAX, Currency::brl());
        let one = Money::new(1, Currency::brl());
        assert_eq!(max.checked_add(&one), Err(MoneyError::Overflow));
        assert_eq!(max.checked_mul(2), Err(MoneyError::Overflow));
        assert_eq!(
            Money::new(i128::MIN, Currency::brl()).neg(),
            Err(MoneyError::Overflow)
        );
        Ok(())
    }

    #[test]
    fn covers_wei_magnitudes_beyond_u64() -> Result<(), MoneyError> {
        // 1 billion ETH in wei: far above u64::MAX.
        let wei = 1_000_000_000_i128 * 10_i128.pow(18);
        assert!(wei > i128::from(u64::MAX));
        let balance = Money::new(wei, Currency::eth());
        let doubled = balance.checked_mul(2)?;
        assert_eq!(doubled.checked_sub(&balance)?, balance);
        assert_eq!(balance.to_string(), "1000000000.000000000000000000 ETH");
        Ok(())
    }

    #[test]
    fn allocate_never_creates_or_loses_units() -> Result<(), MoneyError> {
        let total = Money::new(1_001, Currency::brl());
        let shares = total.allocate(3)?;
        assert_eq!(
            shares.iter().map(Money::amount).collect::<Vec<_>>(),
            vec![334, 334, 333]
        );
        assert_eq!(shares.iter().map(Money::amount).sum::<i128>(), 1_001);

        // Negative amounts allocate without losing units either.
        let debt = Money::new(-10, Currency::brl());
        let shares = debt.allocate(3)?;
        assert_eq!(shares.iter().map(Money::amount).sum::<i128>(), -10);

        assert_eq!(total.allocate(0), Err(MoneyError::InvalidAllocation));
        Ok(())
    }

    #[test]
    fn signs_and_display() -> Result<(), MoneyError> {
        let money = Money::new(-350, Currency::brl());
        assert!(money.is_negative());
        assert!(!money.is_zero());
        assert_eq!(money.abs()?.amount(), 350);
        assert_eq!(money.neg()?.amount(), 350);
        assert_eq!(money.to_string(), "-3.50 BRL");
        assert_eq!(Money::new(5, Currency::brl()).to_string(), "0.05 BRL");
        assert_eq!(Money::new(7, Currency::new("JPY", 0)?).to_string(), "7 JPY");
        assert!(Money::zero(Currency::usd()).is_zero());
        Ok(())
    }

    #[cfg(feature = "serde")]
    #[test]
    fn serde_roundtrips_amount_as_string() -> Result<(), Box<dyn std::error::Error>> {
        let wei = 1_000_000_000_i128 * 10_i128.pow(18);
        let money = Money::new(wei, Currency::eth());
        let json = serde_json::to_string(&money)?;
        assert!(json.contains(&format!("\"{wei}\"")));
        let back: Money = serde_json::from_str(&json)?;
        assert_eq!(back, money);

        let invalid = r#"{"amount":"not-a-number","currency":{"code":"BRL","exponent":2}}"#;
        assert!(serde_json::from_str::<Money>(invalid).is_err());
        Ok(())
    }
}
