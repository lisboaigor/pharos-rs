//! Lossless integer allocation, factored out of [`Money::allocate`](crate::Money::allocate).
//!
//! [`Money`](crate::Money) is one instance of a general pattern — an integer
//! quantity in minor units, split into `parts` shares that sum back to the
//! original — not a special case unique to currency. Any domain built on the
//! same shape (a dimensional quantity, a share count, a token allocation)
//! needs the identical algorithm, and the subtle part is the part most often
//! gotten wrong: using `/` and `%` instead of [`i128::div_euclid`]/
//! [`i128::rem_euclid`] breaks silently for negative totals. This function is
//! that algorithm, free of any currency- or domain-specific type.

/// Errors from [`allocate_minor_units`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AllocationError {
    /// An allocation was requested over zero parts.
    #[error("cannot allocate over zero parts")]
    InvalidAllocation,
    /// An allocation was requested over more parts than the caller's limit.
    #[error("cannot allocate over {parts} parts, over the {max}-part limit")]
    TooManyParts {
        /// The requested part count.
        parts: usize,
        /// The caller-supplied ceiling that was exceeded.
        max: usize,
    },
}

/// Splits `total` into `parts` shares that sum exactly back to `total`: no
/// unit is created or lost. The remainder is spread one unit at a time over
/// the first shares, so shares differ by at most one unit. Correct for
/// negative `total` (uses [`i128::div_euclid`]/[`i128::rem_euclid`], not
/// `/`/`%`).
///
/// `max_parts` guards against an unbounded allocation: `parts` routinely
/// comes straight from request input (an installment count, a payee count)
/// with no natural upper bound of its own, and this function builds one
/// entry per part — a caller-supplied `parts` in the millions turns an
/// ordinary field into a multi-gigabyte allocation, and an allocation
/// failure in Rust aborts the process rather than returning an error. Pass a
/// ceiling appropriate to your domain (ten thousand comfortably covers most
/// legitimate uses).
///
/// Rejects `parts == 0` and `parts > max_parts`.
pub fn allocate_minor_units(
    total: i128,
    parts: usize,
    max_parts: usize,
) -> Result<Vec<i128>, AllocationError> {
    if parts == 0 {
        return Err(AllocationError::InvalidAllocation);
    }
    if parts > max_parts {
        return Err(AllocationError::TooManyParts {
            parts,
            max: max_parts,
        });
    }
    let parts_i128 = parts as i128;
    let base = total.div_euclid(parts_i128);
    let remainder = total.rem_euclid(parts_i128);
    Ok((0..parts_i128)
        .map(|index| base + i128::from(index < remainder))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_evenly_when_divisible() -> Result<(), AllocationError> {
        assert_eq!(allocate_minor_units(100, 4, 10_000)?, vec![25, 25, 25, 25]);
        Ok(())
    }

    #[test]
    fn spreads_the_remainder_over_the_first_shares() -> Result<(), AllocationError> {
        assert_eq!(allocate_minor_units(10, 3, 10_000)?, vec![4, 3, 3]);
        Ok(())
    }

    #[test]
    fn sums_back_to_the_original_for_negative_totals() -> Result<(), AllocationError> {
        let shares = allocate_minor_units(-10, 3, 10_000)?;
        assert_eq!(shares.iter().sum::<i128>(), -10);
        Ok(())
    }

    #[test]
    fn rejects_zero_parts() {
        assert_eq!(
            allocate_minor_units(100, 0, 10_000),
            Err(AllocationError::InvalidAllocation)
        );
    }

    #[test]
    fn rejects_parts_over_the_caller_supplied_ceiling() {
        assert_eq!(
            allocate_minor_units(100, 11, 10),
            Err(AllocationError::TooManyParts { parts: 11, max: 10 })
        );
    }
}
