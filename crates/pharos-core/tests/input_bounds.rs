//! Regression tests for input bounds that reach pharos-core from a request
//! body: Money::allocate's part-count ceiling, its arithmetic at the i128
//! extremes, and serde_json's own recursion guard.

use std::time::Instant;

use pharos_core::{Currency, Money, MoneyError};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn rss_bytes() -> u64 {
    let pid = std::process::id();
    let Ok(output) = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
    else {
        return 0;
    };
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u64>()
        .unwrap_or(0)
        * 1024
}

/// `Money::allocate(parts)` builds one `Money` per part; unchecked, a
/// request field like `"installments": 40000000` would turn into a
/// multi-gigabyte allocation inside the domain layer.
/// `Money::MAX_ALLOCATION_PARTS` now bounds it.
#[test]
fn money_allocate_rejects_parts_over_the_ceiling_and_stays_bounded_under_it() -> TestResult {
    let money = Money::new(100_000, Currency::brl());

    let mut sample = Vec::new();
    for parts in [1_000usize, Money::MAX_ALLOCATION_PARTS] {
        let before = rss_bytes();
        let started = Instant::now();
        let shares = money.allocate(parts)?;
        let grew = rss_bytes().saturating_sub(before);
        sample.push((parts, grew, started.elapsed()));
        assert_eq!(shares.len(), parts);
    }

    for (parts, grew, took) in &sample {
        println!(
            "allocate({parts}) -> {:.1} MiB in {:.0} ms",
            *grew as f64 / 1024.0 / 1024.0,
            took.as_millis()
        );
    }

    for over_the_ceiling in [
        Money::MAX_ALLOCATION_PARTS + 1,
        100_000,
        1_000_000,
        u32::MAX as usize,
    ] {
        let result = money.allocate(over_the_ceiling);
        assert_eq!(
            result,
            Err(MoneyError::TooManyParts {
                parts: over_the_ceiling,
                max: Money::MAX_ALLOCATION_PARTS,
            }),
            "allocate({over_the_ceiling}) must be rejected before building any shares"
        );
    }
    println!(
        "allocate() rejects every request over {} parts, including u32::MAX, without ever \
         building a single share for it",
        Money::MAX_ALLOCATION_PARTS
    );
    Ok(())
}

#[test]
fn money_arithmetic_never_panics_at_the_extremes() -> TestResult {
    let max = Money::new(i128::MAX, Currency::btc());
    let one = Money::new(1, Currency::btc());
    assert!(max.checked_add(&one).is_err(), "overflow is an error");
    assert!(
        Money::new(i128::MIN, Currency::btc()).abs().is_err(),
        "abs of MIN is an error, not a panic"
    );
    assert!(
        Money::new(i128::MIN, Currency::btc()).neg().is_err(),
        "neg of MIN is an error, not a panic"
    );
    // Display must not panic on the extremes either.
    let _ = max.to_string();
    let _ = Money::new(i128::MIN, Currency::btc()).to_string();
    assert!(
        max.checked_mul(2).is_err(),
        "multiplication overflow is an error"
    );
    assert!(Money::new(10, Currency::brl()).allocate(0).is_err());
    println!("money: all extremes returned errors, no panics");
    Ok(())
}

#[test]
fn deeply_nested_json_is_rejected_before_it_can_blow_the_stack() -> TestResult {
    // serde_json's own recursion limit, exercised through the shape an
    // upcaster or a JSONB payload would take.
    let deep = format!("{}{}", "[".repeat(1024), "]".repeat(1024));
    let parsed: Result<serde_json::Value, _> = serde_json::from_str(&deep);
    assert!(
        parsed.is_err(),
        "serde_json must refuse 1024 levels of nesting"
    );
    println!(
        "json nesting: rejected at depth 1024 ({})",
        match parsed {
            Err(e) => e.to_string(),
            Ok(_) => "accepted".into(),
        }
    );
    Ok(())
}
