/// Marker trait for immutable value objects.
pub trait ValueObject: Eq + Clone + Send + Sync + 'static {}

/// Defines a validated, strongly typed value object over a single inner value.
///
/// The constructor is the **only** way to obtain an instance, so an invalid
/// value is unrepresentable — the invariant is checked exactly once, at the
/// edge:
///
/// ```
/// use pharos_core::value_object;
///
/// value_object! {
///     /// Monetary amount in cents; never negative.
///     pub Money: i64, |v| *v >= 0, "money cannot be negative";
/// }
/// value_object! {
///     /// Non-empty, trimmed product description.
///     pub Description: String, |v| !v.trim().is_empty(), "description cannot be blank";
/// }
///
/// assert!(Money::new(-1).is_err());
/// let price = Money::new(4_500)?;
/// assert_eq!(*price.value(), 4_500);
/// # Ok::<(), pharos_core::DomainError>(())
/// ```
///
/// Generates: `new` (validating), `value`, `into_inner`, `TryFrom<inner>`,
/// [`Display`](core::fmt::Display), and the [`ValueObject`](crate::ValueObject)
/// marker. The inner type must be `Eq + Ord + Hash + Display` (e.g. integers
/// and `String`; deliberately not `f64` — floats are not `Eq` and money as
/// float is a bug, not a value object).
#[macro_export]
macro_rules! value_object {
    ($($(#[$meta:meta])* $vis:vis $name:ident : $inner:ty , |$v:ident| $pred:expr , $msg:expr);+ $(;)?) => {
        $(
            $(#[$meta])*
            #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
            $vis struct $name($inner);

            impl $name {
                /// Validates the value and constructs the value object.
                $vis fn new(value: $inner) -> ::core::result::Result<Self, $crate::DomainError> {
                    let $v = &value;
                    if $pred {
                        ::core::result::Result::Ok(Self(value))
                    } else {
                        ::core::result::Result::Err($crate::DomainError::Validation(
                            ::std::string::String::from($msg),
                        ))
                    }
                }

                /// Returns a reference to the inner value.
                $vis fn value(&self) -> &$inner {
                    &self.0
                }

                /// Unwraps into the inner value.
                $vis fn into_inner(self) -> $inner {
                    self.0
                }
            }

            impl $crate::ValueObject for $name {}

            impl ::core::convert::TryFrom<$inner> for $name {
                type Error = $crate::DomainError;
                fn try_from(value: $inner) -> ::core::result::Result<Self, Self::Error> {
                    Self::new(value)
                }
            }

            impl ::core::fmt::Display for $name {
                fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                    ::core::write!(f, "{}", self.0)
                }
            }
        )+
    };
}

#[cfg(test)]
mod tests {
    value_object! {
        /// Quantity of items; at least one.
        pub Quantity: u32, |v| *v >= 1, "quantity must be at least 1";
        /// Non-empty name.
        pub Name: String, |v| !v.is_empty(), "name cannot be empty";
    }

    #[test]
    fn constructor_is_the_single_validation_point() -> Result<(), crate::DomainError> {
        assert!(Quantity::new(0).is_err());
        assert!(Name::new(String::new()).is_err());

        let quantity = Quantity::new(3)?;
        assert_eq!(*quantity.value(), 3);
        assert_eq!(quantity.into_inner(), 3);

        let name = Name::try_from("Rust book".to_string())?;
        assert_eq!(name.value(), "Rust book");
        assert_eq!(name.to_string(), "Rust book");
        assert_eq!(Name::new("x".to_string())?.into_inner(), "x");
        Ok(())
    }
}
