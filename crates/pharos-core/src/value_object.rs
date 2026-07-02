/// Marker trait for immutable value objects.
pub trait ValueObject: Eq + Clone + Send + Sync + 'static {}
