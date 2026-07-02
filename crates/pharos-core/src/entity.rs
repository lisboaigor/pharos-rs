use std::fmt::Debug;
use std::hash::Hash;

/// Represents an entity identified by a stable identity.
pub trait Entity: Send + Sync + 'static {
    /// The entity identifier type.
    type Id: Eq + Hash + Clone + Debug + Send + Sync + 'static;

    /// Returns the entity identity.
    fn id(&self) -> &Self::Id;
}
