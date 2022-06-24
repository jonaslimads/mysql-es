#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(clippy::all)]
// #![warn(clippy::pedantic)]
//! # mysql-es
//!
//! > A MySql implementation of the `EventStore` trait in [cqrs-es](https://crates.io/crates/cqrs-es).
//!
pub use crate::cqrs::*;
pub use crate::error::*;
pub use crate::event_repository::*;
pub use crate::event_repository::*;
pub use crate::sql_query::*;
pub use crate::types::*;
pub use crate::view_repository::*;

mod cqrs;
mod error;
mod event_repository;
pub(crate) mod sql_query;
mod testing;
mod types;
mod view_repository;
