#![forbid(unsafe_code)]
#![deny(
    clippy::all, //
    clippy::cargo, //
    clippy::pedantic, //
)]
#![allow(
    clippy::wildcard_imports,
    clippy::missing_errors_doc, // TODO: docs
    clippy::let_underscore_untyped,
    clippy::module_name_repetitions,
    clippy::multiple_crate_versions, // TODO: check later
)]

#[macro_use]
mod error;

mod checksum;
mod datastore;
mod fs;
mod s3;
mod s3_item_detail;
mod settings;
mod utils;

pub use self::datastore::*;
pub use self::error::*;
pub use self::fs::StorageBackend;
pub use self::s3_item_detail::*;
pub use self::settings::*;
