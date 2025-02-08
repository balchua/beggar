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
mod multipart_upload;
mod multipart_upload_part;
mod s3;
mod s3_item_detail;
mod settings;
mod storage_backend;
mod utils;

pub use self::datastore::*;
pub use self::error::*;
pub use self::multipart_upload::*;
pub use self::multipart_upload_part::*;
pub use self::s3_item_detail::*;
pub use self::settings::*;
pub use self::storage_backend::StorageBackend;
