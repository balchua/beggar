use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::StdError;

use std::fmt;
use std::panic::Location;

use tracing::error;

#[derive(Debug)]
pub struct Error {
    source: StdError,
}

pub type Result<T = (), E = Error> = std::result::Result<T, E>;

impl Error {
    #[must_use]
    #[track_caller]
    pub fn new(source: StdError) -> Self {
        log(&*source);
        Self { source }
    }

    #[must_use]
    #[track_caller]
    pub fn from_string(s: impl Into<String>) -> Self {
        Self::new(s.into().into())
    }

    /// Access the inner error source
    #[must_use]
    pub fn source(&self) -> &StdError {
        &self.source
    }
}

// Add Display implementation for Error
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.source)
    }
}

// Implement std::error::Error for our custom Error
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

// Fix the problematic From implementation to avoid conflicts
// Instead of generic implementation, we'll add specific ones for common error types

// Convert io::Error to our Error type
impl From<std::io::Error> for Error {
    #[track_caller]
    fn from(err: std::io::Error) -> Self {
        Self::new(Box::new(err))
    }
}

// Convert sqlx errors to our Error type
impl From<sqlx::Error> for Error {
    #[track_caller]
    fn from(err: sqlx::Error) -> Self {
        Self::new(Box::new(err))
    }
}

// Convert string errors to our Error type
impl From<String> for Error {
    #[track_caller]
    fn from(s: String) -> Self {
        Self::from_string(s)
    }
}

// Convert &str errors to our Error type
impl From<&str> for Error {
    #[track_caller]
    fn from(s: &str) -> Self {
        Self::from_string(s)
    }
}

// Allow wrapping Box<dyn Error> directly
impl From<Box<dyn std::error::Error + Send + Sync>> for Error {
    #[track_caller]
    fn from(source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::new(source)
    }
}

// Convert sqlx MIgrateError to our Error type
impl From<sqlx::migrate::MigrateError> for Error {
    #[track_caller]
    fn from(err: sqlx::migrate::MigrateError) -> Self {
        Self::new(Box::new(err))
    }
}

// Convert serde_json errors to our Error type
impl From<serde_json::Error> for Error {
    #[track_caller]
    fn from(err: serde_json::Error) -> Self {
        Self::new(Box::new(err))
    }
}

// Make a helper function for other error types that don't have specific implementations
#[track_caller]
pub fn map_err<E>(err: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    Error::new(Box::new(err))
}

impl From<Error> for S3Error {
    fn from(e: Error) -> Self {
        S3Error::with_source(S3ErrorCode::InternalError, e.source)
    }
}

#[inline]
#[track_caller]
pub(crate) fn log(source: &dyn std::error::Error) {
    if cfg!(feature = "binary") {
        let location = Location::caller();
        let span_trace = tracing_error::SpanTrace::capture();

        error!(
            target: "s3s_fs_internal_error",
            %location,
            error=%source,
            "span trace:\n{span_trace}"
        );
    }
}

/// Helper macro for working with S3 errors.
/// Transforms errors into S3-compatible errors.
#[macro_export]
macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                $crate::error::log(&err);
                return Err(::s3s::S3Error::internal_error(err));
            }
        }
    };
}

/// Helper macro to return an early error with proper logging
#[macro_export]
macro_rules! bail {
    ($msg:expr) => {
        return Err($crate::error::Error::from_string($msg))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err($crate::error::Error::from_string(format!($fmt, $($arg)*)))
    };
}

/// Helper macro to convert errors that don't have From implementations
#[macro_export]
macro_rules! map_err {
    ($result:expr) => {
        $result.map_err($crate::error::map_err)
    };
}
