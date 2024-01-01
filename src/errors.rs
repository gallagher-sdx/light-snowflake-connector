/// Error types for the Snowflake client
#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    /// An error occurred while signing a request
    #[error(transparent)]
    Token(#[from] jwt_simple::Error),
    /// An error occurred while sending a request
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    /// Snowflake returned an error
    #[error("Snowflake server error: {code}: {message}")]
    ServerError { code: String, message: String },
    /// An error occurred while parsing JSON (these may also appear wrapped in Request errors)
    #[error(transparent)]
    JSONError(#[from] serde_json::Error),
    /// A certain feature (like a data type) is not supported (yet)
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(&'static str),
    /// The response contains multiple partitions, and you specified that you only want one
    #[error("Response contains multiple partitions")]
    MultiplePartitions,
    /// There was a problem constructing the client
    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
}

pub type SnowflakeResult<T> = Result<T, SnowflakeError>;

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
pub(crate) enum SnowflakeWireResult<T> {
    Ok(T),
    Error { code: String, message: String },
}

impl<T> SnowflakeWireResult<T> {
    /// Convert from the custom wire format to a standard result
    pub fn into_result(self) -> SnowflakeResult<T> {
        match self {
            SnowflakeWireResult::Error { code, message } => {
                Err(SnowflakeError::ServerError { code, message })
            }
            SnowflakeWireResult::Ok(t) => Ok(t),
        }
    }
}
