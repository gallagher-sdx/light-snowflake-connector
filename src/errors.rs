use serde::{de::DeserializeOwned, Deserialize};

#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error(transparent)]
    Token(#[from] jwt_simple::Error),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("Snowflake server error: {code}: {message}")]
    ServerError { code: String, message: String },
    #[error(transparent)]
    JSONError(#[from] serde_json::Error),
    #[error("Unsupported feature: {0}")]
    UnsupportedFeature(&'static str),
    #[error("Response contains multiple partitions")]
    MultiplePartitions,
    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
}

pub type SnowflakeResult<T> = Result<T, SnowflakeError>;

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
pub enum SnowflakeWireResult<T> {
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
