#[derive(thiserror::Error, Debug)]
pub enum SnowflakeError {
    #[error(transparent)]
    Token(#[from] jwt_simple::Error),
    #[error(transparent)]
    Request(#[from] reqwest::Error),
    #[error("Snowflake returned an error: {0}")]
    ServerError(String),
    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
}

pub type SnowflakeResult<T> = Result<T, SnowflakeError>;
