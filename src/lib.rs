use std::collections::HashMap;

use bindings::Binding;
use data_manipulation::DataManipulationResult;
pub use errors::{SnowflakeError, SnowflakeResult};
use jwt_simple::algorithms::RS256KeyPair;
use reqwest::header::{HeaderMap, ACCEPT, AUTHORIZATION, CONTENT_TYPE, USER_AGENT};
use response::QueryResponse;
use serde::Serialize;

pub mod bindings;
pub mod cells;
pub mod data_manipulation;
pub mod errors;
#[cfg(test)]
//#[cfg(feature = "live-tests")]
mod live_tests;
pub mod response;

mod jwt;

#[derive(Debug)]
pub struct ClientConfig {
    pub key_pair: RS256KeyPair,
    pub account: String,
    pub user: String,
    pub database: String,
    pub warehouse: String,
    pub role: Option<String>,
}
impl ClientConfig {
    pub fn build(self) -> SnowflakeResult<Client> {
        let token = jwt::create_token(
            &self.key_pair,
            &self.account.to_ascii_uppercase(),
            &self.user.to_ascii_uppercase(),
        )?;

        let headers = Self::get_headers(&token)?;
        Ok(Client {
            inner: reqwest::Client::builder()
                .default_headers(headers)
                .build()?,
            config: self,
        })
    }

    fn get_headers(token: &str) -> SnowflakeResult<HeaderMap> {
        let mut headers = HeaderMap::with_capacity(5);
        headers.append(CONTENT_TYPE, "application/json".parse()?);
        headers.append(AUTHORIZATION, format!("Bearer {}", token).parse()?);
        headers.append(
            "X-Snowflake-Authorization-Token-Type",
            "KEYPAIR_JWT".parse()?,
        );
        headers.append(ACCEPT, "application/json".parse()?);
        headers.append(
            USER_AGENT,
            concat!(env!("CARGO_PKG_NAME"), '/', env!("CARGO_PKG_VERSION")).parse()?,
        );
        Ok(headers)
    }
}

#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    config: ClientConfig,
}
impl Client {
    pub fn prepare(&self, statement: &str) -> Result<Statement, SnowflakeError> {
        Ok(Statement {
            client: self.inner.clone(),
            host: format!(
                "https://{}.snowflakecomputing.com/api/v2/",
                self.config.account.to_ascii_lowercase(),
            ),
            wire: WireStatement {
                statement: statement.to_owned(),
                timeout: None,
                database: self.config.database.to_ascii_uppercase(),
                warehouse: self.config.warehouse.to_ascii_uppercase(),
                role: self.config.role.as_ref().map(|x| x.to_ascii_uppercase()),
                bindings: HashMap::new(),
            },
            uuid: uuid::Uuid::new_v4(),
        })
    }
}

#[derive(Debug)]
pub struct Statement {
    client: reqwest::Client,
    host: String,
    wire: WireStatement,
    uuid: uuid::Uuid,
}

impl Statement {
    pub async fn raw(self) -> Result<String, SnowflakeError> {
        log::debug!(
            "Sending statement: {}",
            serde_json::to_string_pretty(&self.wire).unwrap()
        );
        Ok(self
            .client
            .post(self.get_url())
            .json(&self.wire)
            .send()
            .await?
            .text()
            .await?)
    }
    pub async fn query(self) -> Result<QueryResponse, SnowflakeError> {
        let raw = self.raw().await?;
        serde_json::from_str(&raw)
            .map_err(|e| SnowflakeError::ServerError(format!("{}: {}", e, raw)))
    }
    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(self) -> Result<DataManipulationResult, SnowflakeError> {
        let raw = self.raw().await?;
        serde_json::from_str(&raw)
            .map_err(|e| SnowflakeError::ServerError(format!("{}: {}", e, raw)))
    }
    pub fn with_timeout(mut self, timeout: u32) -> Statement {
        self.wire.timeout = Some(timeout);
        self
    }
    pub fn add_binding<T: Into<Binding>>(mut self, value: T) -> Statement {
        let bindings = &mut self.wire.bindings;
        bindings.insert((bindings.len() + 1).to_string(), value.into());
        self
    }
    fn get_url(&self) -> String {
        // TODO: make another return type that allows retrying by calling same statement again with retry flag!
        format!(
            "{}statements?nullable=true&requestId={}",
            self.host, self.uuid
        )
    }
}

#[derive(Serialize, Debug)]
struct WireStatement {
    statement: String,
    timeout: Option<u32>,
    database: String,
    warehouse: String,
    role: Option<String>,
    bindings: HashMap<String, Binding>,
}

#[cfg(test)]
mod tests {
    use crate::errors::SnowflakeResult;

    use super::*;

    #[test]
    fn sql() -> SnowflakeResult<()> {
        let key_pair = RS256KeyPair::generate(2048)?;
        let sql = ClientConfig {
            key_pair,
            account: "ACCOUNT".into(),
            user: "USER".into(),
            database: "DB".into(),
            warehouse: "WH".into(),
            role: Some("ROLE".into()),
        }
        .build()?;
        let sql = sql
            .prepare("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")?
            .add_binding(69);
        assert_eq!(sql.wire.bindings.len(), 1);
        let sql = sql.add_binding("JoMama");
        assert_eq!(sql.wire.bindings.len(), 2);
        Ok(())
    }
}
