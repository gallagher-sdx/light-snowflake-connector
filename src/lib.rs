//! A lightweight Snowflake connector for Rust.
//!
//! Example usage:
//!
//! ```rust,no_run
//! use light_snowflake_connector::{Cell, SnowflakeClient, SnowflakeError};
//! use light_snowflake_connector::jwt_simple::algorithms::RS256KeyPair;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), SnowflakeError> {
//!     let key_pair = RS256KeyPair::generate(2048)?;
//!     let config = SnowflakeClient {
//!         key_pair,
//!         account: "ACCOUNT".into(),
//!         user: "USER".into(),
//!         database: "DB".into(),
//!         warehouse: "WH".into(),
//!         role: Some("ROLE".into()),
//!     };
//!
//!     let result = config
//!         .prepare("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")
//!         .add_binding(10)
//!         .add_binding("Henry")
//!         .query()
//!         .await?;
//!
//!     // Get the first partition of the result, and assert that there is only one partition
//!     let partition = result.only_partition()?;
//!     
//!     // Get the results as a Vec<Vec<Cell>>, which is a tagged enum similar to serde_json::Value
//!     let cells = partition.cells();
//!     match &cells[0][0] {
//!         Cell::Int(x) => println!("Got an integer: {}", x),
//!         Cell::Varchar(x) => println!("Got a string: {}", x),
//!         _ => panic!("Got something else"),
//!     }
//!
//!     // Get the results as a Vec<Vec<serde_json::Value>>, which is a list of lists of JSON values
//!     let json_table = partition.json_table();
//!
//!     // Get the results as a Vec<serde_json::Value>, which is a list of JSON objects
//!     let json_objects = partition.json_objects();
//!
//!     Ok(())
//! }
//! ```
use jwt_simple::algorithms::RS256KeyPair;

mod bindings;
mod cells;
mod errors;
#[cfg(test)]
#[cfg(feature = "live-tests")]
mod live_tests;
mod partition;
mod statement;

pub use cells::{Cell, RawCell};
pub use errors::{SnowflakeError, SnowflakeResult};
pub use jwt_simple;
pub use partition::Partition;
pub use statement::{Changes, QueryResponse, Statement};

mod jwt;

/// Configuration for making connections to Snowflake
#[derive(Debug, Clone)]
pub struct SnowflakeClient {
    /// The RSA key pair used to sign the JWT.
    ///
    /// There are many ways to generate or load this key pair depending on your deployment.
    /// * You can generate one with [`jwt_simple::algorithms::RS256KeyPair::generate`]
    /// * You can load one from a PEM file with [`jwt_simple::algorithms::RS256KeyPair::from_pem`]
    /// * You can load one from a DER file with [`jwt_simple::algorithms::RS256KeyPair::from_der`]
    /// * In turn you might combine any of these with volume mounts, PVCs, Vault, Secrets Manager, etc.
    pub key_pair: RS256KeyPair,
    /// The Snowflake account name. This should be two parts separated by a dot,
    /// and it might look like `AAA00000.us-east-1`
    pub account: String,
    /// The Snowflake user name.
    pub user: String,
    /// The Snowflake database name. (This is required and it cannot be `""`)
    pub database: String,
    /// The Snowflake warehouse name. (This is required and it cannot be `""`)
    pub warehouse: String,
    /// The Snowflake role name. This is optional only if you have configured your user
    /// to have a default role.
    pub role: Option<String>,
}
impl SnowflakeClient {
    /// Prepare a SQL statement for execution
    ///
    /// This does not send anything to Snowflake and it's infallible because it does not
    /// interact with the network or test that the SQL is valid.
    pub fn prepare(&self, sql: &str) -> Statement {
        Statement::new(sql, self)
    }
}
