//! This module provides a Snowflake connector for Rust.
//!
//! The `lib.rs` file contains the main implementation of the Snowflake connector.
//! It includes the `ClientConfig` struct, which represents the configuration for connecting to Snowflake,
//! and the `SnowflakeClient` struct, which is responsible for creating SQL statements against Snowflake.
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
//!
//! For more information, refer to the [Snowflake Connector for Rust documentation](https://docs.rs/snowflake_connector).
use jwt_simple::algorithms::RS256KeyPair;

mod bindings;
mod cells;
mod errors;
#[cfg(test)]
#[cfg(feature = "live-tests")]
mod live_tests;
mod partition;
mod statement;

pub use cells::Cell;
pub use errors::{SnowflakeError, SnowflakeResult};
pub use jwt_simple;
pub use partition::Partition;
pub use statement::{QueryResponse, Statement};

mod jwt;

#[derive(Debug, Clone)]
pub struct SnowflakeClient {
    pub key_pair: RS256KeyPair,
    pub account: String,
    pub user: String,
    pub database: String,
    pub warehouse: String,
    pub role: Option<String>,
}
impl SnowflakeClient {
    pub fn prepare(&self, sql: &str) -> Statement {
        Statement::new(sql, &self)
    }
}
