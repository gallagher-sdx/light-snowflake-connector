use std::collections::HashMap;
use std::sync::Arc;

use futures::{StreamExt, TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::bindings::Binding;
use crate::cells::{Cell, RawCell};
use crate::errors::{SnowflakeError, SnowflakeResult, SnowflakeWireResult};
use crate::partition::{Partition, StringTable};
use crate::{jwt, SnowflakeClient};

/// A builder for a prepared statement (created by SnowflakeClient)
///
#[derive(Debug, Clone)]
pub struct Statement {
    host: String,
    wire: WireStatement,
    uuid: uuid::Uuid,
    config: SnowflakeClient,
}

impl Statement {
    /// Create a new statement from a SQL string and a SnowflakeClient
    ///
    /// Usually you will want to use [`SnowflakeClient::prepare`] instead of this method
    /// but the difference is merely ergonomic.
    pub fn new(sql: &str, config: &crate::SnowflakeClient) -> Statement {
        Statement {
            host: format!(
                "https://{}.snowflakecomputing.com",
                config.account.to_ascii_lowercase(),
            ),
            wire: WireStatement {
                statement: sql.to_owned(),
                timeout: Some(30),
                database: config.database.to_ascii_uppercase(),
                warehouse: config.warehouse.to_ascii_uppercase(),
                role: config.role.as_ref().map(|x| x.to_ascii_uppercase()),
                bindings: HashMap::new(),
            },
            uuid: uuid::Uuid::new_v4(),
            config: config.to_owned(),
        }
    }

    pub(crate) fn client(&self) -> SnowflakeResult<reqwest::Client> {
        use reqwest::header::*;
        let token = jwt::create_token(
            &self.config.key_pair,
            &self.config.account.to_ascii_uppercase(),
            &self.config.user.to_ascii_uppercase(),
        )?;

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

        Ok(reqwest::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(
                self.wire.timeout.unwrap_or(30) + 15,
            ))
            .build()?)
    }

    async fn send(&self) -> Result<reqwest::Response, SnowflakeError> {
        log::debug!(
            "Sending statement: {}",
            serde_json::to_string_pretty(&self.wire).unwrap()
        );
        Ok(self
            .client()?
            .post(format!(
                "{}/api/v2/statements?nullable=true&requestId={}",
                self.host, self.uuid
            ))
            .json(&self.wire)
            .send()
            .await?)
    }

    /// Execute SQL that returns a result set
    ///
    /// This supports multiple partitions, which are streamed lazily
    /// but the first partition is buffered immediately.
    ///
    /// For a single partition, consider using [`QueryResponse::only_partition`].
    pub async fn query(&self) -> Result<QueryResponse, SnowflakeError> {
        Ok(self
            .send()
            .await?
            .json::<SnowflakeWireResult<WireQueryResponse>>()
            .await?
            .into_result()?
            .hydrate(self.clone()))
    }

    /// Execute SQL that does not return a result set
    ///
    /// This is useful for DML statements like `INSERT`, `UPDATE`, and `DELETE`
    pub async fn manipulate(&self) -> Result<Changes, SnowflakeError> {
        let dml_reslt = self
            .send()
            .await?
            .json::<SnowflakeWireResult<WireDMLResult>>()
            .await?
            .into_result()?;
        Ok(Changes {
            message: dml_reslt.message,
            rows_inserted: dml_reslt.stats.rows_inserted,
            rows_deleted: dml_reslt.stats.rows_deleted,
            rows_updated: dml_reslt.stats.rows_updated,
            duplicates: dml_reslt.stats.duplicates,
        })
    }

    /// Set the Snowflake-side timeout for the statement
    ///
    /// The client-side timeout will automatically be set to this value plus 15 seconds
    ///
    /// The default server side timeout is 172800 seconds (2 days),
    /// which is far too long for the use cases this library is targeting,
    /// so this library defaults to 30 seconds on the server side if not specified,
    /// implying a client-side timeout of 45 seconds.
    pub fn with_timeout(mut self, timeout_seconds: u64) -> Statement {
        self.wire.timeout = Some(timeout_seconds);
        self
    }
    /// Add a binding to the statement
    ///
    /// Several types are supported:
    ///
    /// * All integers are converted to `i128` and bound as `NUMBER`
    /// * `f64` and `f32` are bound as `REAL`
    /// * `bool`, `&str`, `String`, `chrono::NaiveDate`, `chrono::NaiveDateTime`, and `chrono::NaiveTime` are bound as `TEXT`
    ///
    /// More types may be supported in the future.
    ///
    /// Text is the most flexible type, and for additional types you can usually workaround by
    /// converting to text before binding. Or, you could contribute to this library and add support
    pub fn add_binding<T: Into<Binding>>(mut self, value: T) -> Statement {
        let bindings = &mut self.wire.bindings;
        bindings.insert((bindings.len() + 1).to_string(), value.into());
        self
    }
}

/// The result of SQL that returns rows
///
/// The first partition is included immediately,
/// but additional partitions are streamed lazily and incur additional IO.
///
/// You might consider using [`QueryResponse::only_partition`] if you only need one partition.
#[derive(Debug)]
pub struct QueryResponse {
    result_set_meta_data: WireStatementMetaData,
    data: Arc<StringTable>,
    statement_status_url: String,
    statement: Statement,
}

/// The result of a DML statement
///
/// These are returned by [`Statement::manipulate`] and are almost exactly
/// the same as the response from Snowflake.
#[derive(Debug)]
pub struct Changes {
    pub message: String,
    pub rows_inserted: usize,
    pub rows_deleted: usize,
    pub rows_updated: usize,
    pub duplicates: usize,
}

impl QueryResponse {
    /// Get the number of rows across all partitions
    pub fn num_rows(&self) -> usize {
        self.result_set_meta_data.num_rows
    }

    /// Get the number of columns
    pub fn num_columns(&self) -> usize {
        self.result_set_meta_data.row_type.len()
    }

    /// Get the number of columns in the response
    pub fn num_partitions(&self) -> usize {
        self.result_set_meta_data.partition_info.len()
    }

    /// Column types in the result set
    ///
    /// In most cases Cell should already expose the data you need,
    /// but if you use the raw strings or want information about nullability, etc,
    /// this can be useful.
    pub fn column_types(&self) -> &[ColumnType] {
        &self.result_set_meta_data.row_type
    }

    /// A convenience method to assert that there is only one partition and return it
    ///
    /// This never causes IO, is not async, and can only error with [`SnowflakeError::MultiplePartitions`]
    pub fn only_partition(self) -> SnowflakeResult<Partition> {
        if self.num_partitions() != 1 {
            Err(SnowflakeError::MultiplePartitions)
        } else {
            Ok(Partition {
                index: 0,
                meta_data: self.result_set_meta_data.clone(),
                data: self.data.clone(),
            })
        }
    }

    /// Get a single partition from the response
    ///
    /// If this is the first partition, you get it immediately,
    /// otherwise it will incur an additional request to get the partition
    ///
    /// Returns an error if the requested partition does not exist.
    pub async fn partition(&self, index: usize) -> SnowflakeResult<Option<Partition>> {
        if index == 0 {
            Ok(Some(Partition {
                index,
                meta_data: self.result_set_meta_data.clone(),
                data: self.data.clone(),
            }))
        } else if index >= self.num_partitions() {
            Ok(None)
        } else {
            let url =
                self.statement.host.trim_end_matches('/').to_owned() + &self.statement_status_url;
            let response = self
                .statement
                .client()?
                .get(&url)
                .query(&[("partition", index)])
                .header("Accept", "application/json")
                .send()
                .await?
                .json::<SnowflakeWireResult<WirePartitionResponse>>()
                .await?
                .into_result()?;

            Ok(Some(Partition {
                index,
                meta_data: self.result_set_meta_data.clone(),
                data: response.data,
            }))
        }
    }

    /// Stream over all partitions in the response
    ///
    /// This incurs IO, so try to only use this once.
    ///
    /// In order to improve concurrency, this will buffer one partition,
    /// so you can have one partition in flight while processing another.
    pub fn partitions(&self) -> impl TryStream<Ok = Partition, Error = SnowflakeError> + '_ {
        let partition_futures = (0..self.num_partitions()).map(|index| self.partition(index));
        futures::stream::iter(partition_futures)
            .buffered(1)
            .then(move |partition| async move {
                // We can't be out of bounds, so remove the Option
                partition.map(|opt| opt.unwrap())
            })
    }

    /// Concatenate all partitions into a single partition
    ///
    /// This incurs IO, so try to only use this once.
    ///
    /// This could use an unbounded amount of memory,
    /// but it could save time for uses cases requiring multiple passes.
    pub async fn concat_partitions(&self) -> SnowflakeResult<Partition> {
        let mut cells = Vec::with_capacity(self.num_rows());
        for partition in self.partitions().try_collect::<Vec<_>>().await? {
            // TODO: This could save a clone when Arc::unwrap_or_clone is stable
            cells.extend(partition.data.iter().cloned());
        }
        Ok(Partition {
            index: 0,
            meta_data: self.result_set_meta_data.clone(),
            data: Arc::new(cells),
        })
    }

    /// Stream over all rows in the response
    ///
    /// This incurs IO, so try to only use this once.
    ///
    /// In order to improve concurrency, this will buffer one partition,
    /// so you can have one partition in flight while processing another.
    ///
    /// If you only need one partition, it may be simpler to use `partition`
    /// and then stream over the rows in that partition.
    pub fn rows(&self) -> impl TryStream<Ok = Vec<Cell>, Error = SnowflakeError> + '_ {
        self.partitions()
            .map_ok(|partition| futures::stream::iter(partition.cells()).map(Ok))
            .try_flatten()
    }

    /// Stream over all rows in the response as JSON tables
    ///
    /// This incurs IO, so try to only use this once.
    ///
    /// In order to improve concurrency, this will buffer one partition,
    /// so you can have one partition in flight while processing another.
    pub fn json_tables(
        &self,
    ) -> impl TryStream<Ok = Vec<serde_json::Value>, Error = SnowflakeError> + '_ {
        self.partitions()
            .map_ok(|partition| futures::stream::iter(partition.json_table()).map(Ok))
            .try_flatten()
    }

    /// Stream over all rows in the response as JSON objects
    ///
    /// This incurs IO, so try to only use this once.
    ///
    /// In order to improve concurrency, this will buffer one partition,
    /// so you can have one partition in flight while processing another.
    pub fn json_objects(
        &self,
    ) -> impl TryStream<Ok = serde_json::Value, Error = SnowflakeError> + '_ {
        self.partitions()
            .map_ok(|partition| futures::stream::iter(partition.json_objects()).map(Ok))
            .try_flatten()
    }
}

#[cfg(test)]
mod tests {
    use jwt_simple::algorithms::RS256KeyPair;

    use crate::errors::SnowflakeResult;

    use super::*;

    #[test]
    fn sql() -> SnowflakeResult<()> {
        let key_pair = RS256KeyPair::generate(2048)?;
        let sql = SnowflakeClient {
            key_pair,
            account: "ACCOUNT".into(),
            user: "USER".into(),
            database: "DB".into(),
            warehouse: "WH".into(),
            role: Some("ROLE".into()),
        }
        .prepare("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")
        .add_binding(10);
        assert_eq!(sql.wire.bindings.len(), 1);
        let sql = sql.add_binding("Henry");
        assert_eq!(sql.wire.bindings.len(), 2);
        Ok(())
    }
}

//
// Wire types
//

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WireStatementMetaData {
    pub num_rows: usize,
    //pub format: String,
    pub row_type: Vec<ColumnType>,
    // The partition ino mostly doesn't matter, only the number of partitions
    pub partition_info: Vec<WirePartitionInfo>,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct WirePartitionInfo {
    //pub row_count: usize,
    //pub uncompressed_size: usize,
    //pub compressed_size: Option<usize>,
}

/// The type of a column in the result set
///
/// In most cases Cell should already expose the data you need,
/// but if you use the raw strings or need additional information like nullability, etc,
/// this can be useful.
#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ColumnType {
    /// The name of the column
    pub name: String,
    /// The database the column is in
    pub database: String,
    /// The schema the column is in
    pub schema: String,
    /// The table the column is in
    pub table: String,
    /// How many decimal digits of precision the column has
    /// (this is usually 38)
    pub precision: Option<u32>,
    /// The length of the column in bytes
    pub byte_length: Option<usize>,
    #[serde(rename = "type")]
    /// The format used when serializing the type to String before returning it
    pub data_type: RawCell,
    // The number of decimal digits of scale the column has (after the decimal point, usually 0)
    pub scale: Option<i32>,
    // Whether the column can be null
    pub nullable: bool,
}

#[derive(Deserialize, Debug)]
pub struct WireChanges {
    #[serde(rename = "numRowsInserted")]
    pub rows_inserted: usize,
    #[serde(rename = "numRowsDeleted")]
    pub rows_deleted: usize,
    #[serde(rename = "numRowsUpdated")]
    pub rows_updated: usize,
    #[serde(rename = "numDmlDuplicates")]
    pub duplicates: usize,
}

#[derive(Deserialize, Debug)]
pub struct WireDMLResult {
    pub message: String,
    pub stats: WireChanges,
}

impl WireQueryResponse {
    fn hydrate(self, statement: Statement) -> QueryResponse {
        QueryResponse {
            result_set_meta_data: self.result_set_meta_data,
            data: self.data,
            statement_status_url: self.statement_status_url,
            statement,
        }
    }
}

#[derive(Deserialize, Debug)]
struct WirePartitionResponse {
    data: Arc<StringTable>,
}

#[derive(Serialize, Debug, Clone)]
struct WireStatement {
    statement: String,
    timeout: Option<u64>,
    database: String,
    warehouse: String,
    role: Option<String>,
    bindings: HashMap<String, Binding>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct WireQueryResponse {
    result_set_meta_data: WireStatementMetaData,
    data: Arc<StringTable>,
    // code: String,
    statement_status_url: String,
    // request_id: String,
    // sql_state: String,
    // message: String,
}
