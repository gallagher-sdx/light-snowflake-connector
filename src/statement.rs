use std::collections::HashMap;
use std::sync::Arc;

use futures::{StreamExt, TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};

use crate::bindings::Binding;
use crate::cells::{Cell, RawCell};
use crate::errors::{SnowflakeError, SnowflakeResult, SnowflakeWireResult};
use crate::partition::{Partition, StringTable};
use crate::{jwt, SnowflakeClient};

#[derive(Debug, Clone)]
pub struct Statement {
    host: String,
    wire: WireStatement,
    uuid: uuid::Uuid,
    config: SnowflakeClient,
}

impl Statement {
    pub fn new(sql: &str, config: &crate::SnowflakeClient) -> Statement {
        Statement {
            host: format!(
                "https://{}.snowflakecomputing.com",
                config.account.to_ascii_lowercase(),
            ),
            wire: WireStatement {
                statement: sql.to_owned(),
                timeout: None,
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
    pub async fn query(&self) -> Result<QueryResponse, SnowflakeError> {
        Ok(self
            .send()
            .await?
            .json::<SnowflakeWireResult<WireQueryResponse>>()
            .await?
            .into_result()?
            .hydrate(self.clone()))
    }
    /// Use with `delete`, `insert`, `update` row(s).
    pub async fn manipulate(&self) -> Result<WireDMLResult, SnowflakeError> {
        self.send()
            .await?
            .json::<SnowflakeWireResult<WireDMLResult>>()
            .await?
            .into_result()
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
}

#[derive(Serialize, Debug, Clone)]
struct WireStatement {
    statement: String,
    timeout: Option<u32>,
    database: String,
    warehouse: String,
    role: Option<String>,
    bindings: HashMap<String, Binding>,
}

#[derive(Debug)]
pub struct QueryResponse {
    result_set_meta_data: WireStatementMetaData,
    data: Arc<StringTable>,
    statement_status_url: String,
    statement: Statement,
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

    /// A convenience method to assert that there is only one partition and return it
    ///
    /// This never causes IO, is not async, and can only error with MultiplePartitions
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
        let partition_futures = (0..self.num_partitions())
            .map(|index| self.partition(index));
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
    pub row_type: Vec<WireRowType>,
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

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct WireRowType {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub precision: Option<u32>,
    pub byte_length: Option<usize>,
    #[serde(rename = "type")]
    pub data_type: RawCell,
    pub scale: Option<i32>,
    pub nullable: bool,
    //pub collation: ???,
    //pub length: ???,
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
