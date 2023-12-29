use serde::Deserialize;

use crate::cells::{Cell, RawCell};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResponse {
    pub result_set_meta_data: MetaData,
    pub data: Vec<Vec<Option<String>>>,
    pub code: String,
    pub statement_status_url: String,
    pub request_id: String,
    pub sql_state: String,
    pub message: String,
    //pub created_on: u64,
}

impl QueryResponse {
    /// Convert the response into `Cell`s in a list of lists format
    ///
    /// This most closely matches the format of the response from Snowflake
    pub fn cells(&self) -> Vec<Vec<Cell>> {
        self.data
            .iter()
            .map(|row| {
                row.iter()
                    .zip(&self.result_set_meta_data.row_type)
                    .map(|(value, row_type)| row_type.data_type.to_cell(value))
                    .collect()
            })
            .collect()
    }

    /// Convert the response into `serde_json::Value`s in a list of lists format
    pub fn json_table(&self) -> Vec<Vec<serde_json::Value>> {
        self.cells()
            .into_iter()
            .map(|row| row.into_iter().map(|cell| cell.into()).collect())
            .collect()
    }

    /// Convert the response into `serde_json::Value`s in a list of objects format
    pub fn json_objects(&self) -> Vec<serde_json::Value> {
        self.json_table()
            .into_iter()
            .map(|row| {
                serde_json::Value::Object(
                    row.into_iter()
                        .enumerate()
                        .map(|(i, cell)| (self.result_set_meta_data.row_type[i].name.clone(), cell))
                        .collect(),
                )
            })
            .collect()
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MetaData {
    pub num_rows: usize,
    pub format: String,
    pub row_type: Vec<RowType>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RowType {
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
