use std::sync::Arc;



use crate::{
    cells::{Cell},
    statement::WireStatementMetaData,
};
pub type StringTable = Vec<Vec<Option<String>>>;

pub struct Partition {
    pub(crate) meta_data: WireStatementMetaData,
    pub(crate) data: Arc<StringTable>,
    pub(crate) index: usize,
}

impl Partition {
    /// Get the index of this partition
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get the number of rows in just this partition
    /// This is obtained from data.len() rather than the metadata
    /// because this partition may have been constructed by concatenating
    pub fn num_rows(&self) -> usize {
        self.data.len()
    }

    /// Get the cells in this partition as strings just as they were returned from Snowflake
    ///
    /// This could be more efficient for some use cases than converting to `Cell`s
    /// but without the type information it could be difficult to work with
    pub fn raw_cells(&self) -> &[Vec<Option<String>>] {
        self.data.as_ref()
    }

    /// Convert the response into `Cell`s in a list of lists format
    ///
    /// This most closely matches the format of the response from Snowflake
    pub fn cells(&self) -> Vec<Vec<Cell>> {
        self.data
            .iter()
            .map(|row| {
                row.iter()
                    .zip(&self.meta_data.row_type)
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
                        .map(|(i, cell)| (self.meta_data.row_type[i].name.clone(), cell))
                        .collect(),
                )
            })
            .collect()
    }
}
