use chrono::{
    naive::{NaiveDate, NaiveDateTime, NaiveTime},
    DateTime, Local, TimeZone, Duration,
};

// include getters and setters for the bindings
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawCell {
    Fixed,
    Real,
    Text,
    Binary,
    Boolean,
    Date,
    Time,
    TimestampLtz,
    TimestampNtz,
    TimestampTz,
}

impl RawCell {
    /// Convert a RawCell into a Cell.
    ///
    /// There are many possible panics in this conversion,
    /// but they depend generally on Snowflake returning a value that can be parsed.
    ///
    /// - Decimals are not supported. Number type columns are converted to i128 if possible,
    ///   otherwise f64. So there can be a loss of precision, which is a tradeoff for convenience.
    /// - For the same reason, NUMBER columns can contain mixed types: Int and Float
    pub fn to_cell(&self, value: &Option<String>) -> Cell {
        let value = if let Some(value) = value {
            value
        } else {
            return Cell::Null;
        };
        match self {
            // It seems pretty unlikely snowflake will return a value that can't be parsed.
            // Also, you probably couldn't do much with it anyway,
            // But would Result still be better?
            RawCell::Fixed => match value.trim_end_matches(".0").parse() {
                Ok(value) => Cell::Int(value),
                Err(_) => Cell::Float(value.parse().unwrap()),
            },
            RawCell::Real => Cell::Float(value.parse().unwrap()),
            RawCell::Text => Cell::Varchar(value.to_owned()),
            RawCell::Binary => Cell::Binary(hex::decode(value).unwrap()),
            RawCell::Boolean => Cell::Boolean(value.parse().unwrap()),
            RawCell::Date => Cell::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap() + Duration::days(value.parse().unwrap())),
            RawCell::Time => {
                let seconds_since_epoch: f64 = value.parse().unwrap();
                Cell::Time(
                    NaiveTime::from_num_seconds_from_midnight_opt(
                        seconds_since_epoch as u32,
                        (seconds_since_epoch.fract() * 1e9) as u32,
                    )
                    .unwrap(),
                )
            }
            RawCell::TimestampLtz => {
                let seconds_since_epoch: f64 = value.parse().unwrap();
                Cell::TimestampLtz(Local.timestamp_nanos(
                    seconds_since_epoch as i64 + (seconds_since_epoch.fract() * 1e9) as i64,
                ))
            }
            RawCell::TimestampNtz => {
                let seconds_since_epoch: f64 = value.parse().unwrap();
                Cell::TimestampNtz(
                    NaiveDateTime::from_timestamp_opt(
                        seconds_since_epoch as i64,
                        (seconds_since_epoch.fract() * 1e9) as u32,
                    )
                    .unwrap(),
                )
            }
            RawCell::TimestampTz => {
                // This is just too complex to support yet
                Cell::Null
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Cell {
    Null,
    Int(i128),
    Float(f64),
    Varchar(String),
    Binary(Vec<u8>),
    Boolean(bool),
    Date(NaiveDate),
    Time(NaiveTime),
    TimestampLtz(DateTime<Local>),
    TimestampNtz(NaiveDateTime),
}

impl From<Cell> for serde_json::Value {
    fn from(cell: Cell) -> Self {
        use serde_json::json;
        use Cell::*;
        match cell {
            Null => json!(null),
            Int(value) => json!(value),
            Float(value) => json!(value),
            Varchar(value) => json!(value),
            Binary(value) => json!(hex::encode(value)),
            Boolean(value) => json!(value),
            Date(value) => json!(value),
            Time(value) => json!(value),
            TimestampLtz(value) => json!(value),
            TimestampNtz(value) => json!(value),
        }
    }
}
