use chrono::{
    naive::{NaiveDate, NaiveDateTime, NaiveTime},
    DateTime, Duration, Local, TimeZone,
};

/// The format Snowflake used for serializing data in a column
///
/// This is not usually necessary unless you intend to implement your own
/// deserialization of Snowflake data.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawCell {
    /// A 128-bit signed integer, 38 digits of precision.
    Fixed,
    /// A 64-bit floating point number, 15 digits of precision.
    Real,
    /// A variable length string. It must be valid UTF-8.
    Text,
    /// A variable length binary string.
    Binary,
    /// A boolean value.
    Boolean,
    /// A date without a time zone, as the number of days since 1970-01-01.
    Date,
    /// A time without a time zone, as the number of seconds since midnight.
    Time,
    /// A timestamp with the local time zone.
    TimestampLtz,
    /// A timestamp without a time zone.
    TimestampNtz,
    /// A timestamp with a time zone for each value. This is not supported yet.
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
            RawCell::Date => Cell::Date(
                NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                    + Duration::days(value.parse().unwrap()),
            ),
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

/// Cell types, used for receiving data from Snowflake.
///
/// Snowflake returns these as a list of Strings; these are the result of parsing those strings,
/// and as such there are some caveats to be aware of.
#[derive(Clone, Debug)]
pub enum Cell {
    /// A `NULL` value. Any column could be null unless it is declared as `NOT NULL`,
    /// but the driver is not aware of this information from the metadata.
    Null,
    /// A 128-bit signed integer, 38 digits of precision.
    /// Any NUMBER cell that can be represented as an integer will be, but
    /// this means that NUMBER columns can contain mixed types: Int and Float.
    ///
    /// e.g. `["1", "1.0", "1.1"]` will be parsed as `[Int(1), Int(1), Float(1.1)]`
    Int(i128),
    /// A 64-bit floating point number, 15 digits of precision.
    /// Any NUMBER cell that cannot be represented as an integer will be parsed as a float.
    /// Additionally, all REAL columns will be parsed as floats.
    /// This is lossy, but intended for convenience.
    Float(f64),
    /// A variable length string. It must be valid UTF-8.
    Varchar(String),
    /// A variable length binary string.
    /// (This is serialized over the wire as a hex string, so these are not bandwidth efficient.)
    Binary(Vec<u8>),
    /// A boolean value.
    Boolean(bool),
    /// A date without a time zone.
    Date(NaiveDate),
    /// A time without a time zone.
    Time(NaiveTime),
    /// A timestamp with the local time zone. (This is not extensively tested)
    TimestampLtz(DateTime<Local>),
    /// A timestamp without a time zone. Presumably this is UTC, but it is not specified.
    TimestampNtz(NaiveDateTime),
}

impl From<Cell> for serde_json::Value {
    fn from(cell: Cell) -> Self {
        use serde_json::json;
        use Cell::*;
        match cell {
            Null => json!(null),
            // This is a little hairy because very large numbers
            // can be represented as integers in json (it's unbounded precision)
            // but JS cannot so it's customary to convert numbers to strings
            // if the value can't be represented as an int in a f64.
            Int(value) if value.abs() < (1 << 53) => json!(value as i64),
            Int(value) => json!(value.to_string()),
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
