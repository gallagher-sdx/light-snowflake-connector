use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

/// Binding types, used for serialization and sending data to Snowflake.
///
/// These don't round trip because the format Snowflake returns is different,
/// and those are in `cells::Cell`.
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE", tag = "type")]
pub enum Binding {
    Boolean { value: String },
    Fixed { value: String },
    Real { value: String },
    Text { value: String },
    TimestampNtz { value: String },
    Date { value: String },
    Time { value: String },
}

// impl From<&str> for BindingValue {
//     fn from(value: &str) -> Self {
//         BindingValue::String(value.to_owned())
//     }
// }

macro_rules! impl_binding {
    ($ty: ty, $ex: ident) => {
        impl From<$ty> for Binding {
            fn from(value: $ty) -> Self {
                Binding::$ex {
                    value: value.to_string(),
                }
            }
        }
    };
}
impl_binding!(bool, Text);
impl_binding!(i8, Fixed);
impl_binding!(i16, Fixed);
impl_binding!(i32, Fixed);
impl_binding!(i64, Fixed);
impl_binding!(isize, Fixed);
impl_binding!(u8, Fixed);
impl_binding!(u16, Fixed);
impl_binding!(u32, Fixed);
impl_binding!(u64, Fixed);
impl_binding!(usize, Fixed);
impl_binding!(f32, Real);
impl_binding!(f64, Real);
impl_binding!(char, Text);
impl_binding!(String, Text);
impl_binding!(&str, Text);
impl_binding!(NaiveDateTime, Text);
impl_binding!(NaiveDate, Text);
impl_binding!(NaiveTime, Text);

impl From<&[u8]> for Binding {
    fn from(value: &[u8]) -> Self {
        Binding::Text {
            value: hex::encode(value),
        }
    }
}
