# Light Snowflake Connector
Minimal wrapper around Snowflake's public REST API.

- Easy to use
- Intended for small, tactical queries and point lookups,
  - Particularly for dashboards and other interactive use cases
  - Keep in mind that Snowflake latency is usually >100ms so design carefully
- Designed not to break for large result sets, but not optimized for them either

# Usage
Add following line to Cargo.toml:

```toml
# Cargo.toml
light-snowflake-connector = "0.1.0"
```

 ```rust
 use light_snowflake_connector::{Cell, SnowflakeClient, SnowflakeError};
 use light_snowflake_connector::jwt_simple::algorithms::RS256KeyPair;

 #[tokio::main]
 async fn main() -> Result<(), SnowflakeError> {
     let key_pair = RS256KeyPair::generate(2048)?;
     let config = SnowflakeClient {
         key_pair,
         account: "ACCOUNT".into(),
         user: "USER".into(),
         database: "DB".into(),
         warehouse: "WH".into(),
         role: Some("ROLE".into()),
     };

     let result = config
         .prepare("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")
         .add_binding(10)
         .add_binding("Henry")
         .query()
         .await?;

     // Get the first partition of the result, and assert that there is only one partition
     let partition = result.only_partition()?;
     
     // Get the results as a Vec<Vec<Cell>>, which is a tagged enum similar to serde_json::Value
     let cells = partition.cells();
     match &cells[0][0] {
         Cell::Int(x) => println!("Got an integer: {}", x),
         Cell::Varchar(x) => println!("Got a string: {}", x),
         _ => panic!("Got something else"),
     }

     // Get the results as a Vec<Vec<serde_json::Value>>, which is a list of lists of JSON values
     let json_table = partition.json_table();

     // Get the results as a Vec<serde_json::Value>, which is a list of JSON objects
     let json_objects = partition.json_objects();

     Ok(())
 }
 ```

# Features & Limitations
Authentication:
- [x] Key Pair Authentication
- [ ] OAuth, SSO: This is possible but not implemented yet
- [ ] Username/Password: Not available in Snowflake's REST API 2.0

Querying:
- [x] Prepared Statements with `qmark` "?" Bindings
  - No other bindings are supported
- [x] Rust `async` support (but synchronous from Snowflake's point of view)
- [ ] Snowflake "async" support (for super long running queries)
- [ ] GET and PUT: not supported by Snowflake's REST API 2.0
- [ ] Arrow support: we're trying to keep the dependency tree small
- [x] Streaming support, and multiple batches

Types:
- [x] String, str
- [x] i128
- [x] f64
- [x] bool
- [x] Date, Time, Timestamp_Ntz (NaiveDateTime), Timestamp_Ltz (DateTime<FixedOffset>; not well testes, not sure about the use cases)
- [ ] Timestamp_Tz (DateTime<Utc>)
- [ ] Decimal (dec and rust_decimal have different semantics and precision)

## Implicit Type Conversions
Snowflake's NUMBER type is 128 bit (38 decimal digits) but supports a scale as well. There's no native Rust type that can achieve both of these so we opted for the more convenient (and probably common) use cases:
- Integers are converted to i128, which is lossless
- Floats are converted to f64, which is lossy
- Which to do is determined on a cell by cell basis, so you can have a column with mixed types

This particular workaround could be improved by using a Decimal type, but there are some issues with the available libraries:
- [rust_decimal](https://docs.rs/rust_decimal/1.10.1/rust_decimal/) is a pure Rust implementation, but it doesn't support 128 bit numbers
- [dec](https://docs.rs/dec/0.1.0/dec/) supports 128 bit numbers, but somehow 4 digits less decimal precision. Also, it's a wrapper around a C library, so it could cause issues downstream for WASM users (e.g. FaaS)
- [arrow](https://docs.rs/arrow/5.0.0/arrow/) (and FWIW, arrow2) supports 128 bit numbers, but it's a huge dependency and we'd have to pivot to columnar data structures and a different API.

## Multiple Batches
This library supports multiple batches, which is useful for streaming large result sets. But the results are transferred as JSON, so if high throughput is a concern, you should consider one of the Arrow based libraries instead, like [snowflake-api](https://docs.rs/snowflake-api/latest/snowflake_api/).

# Relationship to other Snowflake Connectors
This is a fork of the [snowflake-connector](https://github.com/Ripper53/snowflake-connector) library, and differs in a few ways:
- It returns Cells rather than deserializing the data into custom structs
- It does not support Decimal types (because it is hard to do correctly)

It differs from [snowflake-api](https://docs.rs/snowflake-api/latest/snowflake_api/) in that:
- It uses only the documented v2 API, which (presently) does not support GET or PUT
- It doesn't use any deprecated or undocumented APIs at all
- It doesn't require Arrow (but it also doesn't get the performance benefits of Arrow)

It differs from most other languages' Snowflake connectors in that:
- It doesn't use Arrow
- It doesn't use the deprecated v1 API
- It doesn't use undocumented APIs
- It doesn't support GET or PUT
- It doesn't support Async Queries
- It's not affiliated with or supported by Snowflake.
- It's not an official product of any company, doesn't have any guarantees, warranties, or support.