# Light Snowflake Connector
Minimal wrapper around Snowflake's public REST API.

# Usage
Add following line to Cargo.toml:

```toml
snowflake-connector = { git = "https://github.com/gallagher-sdx/snowflake-connector.git" }
```

Right now, only [key pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) is supported. You can load these keys with jwt-simple.

## How it Works
Below example is not tested, but you get the gist:
```rust
ClientConfig{
    key_pair,
    account: "ACCOUNT".into(),
    user: "USER".into(),
    database: "DB".into(),
    warehouse: "WH".into(),
    role: Some("ROLE".into())
}.build()?;
let sql = sql
    .prepare("SELECT * FROM TEST_TABLE WHERE id = ? AND name = ?")?
    .add_binding(1)
    .add_binding("test");
// This result gives additional detail about the snowflake response
let result = sql.query()?;
// This result only contains the data in a Vec<Vec<Cell>> format
let cells = result.cells();
```


# Relationship to other Snowflake Connectors
This is a fork of the [snowflake-connector](https://github.com/Ripper53/snowflake-connector) library, and differs in a few ways:
    - It returns Cells rather than deserializing the data into custom structs
    - It does not support Decimal types (because it is hard to do correctly)

It differs from [snowflake-api](https://docs.rs/snowflake-api/latest/snowflake_api/) in that:
    - It uses only the documented v2 API, which (presently) does not support GET or PUT
    - It doesn't use any deprecated or undocumented APIs at all
    - It doesn't require Arrow (but it also doesn't get the performance benefits of Arrow)

