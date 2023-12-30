use chrono::{Datelike, Timelike};
use jwt_simple::algorithms::RS256KeyPair;

use crate::{cells::Cell, Client, ClientConfig, SnowflakeResult};

fn default_client() -> Client {
    let _ = env_logger::try_init();
    let require = |name: &str| std::env::var(name).expect(&format!("{} not set", name));
    let key_path = require("SNOWFLAKE_TRADITIONAL_RSA_KEY_PATH");
    let key_content = std::fs::read_to_string(key_path).expect("failed to read key file");
    let key_pair = RS256KeyPair::from_pem(&key_content).expect("failed to parse key");
    let config = ClientConfig {
        key_pair,
        account: require("SNOWFLAKE_ACCOUNT"),
        user: require("SNOWFLAKE_USER"),
        database: require("SNOWFLAKE_DATABASE"),
        warehouse: require("SNOWFLAKE_WAREHOUSE"),
        role: Some(require("SNOWFLAKE_ROLE")),
    };
    config.build().expect("failed to create client")
}

#[tokio::test]
async fn can_login() -> SnowflakeResult<()> {
    let client = default_client();
    let sql = client.prepare("SELECT 1")?;
    let result = sql.query().await?;
    let cells = result.cells();
    assert_eq!(cells.len(), 1);
    assert_eq!(cells[0].len(), 1);
    assert!(matches!(cells[0][0], Cell::Int(1)));
    Ok(())
}

#[tokio::test]
async fn can_query_many_types() -> SnowflakeResult<()> {
    let client = default_client();
    let sql = client.prepare(
        "SELECT 1,
        'foo',
        1.0,
        true,
        NULL,
        1.1,
        '666f6f'::binary,
        '2023-01-01 01:01:01'::timestamp_ntz,
        '2023-01-01 01:01:01'::timestamp_ltz,
        '2023-01-01 01:01:01Z'::timestamp_tz,
        '2023-01-01'::date,
        '01:01:01'::time
    ")?;
    let result = sql.query().await?;
    let cells = result.cells();
    assert_eq!(cells.len(), 1);
    assert!(matches!(cells[0][0], Cell::Int(1)));
    assert!(matches!(cells[0][1], Cell::Varchar(ref x) if x == "foo"));
    assert!(matches!(cells[0][2], Cell::Int(1)));
    assert!(matches!(cells[0][3], Cell::Boolean(true)));
    assert!(matches!(cells[0][4], Cell::Null));
    assert!(matches!(cells[0][5], Cell::Float(x) if x > 1.0 && x < 1.2));
    assert!(matches!(cells[0][6], Cell::Binary(ref x) if x == b"foo"));
    assert!(matches!(cells[0][7],
        Cell::TimestampNtz(ref x)
        if x.year() == 2023
        && x.month() == 1
        && x.day() == 1
        && x.hour() == 1
        && x.minute() == 1
        && x.second() == 1
    ));
    // TODO: test timezone
    // Not sure how to do this without just comparing two implementations of the same thing
    assert!(matches!(cells[0][8], Cell::TimestampLtz(_)));
    // TIMESTAMP_TZ is just too complex to support yet
    assert!(
        matches!(cells[0][9], Cell::Null
    ));
    assert!(matches!(cells[0][10],
        Cell::Date(ref x)
        if x.year() == 2023
        && x.month() == 1
        && x.day() == 1
    ));
    assert!(matches!(cells[0][11],
        Cell::Time(ref x)
        if x.hour() == 1
        && x.minute() == 1
        && x.second() == 1
    ));
    Ok(())
}

#[tokio::test]
async fn can_query_many_rows() -> SnowflakeResult<()> {
    let client = default_client();
    let sql = client.prepare("SELECT seq4() FROM table(generator(rowcount => 100))")?;
    let result = sql.query().await?;
    let cells = result.cells();
    assert_eq!(cells.len(), 100);
    for row in cells {
        assert_eq!(row.len(), 1);
        assert!(matches!(row[0], Cell::Int(_)));
    }
    Ok(())
}

#[tokio::test]
async fn can_query_with_many_bindings() -> SnowflakeResult<()> {
    let client = default_client();
    let sql = client.prepare("SELECT ?::int, ?::varchar, ?::float, ?::boolean, ?::binary, ?::date, ?::time, ?::timestamp_ntz, ?::timestamp_ltz, ?::timestamp_tz")?;
    let sql = sql
        .add_binding(1)
        .add_binding("foo")
        .add_binding(1.0)
        .add_binding(true)
        .add_binding(b"foo".as_slice())
        .add_binding("2023-01-01")
        .add_binding("01:01:01")
        .add_binding("2023-01-01 01:01:01");
    let result = sql.query().await?;
    let cells = result.cells();
    assert_eq!(cells.len(), 1);
    assert!(matches!(cells[0][0], Cell::Int(1)));
    assert!(matches!(cells[0][1], Cell::Varchar(ref x) if x == "foo"));
    assert!(matches!(cells[0][2], Cell::Int(1)));
    assert!(matches!(cells[0][3], Cell::Boolean(true)));
    assert!(matches!(cells[0][4], Cell::Binary(ref x) if x == b"foo"));
    assert!(matches!(cells[0][5],
        Cell::Date(ref x)
        if x.year() == 2023
        && x.month() == 1
        && x.day() == 1
    ));
    assert!(matches!(cells[0][6],
        Cell::Time(ref x)
        if x.hour() == 1
        && x.minute() == 1
        && x.second() == 1
    ));
    assert!(matches!(cells[0][7],
        Cell::TimestampNtz(ref x)
        if x.year() == 2023
        && x.month() == 1
        && x.day() == 1
        && x.hour() == 1
        && x.minute() == 1
        && x.second() == 1
    ));
    Ok(())
}
