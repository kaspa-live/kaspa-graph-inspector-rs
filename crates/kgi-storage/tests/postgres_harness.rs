use sqlx::{Connection, PgConnection};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ImageExt, runners::AsyncRunner},
};

const POSTGRES_PORT: u16 = 5432;

#[tokio::test]
async fn sqlx_connects_to_isolated_postgres_fixture() {
    let container = Postgres::default().with_tag("17-alpine").start().await.expect("PostgreSQL fixture must start");
    let host = container.get_host().await.expect("fixture host must resolve");
    let port = container.get_host_port_ipv4(POSTGRES_PORT).await.expect("fixture PostgreSQL port must resolve");
    let database_url = format!("postgresql://postgres:postgres@{host}:{port}/postgres?sslmode=disable");

    let mut connection = PgConnection::connect(&database_url).await.expect("SQLx must connect to the PostgreSQL fixture");
    let value: i32 = sqlx::query_scalar("SELECT 1").fetch_one(&mut connection).await.expect("runtime SQL query must succeed");

    assert_eq!(value, 1);
}
