//! Reads across regions, combined: ordered, limited, deduplicated and
//! re-aggregated as the statement asks, end to end.

#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::time::Duration;

use luxbyte::{Config, Server, ServerHandle};
use luxbyte_pool::{Connection, ConnectionTarget, Credentials, QueryResult};
use luxbyte_testkit::{Backend, BackendConfig, Reply};

const INT4: i32 = 23;
const INT8: i32 = 20;
const TEXT: i32 = 25;

struct Fixture {
    eu: Backend,
    us: Backend,
    ap: Backend,
    proxy: ServerHandle,
}

async fn fixture(eu: BackendConfig, us: BackendConfig, ap: BackendConfig) -> Fixture {
    let eu = Backend::start(eu).await.unwrap();
    let us = Backend::start(us).await.unwrap();
    let ap = Backend::start(ap).await.unwrap();
    let config = Config::from_toml(&format!(
        r#"
[server]
listen = "127.0.0.1:0"
node = "test-proxy"

[backend]
user = "app"
database = "shop"

[topology]
home_region = "eu-west"

[[topology.regions]]
id = "eu-west"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.regions]]
id = "us-east"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.regions]]
id = "ap-south"
primary = {{ host = "127.0.0.1", port = {} }}

[[topology.tables]]
table = "users"
placement = "geo_partitioned"
key = "tenant_id"
"#,
        eu.port(),
        us.port(),
        ap.port()
    ))
    .unwrap();
    let proxy = Server::bind(&config).await.unwrap().spawn().unwrap();
    Fixture { eu, us, ap, proxy }
}

async fn client(proxy: &ServerHandle) -> Connection {
    Connection::connect(
        &ConnectionTarget {
            host: "127.0.0.1".to_owned(),
            port: proxy.port(),
            database: "shop".to_owned(),
            user: "app".to_owned(),
        },
        &Credentials::Trust,
        Duration::from_secs(5),
    )
    .await
    .unwrap()
}

fn ids(rows: &[i64]) -> Reply {
    let rows: Vec<Vec<Option<String>>> = rows.iter().map(|id| vec![Some(id.to_string())]).collect();
    Reply::TypedRows {
        columns: vec!["id".to_owned()],
        types: vec![INT8],
        rows,
    }
}

fn column(result: &QueryResult) -> Vec<String> {
    result
        .rows
        .iter()
        .filter_map(|row| row.first().cloned().flatten())
        .collect()
}

/// The statement the home region was asked to finish with.
fn finishing(backend: &Backend) -> String {
    backend
        .statements()
        .into_iter()
        .find(|sql| sql.contains("unnest("))
        .unwrap_or_default()
}

// ------------------------------------------------------------------ rows

#[tokio::test]
async fn an_ordered_limited_read_returns_the_limit_not_the_limit_per_region() {
    let f = fixture(
        BackendConfig::new("eu-west").reply_to("ORDER BY", ids(&[1, 4, 7])),
        BackendConfig::new("us-east").reply_to("ORDER BY", ids(&[2, 3, 9])),
        BackendConfig::new("ap-south").reply_to("ORDER BY", ids(&[5])),
    )
    .await;
    let mut client = client(&f.proxy).await;

    let result = client
        .simple_query("SELECT id FROM users ORDER BY id LIMIT 3 OFFSET 2")
        .await
        .expect("read");
    // All rows in order: 1 2 3 4 5 7 9. Skip two, take three.
    assert_eq!(column(&result), ["3", "4", "5"]);
    assert_eq!(result.tags.last().map(String::as_str), Some("SELECT 3"));
    // Each region was asked for no more than the answer could need, and
    // none was asked to skip rows on its own.
    assert!(f.us.saw("SELECT id FROM users ORDER BY id LIMIT 5"));
    assert!(!f.us.saw("OFFSET"));
}

#[tokio::test]
async fn big_integers_merge_in_exact_order() {
    let f = fixture(
        BackendConfig::new("eu-west").reply_to("ORDER BY", ids(&[9_007_199_254_740_993])),
        BackendConfig::new("us-east").reply_to("ORDER BY", ids(&[9_007_199_254_740_992])),
        BackendConfig::new("ap-south").reply_to("ORDER BY", ids(&[])),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT id FROM users ORDER BY id")
        .await
        .expect("read");
    assert_eq!(column(&result), ["9007199254740992", "9007199254740993"]);
}

#[tokio::test]
async fn a_sort_on_an_unselected_expression_is_honoured_and_hidden() {
    let sorted = |rows: &[(&str, i64)]| Reply::TypedRows {
        columns: vec!["name".to_owned(), "__luxbyte_sort_0".to_owned()],
        types: vec![TEXT, INT8],
        rows: rows
            .iter()
            .map(|(name, key)| vec![Some((*name).to_owned()), Some(key.to_string())])
            .collect(),
    };
    let f = fixture(
        BackendConfig::new("eu-west").reply_to("ORDER BY", sorted(&[("c", 3), ("a", 1)])),
        BackendConfig::new("us-east").reply_to("ORDER BY", sorted(&[("b", 2)])),
        BackendConfig::new("ap-south").reply_to("ORDER BY", sorted(&[])),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT name FROM users ORDER BY length(bio) DESC")
        .await
        .expect("read");
    assert_eq!(result.columns, ["name"], "the sort column stays hidden");
    assert_eq!(column(&result), ["c", "b", "a"]);
}

#[tokio::test]
async fn text_is_sorted_by_postgres_in_the_home_region() {
    // Collation order is PostgreSQL's to apply, so the rows go home to be
    // sorted rather than being compared byte by byte here.
    let names = |values: &[&str]| Reply::TypedRows {
        columns: vec!["name".to_owned()],
        types: vec![TEXT],
        rows: values.iter().map(|v| vec![Some((*v).to_owned())]).collect(),
    };
    let f = fixture(
        BackendConfig::new("eu-west")
            .reply_to("unnest(", names(&["Ada", "bob", "Cy"]))
            .reply_to("ORDER BY", names(&["bob"])),
        BackendConfig::new("us-east").reply_to("ORDER BY", names(&["Ada", "Cy"])),
        BackendConfig::new("ap-south").reply_to("ORDER BY", names(&[])),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT name FROM users ORDER BY name LIMIT 10")
        .await
        .expect("read");
    assert_eq!(column(&result), ["Ada", "bob", "Cy"]);
    let finish = finishing(&f.eu);
    assert!(
        finish.contains("ORDER BY p.c1 ASC NULLS LAST LIMIT 10"),
        "{finish}"
    );
    // Every region's rows, in region order, carried as one typed array.
    assert!(
        finish.contains(r#"unnest(E'{"bob","Ada","Cy"}'::text[])"#),
        "{finish}"
    );
}

#[tokio::test]
async fn union_deduplicates_across_regions() {
    let f = fixture(
        BackendConfig::new("eu-west")
            .reply_to("unnest(", ids(&[1, 2]))
            .reply_to("UNION", ids(&[1, 2])),
        BackendConfig::new("us-east").reply_to("UNION", ids(&[2])),
        BackendConfig::new("ap-south").reply_to("UNION", ids(&[])),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT id FROM users UNION SELECT id FROM users")
        .await
        .expect("read");
    assert_eq!(column(&result), ["1", "2"]);
    assert!(finishing(&f.eu).starts_with("SELECT DISTINCT p.c1"));
}

// ------------------------------------------------------------ aggregates

#[tokio::test]
async fn a_count_across_regions_is_one_number() {
    let count = |n: &str| Reply::typed(&[("count", INT8)], &[&[Some(n)]]);
    let f = fixture(
        BackendConfig::new("eu-west")
            .reply_to("unnest(", count("6"))
            .reply_to("count(*)", count("1")),
        BackendConfig::new("us-east").reply_to("count(*)", count("2")),
        BackendConfig::new("ap-south").reply_to("count(*)", count("3")),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query("SELECT count(*) FROM users")
        .await
        .expect("count");
    assert_eq!(column(&result), ["6"]);
    let finish = finishing(&f.eu);
    // The partial counts, in region order, summed by PostgreSQL.
    assert!(
        finish.starts_with(
            r#"SELECT sum(p.c1)::int8 AS "count" FROM unnest(E'{"3","1","2"}'::int8[])"#
        ),
        "{finish}"
    );
}

#[tokio::test]
async fn groups_are_regrouped_with_having_and_limit_applied_once() {
    let partials = |rows: &[&[Option<&str>]]| {
        Reply::typed(
            &[
                ("country", TEXT),
                ("count", INT8),
                ("sum", INT8),
                ("count", INT8),
            ],
            rows,
        )
    };
    let f = fixture(
        BackendConfig::new("eu-west")
            .reply_to(
                "unnest(",
                Reply::typed(
                    &[("country", TEXT), ("count", INT8), ("avg", 1700)],
                    &[&[Some("DE"), Some("3"), Some("40.0000000000000000")]],
                ),
            )
            .reply_to(
                "GROUP BY",
                partials(&[&[Some("DE"), Some("2"), Some("80"), Some("2")]]),
            ),
        BackendConfig::new("us-east").reply_to(
            "GROUP BY",
            partials(&[&[Some("DE"), Some("1"), Some("40"), Some("1")]]),
        ),
        BackendConfig::new("ap-south").reply_to("GROUP BY", partials(&[])),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let result = client
        .simple_query(
            "SELECT country, count(*), avg(age) FROM users GROUP BY country \
             HAVING count(*) > 1 ORDER BY 2 DESC LIMIT 5",
        )
        .await
        .expect("grouped");
    assert_eq!(result.columns, ["country", "count", "avg"]);
    assert_eq!(
        result.rows.first().cloned().unwrap_or_default(),
        [
            Some("DE".to_owned()),
            Some("3".to_owned()),
            Some("40.0000000000000000".to_owned())
        ]
    );
    // Each region computed partials only; HAVING, ORDER BY and LIMIT ran
    // once, over every region's groups.
    assert!(f.us.saw("SELECT country, count(*), sum(age), count(age) FROM users GROUP BY country"));
    let finish = finishing(&f.eu);
    assert!(
        finish.contains("GROUP BY p.c1 HAVING sum(p.c2)::int8 > 1 ORDER BY 2 DESC LIMIT 5"),
        "{finish}"
    );
    assert!(
        finish.contains("(sum(p.c3)::numeric / nullif(sum(p.c4), 0)) AS \"avg\""),
        "{finish}"
    );
}

#[tokio::test]
async fn an_aggregate_that_cannot_be_combined_is_refused_with_the_reason() {
    let f = fixture(
        BackendConfig::new("eu-west"),
        BackendConfig::new("us-east"),
        BackendConfig::new("ap-south"),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("SELECT count(DISTINCT country) FROM users")
        .await
        .expect_err("refused");
    assert_eq!(error.sqlstate(), Some("0A000"));
    assert!(error.to_string().contains("DISTINCT"), "{error}");
    assert!(!f.us.saw("count(DISTINCT"));
}

#[tokio::test]
async fn an_aggregate_only_the_database_knows_about_is_not_mistaken_for_a_column() {
    // my_median looks like any scalar function. Computed per region, each
    // region's median would come back as a row of the answer.
    let f = fixture(
        BackendConfig::new("eu-west").reply_to(
            "pg_proc",
            Reply::typed(&[("proname", TEXT)], &[&[Some("my_median")]]),
        ),
        BackendConfig::new("us-east"),
        BackendConfig::new("ap-south"),
    )
    .await;
    let mut client = client(&f.proxy).await;
    let error = client
        .simple_query("SELECT my_median(age) FROM users")
        .await
        .expect_err("refused rather than concatenated");
    assert!(error.to_string().contains("my_median"), "{error}");
    assert!(!f.us.saw("my_median"));
    assert_eq!(f.ap.statements(), Vec::<String>::new());
}

#[tokio::test]
async fn inside_a_transaction_a_scattered_read_sees_the_transactions_writes() {
    let count = |n: &str| Reply::typed(&[("count", INT8)], &[&[Some(n)]]);
    let f = fixture(
        BackendConfig::new("eu-west")
            .reply_to("unnest(", count("3"))
            .reply_to("count(*)", count("1")),
        BackendConfig::new("us-east").reply_to("count(*)", count("1")),
        BackendConfig::new("ap-south").reply_to("count(*)", count("1")),
    )
    .await;
    let mut client = client(&f.proxy).await;
    client.simple_query("BEGIN").await.expect("begin");
    client
        .simple_query("SELECT count(*) FROM users")
        .await
        .expect("count");
    client.simple_query("COMMIT").await.expect("commit");
    // Read through each region's participant, inside its BEGIN.
    for backend in [&f.eu, &f.us, &f.ap] {
        let statements = backend.statements();
        let begin = statements.iter().position(|sql| sql == "BEGIN");
        let read = statements.iter().position(|sql| sql.contains("count(*)"));
        assert!(
            begin.is_some() && begin < read,
            "{}: {statements:?}",
            backend.name()
        );
    }
    let _ = INT4;
}
