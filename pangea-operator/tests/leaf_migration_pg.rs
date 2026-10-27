use pangea_leaf::plan::StateMove;
use pangea_operator::backend::leaf_migration::{forward, reverse, Outcome};
use serde_json::json;
use sqlx::PgPool;

const SCHEMA: &str = "pangea_lmtest";

fn fat_state() -> String {
    serde_json::to_string_pretty(&json!({
        "version": 4, "terraform_version": "1.7.0", "serial": 181,
        "lineage": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "outputs": {},
        "resources": [
            {"mode": "managed", "type": "github_repository", "name": "repos", "provider": "p",
             "instances": [
                {"index_key": "alpha", "schema_version": 1, "attributes": {"id": "alpha"}},
                {"index_key": "beta", "schema_version": 1, "attributes": {"id": "beta"}}]}
        ]
    }))
    .unwrap()
}

fn moves(fat: &str) -> Vec<StateMove> {
    ["alpha", "beta"]
        .iter()
        .map(|k| StateMove {
            from_template: fat.into(),
            to_template: format!("{fat}-{k}"),
            pangea_namespace: Some("lmtest".into()),
            addresses: vec![format!("github_repository.repos[\"{k}\"]")],
        })
        .collect()
}

async fn pool() -> Option<PgPool> {
    let url = std::env::var("PANGEA_TEST_DATABASE_URL").ok()?;
    Some(
        PgPool::connect(&url)
            .await
            .expect("PANGEA_TEST_DATABASE_URL connects"),
    )
}

async fn seed(pool: &PgPool, fat: &str) {
    let folded = format!("\"{SCHEMA}_{fat}_states\"");
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {folded} CASCADE"))
        .execute(pool)
        .await
        .unwrap();
    for leaf in ["alpha", "beta"] {
        sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS \"{SCHEMA}_{fat}-{leaf}_states\" CASCADE"
        ))
        .execute(pool)
        .await
        .unwrap();
    }
    sqlx::query("CREATE SCHEMA IF NOT EXISTS pangea_meta")
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM pangea_meta.artifacts WHERE template_name = $1")
        .bind(fat)
        .execute(pool)
        .await
        .ok();
    sqlx::query(&format!("CREATE SCHEMA {folded}"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {folded}.states (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE, data TEXT)"
    ))
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {folded}.states (name, data) VALUES ('default', $1)"
    ))
    .bind(fat_state())
    .execute(pool)
    .await
    .unwrap();
}

async fn row(pool: &PgPool, template: &str, name: &str) -> Option<String> {
    let q = format!("SELECT data FROM \"{SCHEMA}_{template}_states\".states WHERE name = $1");
    sqlx::query_scalar::<_, Option<String>>(&q)
        .bind(name)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .flatten()
}

async fn artifact(pool: &PgPool, fat: &str) -> bool {
    sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM pangea_meta.artifacts WHERE template_name = $1 AND kind = 'leaf-migration'",
    )
    .bind(fat)
    .fetch_one(pool)
    .await
    .map(|n| n > 0)
    .unwrap_or(false)
}

#[tokio::test]
async fn a_forged_bijection_writes_nothing() {
    let Some(pool) = pool().await else { return };
    let fat = "lm-forged";
    seed(&pool, fat).await;
    let mut forged = moves(fat);
    forged[1]
        .addresses
        .push("github_repository.repos[\"alpha\"]".into());
    let out = forward(&pool, SCHEMA, fat, &forged).await.unwrap();
    assert!(matches!(out, Outcome::Refused(_)), "{out:?}");
    assert_eq!(row(&pool, fat, "default").await, Some(fat_state()));
    assert_eq!(row(&pool, fat, "pre-leaf").await, None);
    assert_eq!(row(&pool, &format!("{fat}-alpha"), "default").await, None);
    assert!(!artifact(&pool, fat).await);
}

#[tokio::test]
async fn a_good_move_writes_n_rows_and_reverse_restores_the_fat_row_byte_equal() {
    let Some(pool) = pool().await else { return };
    let fat = "lm-good";
    seed(&pool, fat).await;
    let out = forward(&pool, SCHEMA, fat, &moves(fat)).await.unwrap();
    let Outcome::Migrated(receipt) = out else {
        panic!("{out:?}")
    };
    assert_eq!(receipt.leaves.len(), 2);
    assert_eq!(receipt.hash_before, receipt.hash_after);
    assert_eq!(row(&pool, fat, "default").await, Some(fat_state()));
    assert_eq!(row(&pool, fat, "pre-leaf").await, Some(fat_state()));
    let mut addrs = Vec::new();
    for leaf in ["alpha", "beta"] {
        let data = row(&pool, &format!("{fat}-{leaf}"), "default")
            .await
            .unwrap();
        addrs.extend(pangea_leaf::migrate::managed_addresses(data.as_bytes()).unwrap());
    }
    assert_eq!(
        pangea_leaf::partition::multiset_hash(&addrs),
        receipt.hash_before
    );
    assert!(artifact(&pool, fat).await);
    assert!(matches!(
        forward(&pool, SCHEMA, fat, &moves(fat)).await.unwrap(),
        Outcome::AlreadyMigrated(_)
    ));

    sqlx::query(&format!(
        "UPDATE \"{SCHEMA}_{fat}_states\".states SET data = 'x' WHERE name = 'default'"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let back = reverse(&pool, SCHEMA, fat).await.unwrap();
    assert!(matches!(back, Outcome::Reversed(_)), "{back:?}");
    assert_eq!(row(&pool, fat, "default").await, Some(fat_state()));
    assert_eq!(row(&pool, &format!("{fat}-alpha"), "default").await, None);
    assert!(row(&pool, &format!("{fat}-alpha"), "reverted")
        .await
        .is_some());
}

#[tokio::test]
async fn a_leaf_that_already_has_state_aborts_the_whole_move() {
    let Some(pool) = pool().await else { return };
    let fat = "lm-taken";
    seed(&pool, fat).await;
    let taken = format!("\"{SCHEMA}_{fat}-beta_states\"");
    sqlx::query(&format!("CREATE SCHEMA {taken}"))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(&format!(
        "CREATE TABLE {taken}.states (id BIGSERIAL PRIMARY KEY, name TEXT UNIQUE, data TEXT)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(&format!(
        "INSERT INTO {taken}.states (name, data) VALUES ('default', '{{}}')"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let out = forward(&pool, SCHEMA, fat, &moves(fat)).await.unwrap();
    assert!(matches!(out, Outcome::Refused(_)), "{out:?}");
    assert_eq!(row(&pool, fat, "pre-leaf").await, None);
    assert_eq!(row(&pool, &format!("{fat}-alpha"), "default").await, None);
    assert!(!artifact(&pool, fat).await);
}
