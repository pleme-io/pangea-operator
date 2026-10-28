use std::time::Duration;

use pangea_leaf::queue::{LeafAction, LeafWork};
use pangea_operator::leaf_queue::{work_on, JetStreamQueue, LeafQueue, Published, QueueSettings};

fn nats_url() -> Option<String> {
    let url = std::env::var("PANGEA_TEST_NATS_URL")
        .ok()
        .filter(|u| !u.is_empty());
    if url.is_none() && std::env::var("PANGEA_REQUIRE_TEST_NATS").as_deref() == Ok("1") {
        panic!("PANGEA_REQUIRE_TEST_NATS=1 but PANGEA_TEST_NATS_URL is not set");
    }
    url
}

fn settings(tag: &str, ack_wait: Duration) -> QueueSettings {
    let run = [
        tag,
        &std::process::id().to_string(),
        &std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string(),
    ]
    .join("_");
    QueueSettings {
        stream: ["PANGEA_LEAF_TEST", &run].join("_"),
        subject_root: ["pangea", "leaftest", &run].join("."),
        ack_wait,
        max_deliver: 5,
        duplicate_window: Duration::from_secs(600),
        max_ack_pending: 8,
    }
}

fn work(leaf: &str, digest: &str) -> LeafWork {
    LeafWork {
        leaf: leaf.into(),
        credential: "3f2a9c".into(),
        action: LeafAction::Plan,
        digest: digest.into(),
        cost: 3,
    }
}

#[tokio::test]
async fn a_republished_action_is_dropped_by_the_stream() {
    let Some(url) = nats_url() else { return };
    let q = JetStreamQueue::connect(&url, settings("dup", Duration::from_secs(5)))
        .await
        .unwrap();
    assert_eq!(
        q.publish(&work("org-a", "d1")).await.unwrap(),
        Published::Queued
    );
    assert_eq!(
        q.publish(&work("org-a", "d1")).await.unwrap(),
        Published::Duplicate
    );
    assert_eq!(
        q.publish(&work("org-a", "d2")).await.unwrap(),
        Published::Queued
    );
    let claims = q.pull("3f2a9c", 10).await.unwrap();
    assert_eq!(claims.len(), 2);
    for c in claims {
        c.ack().await.unwrap();
    }
}

#[tokio::test]
async fn a_killed_workers_message_reaches_the_other_worker_after_ack_wait() {
    let Some(url) = nats_url() else { return };
    let s = settings("redeliver", Duration::from_secs(2));
    let leader = JetStreamQueue::connect(&url, s.clone()).await.unwrap();
    let worker_a = JetStreamQueue::connect(&url, s.clone()).await.unwrap();
    let worker_b = JetStreamQueue::connect(&url, s).await.unwrap();
    leader.publish(&work("org-a", "d1")).await.unwrap();
    let held = worker_a.pull("3f2a9c", 1).await.unwrap();
    assert_eq!(held.len(), 1);
    drop(held);
    drop(worker_a);
    assert!(worker_b.pull("3f2a9c", 1).await.unwrap().is_empty());
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let mut got = worker_b.pull("3f2a9c", 1).await.unwrap();
    assert_eq!(got.len(), 1);
    let claim = got.pop().unwrap();
    assert_eq!(claim.work().leaf, "org-a");
    assert_eq!(claim.delivered(), 2);
    claim.ack().await.unwrap();
}

#[tokio::test]
async fn in_progress_acks_are_the_heartbeat_that_keeps_a_live_workers_message() {
    let Some(url) = nats_url() else { return };
    let s = settings("wip", Duration::from_secs(2));
    let worker_a = JetStreamQueue::connect(&url, s.clone()).await.unwrap();
    let worker_b = JetStreamQueue::connect(&url, s.clone()).await.unwrap();
    worker_a.publish(&work("org-a", "d1")).await.unwrap();
    let claim = worker_a.pull("3f2a9c", 1).await.unwrap().pop().unwrap();
    let out = work_on(claim, s.heartbeat(), async {
        tokio::time::sleep(Duration::from_millis(4_500)).await;
        worker_b.pull("3f2a9c", 1).await.map(|c| c.len())
    })
    .await
    .unwrap();
    assert_eq!(out.unwrap(), 0);
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    assert!(worker_b.pull("3f2a9c", 1).await.unwrap().is_empty());
}
