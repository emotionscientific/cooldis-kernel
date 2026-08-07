#![allow(dead_code)]

//! Scenario invariants for ADR 0004 Decision 4 (inv7 and inv8).
//!
//! Both invariants consume normalized transcript events, which the runner must
//! emit only after reading the corresponding durable store records. Fork
//! claims are themselves reservation witnesses: the `fork` intent carries
//! `child_thread_id`, and `thread.spawned.fork.claim_event_id` joins creation
//! to that reservation.
//!
//! Root reservations live in daemon route SQLite rather than `RuntimeStore`,
//! so inv8 also requires the EMO-403 runner to emit a receipt immediately after
//! observing the committed row and before collecting any record for that
//! thread:
//!
//! ```text
//! kind = "receipt", label = "thread.reservation"
//! value = {"kind":"thread.reservation","thread_id":"...","reservation_kind":"initial_route"}
//! ```
//!
//! This receipt is a durable-truth probe, not a host-internal witness. The
//! shape is a deviation for architect ratification because `ScenarioWorld`
//! intentionally has no daemon route-store handle and the runner lands in
//! EMO-403.

pub const INV7_ONE_CHILD_PER_FORK_CLAIM: &str = "inv7-one-child-per-fork-claim";
pub const INV8_RESERVED_BEFORE_CREATED: &str = "inv8-reserved-before-created";

#[derive(Clone, Copy, Debug, Default)]
pub struct OneChildPerForkClaimInvariant;

#[derive(Clone, Copy, Debug, Default)]
pub struct ReservedBeforeCreatedInvariant;

pub fn fork_invariants_v1() -> Vec<Box<dyn crate::support::scenario::ScenarioInvariant>> {
    vec![
        Box::new(OneChildPerForkClaimInvariant),
        Box::new(ReservedBeforeCreatedInvariant),
    ]
}

#[async_trait::async_trait]
impl crate::support::scenario::ScenarioInvariant for OneChildPerForkClaimInvariant {
    fn name(&self) -> &'static str {
        INV7_ONE_CHILD_PER_FORK_CLAIM
    }

    async fn check(
        &self,
        world: &crate::support::scenario::ScenarioWorld<'_>,
    ) -> Vec<crate::support::scenario::InvariantViolation> {
        inv7_violations(&world.transcript.items)
            .into_iter()
            .map(|detail| crate::support::scenario::InvariantViolation {
                invariant: self.name(),
                detail,
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl crate::support::scenario::ScenarioInvariant for ReservedBeforeCreatedInvariant {
    fn name(&self) -> &'static str {
        INV8_RESERVED_BEFORE_CREATED
    }

    async fn check(
        &self,
        world: &crate::support::scenario::ScenarioWorld<'_>,
    ) -> Vec<crate::support::scenario::InvariantViolation> {
        inv8_violations(&world.transcript.items)
            .into_iter()
            .map(|detail| crate::support::scenario::InvariantViolation {
                invariant: self.name(),
                detail,
            })
            .collect()
    }
}

fn event_kind(item: &crate::support::transcript::NormalizedTranscriptItem) -> Option<&str> {
    (item.kind == "event")
        .then(|| item.value.get("kind").and_then(serde_json::Value::as_str))
        .flatten()
}

fn inv7_violations(items: &[crate::support::transcript::NormalizedTranscriptItem]) -> Vec<String> {
    let mut reservations = std::collections::BTreeMap::<String, String>::new();
    let mut spawned_children = std::collections::BTreeMap::<String, Vec<String>>::new();
    let mut violations = Vec::new();

    for item in items {
        match event_kind(item) {
            Some("io.ingress.claimed")
                if item
                    .value
                    .pointer("/payload/intent/outcome")
                    .and_then(serde_json::Value::as_str)
                    == Some("fork") =>
            {
                let Some(claim_id) = item.value.get("id").and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(child_thread_id) = item
                    .value
                    .pointer("/payload/intent/child_thread_id")
                    .and_then(serde_json::Value::as_str)
                else {
                    violations.push(format!(
                        "fork claim {claim_id} does not reserve a child thread id"
                    ));
                    continue;
                };
                if let Some(previous) =
                    reservations.insert(claim_id.to_string(), child_thread_id.to_string())
                    && previous != child_thread_id
                {
                    violations.push(format!(
                        "fork claim {claim_id} reserves both {previous} and {child_thread_id}"
                    ));
                }
            }
            Some("thread.spawned") => {
                let Some(claim_id) = item
                    .value
                    .pointer("/payload/fork/claim_event_id")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(child_thread_id) = item
                    .value
                    .pointer("/payload/child_thread_id")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                spawned_children
                    .entry(claim_id.to_string())
                    .or_default()
                    .push(child_thread_id.to_string());
            }
            _ => {}
        }
    }

    for (claim_id, children) in spawned_children {
        let Some(reserved) = reservations.get(&claim_id) else {
            violations.push(format!(
                "fork claim {claim_id} has spawned evidence without a reservation"
            ));
            continue;
        };
        if children.len() > 1 {
            violations.push(format!(
                "fork claim {claim_id} has multiple topology joins: {}",
                children.into_iter().collect::<Vec<_>>().join(", ")
            ));
        } else if children.first() != Some(reserved) {
            violations.push(format!(
                "fork claim {claim_id} reserved {reserved}, but spawned {}",
                children.into_iter().next().unwrap_or_default()
            ));
        }
    }
    violations
}

fn inv8_violations(items: &[crate::support::transcript::NormalizedTranscriptItem]) -> Vec<String> {
    let mut reservation_index = std::collections::BTreeMap::<String, usize>::new();
    let mut first_record_index = std::collections::BTreeMap::<String, usize>::new();

    for (index, item) in items.iter().enumerate() {
        if event_kind(item) == Some("io.ingress.claimed")
            && item
                .value
                .pointer("/payload/intent/outcome")
                .and_then(serde_json::Value::as_str)
                == Some("fork")
            && let Some(thread_id) = item
                .value
                .pointer("/payload/intent/child_thread_id")
                .and_then(serde_json::Value::as_str)
        {
            reservation_index
                .entry(thread_id.to_string())
                .or_insert(index);
        }
        if item.kind == "receipt"
            && item.value.get("kind").and_then(serde_json::Value::as_str)
                == Some("thread.reservation")
            && let Some(thread_id) = item
                .value
                .get("thread_id")
                .and_then(serde_json::Value::as_str)
        {
            reservation_index
                .entry(thread_id.to_string())
                .or_insert(index);
        }
        if item.kind == "event"
            && let Some(thread_id) = item
                .value
                .pointer("/coordinates/thread_id")
                .and_then(serde_json::Value::as_str)
        {
            first_record_index
                .entry(thread_id.to_string())
                .or_insert(index);
        }
    }

    first_record_index
        .into_iter()
        .filter_map(|(thread_id, first_index)| match reservation_index.get(&thread_id) {
            Some(reserved_index) if *reserved_index < first_index => None,
            Some(reserved_index) => Some(format!(
                "thread {thread_id} first record at transcript item {first_index} does not follow its reservation at item {reserved_index}"
            )),
            None => Some(format!(
                "thread {thread_id} has a durable record without a reservation witness"
            )),
        })
        .collect()
}

#[cfg(test)]
mod tests {

    fn event(value: serde_json::Value) -> crate::support::transcript::NormalizedTranscriptItem {
        crate::support::transcript::NormalizedTranscriptItem {
            kind: "event".to_string(),
            label: "durable".to_string(),
            value,
        }
    }

    fn receipt(value: serde_json::Value) -> crate::support::transcript::NormalizedTranscriptItem {
        crate::support::transcript::NormalizedTranscriptItem {
            kind: "receipt".to_string(),
            label: "thread.reservation".to_string(),
            value,
        }
    }

    #[test]
    fn inv7_rejects_a_spawn_that_does_not_match_the_claim_reservation() {
        let items = vec![
            event(serde_json::json!({
                "id": "$claim-1",
                "kind": "io.ingress.claimed",
                "payload": {"intent": {
                    "outcome": "fork",
                    "child_thread_id": "$thread-1"
                }}
            })),
            event(serde_json::json!({
                "kind": "thread.spawned",
                "payload": {
                    "child_thread_id": "$thread-2",
                    "fork": {"claim_event_id": "$claim-1"}
                }
            })),
        ];

        assert_eq!(
            crate::support::invariant_forks::inv7_violations(&items),
            vec!["fork claim $claim-1 reserved $thread-1, but spawned $thread-2"]
        );
    }

    #[test]
    fn inv8_accepts_fork_and_root_records_only_after_their_reservations() {
        let items = vec![
            receipt(serde_json::json!({
                "kind": "thread.reservation",
                "thread_id": "$parent",
                "reservation_kind": "initial_route"
            })),
            event(serde_json::json!({
                "id": "$claim-1",
                "kind": "io.ingress.claimed",
                "coordinates": {"thread_id": "$parent"},
                "payload": {"intent": {
                    "outcome": "fork",
                    "child_thread_id": "$child"
                }}
            })),
            event(serde_json::json!({
                "kind": "session.entry.appended",
                "coordinates": {"thread_id": "$child"},
                "payload": {}
            })),
            receipt(serde_json::json!({
                "kind": "thread.reservation",
                "thread_id": "$root",
                "reservation_kind": "initial_route"
            })),
            event(serde_json::json!({
                "kind": "session.entry.appended",
                "coordinates": {"thread_id": "$root"},
                "payload": {}
            })),
        ];

        assert!(crate::support::invariant_forks::inv8_violations(&items).is_empty());
    }

    #[test]
    fn inv8_rejects_creation_before_reservation() {
        let items = vec![
            event(serde_json::json!({
                "kind": "session.entry.appended",
                "coordinates": {"thread_id": "$root"},
                "payload": {}
            })),
            receipt(serde_json::json!({
                "kind": "thread.reservation",
                "thread_id": "$root",
                "reservation_kind": "initial_route"
            })),
        ];

        assert_eq!(
            crate::support::invariant_forks::inv8_violations(&items),
            vec![
                "thread $root first record at transcript item 0 does not follow its reservation at item 1"
            ]
        );
    }
}
