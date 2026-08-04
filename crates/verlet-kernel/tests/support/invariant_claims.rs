#![allow(dead_code)]

use super::{InvariantViolation, ScenarioInvariant, ScenarioWorld};
use async_trait::async_trait;
use std::collections::BTreeMap;

/// ADR 0004 invariant 6: once a scenario is quiescent, every ingress claim
/// visible anywhere in the normalized transcript has exactly one settle.
pub struct Inv6ClaimsSettle;

#[async_trait]
impl ScenarioInvariant for Inv6ClaimsSettle {
    fn name(&self) -> &'static str {
        "inv6-claims-settle"
    }

    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation> {
        // The skeleton exposes shutdown as its only explicit quiescence bit.
        // Intermediate steps may legitimately contain a live unsettled claim.
        if !world.shut_down || world.queue.is_none() {
            return Vec::new();
        }

        claim_settle_violations(world.transcript)
            .into_iter()
            .map(|detail| InvariantViolation {
                invariant: self.name(),
                detail,
            })
            .collect()
    }
}

fn claim_settle_violations(transcript: &super::transcript::NormalizedTranscript) -> Vec<String> {
    let mut claim_settles = BTreeMap::<String, usize>::new();
    for item in &transcript.items {
        if item.kind != "event" {
            continue;
        }
        match item.value.get("kind").and_then(serde_json::Value::as_str) {
            Some("io.ingress.claimed") => {
                if let Some(claim_id) = item.value.get("id").and_then(serde_json::Value::as_str) {
                    claim_settles.entry(claim_id.to_string()).or_default();
                }
            }
            Some("io.ingress.settled") => {
                if let Some(claim_id) = item
                    .value
                    .get("payload")
                    .and_then(|payload| payload.get("claim_event_id"))
                    .and_then(serde_json::Value::as_str)
                {
                    *claim_settles.entry(claim_id.to_string()).or_default() += 1;
                }
            }
            _ => {}
        }
    }

    claim_settles
        .into_iter()
        .filter_map(|(claim_id, settles)| match settles {
            1 => None,
            0 => Some(format!(
                "claim {claim_id} remains unsettled after global quiescence"
            )),
            count => Some(format!(
                "claim {claim_id} has {count} settles after global quiescence"
            )),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::transcript::{NormalizedTranscript, NormalizedTranscriptItem};
    use super::*;
    use serde_json::json;

    #[test]
    fn inv6_scans_claims_globally_across_streams() {
        let transcript = NormalizedTranscript {
            items: vec![
                NormalizedTranscriptItem {
                    kind: "event".to_string(),
                    label: "parent".to_string(),
                    value: json!({
                        "id": "$event-1",
                        "kind": "io.ingress.claimed",
                        "stream_id": "control:$thread-1",
                        "payload": {},
                    }),
                },
                NormalizedTranscriptItem {
                    kind: "event".to_string(),
                    label: "child".to_string(),
                    value: json!({
                        "id": "$event-2",
                        "kind": "io.ingress.claimed",
                        "stream_id": "control:$thread-2",
                        "payload": {},
                    }),
                },
                NormalizedTranscriptItem {
                    kind: "event".to_string(),
                    label: "parent".to_string(),
                    value: json!({
                        "id": "$event-3",
                        "kind": "io.ingress.settled",
                        "stream_id": "control:$thread-1",
                        "payload": {"claim_event_id": "$event-1"},
                    }),
                },
            ],
        };

        assert_eq!(
            claim_settle_violations(&transcript),
            vec!["claim $event-2 remains unsettled after global quiescence"]
        );
    }
}
