#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ThreadBinding {
    pub attach_event_id: verlet_history::EventRecordId,
    pub payload: verlet_history::BindingAttachedPayload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ThreadBindingAnomaly {
    UndecodableAttached {
        event_id: verlet_history::EventRecordId,
        message: String,
    },
    UndecodableDetached {
        event_id: verlet_history::EventRecordId,
        message: String,
    },
    UnknownDetach {
        detach_event_id: verlet_history::EventRecordId,
        attach_event_id: verlet_history::EventRecordId,
    },
    AlreadyDetached {
        detach_event_id: verlet_history::EventRecordId,
        attach_event_id: verlet_history::EventRecordId,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ThreadBindingsFold {
    pub active: Vec<ThreadBinding>,
    pub anomalies: Vec<ThreadBindingAnomaly>,
}

impl ThreadBindingsFold {
    pub(crate) fn anomaly_message(&self) -> Option<String> {
        (!self.anomalies.is_empty())
            .then(|| format!("thread binding history is anomalous: {:?}", self.anomalies))
    }
}

fn binding_payload_identity_matches(
    left: &verlet_history::BindingAttachedPayload,
    right: &verlet_history::BindingAttachedPayload,
) -> bool {
    left.name == right.name
        && left.artifact_hash == right.artifact_hash
        && left.operations == right.operations
        && left.direct_tools == right.direct_tools
        && left.attachment_config == right.attachment_config
        && left.effect_class == right.effect_class
}

pub(crate) struct ThreadBindingDelta {
    pub removed_attach_event_ids: Vec<verlet_history::EventRecordId>,
    pub added_bindings: Vec<verlet_history::BindingAttachedPayload>,
}

pub(crate) fn reconcile_thread_bindings(
    active: &[ThreadBinding],
    desired: Vec<verlet_history::BindingAttachedPayload>,
) -> ThreadBindingDelta {
    let mut unmatched_active = vec![true; active.len()];
    let mut added_bindings = Vec::new();
    for desired_binding in desired {
        if let Some((index, _)) = active.iter().enumerate().find(|(index, active_binding)| {
            unmatched_active[*index]
                && binding_payload_identity_matches(&active_binding.payload, &desired_binding)
        }) {
            unmatched_active[index] = false;
        } else {
            added_bindings.push(desired_binding);
        }
    }
    let removed_attach_event_ids = active
        .iter()
        .zip(unmatched_active)
        .filter_map(|(binding, unmatched)| unmatched.then_some(binding.attach_event_id))
        .collect();
    ThreadBindingDelta {
        removed_attach_event_ids,
        added_bindings,
    }
}

#[derive(Default)]
struct BindingBatchObservation {
    attached_count: usize,
    attached_bindings: Vec<crate::agent::manifest_bind::AgentManifestOperationBinding>,
    has_detach: bool,
}

fn full_reemission_bind_batches(
    events: &[verlet_history::EventRecord],
) -> std::collections::HashMap<verlet_history::EventRecordId, usize> {
    let expected = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .filter_map(|event| {
            let mut bindings = serde_json::from_value::<
                Vec<crate::agent::manifest_bind::AgentManifestOperationBinding>,
            >(event.payload.get("operation_bindings")?.clone())
            .ok()?;
            bindings.sort();
            Some((event.id, bindings))
        })
        .collect::<std::collections::HashMap<_, _>>();
    let mut observed = std::collections::HashMap::<_, BindingBatchObservation>::new();
    for event in events {
        let Some(bind_event_id) = event.provenance.source_event_ids.first().copied() else {
            continue;
        };
        if !expected.contains_key(&bind_event_id) {
            continue;
        }
        match event.kind {
            verlet_history::EventKind::BindingAttached => {
                let observation = observed.entry(bind_event_id).or_default();
                observation.attached_count += 1;
                if let Ok(payload) = serde_json::from_value::<verlet_history::BindingAttachedPayload>(
                    event.payload.clone(),
                ) {
                    observation.attached_bindings.push(
                        crate::agent::manifest_bind::operation_binding_from_attached_payload(
                            payload,
                        ),
                    );
                }
            }
            verlet_history::EventKind::BindingDetached => {
                observed.entry(bind_event_id).or_default().has_detach = true;
            }
            _ => {}
        }
    }

    expected
        .into_iter()
        .filter_map(|(bind_event_id, expected_bindings)| {
            let observation = observed.get(&bind_event_id);
            let attached_count = observation.map_or(0, |batch| batch.attached_count);
            if observation.is_some_and(|batch| batch.has_detach)
                || attached_count != expected_bindings.len()
            {
                return None;
            }
            let mut attached_bindings = observation
                .map(|batch| batch.attached_bindings.clone())
                .unwrap_or_default();
            if attached_bindings.len() != attached_count {
                return None;
            }
            attached_bindings.sort();
            (attached_bindings == expected_bindings).then_some((bind_event_id, attached_count))
        })
        .collect()
}

pub fn fold_thread_bindings(events: &[verlet_history::EventRecord]) -> ThreadBindingsFold {
    let mut folded = ThreadBindingsFold::default();
    let mut inactive = std::collections::HashSet::new();
    // EMO-584 briefly wrote each bind receipt followed by a complete attached
    // snapshot and no detaches. Only receipt-proven full snapshots retain
    // generation semantics; every delta-era batch folds strictly by attach id.
    let full_reemission_batches = full_reemission_bind_batches(events);
    let mut applied_full_reemission_batches = std::collections::HashSet::new();

    for event in events {
        match event.kind {
            verlet_history::EventKind::ManifestBindCompleted
                if full_reemission_batches.get(&event.id) == Some(&0) =>
            {
                inactive.extend(
                    folded
                        .active
                        .drain(..)
                        .map(|binding| binding.attach_event_id),
                );
                applied_full_reemission_batches.insert(event.id);
            }
            verlet_history::EventKind::BindingAttached => {
                if let Some(bind_batch_id) = event.provenance.source_event_ids.first().copied()
                    && full_reemission_batches.contains_key(&bind_batch_id)
                    && applied_full_reemission_batches.insert(bind_batch_id)
                {
                    inactive.extend(
                        folded
                            .active
                            .drain(..)
                            .map(|binding| binding.attach_event_id),
                    );
                }
                match serde_json::from_value::<verlet_history::BindingAttachedPayload>(
                    event.payload.clone(),
                ) {
                    Ok(payload) => {
                        folded.active.push(ThreadBinding {
                            attach_event_id: event.id,
                            payload,
                        });
                    }
                    Err(err) => folded
                        .anomalies
                        .push(ThreadBindingAnomaly::UndecodableAttached {
                            event_id: event.id,
                            message: err.to_string(),
                        }),
                }
            }
            verlet_history::EventKind::BindingDetached => {
                let payload = match serde_json::from_value::<verlet_history::BindingDetachedPayload>(
                    event.payload.clone(),
                ) {
                    Ok(payload) => payload,
                    Err(err) => {
                        folded
                            .anomalies
                            .push(ThreadBindingAnomaly::UndecodableDetached {
                                event_id: event.id,
                                message: err.to_string(),
                            });
                        continue;
                    }
                };
                if let Some(index) = folded
                    .active
                    .iter()
                    .position(|binding| binding.attach_event_id == payload.attach_event_id)
                {
                    folded.active.remove(index);
                    inactive.insert(payload.attach_event_id);
                } else if inactive.contains(&payload.attach_event_id) {
                    folded
                        .anomalies
                        .push(ThreadBindingAnomaly::AlreadyDetached {
                            detach_event_id: event.id,
                            attach_event_id: payload.attach_event_id,
                        });
                } else {
                    folded.anomalies.push(ThreadBindingAnomaly::UnknownDetach {
                        detach_event_id: event.id,
                        attach_event_id: payload.attach_event_id,
                    });
                }
            }
            _ => {}
        }
    }

    folded
}

#[cfg(test)]
mod tests {
    fn event(
        sequence: i64,
        id: u128,
        kind: verlet_history::EventKind,
        payload: serde_json::Value,
    ) -> verlet_history::EventRecord {
        let coordinates =
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let mut event =
            verlet_history::NewEventRecord::witnessed(coordinates.clone(), kind, payload);
        event.id = verlet_history::EventRecordId::from_uuid(uuid::Uuid::from_u128(id));
        verlet_history::EventRecord::from_new(
            verlet_history::EventStreamId::for_thread(&coordinates),
            verlet_history::EventSequence::new(sequence),
            event,
        )
    }

    fn attached(name: &str) -> verlet_history::BindingAttachedPayload {
        verlet_history::BindingAttachedPayload {
            name: name.to_string(),
            artifact_hash: format!("sha256:{name}"),
            operations: Vec::new(),
            direct_tools: Vec::new(),
            attachment_config: verlet_history::BindingAttachmentConfig::default(),
            effect_class: verlet_history::BindingEffectClass::AtMostOnce,
            requested_by: "principal:operator".to_string(),
            decided_by: "principal:operator".to_string(),
            decision_event_id: None,
        }
    }

    fn attach_event(sequence: i64, id: u128, name: &str) -> verlet_history::EventRecord {
        event(
            sequence,
            id,
            verlet_history::EventKind::BindingAttached,
            serde_json::to_value(attached(name)).unwrap(),
        )
    }

    fn attach_event_for_bind(
        sequence: i64,
        id: u128,
        name: &str,
        bind_id: u128,
    ) -> verlet_history::EventRecord {
        let mut event = attach_event(sequence, id, name);
        event.provenance.source_event_ids = vec![verlet_history::EventRecordId::from_uuid(
            uuid::Uuid::from_u128(bind_id),
        )];
        event
    }

    fn bind_event(
        sequence: i64,
        id: u128,
        operation_bindings: serde_json::Value,
    ) -> verlet_history::EventRecord {
        event(
            sequence,
            id,
            verlet_history::EventKind::ManifestBindCompleted,
            serde_json::json!({"operation_bindings": operation_bindings}),
        )
    }

    fn detach_event(
        sequence: i64,
        id: u128,
        attach_event_id: verlet_history::EventRecordId,
    ) -> verlet_history::EventRecord {
        event(
            sequence,
            id,
            verlet_history::EventKind::BindingDetached,
            serde_json::to_value(verlet_history::BindingDetachedPayload {
                attach_event_id,
                requested_by: "principal:operator".to_string(),
                decided_by: "principal:operator".to_string(),
                decision_event_id: None,
            })
            .unwrap(),
        )
    }

    fn detach_event_for_bind(
        sequence: i64,
        id: u128,
        attach_event_id: verlet_history::EventRecordId,
        bind_id: u128,
    ) -> verlet_history::EventRecord {
        let mut event = detach_event(sequence, id, attach_event_id);
        event.provenance.source_event_ids = vec![verlet_history::EventRecordId::from_uuid(
            uuid::Uuid::from_u128(bind_id),
        )];
        event
    }

    #[test]
    fn empty_and_unrelated_streams_fold_to_empty() {
        assert_eq!(
            super::fold_thread_bindings(&[]),
            super::ThreadBindingsFold::default()
        );
        let unrelated = event(
            1,
            1,
            verlet_history::EventKind::TurnSubmitted,
            serde_json::json!({"turn_id": "turn-1"}),
        );

        assert_eq!(
            super::fold_thread_bindings(&[unrelated]),
            super::ThreadBindingsFold::default()
        );
    }

    #[test]
    fn attach_detach_interleavings_and_reattach_are_deterministic() {
        let first = attach_event(1, 1, "search-tools");
        let second = attach_event(2, 2, "file-tools");
        let unrelated = event(
            3,
            3,
            verlet_history::EventKind::TurnCompleted,
            serde_json::json!({"turn_id": "turn-1"}),
        );
        let detach_first = detach_event(4, 4, first.id);
        let reattached = attach_event(5, 5, "search-tools");
        let events = vec![
            first,
            second.clone(),
            unrelated,
            detach_first,
            reattached.clone(),
        ];

        let folded = super::fold_thread_bindings(&events);

        assert_eq!(folded, super::fold_thread_bindings(&events));
        assert_eq!(folded.anomalies, Vec::new());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| (binding.attach_event_id, binding.payload.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(second.id, "file-tools"), (reattached.id, "search-tools")]
        );
    }

    #[test]
    fn repeated_bind_batches_replace_same_operation_artifacts_in_stream_order() {
        let first_bind = bind_event(
            1,
            900,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let first_search = attach_event_for_bind(2, 400, "search-tools", 900);
        let first_files = attach_event_for_bind(3, 300, "file-tools", 900);
        let second_bind = bind_event(
            4,
            901,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools-v2"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let mut rebound_search = attach_event_for_bind(5, 200, "search-tools", 901);
        rebound_search.payload["artifact_hash"] = serde_json::json!("sha256:search-tools-v2");
        let rebound_files = attach_event_for_bind(6, 100, "file-tools", 901);

        let folded = super::fold_thread_bindings(&[
            first_bind,
            first_search,
            first_files,
            second_bind,
            rebound_search.clone(),
            rebound_files.clone(),
        ]);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| (binding.attach_event_id, binding.payload.name.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (rebound_search.id, "search-tools"),
                (rebound_files.id, "file-tools"),
            ]
        );
    }

    #[test]
    fn emo_584_full_reemission_fixture_folds_to_the_latest_complete_generation() {
        let first_bind = bind_event(
            1,
            900,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let first_search = attach_event_for_bind(2, 400, "search-tools", 900);
        let first_files = attach_event_for_bind(3, 300, "file-tools", 900);
        let second_bind = bind_event(
            4,
            901,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools-v2"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let mut rebound_search = attach_event_for_bind(5, 200, "search-tools", 901);
        rebound_search.payload["artifact_hash"] = serde_json::json!("sha256:search-tools-v2");
        let rebound_files = attach_event_for_bind(6, 100, "file-tools", 901);

        let folded = super::fold_thread_bindings(&[
            first_bind,
            first_search,
            first_files,
            second_bind,
            rebound_search.clone(),
            rebound_files.clone(),
        ]);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            vec![rebound_search.id, rebound_files.id]
        );
    }

    #[test]
    fn add_only_delta_after_historical_generations_preserves_prior_attach_ids() {
        let first_bind = bind_event(
            1,
            900,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let first_search = attach_event_for_bind(2, 400, "search-tools", 900);
        let first_files = attach_event_for_bind(3, 300, "file-tools", 900);
        let delta_bind = bind_event(
            4,
            901,
            serde_json::json!([
                {"name": "clock-tools", "artifact_hash": "sha256:clock-tools"},
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let added_clock = attach_event_for_bind(5, 200, "clock-tools", 901);

        let folded = super::fold_thread_bindings(&[
            first_bind,
            first_search.clone(),
            first_files.clone(),
            delta_bind,
            added_clock.clone(),
        ]);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            vec![first_search.id, first_files.id, added_clock.id]
        );
    }

    #[test]
    fn modern_changed_delta_is_not_mistaken_for_a_full_reemission() {
        let first_bind = bind_event(
            1,
            900,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let first_search = attach_event_for_bind(2, 400, "search-tools", 900);
        let first_files = attach_event_for_bind(3, 300, "file-tools", 900);
        let second_bind = bind_event(
            4,
            901,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools-v2"},
                {"name": "file-tools", "artifact_hash": "sha256:file-tools"}
            ]),
        );
        let mut second_search = attach_event_for_bind(5, 200, "search-tools", 901);
        second_search.payload["artifact_hash"] = serde_json::json!("sha256:search-tools-v2");
        let detach_first_search = detach_event_for_bind(6, 100, first_search.id, 901);

        let folded = super::fold_thread_bindings(&[
            first_bind,
            first_search,
            first_files.clone(),
            second_bind,
            second_search.clone(),
            detach_first_search,
        ]);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            vec![first_files.id, second_search.id]
        );
    }

    #[test]
    fn many_emo_584_full_reemissions_keep_only_the_latest_generation() {
        let mut events = Vec::new();
        let mut sequence = 1;
        let mut expected_ids = Vec::new();
        for generation in 0..12_u128 {
            let bind_id = 1_000 + generation;
            let search_name = format!("search-tools-{generation}");
            let file_name = format!("file-tools-{generation}");
            events.push(bind_event(
                sequence,
                bind_id,
                serde_json::json!([
                    {
                        "name": search_name,
                        "artifact_hash": format!("sha256:search-tools-{generation}")
                    },
                    {
                        "name": file_name,
                        "artifact_hash": format!("sha256:file-tools-{generation}")
                    }
                ]),
            ));
            sequence += 1;
            let search =
                attach_event_for_bind(sequence, 2_000 + generation * 2, &search_name, bind_id);
            sequence += 1;
            let files =
                attach_event_for_bind(sequence, 2_001 + generation * 2, &file_name, bind_id);
            sequence += 1;
            expected_ids = vec![search.id, files.id];
            events.extend([search, files]);
        }

        let folded = super::fold_thread_bindings(&events);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            expected_ids
        );
    }

    #[test]
    fn emo_584_empty_reemission_retires_the_prior_generation() {
        let first_bind = bind_event(
            1,
            900,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"}
            ]),
        );
        let first_search = attach_event_for_bind(2, 400, "search-tools", 900);
        let empty_bind = bind_event(3, 901, serde_json::json!([]));

        let folded = super::fold_thread_bindings(&[first_bind, first_search, empty_bind]);

        assert!(folded.anomalies.is_empty());
        assert!(folded.active.is_empty());
    }

    #[test]
    fn ordinary_attaches_remain_distinct_until_their_ids_are_detached() {
        let first = attach_event(1, 1, "search-tools");
        let second = attach_event(2, 2, "search-tools");

        let folded = super::fold_thread_bindings(&[first.clone(), second.clone()]);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
    }

    #[test]
    fn distinct_artifacts_with_the_same_operation_name_remain_active() {
        let first = attach_event_for_bind(1, 1, "search-tools", 900);
        let mut second_payload = attached("search-tools");
        second_payload.artifact_hash = "sha256:search-tools-v2".to_string();
        let mut second = event(
            2,
            2,
            verlet_history::EventKind::BindingAttached,
            serde_json::to_value(second_payload).unwrap(),
        );
        second.provenance.source_event_ids = first.provenance.source_event_ids.clone();

        let folded = super::fold_thread_bindings(&[first.clone(), second.clone()]);

        assert!(folded.anomalies.is_empty());
        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            vec![first.id, second.id]
        );
    }

    #[test]
    fn unknown_and_already_detached_ids_are_typed_anomalies() {
        let attached = attach_event(1, 1, "search-tools");
        let detached = detach_event(2, 2, attached.id);
        let repeated = detach_event(3, 3, attached.id);
        let unknown_id = verlet_history::EventRecordId::from_uuid(uuid::Uuid::from_u128(99));
        let unknown = detach_event(4, 4, unknown_id);

        let folded =
            super::fold_thread_bindings(&[attached, detached, repeated.clone(), unknown.clone()]);

        assert!(folded.active.is_empty());
        assert_eq!(
            folded.anomalies,
            vec![
                super::ThreadBindingAnomaly::AlreadyDetached {
                    detach_event_id: repeated.id,
                    attach_event_id: verlet_history::EventRecordId::from_uuid(
                        uuid::Uuid::from_u128(1)
                    ),
                },
                super::ThreadBindingAnomaly::UnknownDetach {
                    detach_event_id: unknown.id,
                    attach_event_id: unknown_id,
                },
            ]
        );
    }

    #[test]
    fn undecodable_binding_payloads_are_typed_anomalies() {
        let bad_attach = event(
            1,
            1,
            verlet_history::EventKind::BindingAttached,
            serde_json::json!({"name": "missing-fields"}),
        );
        let bad_detach = event(
            2,
            2,
            verlet_history::EventKind::BindingDetached,
            serde_json::json!({"attach_event_id": 42}),
        );

        let folded = super::fold_thread_bindings(&[bad_attach.clone(), bad_detach.clone()]);

        assert!(folded.active.is_empty());
        assert_eq!(folded.anomalies.len(), 2);
        assert!(matches!(
            &folded.anomalies[0],
            super::ThreadBindingAnomaly::UndecodableAttached { event_id, message }
                if *event_id == bad_attach.id && !message.is_empty()
        ));
        assert!(matches!(
            &folded.anomalies[1],
            super::ThreadBindingAnomaly::UndecodableDetached { event_id, message }
                if *event_id == bad_detach.id && !message.is_empty()
        ));
    }

    #[test]
    fn undecodable_attach_cannot_prove_a_full_reemission_batch() {
        let old_bind = bind_event(
            1,
            900,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools"}
            ]),
        );
        let old = attach_event_for_bind(2, 1, "search-tools", 900);
        let new_bind = bind_event(
            3,
            901,
            serde_json::json!([
                {"name": "search-tools", "artifact_hash": "sha256:search-tools-v2"}
            ]),
        );
        let mut bad_attach = event(
            4,
            2,
            verlet_history::EventKind::BindingAttached,
            serde_json::json!({"name": "missing-fields"}),
        );
        bad_attach.provenance.source_event_ids = vec![verlet_history::EventRecordId::from_uuid(
            uuid::Uuid::from_u128(901),
        )];

        let folded = super::fold_thread_bindings(&[old_bind, old, new_bind, bad_attach.clone()]);

        assert_eq!(
            folded
                .active
                .iter()
                .map(|binding| binding.attach_event_id)
                .collect::<Vec<_>>(),
            vec![verlet_history::EventRecordId::from_uuid(
                uuid::Uuid::from_u128(1)
            )]
        );
        assert!(matches!(
            folded.anomalies.as_slice(),
            [super::ThreadBindingAnomaly::UndecodableAttached { event_id, .. }]
                if *event_id == bad_attach.id
        ));
    }

    #[test]
    fn adversarial_binding_payloads_report_anomalies_without_panicking() {
        let garbage = [
            serde_json::Value::Null,
            serde_json::json!(false),
            serde_json::json!(17),
            serde_json::json!("not-an-object"),
            serde_json::json!(["not", "an", "object"]),
            serde_json::json!({"unexpected": true}),
        ];
        let mut events = Vec::new();
        for (index, payload) in garbage.into_iter().enumerate() {
            let sequence = index as i64 * 2 + 1;
            events.push(event(
                sequence,
                sequence as u128,
                verlet_history::EventKind::BindingAttached,
                payload.clone(),
            ));
            events.push(event(
                sequence + 1,
                sequence as u128 + 1,
                verlet_history::EventKind::BindingDetached,
                payload,
            ));
        }

        let folded = super::fold_thread_bindings(&events);

        assert!(folded.active.is_empty());
        assert_eq!(folded.anomalies.len(), events.len());
        assert_eq!(
            folded
                .anomalies
                .iter()
                .filter(|anomaly| matches!(
                    anomaly,
                    super::ThreadBindingAnomaly::UndecodableAttached { .. }
                ))
                .count(),
            events.len() / 2
        );
        assert_eq!(
            folded
                .anomalies
                .iter()
                .filter(|anomaly| matches!(
                    anomaly,
                    super::ThreadBindingAnomaly::UndecodableDetached { .. }
                ))
                .count(),
            events.len() / 2
        );
    }
}
