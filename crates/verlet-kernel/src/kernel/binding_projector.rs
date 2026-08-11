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

pub fn fold_thread_bindings(events: &[verlet_history::EventRecord]) -> ThreadBindingsFold {
    let mut folded = ThreadBindingsFold::default();
    let mut detached = std::collections::HashSet::new();

    for event in events {
        match event.kind {
            verlet_history::EventKind::BindingAttached => {
                match serde_json::from_value::<verlet_history::BindingAttachedPayload>(
                    event.payload.clone(),
                ) {
                    Ok(payload) => folded.active.push(ThreadBinding {
                        attach_event_id: event.id,
                        payload,
                    }),
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
                    detached.insert(payload.attach_event_id);
                } else if detached.contains(&payload.attach_event_id) {
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
}
