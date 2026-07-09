use crate::{CouplingRole, EventKind};
use serde::{Deserialize, Serialize};

pub const COUPLING_TEMPLATE_CATALOG_SCHEMA_V1: &str = "cooldis.coupling.template_catalog/1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingTemplateCatalogV1 {
    pub schema: String,
    pub templates: Vec<CouplingTemplateV1>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingTemplateV1 {
    pub id: String,
    pub maturity: CouplingTemplateMaturity,
    pub role: CouplingRole,
    pub runtime_executable: bool,
    pub trigger_kinds: Vec<EventKind>,
    pub source: CouplingTemplateStreamPattern,
    pub sink: CouplingTemplateStreamPattern,
    pub must_have: bool,
    pub channel_decision_required: bool,
    pub summary: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CouplingTemplateMaturity {
    KernelBacked,
    InterfaceOnly,
    ReferenceOnly,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CouplingTemplateStreamPattern {
    pub stream: String,
    pub kinds: Vec<EventKind>,
}

pub fn coupling_template_catalog_v1() -> CouplingTemplateCatalogV1 {
    CouplingTemplateCatalogV1 {
        schema: COUPLING_TEMPLATE_CATALOG_SCHEMA_V1.to_string(),
        templates: vec![
            template(
                "std::queue.task",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::TurnSubmitted],
                "thread",
                &[EventKind::TurnSubmitted],
                "control",
                &[EventKind::TurnWaiting, EventKind::CouplingRunCompleted],
                true,
                false,
                "Turn an event into durable queued work with a later completion fact.",
            ),
            template(
                "std::queue.completion_callback",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[
                    EventKind::CouplingRunCompleted,
                    EventKind::ToolCallCompleted,
                ],
                "control",
                &[EventKind::CouplingRunCompleted],
                "control",
                &[EventKind::TurnContinueRequested, EventKind::LoopCompleted],
                true,
                false,
                "React to completed queued work by continuing, notifying, or closing the loop.",
            ),
            template(
                "std::context.spill",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Projection,
                &[EventKind::ContextCompileCompleted],
                "thread",
                &[EventKind::ContextCompileCompleted],
                "derived:context",
                &[
                    EventKind::ContextSummaryCompleted,
                    EventKind::ContextReadPlanSet,
                ],
                true,
                false,
                "Project over-budget context into durable context facts for later assembly.",
            ),
            template(
                "std::context.truncate",
                CouplingTemplateMaturity::KernelBacked,
                CouplingRole::Controller,
                &[EventKind::ContextCompileCompleted],
                "thread",
                &[EventKind::ContextCompileCompleted],
                "control",
                &[EventKind::ContextReadPlanSet],
                false,
                false,
                "Select a bounded read plan when the context budget requires dropping raw ranges.",
            ),
            template(
                "std::context.summarize",
                CouplingTemplateMaturity::KernelBacked,
                CouplingRole::Projection,
                &[EventKind::ContextCompileCompleted, EventKind::TurnCompleted],
                "thread",
                &[EventKind::SessionEntryAppended, EventKind::TurnCompleted],
                "derived:context",
                &[
                    EventKind::ContextSummaryCompleted,
                    EventKind::ContextReadPlanSet,
                ],
                false,
                false,
                "Discharge a summary checkpoint plus a read plan over a witnessed source range.",
            ),
            template(
                "std::memory.extract",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Projection,
                &[EventKind::TurnCompleted, EventKind::ToolCallCompleted],
                "thread",
                &[EventKind::TurnCompleted, EventKind::ToolCallCompleted],
                "derived:memory",
                &[EventKind::ContextSummaryCompleted],
                false,
                false,
                "Extract durable memory facts from completed turns or tool results.",
            ),
            template(
                "std::memory.recall",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Projection,
                &[EventKind::TurnSubmitted, EventKind::ContextCompileCompleted],
                "derived:memory",
                &[EventKind::ContextSummaryCompleted],
                "derived:context",
                &[EventKind::ContextReadPlanSet],
                false,
                false,
                "Select memory facts for future context assembly without making memory a primitive.",
            ),
            template(
                "std::permission.approval_gate",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::ToolCallRequested],
                "thread",
                &[EventKind::ToolCallRequested],
                "control",
                &[EventKind::ApprovalRequested, EventKind::ToolCallSuspended],
                false,
                false,
                "Suspend a tool call behind an abstract approval fact; channel-specific HITL delivery remains deferred.",
            ),
            template(
                "std::permission.tool_gate",
                CouplingTemplateMaturity::KernelBacked,
                CouplingRole::Controller,
                &[EventKind::ToolCallRequested],
                "thread",
                &[EventKind::ToolCallRequested],
                "control",
                &[EventKind::ToolCallDecision, EventKind::ToolCallSuspended],
                false,
                false,
                "Allow, deny, or suspend tool calls through durable control-stream facts.",
            ),
            template(
                "std::prompt.steer",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::TurnCompleted, EventKind::ApprovalResolved],
                "thread",
                &[EventKind::TurnCompleted],
                "control",
                &[
                    EventKind::TurnContinueRequested,
                    EventKind::ContextReadPlanSet,
                ],
                false,
                false,
                "Steer future turns by writing witnessed control facts instead of hidden prompt hooks.",
            ),
            template(
                "std::prompt.dynamic_instructions",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Projection,
                &[
                    EventKind::ManifestBindCompleted,
                    EventKind::ContextCompileCompleted,
                ],
                "thread",
                &[
                    EventKind::ManifestBindCompleted,
                    EventKind::ContextCompileCompleted,
                ],
                "derived:context",
                &[EventKind::ContextReadPlanSet],
                false,
                false,
                "Select versioned instruction material for future context assembly.",
            ),
            template(
                "std::io.channel_ingress",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::TurnSubmitted],
                "thread",
                &[EventKind::TurnSubmitted],
                "control",
                &[EventKind::TurnWaiting, EventKind::TurnContinueRequested],
                false,
                true,
                "Map channel ingress into durable turn/control facts with channel authority outside the kernel.",
            ),
            template(
                "std::io.channel_egress",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::TurnCompleted, EventKind::LoopCompleted],
                "thread",
                &[EventKind::TurnCompleted],
                "control",
                &[
                    EventKind::CouplingRunCompleted,
                    EventKind::CouplingRunFailed,
                ],
                false,
                true,
                "Emit channel egress as a durable operation/coupling result rather than a hidden callback.",
            ),
            template(
                "std::schedule.cron",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::TimerFired],
                "control",
                &[
                    EventKind::MandateStarted,
                    EventKind::MandateRevoked,
                    EventKind::TimerFired,
                ],
                "control",
                &[
                    EventKind::TurnContinueRequested,
                    EventKind::LoopBudgetExhausted,
                ],
                false,
                false,
                "Turn witnessed timer firings for a standing mandate into bounded future turn requests.",
            ),
            template(
                "std::supervisor.spawn",
                CouplingTemplateMaturity::KernelBacked,
                CouplingRole::Controller,
                &[EventKind::TurnSubmitted, EventKind::ToolCallRequested],
                "thread",
                &[EventKind::TurnSubmitted, EventKind::ToolCallRequested],
                "control",
                &[
                    EventKind::ThreadSpawnRequested,
                    EventKind::TurnWaiting,
                    EventKind::CouplingRunCompleted,
                ],
                false,
                false,
                "Spawn supervised child work through the thread/turn kernel package.",
            ),
            template(
                "std::supervisor.child_completion",
                CouplingTemplateMaturity::KernelBacked,
                CouplingRole::Controller,
                &[EventKind::TurnCompleted, EventKind::CouplingRunCompleted],
                "thread",
                &[EventKind::TurnCompleted],
                "control",
                &[EventKind::TurnContinueRequested, EventKind::LoopCompleted],
                false,
                false,
                "Join child thread completion back into a parent continuation or terminal fact.",
            ),
            template(
                "std::retry.with_budget",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Controller,
                &[EventKind::CouplingRunFailed, EventKind::ToolCallCompleted],
                "control",
                &[EventKind::CouplingRunFailed],
                "control",
                &[
                    EventKind::TurnContinueRequested,
                    EventKind::LoopBudgetExhausted,
                ],
                false,
                false,
                "Retry only from typed failure facts and explicit budget, never from blanket replay.",
            ),
            template(
                "std::failure.deadletter",
                CouplingTemplateMaturity::ReferenceOnly,
                CouplingRole::Projection,
                &[EventKind::CouplingRunFailed, EventKind::LoopBlocked],
                "control",
                &[EventKind::CouplingRunFailed, EventKind::LoopBlocked],
                "derived:deadletter",
                &[EventKind::CouplingRunFailed],
                false,
                false,
                "Project exhausted or blocked work into a durable deadletter stream for inspection.",
            ),
        ],
    }
}

pub fn coupling_template_ids_v1() -> Vec<&'static str> {
    vec![
        "std::queue.task",
        "std::queue.completion_callback",
        "std::context.spill",
        "std::context.truncate",
        "std::context.summarize",
        "std::memory.extract",
        "std::memory.recall",
        "std::permission.approval_gate",
        "std::permission.tool_gate",
        "std::prompt.steer",
        "std::prompt.dynamic_instructions",
        "std::io.channel_ingress",
        "std::io.channel_egress",
        "std::schedule.cron",
        "std::supervisor.spawn",
        "std::supervisor.child_completion",
        "std::retry.with_budget",
        "std::failure.deadletter",
    ]
}

fn template(
    id: &str,
    maturity: CouplingTemplateMaturity,
    role: CouplingRole,
    trigger_kinds: &[EventKind],
    source_stream: &str,
    source_kinds: &[EventKind],
    sink_stream: &str,
    sink_kinds: &[EventKind],
    must_have: bool,
    channel_decision_required: bool,
    summary: &str,
) -> CouplingTemplateV1 {
    CouplingTemplateV1 {
        id: id.to_string(),
        maturity,
        role,
        runtime_executable: coupling_template_runtime_executable(id),
        trigger_kinds: trigger_kinds.to_vec(),
        source: CouplingTemplateStreamPattern {
            stream: source_stream.to_string(),
            kinds: source_kinds.to_vec(),
        },
        sink: CouplingTemplateStreamPattern {
            stream: sink_stream.to_string(),
            kinds: sink_kinds.to_vec(),
        },
        must_have,
        channel_decision_required,
        summary: summary.to_string(),
    }
}

fn coupling_template_runtime_executable(id: &str) -> bool {
    matches!(
        id,
        "std::queue.task"
            | "std::queue.completion_callback"
            | "std::context.spill"
            | "std::context.truncate"
            | "std::context.summarize"
            | "std::memory.extract"
            | "std::memory.recall"
            | "std::permission.approval_gate"
            | "std::prompt.steer"
            | "std::permission.tool_gate"
            | "std::prompt.dynamic_instructions"
            | "std::schedule.cron"
            | "std::supervisor.child_completion"
            | "std::retry.with_budget"
            | "std::failure.deadletter"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn coupling_template_catalog_freezes_v1_ids_and_kernel_vocabulary() {
        let catalog = coupling_template_catalog_v1();
        assert_eq!(catalog.schema, COUPLING_TEMPLATE_CATALOG_SCHEMA_V1);
        let ids = catalog
            .templates
            .iter()
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, coupling_template_ids_v1());
        assert_eq!(ids.iter().collect::<BTreeSet<_>>().len(), ids.len());
        let runtime_executable = catalog
            .templates
            .iter()
            .filter(|template| template.runtime_executable)
            .map(|template| template.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            runtime_executable,
            vec![
                "std::queue.task",
                "std::queue.completion_callback",
                "std::context.spill",
                "std::context.truncate",
                "std::context.summarize",
                "std::memory.extract",
                "std::memory.recall",
                "std::permission.approval_gate",
                "std::permission.tool_gate",
                "std::prompt.steer",
                "std::prompt.dynamic_instructions",
                "std::schedule.cron",
                "std::supervisor.child_completion",
                "std::retry.with_budget",
                "std::failure.deadletter"
            ]
        );

        for template in &catalog.templates {
            assert!(!template.trigger_kinds.is_empty(), "{}", template.id);
            assert!(!template.source.kinds.is_empty(), "{}", template.id);
            assert!(!template.sink.kinds.is_empty(), "{}", template.id);
            if template.runtime_executable {
                assert!(!template.channel_decision_required, "{}", template.id);
            }
            match template.role {
                CouplingRole::Controller => assert_eq!(template.sink.stream, "control"),
                CouplingRole::Projection => {
                    assert!(
                        template.sink.stream.starts_with("derived:"),
                        "{}",
                        template.id
                    )
                }
            }
            if template.channel_decision_required {
                assert_ne!(template.maturity, CouplingTemplateMaturity::KernelBacked);
            }
        }
    }
}
