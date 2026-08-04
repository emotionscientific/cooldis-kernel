use verlet_guest_sdk::prelude::*;
use serde_json::json;

#[derive(Deserialize)]
struct CounterConfig {
    #[serde(default = "default_every")]
    every: u64,
    #[serde(default = "default_sink_stream")]
    sink_stream: String,
    #[serde(default = "default_sink_kind")]
    sink_kind: String,
}

#[coupling]
pub fn fold_counter(ctx: CouplingContext) -> Result<Discharge, GuestError> {
    let config: CounterConfig = ctx.config()?;
    let every = config.every.max(1);
    let count = ctx.sources().len() as u64;
    if count == 0 || count % every != 0 {
        return Ok(Discharge::empty());
    }
    Discharge::empty().event(
        config.sink_stream,
        config.sink_kind,
        json!({
            "schema": "cooldis.example.counter_fold/1",
            "count": count,
            "trigger_event_id": ctx.trigger().id.clone(),
            "coupling_id": ctx.meta().coupling_id.clone(),
        }),
    )
}

fn default_every() -> u64 {
    3
}

fn default_sink_stream() -> String {
    "derived:counter".to_string()
}

fn default_sink_kind() -> String {
    "placement.decision".to_string()
}
