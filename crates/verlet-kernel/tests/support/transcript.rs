use super::kernel_test::EventRecord;
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
enum TypedTranscriptItem {
    Event { label: String, value: Value },
    Receipt { label: String, value: Value },
}

#[derive(Clone, Debug, Default)]
pub struct TypedTranscript {
    items: Vec<TypedTranscriptItem>,
    preserved_ids: BTreeSet<String>,
}

impl TypedTranscript {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn preserve_id(&mut self, id: impl Into<String>) {
        self.preserved_ids.insert(id.into());
    }

    pub fn push_event(&mut self, label: impl Into<String>, event: &EventRecord) {
        self.items.push(TypedTranscriptItem::Event {
            label: label.into(),
            value: serde_json::to_value(event).expect("event transcript serialization"),
        });
    }

    pub fn push_receipt<T: Serialize>(&mut self, label: impl Into<String>, receipt: &T) {
        self.items.push(TypedTranscriptItem::Receipt {
            label: label.into(),
            value: serde_json::to_value(receipt).expect("receipt transcript serialization"),
        });
    }

    pub fn normalize(&self) -> NormalizedTranscript {
        let mut aliases = AliasTable::new(&self.preserved_ids);
        for item in &self.items {
            aliases.collect(item.value());
        }
        NormalizedTranscript {
            items: self
                .items
                .iter()
                .map(|item| NormalizedTranscriptItem {
                    kind: item.kind().to_string(),
                    label: item.label().to_string(),
                    value: aliases.normalize(item.value(), None),
                })
                .collect(),
        }
    }
}

impl TypedTranscriptItem {
    fn kind(&self) -> &'static str {
        match self {
            Self::Event { .. } => "event",
            Self::Receipt { .. } => "receipt",
        }
    }

    fn label(&self) -> &str {
        match self {
            Self::Event { label, .. } | Self::Receipt { label, .. } => label,
        }
    }

    fn value(&self) -> &Value {
        match self {
            Self::Event { value, .. } | Self::Receipt { value, .. } => value,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NormalizedTranscript {
    pub items: Vec<NormalizedTranscriptItem>,
}

impl NormalizedTranscript {
    pub fn render(&self) -> String {
        serde_json::to_string_pretty(self).expect("normalized transcript serialization")
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct NormalizedTranscriptItem {
    pub kind: String,
    pub label: String,
    pub value: Value,
}

struct AliasTable<'a> {
    aliases: BTreeMap<String, String>,
    counts: BTreeMap<&'static str, usize>,
    preserved_ids: &'a BTreeSet<String>,
}

impl<'a> AliasTable<'a> {
    fn new(preserved_ids: &'a BTreeSet<String>) -> Self {
        Self {
            aliases: BTreeMap::new(),
            counts: BTreeMap::new(),
            preserved_ids,
        }
    }

    fn collect(&mut self, value: &Value) {
        self.collect_at(value, None);
    }

    fn collect_at(&mut self, value: &Value, key: Option<&str>) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    self.collect_at(value, Some(key));
                }
            }
            Value::Array(values) => {
                for value in values {
                    self.collect_at(value, key);
                }
            }
            Value::String(id)
                if key.is_some_and(is_id_key)
                    && !id.is_empty()
                    && !self.preserved_ids.contains(id) =>
            {
                if !self.aliases.contains_key(id) {
                    let prefix = alias_prefix(key.unwrap());
                    let next = self.counts.entry(prefix).or_default();
                    *next += 1;
                    self.aliases.insert(id.clone(), format!("${prefix}-{next}"));
                }
            }
            _ => {}
        }
    }

    fn normalize(&self, value: &Value, key: Option<&str>) -> Value {
        if key.is_some_and(is_timestamp_key) {
            return Value::String("$timestamp".to_string());
        }
        if key.is_some_and(is_duration_key) {
            return Value::String("$duration".to_string());
        }
        match value {
            Value::Object(object) => Value::Object(
                object
                    .iter()
                    .map(|(key, value)| (key.clone(), self.normalize(value, Some(key))))
                    .collect::<Map<_, _>>(),
            ),
            Value::Array(values) => Value::Array(
                values
                    .iter()
                    .map(|value| self.normalize(value, key))
                    .collect(),
            ),
            Value::String(text) => Value::String(self.normalize_string(text)),
            _ => value.clone(),
        }
    }

    fn normalize_string(&self, text: &str) -> String {
        if self.preserved_ids.contains(text) {
            return text.to_string();
        }
        if let Some(alias) = self.aliases.get(text) {
            return alias.clone();
        }
        let mut normalized = text.to_string();
        let mut aliases = self.aliases.iter().collect::<Vec<_>>();
        aliases.sort_by_key(|(id, _)| std::cmp::Reverse(id.len()));
        for (id, alias) in aliases {
            if !self.preserved_ids.contains(id) {
                normalized = normalized.replace(id, alias);
            }
        }
        normalized
    }
}

fn is_id_key(key: &str) -> bool {
    key == "id"
        || key.ends_with("_id")
        || key.ends_with("_ids")
        || key.ends_with("Id")
        || key.ends_with("Ids")
}

fn alias_prefix(key: &str) -> &'static str {
    if key.contains("entry") {
        "entry"
    } else if key.contains("event") || key == "id" {
        "event"
    } else if key.contains("thread") {
        "thread"
    } else if key.contains("checkpoint") {
        "checkpoint"
    } else if key.contains("turn") {
        "turn"
    } else if key.contains("call") {
        "call"
    } else if key.contains("observation") {
        "observation"
    } else {
        "id"
    }
}

fn is_timestamp_key(key: &str) -> bool {
    key.contains("timestamp") || key.ends_with("_at_ms") || key.ends_with("AtMs")
}

fn is_duration_key(key: &str) -> bool {
    (key.ends_with("_ms") || key.ends_with("Ms"))
        && ["duration", "elapsed", "timeout", "delay", "backoff"]
            .iter()
            .any(|part| key.contains(part))
}
