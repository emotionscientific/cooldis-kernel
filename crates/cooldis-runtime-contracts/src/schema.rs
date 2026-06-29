//! Shared schema validation for Cooldis runtime contracts.
//!
//! V1 intentionally supports the JSON Schema subset we already accept at
//! runtime boundaries: `type`, `required`, `properties`, `enum`, `items`, and
//! `additionalProperties`, plus common annotation keywords. Unsupported
//! assertion keywords fail closed so a schema never silently means less than it
//! says.

use serde_json::Value;
use std::collections::BTreeMap;

pub const MAX_JSON_SCHEMA_SUBSET_DEPTH: usize = 64;

pub type JsonSchemaResult<T> = Result<T, JsonSchemaValidationError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonSchemaValidationError {
    label: String,
    path: String,
    message: String,
}

impl JsonSchemaValidationError {
    fn new(label: &str, path: &str, message: impl Into<String>) -> Self {
        Self {
            label: label.to_string(),
            path: path.to_string(),
            message: message.into(),
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for JsonSchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} schema validation failed at {}: {}",
            self.label, self.path, self.message
        )
    }
}

impl std::error::Error for JsonSchemaValidationError {}

#[derive(Clone, Debug, Default)]
pub struct SchemaRegistry {
    schemas: BTreeMap<String, Value>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        schema_id: impl Into<String>,
        schema: Value,
    ) -> JsonSchemaResult<()> {
        let schema_id = schema_id.into();
        validate_json_schema_subset(&schema, &schema_id)?;
        self.schemas.insert(schema_id, schema);
        Ok(())
    }

    pub fn validate(&self, schema_id: &str, value: &Value) -> JsonSchemaResult<()> {
        let schema = self.schemas.get(schema_id).ok_or_else(|| {
            JsonSchemaValidationError::new(
                schema_id,
                "$",
                format!("unknown schema id {schema_id:?}; fail closed"),
            )
        })?;
        validate_json_value_against_schema(schema, value, schema_id)
    }
}

pub fn validate_json_schema_subset(schema: &Value, label: &str) -> JsonSchemaResult<()> {
    validate_schema_interpretable(schema, "$", label, 0)
}

pub fn validate_json_value_against_schema(
    schema: &Value,
    value: &Value,
    label: &str,
) -> JsonSchemaResult<()> {
    validate_schema_interpretable(schema, "$", label, 0)?;
    validate_schema_value(schema, value, "$", label, 0)
}

fn validate_schema_value(
    schema: &Value,
    value: &Value,
    path: &str,
    label: &str,
    depth: usize,
) -> JsonSchemaResult<()> {
    check_schema_depth(label, path, depth)?;
    let object = schema
        .as_object()
        .ok_or_else(|| schema_error(label, path, "schema must be a JSON object"))?;
    for key in object.keys() {
        if !is_supported_schema_key(key) {
            return Err(schema_error(
                label,
                path,
                format!("unsupported schema keyword {key:?}; fail closed"),
            ));
        }
    }
    if let Some(enumeration) = object.get("enum") {
        let values = enumeration
            .as_array()
            .ok_or_else(|| schema_error(label, path, "\"enum\" must be an array"))?;
        if !values.iter().any(|candidate| candidate == value) {
            return Err(schema_error(
                label,
                path,
                format!("value {value} is not one of the allowed enum values"),
            ));
        }
    }
    let Some(schema_type_value) = object.get("type") else {
        if object.contains_key("enum") {
            return Ok(());
        }
        return Err(schema_error(
            label,
            path,
            "schema must declare a string \"type\"",
        ));
    };
    let schema_types = schema_type_names(schema_type_value, label, path)?;
    for schema_type in &schema_types {
        if schema_value_type_matches(schema_type, value) {
            return match schema_type.as_str() {
                "object" => validate_object_schema(object, value, path, label, depth),
                "array" => validate_array_schema(object, value, path, label, depth),
                "string" | "number" | "integer" | "boolean" | "null" => Ok(()),
                other => Err(schema_error(
                    label,
                    path,
                    format!("unsupported schema type {other:?}; fail closed"),
                )),
            };
        }
    }
    if schema_types.len() == 1 {
        Err(schema_error(
            label,
            path,
            format!(
                "expected {}, got {}",
                schema_types[0],
                json_type_name(value)
            ),
        ))
    } else {
        Err(schema_error(
            label,
            path,
            format!(
                "expected one of [{}], got {}",
                schema_types.join(", "),
                json_type_name(value)
            ),
        ))
    }
}

fn validate_object_schema(
    schema: &serde_json::Map<String, Value>,
    value: &Value,
    path: &str,
    label: &str,
    depth: usize,
) -> JsonSchemaResult<()> {
    let value = value.as_object().ok_or_else(|| {
        schema_error(
            label,
            path,
            format!("expected object, got {}", json_type_name(value)),
        )
    })?;
    let required = schema
        .get("required")
        .map(|value| required_properties(value, label, path))
        .transpose()?
        .unwrap_or_default();
    for property in required {
        if !value.contains_key(&property) {
            return Err(schema_error(
                label,
                path,
                format!("missing required property {property:?}"),
            ));
        }
    }
    let properties = schema
        .get("properties")
        .map(|value| schema_properties(value, label, path))
        .transpose()?
        .unwrap_or_default();
    for (name, property_schema) in &properties {
        if let Some(property_value) = value.get(name) {
            validate_schema_value(
                property_schema,
                property_value,
                &format!("{path}.{name}"),
                label,
                depth + 1,
            )?;
        }
    }
    for (name, property_value) in value {
        if properties.contains_key(name) {
            continue;
        }
        match schema.get("additionalProperties") {
            Some(Value::Bool(true)) => {}
            Some(Value::Bool(false)) | None => {
                return Err(schema_error(
                    label,
                    path,
                    format!("unexpected property {name:?}; fail closed"),
                ));
            }
            Some(extra_schema @ Value::Object(_)) => {
                validate_schema_value(
                    extra_schema,
                    property_value,
                    &format!("{path}.{name}"),
                    label,
                    depth + 1,
                )?;
            }
            Some(_) => {
                return Err(schema_error(
                    label,
                    path,
                    "\"additionalProperties\" must be a boolean or schema object",
                ));
            }
        }
    }
    Ok(())
}

fn validate_array_schema(
    schema: &serde_json::Map<String, Value>,
    value: &Value,
    path: &str,
    label: &str,
    depth: usize,
) -> JsonSchemaResult<()> {
    let value = value.as_array().ok_or_else(|| {
        schema_error(
            label,
            path,
            format!("expected array, got {}", json_type_name(value)),
        )
    })?;
    let items = schema.get("items").ok_or_else(|| {
        schema_error(
            label,
            path,
            "array schema must declare \"items\"; fail closed",
        )
    })?;
    match items {
        Value::Bool(true) => Ok(()),
        Value::Bool(false) => {
            if value.is_empty() {
                Ok(())
            } else {
                Err(schema_error(label, path, "array schema disallows items"))
            }
        }
        Value::Object(_) => {
            for (index, item) in value.iter().enumerate() {
                validate_schema_value(items, item, &format!("{path}[{index}]"), label, depth + 1)?;
            }
            Ok(())
        }
        _ => Err(schema_error(
            label,
            path,
            "\"items\" must be a boolean or schema object",
        )),
    }
}

fn validate_schema_interpretable(
    schema: &Value,
    path: &str,
    label: &str,
    depth: usize,
) -> JsonSchemaResult<()> {
    check_schema_depth(label, path, depth)?;
    let object = schema
        .as_object()
        .ok_or_else(|| schema_error(label, path, "schema must be a JSON object"))?;
    for key in object.keys() {
        if !is_supported_schema_key(key) {
            return Err(schema_error(
                label,
                path,
                format!("unsupported schema keyword {key:?}; fail closed"),
            ));
        }
    }
    if let Some(enumeration) = object.get("enum")
        && !enumeration.is_array()
    {
        return Err(schema_error(label, path, "\"enum\" must be an array"));
    }
    validate_nested_schema_keywords(object, path, label, depth)?;
    let Some(schema_type_value) = object.get("type") else {
        if object.contains_key("enum") {
            return Ok(());
        }
        return Err(schema_error(
            label,
            path,
            "schema must declare a string \"type\"",
        ));
    };
    let schema_types = schema_type_names(schema_type_value, label, path)?;
    for schema_type in schema_types {
        match schema_type.as_str() {
            "object" => {}
            "array" => {
                if !object.contains_key("items") {
                    return Err(schema_error(
                        label,
                        path,
                        "array schema must declare \"items\"; fail closed",
                    ));
                }
            }
            "string" | "number" | "integer" | "boolean" | "null" => {}
            other => {
                return Err(schema_error(
                    label,
                    path,
                    format!("unsupported schema type {other:?}; fail closed"),
                ));
            }
        }
    }
    Ok(())
}

fn validate_nested_schema_keywords(
    schema: &serde_json::Map<String, Value>,
    path: &str,
    label: &str,
    depth: usize,
) -> JsonSchemaResult<()> {
    if let Some(required) = schema.get("required") {
        required_properties(required, label, path)?;
    }
    if let Some(properties) = schema.get("properties") {
        for (name, property_schema) in schema_properties(properties, label, path)? {
            validate_schema_interpretable(
                &property_schema,
                &format!("{path}.{name}"),
                label,
                depth + 1,
            )?;
        }
    }
    match schema.get("additionalProperties") {
        Some(Value::Bool(_)) | None => {}
        Some(extra_schema @ Value::Object(_)) => {
            validate_schema_interpretable(extra_schema, &format!("{path}.*"), label, depth + 1)?;
        }
        Some(_) => {
            return Err(schema_error(
                label,
                path,
                "\"additionalProperties\" must be a boolean or schema object",
            ));
        }
    }
    match schema.get("items") {
        Some(Value::Bool(_)) | None => {}
        Some(items @ Value::Object(_)) => {
            validate_schema_interpretable(items, &format!("{path}[]"), label, depth + 1)?;
        }
        Some(_) => {
            return Err(schema_error(
                label,
                path,
                "\"items\" must be a boolean or schema object",
            ));
        }
    }
    Ok(())
}

fn schema_type_names(value: &Value, label: &str, path: &str) -> JsonSchemaResult<Vec<String>> {
    match value {
        Value::String(name) => Ok(vec![validate_schema_type_name(name, label, path)?]),
        Value::Array(values) => {
            if values.is_empty() {
                return Err(schema_error(
                    label,
                    path,
                    "\"type\" union must not be empty",
                ));
            }
            let mut names = Vec::with_capacity(values.len());
            for value in values {
                let name = value.as_str().ok_or_else(|| {
                    schema_error(label, path, "\"type\" union entries must be strings")
                })?;
                let name = validate_schema_type_name(name, label, path)?;
                if !names.contains(&name) {
                    names.push(name);
                }
            }
            Ok(names)
        }
        _ => Err(schema_error(
            label,
            path,
            "\"type\" must be a string or array of strings",
        )),
    }
}

fn validate_schema_type_name(name: &str, label: &str, path: &str) -> JsonSchemaResult<String> {
    match name {
        "object" | "array" | "string" | "number" | "integer" | "boolean" | "null" => {
            Ok(name.to_string())
        }
        other => Err(schema_error(
            label,
            path,
            format!("unsupported schema type {other:?}; fail closed"),
        )),
    }
}

fn schema_value_type_matches(schema_type: &str, value: &Value) -> bool {
    match schema_type {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => is_json_integer(value),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    }
}

fn check_schema_depth(label: &str, path: &str, depth: usize) -> JsonSchemaResult<()> {
    if depth > MAX_JSON_SCHEMA_SUBSET_DEPTH {
        Err(schema_error(
            label,
            path,
            format!("schema nesting exceeds limit of {MAX_JSON_SCHEMA_SUBSET_DEPTH}; fail closed"),
        ))
    } else {
        Ok(())
    }
}

fn required_properties(value: &Value, label: &str, path: &str) -> JsonSchemaResult<Vec<String>> {
    let values = value
        .as_array()
        .ok_or_else(|| schema_error(label, path, "\"required\" must be an array"))?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| schema_error(label, path, "\"required\" entries must be strings"))
        })
        .collect()
}

fn schema_properties(
    value: &Value,
    label: &str,
    path: &str,
) -> JsonSchemaResult<serde_json::Map<String, Value>> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| schema_error(label, path, "\"properties\" must be an object"))
}

fn is_supported_schema_key(key: &str) -> bool {
    matches!(
        key,
        "type"
            | "required"
            | "properties"
            | "enum"
            | "items"
            | "additionalProperties"
            | "$schema"
            | "$id"
            | "title"
            | "description"
            | "default"
            | "examples"
            | "deprecated"
            | "readOnly"
            | "writeOnly"
    )
}

fn is_json_integer(value: &Value) -> bool {
    value.as_i64().is_some()
        || value.as_u64().is_some()
        || value
            .as_f64()
            .is_some_and(|value| value.is_finite() && value.fract() == 0.0)
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn schema_error(label: &str, path: &str, message: impl Into<String>) -> JsonSchemaValidationError {
    JsonSchemaValidationError::new(label, path, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn schema_engine_validates_the_supported_runtime_subset() {
        let schema = json!({
            "type": "object",
            "required": ["message", "tags"],
            "additionalProperties": false,
            "properties": {
                "message": {"type": "string", "enum": ["hello"]},
                "count": {"type": "integer"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "metadata": {
                    "type": "object",
                    "additionalProperties": {"type": "number"}
                }
            }
        });

        validate_json_value_against_schema(
            &schema,
            &json!({
                "message": "hello",
                "count": 2,
                "tags": ["a", "b"],
                "metadata": {"rank": 1.5}
            }),
            "fixture.tool.input/1",
        )
        .unwrap();
    }

    #[test]
    fn schema_engine_fails_closed_for_bad_values_and_unsupported_keywords() {
        let schema = json!({
            "type": "object",
            "required": ["message"],
            "additionalProperties": false,
            "properties": {"message": {"type": "string"}}
        });

        let missing = validate_json_value_against_schema(&schema, &json!({}), "tool").unwrap_err();
        assert!(missing.to_string().contains("missing required"));
        assert_eq!(missing.path(), "$");

        let extra = validate_json_value_against_schema(
            &schema,
            &json!({"message": "ok", "extra": true}),
            "tool",
        )
        .unwrap_err();
        assert!(extra.to_string().contains("unexpected property"));

        let unsupported = json!({
            "type": "object",
            "oneOf": [{"type": "object"}]
        });
        let err = validate_json_schema_subset(&unsupported, "tool").unwrap_err();
        assert!(err.to_string().contains("unsupported schema keyword"));
    }

    #[test]
    fn schema_engine_accepts_nullable_type_unions() {
        let schema = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message": {"type": "string"},
                "note": {"type": ["string", "null"]},
                "metadata": {
                    "type": ["object", "null"],
                    "additionalProperties": {"type": ["number", "null"]}
                }
            }
        });

        validate_json_value_against_schema(
            &schema,
            &json!({
                "message": "ok",
                "note": null,
                "metadata": {"rank": 1.5, "score": null}
            }),
            "tool",
        )
        .unwrap();
        validate_json_value_against_schema(
            &schema,
            &json!({
                "message": "ok",
                "note": "present",
                "metadata": null
            }),
            "tool",
        )
        .unwrap();

        let err = validate_json_value_against_schema(
            &schema,
            &json!({"message": "ok", "note": false}),
            "tool",
        )
        .unwrap_err();
        assert!(err.to_string().contains("expected one of"));
        assert_eq!(err.path(), "$.note");
    }

    #[test]
    fn schema_engine_preflights_bad_type_unions_in_unreached_branches() {
        let invalid_optional = json!({
            "type": "object",
            "additionalProperties": false,
            "properties": {
                "message": {"type": "string"},
                "unused": {"type": ["string", "wat"]}
            }
        });
        let err = validate_json_value_against_schema(
            &invalid_optional,
            &json!({"message": "ok"}),
            "tool",
        )
        .unwrap_err();
        assert!(err.to_string().contains("unsupported schema type"));
        assert_eq!(err.path(), "$.unused");

        let empty_union = json!({
            "type": "object",
            "properties": {
                "unused": {"type": []}
            }
        });
        let err = validate_json_schema_subset(&empty_union, "tool").unwrap_err();
        assert!(err.to_string().contains("\"type\" union must not be empty"));
        assert_eq!(err.path(), "$.unused");
    }

    #[test]
    fn schema_registry_validates_by_schema_id() {
        let mut registry = SchemaRegistry::new();
        registry
            .register(
                "cooldis.fixture/1",
                json!({
                    "type": "object",
                    "required": ["schema"],
                    "properties": {"schema": {"enum": ["cooldis.fixture/1"]}},
                    "additionalProperties": true
                }),
            )
            .unwrap();

        registry
            .validate(
                "cooldis.fixture/1",
                &json!({"schema": "cooldis.fixture/1", "ok": true}),
            )
            .unwrap();

        let missing = registry
            .validate("cooldis.missing/1", &json!({}))
            .unwrap_err();
        assert!(missing.to_string().contains("unknown schema id"));
    }
}
