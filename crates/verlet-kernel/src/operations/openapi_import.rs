const INPUT_CAPACITY: u32 = 1024 * 1024;
const OUTPUT_CAPACITY: u32 = 512 * 1024;

pub fn render_openapi_import_artifact(
    plan: &crate::OperationImportPlan,
) -> crate::VerletResult<Vec<u8>> {
    let manifest = verlet_abi::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: plan
            .operations
            .iter()
            .map(|operation| verlet_abi::WasmOperationDefinition {
                id: operation.id,
                name: operation.name.clone(),
                input: verlet_abi::WasmOperationValueKind::Json,
                output: verlet_abi::WasmOperationValueKind::Json,
                events: verlet_abi::WasmOperationEventKind::Jsonl,
                mode: verlet_abi::WasmOperationMode::Sync,
                required_capabilities: operation.required_capabilities.iter().cloned().collect(),
            })
            .collect(),
    };
    let manifest = serde_json::to_vec(&manifest).map_err(|error| {
        crate::VerletError::RuntimeFactory(format!("failed to encode import manifest: {error}"))
    })?;
    let mut requests = Vec::with_capacity(plan.operations.len());
    for operation in &plan.operations {
        let url = format!(
            "{}/{}",
            operation.server_url.trim_end_matches('/'),
            operation.path_template.trim_start_matches('/')
        );
        let request = serde_json::json!({
            "abi": "cooldis.net.http/0.1",
            "method": operation.method,
            "url": url,
            "headers": if operation.request_body.is_some() {
                vec![("content-type", "application/json")]
            } else {
                Vec::new()
            },
            "secret_headers": operation.secret_headers.iter().filter(|header| {
                header.prefix.is_empty()
            }).map(|header| (&header.name, &header.secret)).collect::<Vec<_>>(),
            "secret_header_prefixes": operation.secret_headers.iter().filter(|header| {
                !header.prefix.is_empty()
            }).map(|header| (&header.name, &header.secret, &header.prefix)).collect::<Vec<_>>(),
            "input_mapping": {
                "input_schema": operation.input_schema,
                "parameters": operation.parameters,
                "request_body": operation.request_body
            },
            "response_envelope": true,
            "timeout_ms": 30000,
            "max_response_bytes": 262144
        });
        requests.push(serde_json::to_vec(&request).map_err(|error| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to encode import request plan: {error}"
            ))
        })?);
    }
    encode_module(&manifest, &requests, plan)
}

fn encode_module(
    manifest: &[u8],
    requests: &[Vec<u8>],
    plan: &crate::OperationImportPlan,
) -> crate::VerletResult<Vec<u8>> {
    let manifest_offset = 1024_u32;
    let manifest_len = artifact_u32("manifest length", manifest.len())?;
    let mut next_offset = checked_align(
        artifact_add("manifest data end", manifest_offset, manifest_len)?,
        8,
    )?;
    let mut request_rows = Vec::with_capacity(requests.len());
    for request in requests {
        let request_len = artifact_u32("request plan length", request.len())?;
        let offset = next_offset;
        next_offset = checked_align(
            artifact_add("request plan data end", next_offset, request_len)?,
            8,
        )?;
        request_rows.push((offset, request));
    }
    let input_offset = checked_align(next_offset, 65536)?;
    let output_offset = artifact_add("input buffer end", input_offset, INPUT_CAPACITY)?;
    let memory_bytes = artifact_add("output buffer end", output_offset, OUTPUT_CAPACITY)?;
    let memory_pages = memory_bytes.div_ceil(65536);
    let max_memory_pages =
        u32::try_from(verlet_wasm::DEFAULT_MEMORY_LIMIT_BYTES / 65536).map_err(|_| {
            crate::VerletError::RuntimeFactory(
                "configured Wasm memory limit does not fit the artifact encoder".to_string(),
            )
        })?;
    if memory_pages > max_memory_pages {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "OpenAPI import artifact requires {memory_pages} Wasm memory pages, exceeding the runtime limit of {max_memory_pages}"
        )));
    }

    let mut module = b"\0asm\x01\0\0\0".to_vec();
    let mut types = Vec::new();
    encode_u32(5, &mut types);
    encode_function_type(3, 1, &mut types);
    encode_function_type(3, 1, &mut types);
    encode_function_type(7, 1, &mut types);
    encode_function_type(1, 1, &mut types);
    encode_function_type(5, 1, &mut types);
    push_section(1, types, &mut module);

    let mut imports = Vec::new();
    encode_u32(3, &mut imports);
    encode_import("cooldis_0.1", "source_read", 0, &mut imports);
    encode_import("cooldis_0.1", "sink_write", 1, &mut imports);
    encode_import("cooldis_0.1", "http_request", 2, &mut imports);
    push_section(2, imports, &mut module);

    let mut functions = Vec::new();
    encode_u32(2, &mut functions);
    encode_u32(3, &mut functions);
    encode_u32(4, &mut functions);
    push_section(3, functions, &mut module);

    let mut memories = Vec::new();
    encode_u32(1, &mut memories);
    memories.push(0);
    encode_u32(memory_pages, &mut memories);
    push_section(5, memories, &mut module);

    let mut exports = Vec::new();
    encode_u32(3, &mut exports);
    encode_export("memory", 2, 0, &mut exports);
    encode_export("__verlet_describe_module__", 0, 3, &mut exports);
    encode_export("__verlet_call_operation__", 0, 4, &mut exports);
    push_section(7, exports, &mut module);

    let mut code = Vec::new();
    encode_u32(2, &mut code);
    let describe = describe_body(manifest_offset, manifest_len);
    encode_bytes(&describe, &mut code);
    let call = call_body(plan, &request_rows, input_offset, output_offset);
    encode_bytes(&call, &mut code);
    push_section(10, code, &mut module);

    let mut data = Vec::new();
    let data_count = request_rows
        .len()
        .checked_add(1)
        .ok_or_else(|| {
            crate::VerletError::RuntimeFactory(
                "OpenAPI import artifact data segment count overflowed".to_string(),
            )
        })
        .and_then(|count| artifact_u32("data segment count", count))?;
    encode_u32(data_count, &mut data);
    encode_data(manifest_offset, manifest, &mut data);
    for (offset, request) in request_rows {
        encode_data(offset, request, &mut data);
    }
    push_section(11, data, &mut module);
    Ok(module)
}

fn describe_body(manifest_offset: u32, manifest_len: u32) -> Vec<u8> {
    let mut body = vec![0];
    i32_const(0, &mut body);
    i32_const(manifest_len as i32, &mut body);
    body.push(0x36);
    encode_u32(2, &mut body);
    encode_u32(0, &mut body);
    local_get(0, &mut body);
    i32_const(manifest_offset as i32, &mut body);
    i32_const(0, &mut body);
    call(1, &mut body);
    body.push(0x0b);
    body
}

fn call_body(
    plan: &crate::OperationImportPlan,
    requests: &[(u32, &Vec<u8>)],
    input_offset: u32,
    output_offset: u32,
) -> Vec<u8> {
    let mut body = Vec::new();
    encode_u32(1, &mut body);
    encode_u32(2, &mut body);
    body.push(0x7f);
    for (operation, (request_offset, request)) in plan.operations.iter().zip(requests) {
        local_get(0, &mut body);
        i32_const(operation.id as i32, &mut body);
        body.push(0x46);
        body.extend([0x04, 0x40]);

        i32_const(0, &mut body);
        i32_const(INPUT_CAPACITY as i32, &mut body);
        body.push(0x36);
        encode_u32(2, &mut body);
        encode_u32(0, &mut body);
        local_get(2, &mut body);
        i32_const(input_offset as i32, &mut body);
        i32_const(0, &mut body);
        call(0, &mut body);
        body.push(0x1a);
        i32_const(0, &mut body);
        body.push(0x28);
        encode_u32(2, &mut body);
        encode_u32(0, &mut body);
        local_set(6, &mut body);

        local_get(1, &mut body);
        i32_const(*request_offset as i32, &mut body);
        i32_const(request.len() as i32, &mut body);
        i32_const(input_offset as i32, &mut body);
        local_get(6, &mut body);
        i32_const(8, &mut body);
        local_get(4, &mut body);
        call(2, &mut body);
        local_set(5, &mut body);
        return_status_if_nonzero(5, &mut body);

        i32_const(0, &mut body);
        i32_const(OUTPUT_CAPACITY as i32, &mut body);
        body.push(0x36);
        encode_u32(2, &mut body);
        encode_u32(0, &mut body);
        i32_const(12, &mut body);
        body.push(0x28);
        encode_u32(2, &mut body);
        encode_u32(0, &mut body);
        i32_const(output_offset as i32, &mut body);
        i32_const(0, &mut body);
        call(0, &mut body);
        body.push(0x1a);

        local_get(3, &mut body);
        i32_const(output_offset as i32, &mut body);
        i32_const(0, &mut body);
        call(1, &mut body);
        local_set(5, &mut body);
        return_status_if_nonzero(5, &mut body);
        i32_const(0, &mut body);
        body.push(0x0f);
        body.push(0x0b);
    }
    i32_const(2, &mut body);
    body.push(0x0b);
    body
}

fn return_status_if_nonzero(local: u32, body: &mut Vec<u8>) {
    local_get(local, body);
    i32_const(0, body);
    body.push(0x47);
    body.extend([0x04, 0x40]);
    local_get(local, body);
    body.push(0x0f);
    body.push(0x0b);
}

fn encode_function_type(parameters: u32, results: u32, bytes: &mut Vec<u8>) {
    bytes.push(0x60);
    encode_u32(parameters, bytes);
    bytes.extend(std::iter::repeat_n(0x7f, parameters as usize));
    encode_u32(results, bytes);
    bytes.extend(std::iter::repeat_n(0x7f, results as usize));
}

fn encode_import(module: &str, name: &str, type_index: u32, bytes: &mut Vec<u8>) {
    encode_name(module, bytes);
    encode_name(name, bytes);
    bytes.push(0);
    encode_u32(type_index, bytes);
}

fn encode_export(name: &str, kind: u8, index: u32, bytes: &mut Vec<u8>) {
    encode_name(name, bytes);
    bytes.push(kind);
    encode_u32(index, bytes);
}

fn encode_data(offset: u32, value: &[u8], bytes: &mut Vec<u8>) {
    bytes.push(0);
    i32_const(offset as i32, bytes);
    bytes.push(0x0b);
    encode_bytes(value, bytes);
}

fn push_section(id: u8, payload: Vec<u8>, module: &mut Vec<u8>) {
    module.push(id);
    encode_bytes(&payload, module);
}

fn encode_name(value: &str, bytes: &mut Vec<u8>) {
    encode_bytes(value.as_bytes(), bytes);
}

fn encode_bytes(value: &[u8], bytes: &mut Vec<u8>) {
    encode_u32(value.len() as u32, bytes);
    bytes.extend_from_slice(value);
}

fn encode_u32(mut value: u32, bytes: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(byte);
            return;
        }
        bytes.push(byte | 0x80);
    }
}

fn encode_i32(mut value: i32, bytes: &mut Vec<u8>) {
    loop {
        let byte = (value as u8) & 0x7f;
        value >>= 7;
        let done = (value == 0 && byte & 0x40 == 0) || (value == -1 && byte & 0x40 != 0);
        bytes.push(if done { byte } else { byte | 0x80 });
        if done {
            return;
        }
    }
}

fn i32_const(value: i32, bytes: &mut Vec<u8>) {
    bytes.push(0x41);
    encode_i32(value, bytes);
}

fn local_get(index: u32, bytes: &mut Vec<u8>) {
    bytes.push(0x20);
    encode_u32(index, bytes);
}

fn local_set(index: u32, bytes: &mut Vec<u8>) {
    bytes.push(0x21);
    encode_u32(index, bytes);
}

fn call(index: u32, bytes: &mut Vec<u8>) {
    bytes.push(0x10);
    encode_u32(index, bytes);
}

fn artifact_u32(label: &str, value: usize) -> crate::VerletResult<u32> {
    u32::try_from(value).map_err(|_| {
        crate::VerletError::RuntimeFactory(format!(
            "OpenAPI import artifact {label} {value} exceeds the Wasm32 address space"
        ))
    })
}

fn artifact_add(label: &str, left: u32, right: u32) -> crate::VerletResult<u32> {
    left.checked_add(right).ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "OpenAPI import artifact {label} overflowed the Wasm32 address space"
        ))
    })
}

fn checked_align(value: u32, alignment: u32) -> crate::VerletResult<u32> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "OpenAPI import artifact alignment {alignment} is invalid"
        )));
    }
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| {
            crate::VerletError::RuntimeFactory(
                "OpenAPI import artifact alignment overflowed the Wasm32 address space".to_string(),
            )
        })
}

#[cfg(test)]
mod tests {
    #[test]
    fn artifact_layout_arithmetic_rejects_overflow_without_panicking() {
        assert!(crate::operations::openapi_import::checked_align(u32::MAX, 8).is_err());
        assert!(crate::operations::openapi_import::artifact_add("test", u32::MAX, 1).is_err());
        assert!(crate::operations::openapi_import::checked_align(1, 0).is_err());
    }
}
