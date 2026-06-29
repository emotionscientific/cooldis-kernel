use serde_json::{Value, json};
use std::io::{self, BufRead, Write};

const TOOL_NAME: &str = "cooldis_mcp_echo";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = serde_json::from_str(&line)?;
        let method = request.get("method").and_then(Value::as_str).unwrap_or("");
        match method {
            "initialize" => write_response(
                &mut stdout,
                request.get("id").cloned(),
                json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": { "tools": {} },
                    "serverInfo": {
                        "name": "cooldis-mcp-echo-server",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )?,
            "notifications/initialized" | "initialized" => {}
            "tools/list" => write_response(
                &mut stdout,
                request.get("id").cloned(),
                json!({
                    "tools": [{
                        "name": TOOL_NAME,
                        "description": "Echo a message through a real local MCP stdio server.",
                        "inputSchema": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "message": {
                                    "type": "string",
                                    "description": "Message to echo."
                                }
                            },
                            "required": ["message"]
                        }
                    }]
                }),
            )?,
            "tools/call" => {
                let params = request.get("params").cloned().unwrap_or(Value::Null);
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                if name != TOOL_NAME {
                    write_error(
                        &mut stdout,
                        request.get("id").cloned(),
                        -32602,
                        format!("unknown tool `{name}`"),
                    )?;
                    continue;
                }
                let message = params
                    .get("arguments")
                    .and_then(|arguments| arguments.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                write_response(
                    &mut stdout,
                    request.get("id").cloned(),
                    json!({
                        "content": [{
                            "type": "text",
                            "text": format!("COOLDIS_MCP_TOOL_OK message={message}")
                        }],
                        "isError": false
                    }),
                )?;
            }
            _ => write_error(
                &mut stdout,
                request.get("id").cloned(),
                -32601,
                format!("unknown method `{method}`"),
            )?,
        }
    }
    Ok(())
}

fn write_response(
    stdout: &mut io::Stdout,
    id: Option<Value>,
    result: Value,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        stdout,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": id.unwrap_or(Value::Null),
            "result": result
        })
    )?;
    stdout.flush()?;
    Ok(())
}

fn write_error(
    stdout: &mut io::Stdout,
    id: Option<Value>,
    code: i64,
    message: String,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        stdout,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": id.unwrap_or(Value::Null),
            "error": {
                "code": code,
                "message": message
            }
        })
    )?;
    stdout.flush()?;
    Ok(())
}
