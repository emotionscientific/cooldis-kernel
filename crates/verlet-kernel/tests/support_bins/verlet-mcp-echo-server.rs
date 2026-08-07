use std::io::BufRead as _;
use std::io::Write as _;

const TOOL_NAME: &str = "verlet_mcp_echo";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let request: serde_json::Value = serde_json::from_str(&line)?;
        let method = request
            .get("method")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        match method {
            "initialize" => write_response(
                &mut stdout,
                request.get("id").cloned(),
                serde_json::json!({
                    "protocolVersion": "2025-06-18",
                    "capabilities": { "tools": {} },
                    "serverInfo": {
                        "name": "verlet-mcp-echo-server",
                        "version": env!("CARGO_PKG_VERSION")
                    }
                }),
            )?,
            "notifications/initialized" | "initialized" => {}
            "tools/list" => write_response(
                &mut stdout,
                request.get("id").cloned(),
                serde_json::json!({
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
                let params = request
                    .get("params")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                let name = params
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
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
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                write_response(
                    &mut stdout,
                    request.get("id").cloned(),
                    serde_json::json!({
                        "content": [{
                            "type": "text",
                            "text": format!("VERLET_MCP_TOOL_OK message={message}")
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
    stdout: &mut std::io::Stdout,
    id: Option<serde_json::Value>,
    result: serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        stdout,
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id.unwrap_or(serde_json::Value::Null),
            "result": result
        })
    )?;
    stdout.flush()?;
    Ok(())
}

fn write_error(
    stdout: &mut std::io::Stdout,
    id: Option<serde_json::Value>,
    code: i64,
    message: String,
) -> Result<(), Box<dyn std::error::Error>> {
    writeln!(
        stdout,
        "{}",
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id.unwrap_or(serde_json::Value::Null),
            "error": {
                "code": code,
                "message": message
            }
        })
    )?;
    stdout.flush()?;
    Ok(())
}
