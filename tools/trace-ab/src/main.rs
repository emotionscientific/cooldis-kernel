use std::io::Write as _;

fn main() {
    if let Err(err) = run() {
        eprintln!("trace-ab: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        print_help();
        return Ok(());
    };
    if command == "--help" || command == "-h" || command == "help" {
        print_help();
        return Ok(());
    }
    let options = parse_flags(args.collect())?;
    match command.as_str() {
        "convert-pi" => {
            let input = required_path(&options, "input")?;
            let output = required_path(&options, "output")?;
            let records = verlet_trace_ab::convert_pi(std::io::BufReader::new(
                std::fs::File::open(&input)
                    .map_err(|err| format!("failed to open {}: {err}", input.display()))?,
            ))?;
            verlet_trace_ab::write_common_jsonl(
                &records,
                std::fs::File::create(&output)
                    .map_err(|err| format!("failed to create {}: {err}", output.display()))?,
            )
        }
        "convert-verlet" => {
            let input = required_path(&options, "input")?;
            let output = required_path(&options, "output")?;
            let value: serde_json::Value = serde_json::from_reader(
                std::fs::File::open(&input)
                    .map_err(|err| format!("failed to open {}: {err}", input.display()))?,
            )
            .map_err(|err| format!("failed to parse {}: {err}", input.display()))?;
            let records = verlet_trace_ab::convert_verlet_export(&value)?;
            verlet_trace_ab::write_common_jsonl(
                &records,
                std::fs::File::create(&output)
                    .map_err(|err| format!("failed to create {}: {err}", output.display()))?,
            )
        }
        "diff" => {
            let pi = required_path(&options, "pi")?;
            let verlet = required_path(&options, "verlet")?;
            let pi = verlet_trace_ab::read_common_jsonl(std::io::BufReader::new(
                std::fs::File::open(&pi)
                    .map_err(|err| format!("failed to open {}: {err}", pi.display()))?,
            ))?;
            let verlet = verlet_trace_ab::read_common_jsonl(std::io::BufReader::new(
                std::fs::File::open(&verlet)
                    .map_err(|err| format!("failed to open {}: {err}", verlet.display()))?,
            ))?;
            let rendered = verlet_trace_ab::render_diff(&pi, &verlet);
            if let Some(output) = options.get("output") {
                std::fs::write(output, rendered)
                    .map_err(|err| format!("failed to write {output}: {err}"))?;
            } else {
                std::io::stdout()
                    .write_all(rendered.as_bytes())
                    .map_err(|err| format!("failed to write diff: {err}"))?;
            }
            Ok(())
        }
        "run" => {
            let prompt = match (options.get("prompt"), options.get("prompt-file")) {
                (Some(prompt), None) => prompt.clone(),
                (None, Some(path)) => std::fs::read_to_string(path)
                    .map_err(|err| format!("failed to read prompt file {path}: {err}"))?,
                (Some(_), Some(_)) => {
                    return Err(
                        "run accepts either --prompt or --prompt-file, not both".to_string()
                    );
                }
                (None, None) => return Err("run requires --prompt or --prompt-file".to_string()),
            };
            let timeout_secs = options.get("timeout-secs").map_or(Ok(900_u64), |value| {
                value
                    .parse::<u64>()
                    .map_err(|err| format!("invalid --timeout-secs: {err}"))
            })?;
            let run_options = verlet_trace_ab::runner::RunOptions {
                prompt,
                workspace: required_path(&options, "workspace")?,
                output_dir: required_path(&options, "output")?,
                provider: required(&options, "provider")?.to_string(),
                model: required(&options, "model")?.to_string(),
                verlet_agent_ref: required(&options, "verlet-agent-ref")?.to_string(),
                verlet_url: options
                    .get("verlet-url")
                    .cloned()
                    .unwrap_or_else(|| "ws://127.0.0.1:49200/rpc".to_string()),
                verlet_bin: std::path::PathBuf::from(
                    options
                        .get("verlet-bin")
                        .map(String::as_str)
                        .unwrap_or("verlet"),
                ),
                npx_bin: std::path::PathBuf::from(
                    options.get("npx-bin").map(String::as_str).unwrap_or("npx"),
                ),
                max_tool_rounds: options
                    .get("max-tool-rounds")
                    .cloned()
                    .unwrap_or_else(|| "64".to_string()),
                timeout: std::time::Duration::from_secs(timeout_secs),
            };
            let artifacts = verlet_trace_ab::runner::run_ab(&run_options)?;
            println!("pi trace: {}", artifacts.pi_trace.display());
            println!("verlet trace: {}", artifacts.verlet_trace.display());
            println!("diff: {}", artifacts.diff.display());
            println!("verlet thread: {}", artifacts.verlet_thread_id);
            Ok(())
        }
        other => Err(format!("unknown command {other:?}; use --help")),
    }
}

fn parse_flags(args: Vec<String>) -> Result<std::collections::BTreeMap<String, String>, String> {
    let mut parsed = std::collections::BTreeMap::new();
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let Some(name) = flag.strip_prefix("--") else {
            return Err(format!("unexpected positional argument {flag:?}"));
        };
        let value = args
            .get(index + 1)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        if value.starts_with("--") {
            return Err(format!("{flag} requires a value"));
        }
        if parsed.insert(name.to_string(), value.clone()).is_some() {
            return Err(format!("{flag} was provided more than once"));
        }
        index += 2;
    }
    Ok(parsed)
}

fn required<'a>(
    options: &'a std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, String> {
    options
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("--{name} is required"))
}

fn required_path(
    options: &std::collections::BTreeMap<String, String>,
    name: &str,
) -> Result<std::path::PathBuf, String> {
    required(options, name).map(std::path::PathBuf::from)
}

fn print_help() {
    println!(
        r#"verlet-trace-ab

Offline commands:
  verlet-trace-ab convert-pi --input SESSION.jsonl --output TRACE.jsonl
  verlet-trace-ab convert-verlet --input EXPORT.json --output TRACE.jsonl
  verlet-trace-ab diff --pi PI.jsonl --verlet VERLET.jsonl [--output DIFF.txt]

Live A/B:
  verlet-trace-ab run --prompt TEXT --workspace DIR --output NEW_DIR \
    --provider PROVIDER --model MODEL --verlet-agent-ref AGENT_REF \
    [--verlet-url URL] [--verlet-bin PATH] [--npx-bin PATH] \
    [--max-tool-rounds 64|unlimited] [--timeout-secs 900]

Use --prompt-file FILE instead of --prompt TEXT for long tasks."#
    );
}
