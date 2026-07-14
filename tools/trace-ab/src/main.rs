use cooldis_trace_ab::{
    RunOptions, convert_cooldis_export, convert_pi, read_common_jsonl, render_diff, run_ab,
    write_common_jsonl,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    if let Err(err) = run() {
        eprintln!("trace-ab: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
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
            let records =
                convert_pi(BufReader::new(File::open(&input).map_err(|err| {
                    format!("failed to open {}: {err}", input.display())
                })?))?;
            write_common_jsonl(
                &records,
                File::create(&output)
                    .map_err(|err| format!("failed to create {}: {err}", output.display()))?,
            )
        }
        "convert-cooldis" => {
            let input = required_path(&options, "input")?;
            let output = required_path(&options, "output")?;
            let value: Value = serde_json::from_reader(
                File::open(&input)
                    .map_err(|err| format!("failed to open {}: {err}", input.display()))?,
            )
            .map_err(|err| format!("failed to parse {}: {err}", input.display()))?;
            let records = convert_cooldis_export(&value)?;
            write_common_jsonl(
                &records,
                File::create(&output)
                    .map_err(|err| format!("failed to create {}: {err}", output.display()))?,
            )
        }
        "diff" => {
            let pi = required_path(&options, "pi")?;
            let cooldis = required_path(&options, "cooldis")?;
            let pi = read_common_jsonl(BufReader::new(
                File::open(&pi).map_err(|err| format!("failed to open {}: {err}", pi.display()))?,
            ))?;
            let cooldis = read_common_jsonl(BufReader::new(
                File::open(&cooldis)
                    .map_err(|err| format!("failed to open {}: {err}", cooldis.display()))?,
            ))?;
            let rendered = render_diff(&pi, &cooldis);
            if let Some(output) = options.get("output") {
                fs::write(output, rendered)
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
                (None, Some(path)) => fs::read_to_string(path)
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
            let run_options = RunOptions {
                prompt,
                workspace: required_path(&options, "workspace")?,
                output_dir: required_path(&options, "output")?,
                provider: required(&options, "provider")?.to_string(),
                model: required(&options, "model")?.to_string(),
                cooldis_agent_ref: required(&options, "cooldis-agent-ref")?.to_string(),
                cooldis_url: options
                    .get("cooldis-url")
                    .cloned()
                    .unwrap_or_else(|| "ws://127.0.0.1:49200/rpc".to_string()),
                cooldis_bin: PathBuf::from(
                    options
                        .get("cooldis-bin")
                        .map(String::as_str)
                        .unwrap_or("cooldis"),
                ),
                npx_bin: PathBuf::from(options.get("npx-bin").map(String::as_str).unwrap_or("npx")),
                max_tool_rounds: options
                    .get("max-tool-rounds")
                    .cloned()
                    .unwrap_or_else(|| "64".to_string()),
                timeout: Duration::from_secs(timeout_secs),
            };
            let artifacts = run_ab(&run_options)?;
            println!("pi trace: {}", artifacts.pi_trace.display());
            println!("cooldis trace: {}", artifacts.cooldis_trace.display());
            println!("diff: {}", artifacts.diff.display());
            println!("cooldis thread: {}", artifacts.cooldis_thread_id);
            Ok(())
        }
        other => Err(format!("unknown command {other:?}; use --help")),
    }
}

fn parse_flags(args: Vec<String>) -> Result<BTreeMap<String, String>, String> {
    let mut parsed = BTreeMap::new();
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

fn required<'a>(options: &'a BTreeMap<String, String>, name: &str) -> Result<&'a str, String> {
    options
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("--{name} is required"))
}

fn required_path(options: &BTreeMap<String, String>, name: &str) -> Result<PathBuf, String> {
    required(options, name).map(PathBuf::from)
}

fn print_help() {
    println!(
        r#"cooldis-trace-ab

Offline commands:
  cooldis-trace-ab convert-pi --input SESSION.jsonl --output TRACE.jsonl
  cooldis-trace-ab convert-cooldis --input EXPORT.json --output TRACE.jsonl
  cooldis-trace-ab diff --pi PI.jsonl --cooldis COOLDIS.jsonl [--output DIFF.txt]

Live A/B:
  cooldis-trace-ab run --prompt TEXT --workspace DIR --output NEW_DIR \
    --provider PROVIDER --model MODEL --cooldis-agent-ref AGENT_REF \
    [--cooldis-url URL] [--cooldis-bin PATH] [--npx-bin PATH] \
    [--max-tool-rounds 64|unlimited] [--timeout-secs 900]

Use --prompt-file FILE instead of --prompt TEXT for long tasks."#
    );
}
