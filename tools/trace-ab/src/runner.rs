use std::io::BufRead as _;

const PI_PACKAGE: &str = "@mariozechner/pi-coding-agent@0.70.2";

#[derive(Clone, Debug)]
pub struct RunOptions {
    pub prompt: String,
    pub workspace: std::path::PathBuf,
    pub output_dir: std::path::PathBuf,
    pub provider: String,
    pub model: String,
    pub verlet_agent_ref: String,
    pub verlet_url: String,
    pub verlet_bin: std::path::PathBuf,
    pub npx_bin: std::path::PathBuf,
    pub max_tool_rounds: String,
    pub timeout: std::time::Duration,
}

#[derive(Clone, Debug)]
pub struct RunArtifacts {
    pub pi_trace: std::path::PathBuf,
    pub verlet_trace: std::path::PathBuf,
    pub diff: std::path::PathBuf,
    pub pi_workspace: std::path::PathBuf,
    pub verlet_workspace: std::path::PathBuf,
    pub verlet_thread_id: String,
}

pub fn run_ab(options: &RunOptions) -> Result<RunArtifacts, String> {
    validate_options(options)?;
    let source = std::fs::canonicalize(&options.workspace).map_err(|err| {
        format!(
            "failed to resolve workspace {}: {err}",
            options.workspace.display()
        )
    })?;
    prepare_output_dir(&source, &options.output_dir)?;
    let output_dir = std::fs::canonicalize(&options.output_dir).map_err(|err| {
        format!(
            "failed to resolve output directory {}: {err}",
            options.output_dir.display()
        )
    })?;
    let pi_workspace = output_dir.join("pi-workspace");
    let verlet_workspace = output_dir.join("verlet-workspace");
    clone_workspace(&source, &pi_workspace)?;
    clone_workspace(&source, &verlet_workspace)?;

    let mut errors = Vec::new();
    let pi_session = match run_pi(options, &pi_workspace, &output_dir) {
        Ok(path) => Some(path),
        Err(err) => {
            errors.push(format!("pi: {err}"));
            let partial = output_dir.join("pi.session.jsonl");
            partial.exists().then_some(partial)
        }
    };
    let pi_records = match pi_session {
        Some(path) => match std::fs::File::open(&path)
            .map_err(|err| format!("failed to open pi session {}: {err}", path.display()))
            .and_then(|file| crate::convert_pi(std::io::BufReader::new(file)))
        {
            Ok(records) => records,
            Err(err) => {
                errors.push(format!("pi conversion: {err}"));
                Vec::new()
            }
        },
        None => Vec::new(),
    };
    let pi_trace = output_dir.join("pi.common.jsonl");
    crate::write_common_jsonl(
        &pi_records,
        std::fs::File::create(&pi_trace)
            .map_err(|err| format!("failed to create {}: {err}", pi_trace.display()))?,
    )?;

    let verlet_run = run_verlet(options, &verlet_workspace, &output_dir);
    let (verlet_export, verlet_thread_id) = match verlet_run {
        Ok((export, thread_id)) => (Some(export), Some(thread_id)),
        Err(err) => {
            errors.push(format!("verlet: {err}"));
            let export_path = output_dir.join("verlet.export.json");
            let export = std::fs::read(&export_path)
                .ok()
                .and_then(|bytes| serde_json::from_slice(&bytes).ok());
            let thread_id = std::fs::read_to_string(output_dir.join("cooldis.thread-id"))
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
            (export, thread_id)
        }
    };
    let verlet_records = match verlet_export {
        Some(export) => match crate::convert_verlet_export(&export) {
            Ok(records) => records,
            Err(err) => {
                errors.push(format!("verlet conversion: {err}"));
                Vec::new()
            }
        },
        None => Vec::new(),
    };
    let verlet_trace = output_dir.join("verlet.common.jsonl");
    crate::write_common_jsonl(
        &verlet_records,
        std::fs::File::create(&verlet_trace)
            .map_err(|err| format!("failed to create {}: {err}", verlet_trace.display()))?,
    )?;

    let diff = output_dir.join("diff.txt");
    std::fs::write(&diff, crate::render_diff(&pi_records, &verlet_records))
        .map_err(|err| format!("failed to write {}: {err}", diff.display()))?;
    if !errors.is_empty() {
        return Err(format!(
            "A/B run incomplete; preserved available artifacts in {}:\n- {}",
            output_dir.display(),
            errors.join("\n- ")
        ));
    }
    Ok(RunArtifacts {
        pi_trace,
        verlet_trace,
        diff,
        pi_workspace,
        verlet_workspace,
        verlet_thread_id: verlet_thread_id
            .ok_or_else(|| "Verlet completed without a thread id".to_string())?,
    })
}

fn validate_options(options: &RunOptions) -> Result<(), String> {
    if options.prompt.trim().is_empty() {
        return Err("prompt must not be empty".to_string());
    }
    if options.provider.trim().is_empty() || options.model.trim().is_empty() {
        return Err("provider and model must not be empty".to_string());
    }
    if options.verlet_agent_ref.trim().is_empty() {
        return Err("verlet agent ref must not be empty".to_string());
    }
    if options.timeout.is_zero() {
        return Err("timeout must be greater than zero".to_string());
    }
    match options.max_tool_rounds.as_str() {
        "unlimited" => Ok(()),
        value => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .map(|_| ())
            .ok_or_else(|| "max tool rounds must be a positive integer or unlimited".to_string()),
    }
}

fn prepare_output_dir(workspace: &std::path::Path, output: &std::path::Path) -> Result<(), String> {
    if output.exists() {
        let mut entries = std::fs::read_dir(output).map_err(|err| {
            format!(
                "failed to inspect output directory {}: {err}",
                output.display()
            )
        })?;
        if entries.next().is_some() {
            return Err(format!(
                "output directory {} is not empty; use a fresh directory",
                output.display()
            ));
        }
    } else {
        std::fs::create_dir_all(output).map_err(|err| {
            format!(
                "failed to create output directory {}: {err}",
                output.display()
            )
        })?;
    }
    let output = std::fs::canonicalize(output).map_err(|err| {
        format!(
            "failed to resolve output directory {}: {err}",
            output.display()
        )
    })?;
    if output.starts_with(workspace) {
        return Err("output directory must be outside the seed workspace".to_string());
    }
    Ok(())
}

fn clone_workspace(source: &std::path::Path, destination: &std::path::Path) -> Result<(), String> {
    if !source.is_dir() {
        return Err(format!(
            "seed workspace {} is not a directory",
            source.display()
        ));
    }
    copy_workspace_directory(source, destination, source, destination)
}

fn copy_workspace_directory(
    source: &std::path::Path,
    destination: &std::path::Path,
    source_root: &std::path::Path,
    destination_root: &std::path::Path,
) -> Result<(), String> {
    std::fs::create_dir(destination).map_err(|err| {
        format!(
            "failed to create workspace clone directory {}: {err}",
            destination.display()
        )
    })?;
    let mut entries = std::fs::read_dir(source)
        .map_err(|err| {
            format!(
                "failed to read workspace directory {}: {err}",
                source.display()
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            format!(
                "failed to read workspace entry in {}: {err}",
                source.display()
            )
        })?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        if entry.file_name() == ".git" {
            continue;
        }
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata = std::fs::symlink_metadata(&source_path).map_err(|err| {
            format!(
                "failed to inspect workspace entry {}: {err}",
                source_path.display()
            )
        })?;
        let file_type = metadata.file_type();
        if file_type.is_dir() {
            copy_workspace_directory(
                &source_path,
                &destination_path,
                source_root,
                destination_root,
            )?;
            std::fs::set_permissions(&destination_path, metadata.permissions()).map_err(|err| {
                format!(
                    "failed to preserve directory permissions on {}: {err}",
                    destination_path.display()
                )
            })?;
        } else if file_type.is_file() {
            std::fs::copy(&source_path, &destination_path).map_err(|err| {
                format!(
                    "failed to copy workspace file {} to {}: {err}",
                    source_path.display(),
                    destination_path.display()
                )
            })?;
            std::fs::set_permissions(&destination_path, metadata.permissions()).map_err(|err| {
                format!(
                    "failed to preserve file permissions on {}: {err}",
                    destination_path.display()
                )
            })?;
        } else if file_type.is_symlink() {
            copy_workspace_symlink(
                &source_path,
                &destination_path,
                source_root,
                destination_root,
            )?;
        } else {
            return Err(format!(
                "workspace contains unsupported special file {}",
                source_path.display()
            ));
        }
    }
    let permissions = std::fs::symlink_metadata(source)
        .map_err(|err| format!("failed to inspect {}: {err}", source.display()))?
        .permissions();
    std::fs::set_permissions(destination, permissions).map_err(|err| {
        format!(
            "failed to preserve directory permissions on {}: {err}",
            destination.display()
        )
    })
}

#[cfg(unix)]
fn copy_workspace_symlink(
    source: &std::path::Path,
    destination: &std::path::Path,
    source_root: &std::path::Path,
    destination_root: &std::path::Path,
) -> Result<(), String> {
    let target = std::fs::read_link(source).map_err(|err| {
        format!(
            "failed to read workspace symlink {}: {err}",
            source.display()
        )
    })?;
    let link_target = if target.is_absolute() {
        let normalized_target = normalize_path(&target);
        let relative = normalized_target.strip_prefix(source_root).map_err(|_| {
            format!(
                "workspace symlink {} points outside the seed workspace to {}",
                source.display(),
                target.display()
            )
        })?;
        destination_root.join(relative)
    } else {
        let resolved = normalize_path(&source.parent().unwrap_or(source_root).join(&target));
        if !resolved.starts_with(source_root) {
            return Err(format!(
                "workspace symlink {} escapes the seed workspace via {}",
                source.display(),
                target.display()
            ));
        }
        target
    };
    std::os::unix::fs::symlink(&link_target, destination).map_err(|err| {
        format!(
            "failed to copy workspace symlink {} to {}: {err}",
            source.display(),
            destination.display()
        )
    })
}

#[cfg(not(unix))]
fn copy_workspace_symlink(
    source: &std::path::Path,
    _destination: &std::path::Path,
    _source_root: &std::path::Path,
    _destination_root: &std::path::Path,
) -> Result<(), String> {
    Err(format!(
        "workspace symlinks are not supported on this platform: {}",
        source.display()
    ))
}

fn normalize_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Prefix(_)
            | std::path::Component::RootDir
            | std::path::Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn run_pi(
    options: &RunOptions,
    workspace: &std::path::Path,
    output: &std::path::Path,
) -> Result<std::path::PathBuf, String> {
    let sessions = output.join("pi-sessions");
    std::fs::create_dir_all(&sessions)
        .map_err(|err| format!("failed to create {}: {err}", sessions.display()))?;
    let mut command = std::process::Command::new(&options.npx_bin);
    command
        .arg("--yes")
        .arg(PI_PACKAGE)
        .arg("--mode")
        .arg("rpc")
        .arg("--provider")
        .arg(&options.provider)
        .arg("--model")
        .arg(&options.model)
        .arg("--session-dir")
        .arg(&sessions)
        .arg("--no-extensions")
        .arg("--no-prompt-templates")
        .arg("--no-themes")
        .current_dir(workspace)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    configure_process_group(&mut command);
    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to start pinned pi through npx: {err}"))?;
    let Some(mut stdin) = child.stdin.take() else {
        let _ = terminate(&mut child);
        return Err("pi stdin was not piped".to_string());
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = terminate(&mut child);
        return Err("pi stdout was not piped".to_string());
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = terminate(&mut child);
        return Err("pi stderr was not piped".to_string());
    };
    let (sender, receiver) = std::sync::mpsc::channel();
    let stdout_handle = std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout).lines() {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let stderr_handle = std::thread::spawn(move || read_all(stderr));
    let mut transcript = Vec::new();
    let interaction = (|| -> Result<std::path::PathBuf, String> {
        send_rpc(
            &mut stdin,
            &serde_json::json!({"id":"trace-ab-prompt","type":"prompt","message":options.prompt}),
        )?;
        let deadline = std::time::Instant::now() + options.timeout;
        let mut prompt_accepted = false;
        loop {
            let value = receive_pi_value(&receiver, deadline, &mut transcript, &mut child)?;
            if value.get("id").and_then(serde_json::Value::as_str) == Some("trace-ab-prompt") {
                if value.get("success").and_then(serde_json::Value::as_bool) != Some(true) {
                    return Err(format!(
                        "pi rejected prompt: {}",
                        value
                            .get("error")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("unknown error")
                    ));
                }
                prompt_accepted = true;
            }
            if value.get("type").and_then(serde_json::Value::as_str) == Some("agent_end") {
                break;
            }
        }
        if !prompt_accepted {
            return Err("pi ended without accepting the prompt".to_string());
        }
        send_rpc(
            &mut stdin,
            &serde_json::json!({"id":"trace-ab-state","type":"get_state"}),
        )?;
        loop {
            let value = receive_pi_value(&receiver, deadline, &mut transcript, &mut child)?;
            if value.get("id").and_then(serde_json::Value::as_str) != Some("trace-ab-state") {
                continue;
            }
            if value.get("success").and_then(serde_json::Value::as_bool) != Some(true) {
                return Err("pi get_state failed".to_string());
            }
            let path = value
                .pointer("/data/sessionFile")
                .and_then(serde_json::Value::as_str)
                .map(std::path::PathBuf::from)
                .ok_or_else(|| "pi get_state returned no sessionFile".to_string())?;
            break Ok(if path.is_absolute() {
                path
            } else {
                workspace.join(path)
            });
        }
    })();
    drop(stdin);
    let mut cleanup_errors = Vec::new();
    if let Err(err) = wait_or_kill(&mut child, std::time::Duration::from_secs(10)) {
        cleanup_errors.push(err);
    }
    if stdout_handle.join().is_err() {
        cleanup_errors.push("pi stdout reader panicked".to_string());
    }
    let stderr = match stderr_handle.join() {
        Ok(Ok(stderr)) => stderr,
        Ok(Err(err)) => {
            cleanup_errors.push(err);
            Vec::new()
        }
        Err(_) => {
            cleanup_errors.push("pi stderr reader panicked".to_string());
            Vec::new()
        }
    };
    if let Err(err) = std::fs::write(output.join("pi.stderr.log"), stderr) {
        cleanup_errors.push(format!("failed to write pi stderr log: {err}"));
    }
    if let Err(err) = std::fs::write(output.join("pi.rpc.jsonl"), transcript.concat()) {
        cleanup_errors.push(format!("failed to write pi RPC transcript: {err}"));
    }
    let session_file = interaction
        .as_ref()
        .ok()
        .filter(|path| path.is_file())
        .cloned()
        .or_else(|| newest_jsonl_file(&sessions));
    let stable_session = output.join("pi.session.jsonl");
    if let Some(session_file) = session_file
        && let Err(err) = std::fs::copy(&session_file, &stable_session)
    {
        cleanup_errors.push(format!(
            "failed to copy pi session {} to {}: {err}",
            session_file.display(),
            stable_session.display()
        ));
    }
    if let Err(err) = interaction {
        cleanup_errors.insert(0, err);
    }
    if !stable_session.is_file() {
        cleanup_errors.push("pi produced no session JSONL artifact".to_string());
    }
    if cleanup_errors.is_empty() {
        Ok(stable_session)
    } else {
        Err(cleanup_errors.join("; "))
    }
}

fn newest_jsonl_file(root: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut candidates = Vec::new();
    while let Some(directory) = pending.pop() {
        let mut entries = std::fs::read_dir(directory)
            .ok()?
            .collect::<Result<Vec<_>, _>>()
            .ok()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let file_type = entry.file_type().ok()?;
            if file_type.is_dir() {
                pending.push(path);
            } else if file_type.is_file()
                && path.extension().and_then(|extension| extension.to_str()) == Some("jsonl")
            {
                let modified = entry.metadata().ok()?.modified().ok()?;
                candidates.push((modified, path));
            }
        }
    }
    candidates.sort();
    candidates.pop().map(|(_, path)| path)
}

fn run_verlet(
    options: &RunOptions,
    workspace: &std::path::Path,
    output: &std::path::Path,
) -> Result<(serde_json::Value, String), String> {
    let max_tool_rounds = options
        .max_tool_rounds
        .parse::<u64>()
        .map(serde_json::Value::from)
        .unwrap_or_else(|_| serde_json::Value::String("unlimited".to_string()));
    let start_params = serde_json::json!({
        "agentRef": options.verlet_agent_ref,
        "modelProvider": options.provider,
        "model": options.model,
        "cwd": workspace,
        "workspace": {"hostPath": workspace, "mode": "rw"},
        "runtimeOverrides": {"maxToolRounds": max_tool_rounds},
    });
    let start = verlet_call_capture(
        options,
        "thread/start",
        &start_params,
        std::time::Duration::from_secs(60),
    )?;
    write_captured(output, "verlet.start", &start)?;
    ensure_success("thread/start", &start)?;
    let start_value: serde_json::Value = serde_json::from_slice(&start.stdout)
        .map_err(|err| format!("thread/start returned invalid JSON: {err}"))?;
    let thread_id = start_value
        .pointer("/thread/id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| "thread/start response has no thread.id".to_string())?;
    std::fs::write(output.join("cooldis.thread-id"), format!("{thread_id}\n"))
        .map_err(|err| format!("failed to write Verlet thread id: {err}"))?;

    let mut turn_command = std::process::Command::new(&options.verlet_bin);
    turn_command
        .arg("debug")
        .arg("rpc")
        .arg("turn")
        .arg("--thread")
        .arg(&thread_id)
        .arg("--json")
        .arg(&options.prompt)
        .arg("--url")
        .arg(&options.verlet_url);
    let turn_error = match run_captured(&mut turn_command, options.timeout) {
        Ok(turn) => {
            std::fs::write(output.join("verlet.rpc.jsonl"), &turn.stdout)
                .map_err(|err| format!("failed to write Verlet RPC transcript: {err}"))?;
            std::fs::write(output.join("verlet.stderr.log"), &turn.stderr)
                .map_err(|err| format!("failed to write Verlet stderr log: {err}"))?;
            ensure_success("verlet turn", &turn).err()
        }
        Err(err) => Some(err),
    };

    let export_params = serde_json::json!({
        "threadId": thread_id,
        "streams": ["thread", "control"],
        "includeThread": true,
        "maxEventsPerStream": 10000,
        "redact": true,
    });
    let export = verlet_call_capture(
        options,
        "thread/debug/export",
        &export_params,
        std::time::Duration::from_secs(60),
    );
    let export = match export {
        Ok(export) => export,
        Err(export_error) => {
            return Err(match turn_error {
                Some(turn_error) => {
                    format!("{turn_error}; thread/debug/export also failed: {export_error}")
                }
                None => export_error,
            });
        }
    };
    write_captured(output, "verlet.export", &export)?;
    if let Err(export_error) = ensure_success("thread/debug/export", &export) {
        return Err(match turn_error {
            Some(turn_error) => format!("{turn_error}; {export_error}"),
            None => export_error,
        });
    }
    let export_value: serde_json::Value = serde_json::from_slice(&export.stdout)
        .map_err(|err| format!("thread/debug/export returned invalid JSON: {err}"))?;
    let export_path = output.join("verlet.export.json");
    std::fs::write(
        &export_path,
        serde_json::to_vec_pretty(&export_value)
            .map_err(|err| format!("failed to encode Verlet export: {err}"))?,
    )
    .map_err(|err| format!("failed to write {}: {err}", export_path.display()))?;
    if let Some(turn_error) = turn_error {
        return Err(format!(
            "{turn_error}; Verlet export was preserved after the failed turn"
        ));
    }
    Ok((export_value, thread_id))
}

fn verlet_call_capture(
    options: &RunOptions,
    method: &str,
    params: &serde_json::Value,
    timeout: std::time::Duration,
) -> Result<Captured, String> {
    let mut command = std::process::Command::new(&options.verlet_bin);
    command
        .arg("debug")
        .arg("rpc")
        .arg("call")
        .arg(method)
        .arg(params.to_string())
        .arg("--url")
        .arg(&options.verlet_url);
    run_captured(&mut command, timeout)
}

fn write_captured(output: &std::path::Path, stem: &str, captured: &Captured) -> Result<(), String> {
    std::fs::write(output.join(format!("{stem}.stdout")), &captured.stdout)
        .map_err(|err| format!("failed to write {stem} stdout: {err}"))?;
    std::fs::write(output.join(format!("{stem}.stderr")), &captured.stderr)
        .map_err(|err| format!("failed to write {stem} stderr: {err}"))
}

fn send_rpc(stdin: &mut impl std::io::Write, value: &serde_json::Value) -> Result<(), String> {
    serde_json::to_writer(&mut *stdin, value)
        .map_err(|err| format!("failed to encode pi RPC command: {err}"))?;
    stdin
        .write_all(b"\n")
        .and_then(|_| stdin.flush())
        .map_err(|err| format!("failed to send pi RPC command: {err}"))
}

fn receive_pi_value(
    receiver: &std::sync::mpsc::Receiver<Result<String, std::io::Error>>,
    deadline: std::time::Instant,
    transcript: &mut Vec<Vec<u8>>,
    child: &mut std::process::Child,
) -> Result<serde_json::Value, String> {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return Err("pi RPC timed out".to_string());
    }
    let line = match receiver.recv_timeout(remaining) {
        Ok(line) => line,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            return Err("pi RPC timed out".to_string());
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            let status = child.try_wait().map_err(|err| {
                format!("pi RPC closed and child status could not be read: {err}")
            })?;
            return Err(match status {
                Some(status) => format!("pi exited before RPC completion with {status}"),
                None => "pi RPC output closed before completion".to_string(),
            });
        }
    }
    .map_err(|err| format!("failed to read pi RPC output: {err}"))?;
    transcript.push(format!("{line}\n").into_bytes());
    serde_json::from_str(&line).map_err(|err| format!("pi RPC emitted invalid JSON: {err}"))
}

struct Captured {
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    timed_out: bool,
}

fn run_captured(
    command: &mut std::process::Command,
    timeout: std::time::Duration,
) -> Result<Captured, String> {
    configure_process_group(command);
    let mut child = command
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start {:?}: {err}", command.get_program()))?;
    let Some(stdout) = child.stdout.take() else {
        let _ = terminate(&mut child);
        return Err("child stdout was not piped".to_string());
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = terminate(&mut child);
        return Err("child stderr was not piped".to_string());
    };
    let stdout_handle = std::thread::spawn(move || read_all(stdout));
    let stderr_handle = std::thread::spawn(move || read_all(stderr));
    let deadline = std::time::Instant::now() + timeout;
    let (status, timed_out) = loop {
        match child.try_wait() {
            Ok(Some(status)) => break (status, false),
            Ok(None) => {}
            Err(err) => {
                let _ = terminate(&mut child);
                return Err(format!("failed to poll child: {err}"));
            }
        }
        if std::time::Instant::now() >= deadline {
            let status = terminate(&mut child)
                .map_err(|err| format!("failed to reap timed-out child: {err}"))?;
            break (status, true);
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    };
    let stdout = stdout_handle
        .join()
        .map_err(|_| "stdout reader panicked".to_string())??;
    let stderr = stderr_handle
        .join()
        .map_err(|_| "stderr reader panicked".to_string())??;
    Ok(Captured {
        status,
        stdout,
        stderr,
        timed_out,
    })
}

fn read_all(mut reader: impl std::io::Read) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|err| format!("failed to read child output: {err}"))?;
    Ok(bytes)
}

fn ensure_success(label: &str, captured: &Captured) -> Result<(), String> {
    if captured.timed_out {
        return Err(format!(
            "{label} timed out: {}",
            String::from_utf8_lossy(&captured.stderr).trim()
        ));
    }
    if captured.status.success() {
        return Ok(());
    }
    Err(format!(
        "{label} failed with {}: {}",
        captured.status,
        String::from_utf8_lossy(&captured.stderr).trim()
    ))
}

fn wait_or_kill(
    child: &mut std::process::Child,
    timeout: std::time::Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) => {}
            Err(err) => {
                let _ = terminate(child);
                return Err(format!("failed to poll child: {err}"));
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = terminate(child);
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut std::process::Command) {
    use std::os::unix::process::CommandExt as _;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut std::process::Command) {}

/// Signals the child's process group directly because procps `kill` mangles negative-pid arguments.
fn terminate(child: &mut std::process::Child) -> std::io::Result<std::process::ExitStatus> {
    #[cfg(unix)]
    {
        if let Ok(pgid) = libc::pid_t::try_from(child.id())
            && pgid > 1
        {
            // SAFETY: pgid is the positive process-group id assigned to this child.
            let _ = unsafe { libc::killpg(pgid, libc::SIGKILL) };
        }
    }
    let _ = child.kill();
    child.wait()
}

#[cfg(all(test, unix))]
mod tests {
    use std::io::BufRead as _;
    use std::io::Read as _;
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::PermissionsExt as _;

    #[test]
    fn workspace_copy_preserves_safe_links_and_modes_but_excludes_git() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "verlet-trace-ab-copy-{}-{nonce}",
            std::process::id()
        ));
        let source = root.join("source");
        let destination = root.join("destination");
        std::fs::create_dir_all(source.join("bin")).unwrap();
        std::fs::create_dir(source.join(".git")).unwrap();
        std::fs::write(source.join(".git/config"), "must not copy").unwrap();
        std::fs::write(source.join("bin/tool"), "fixture").unwrap();
        std::fs::set_permissions(
            source.join("bin/tool"),
            std::fs::Permissions::from_mode(0o740),
        )
        .unwrap();
        std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o750)).unwrap();
        std::os::unix::fs::symlink("bin/tool", source.join("tool-link")).unwrap();
        std::os::unix::fs::symlink(source.join("bin/tool"), source.join("absolute-tool-link"))
            .unwrap();

        crate::runner::clone_workspace(&source, &destination).unwrap();

        assert!(!destination.join(".git").exists());
        assert_eq!(
            std::fs::read_link(destination.join("tool-link")).unwrap(),
            std::path::PathBuf::from("bin/tool")
        );
        assert_eq!(
            std::fs::read_link(destination.join("absolute-tool-link")).unwrap(),
            destination.join("bin/tool")
        );
        assert_eq!(
            std::fs::metadata(destination.join("bin/tool"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o740
        );
        assert_eq!(
            std::fs::metadata(&destination)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o750
        );

        std::fs::write(root.join("outside"), "outside").unwrap();
        std::os::unix::fs::symlink("../outside", source.join("escape-link")).unwrap();
        let error = crate::runner::clone_workspace(&source, &root.join("rejected")).unwrap_err();
        assert!(error.contains("escapes the seed workspace"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn captured_timeout_reaps_the_process_group_and_keeps_output() {
        let captured = crate::runner::run_captured(
            std::process::Command::new("sh")
                .arg("-c")
                .arg("printf before-timeout; sleep 5"),
            std::time::Duration::from_millis(50),
        )
        .unwrap();

        assert_eq!(captured.stdout, b"before-timeout");
        assert!(captured.timed_out);
        assert!(
            crate::runner::ensure_success("fixture", &captured)
                .unwrap_err()
                .contains("timed out")
        );
    }

    #[test]
    fn terminate_kills_grandchild_in_process_group() {
        let mut command = std::process::Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 5 & printf 'ready\\n'; wait")
            .stdout(std::process::Stdio::piped());
        crate::runner::configure_process_group(&mut command);
        let mut child = command.spawn().unwrap();
        let mut stdout = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        stdout.read_line(&mut ready).unwrap();
        assert_eq!(ready, "ready\n");

        crate::runner::terminate(&mut child).unwrap();

        let mut poll_fd = libc::pollfd {
            fd: stdout.get_ref().as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll_fd points to one valid pollfd for the duration of this call.
        let poll_result = unsafe { libc::poll(&mut poll_fd, 1, 2_000) };
        let mut remainder = Vec::new();
        stdout.read_to_end(&mut remainder).unwrap();
        assert!(
            poll_result > 0 && poll_fd.revents & libc::POLLHUP != 0,
            "grandchild kept the captured stdout pipe open"
        );
        assert!(remainder.is_empty());
    }

    #[test]
    fn failed_verlet_turn_still_preserves_both_trace_artifacts() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "verlet-trace-ab-failure-{}-{nonce}",
            std::process::id()
        ));
        let workspace = root.join("seed");
        let output = root.join("output");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("fixture.txt"), "seed").unwrap();

        let fake_npx = root.join("fake-npx");
        write_script(
            &fake_npx,
            r#"#!/bin/sh
session_dir=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--session-dir" ]; then session_dir="$2"; shift 2; else shift; fi
done
mkdir -p "$session_dir"
session="$session_dir/session.jsonl"
printf '%s\n' \
  '{"type":"session","version":3,"id":"fixture"}' \
  '{"type":"message","message":{"role":"user","content":"task","timestamp":1000}}' \
  '{"type":"message","message":{"role":"assistant","content":"done","timestamp":1010}}' > "$session"
while IFS= read -r line; do
  case "$line" in
    *trace-ab-prompt*)
      printf '%s\n' '{"id":"trace-ab-prompt","success":true}' '{"type":"agent_end"}'
      ;;
    *trace-ab-state*)
      printf '{"id":"trace-ab-state","success":true,"data":{"sessionFile":"%s"}}\n' "$session"
      ;;
  esac
done
"#,
        );
        let fake_verlet = root.join("fake-verlet");
        write_script(
            &fake_verlet,
            r#"#!/bin/sh
if [ "$1 $2 $3" = "debug rpc call" ] && [ "$4" = "thread/start" ]; then
  printf '%s\n' '{"thread":{"id":"thread-fixture"}}'
  exit 0
fi
if [ "$1 $2 $3" = "debug rpc turn" ]; then
  printf '%s\n' 'fixture turn failed' >&2
  exit 2
fi
if [ "$1 $2 $3" = "debug rpc call" ] && [ "$4" = "thread/debug/export" ]; then
  printf '%s\n' '{"schema":"cooldis.debug.thread_export/1","threadId":"thread-fixture","receipts":[],"thread":{"id":"thread-fixture","turns":[]},"streams":[{"selector":"thread","streamId":"thread:fixture","data":[{"kind":"turn.submitted","created_at_ms":2000,"sequence":1,"payload":{"turn_id":"turn-1","input_text":"task"}},{"kind":"turn.completed","created_at_ms":2010,"sequence":2,"payload":{"turn_id":"turn-1"}}]}]}'
  exit 0
fi
exit 1
"#,
        );

        let error = crate::runner::run_ab(&crate::runner::RunOptions {
            prompt: "task".to_string(),
            workspace,
            output_dir: output.clone(),
            provider: "fixture".to_string(),
            model: "fixture".to_string(),
            verlet_agent_ref: "agent://fixture@1".to_string(),
            verlet_url: "ws://fixture".to_string(),
            verlet_bin: fake_verlet,
            npx_bin: fake_npx,
            max_tool_rounds: "8".to_string(),
            timeout: std::time::Duration::from_secs(5),
        })
        .unwrap_err();

        assert!(error.contains("verlet turn failed"));
        for artifact in [
            "pi.session.jsonl",
            "pi.common.jsonl",
            "verlet.export.json",
            "verlet.common.jsonl",
            "diff.txt",
        ] {
            assert!(output.join(artifact).is_file(), "missing {artifact}");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    fn write_script(path: &std::path::Path, contents: &str) {
        std::fs::write(path, contents).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
