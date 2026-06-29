use crate::{CooldisError, CooldisResult};
use serde_json::Value;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

const WASM_TARGET: &str = "wasm32-unknown-unknown";

#[derive(Clone, Debug)]
pub struct RustWasmBuildOptions {
    pub module_path: PathBuf,
    pub release: bool,
}

impl RustWasmBuildOptions {
    pub fn new(module_path: impl Into<PathBuf>) -> Self {
        Self {
            module_path: module_path.into(),
            release: true,
        }
    }

    pub fn with_release(mut self, release: bool) -> Self {
        self.release = release;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RustWasmBuildOutput {
    pub manifest_path: PathBuf,
    pub artifact_path: PathBuf,
}

pub fn build_rust_wasm_module(options: RustWasmBuildOptions) -> CooldisResult<RustWasmBuildOutput> {
    let manifest_path = resolve_manifest_path(&options.module_path)?;
    let mut command = rust_wasm_cargo_command();
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .args(["--target", WASM_TARGET])
        .arg("--message-format=json-render-diagnostics");
    if options.release {
        command.arg("--release");
    }

    let output = command.output().map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to run cargo for Rust Wasm build: {err}"))
    })?;
    if !output.status.success() {
        return Err(CooldisError::RuntimeFactory(format!(
            "Rust Wasm build failed for {}:\n{}{}",
            manifest_path.display(),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        )));
    }
    let cargo_artifact_path = find_wasm_artifact_path(&output.stdout).ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "Rust Wasm build for {} did not report a .wasm compiler artifact",
            manifest_path.display()
        ))
    })?;
    let artifact_path = copy_wasm_artifact(&manifest_path, &cargo_artifact_path)?;

    Ok(RustWasmBuildOutput {
        manifest_path,
        artifact_path,
    })
}

fn resolve_manifest_path(module_path: &Path) -> CooldisResult<PathBuf> {
    let path = if module_path.file_name() == Some(OsStr::new("Cargo.toml")) {
        module_path.to_path_buf()
    } else {
        module_path.join("Cargo.toml")
    };
    if !path.exists() {
        return Err(CooldisError::RuntimeFactory(format!(
            "Rust Wasm module manifest not found at {}",
            path.display()
        )));
    }
    Ok(path)
}

fn rust_wasm_cargo_command() -> Command {
    let mut probe = Command::new("rustup");
    clean_rust_wasm_cargo_env(&mut probe);
    let rustup_stable_cargo = probe
        .args(["run", "stable", "cargo", "--version"])
        .output()
        .is_ok_and(|output| output.status.success());

    let mut command = if rustup_stable_cargo {
        let mut command = Command::new("rustup");
        command.args(["run", "stable", "cargo"]);
        command
    } else {
        Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()))
    };
    clean_rust_wasm_cargo_env(&mut command);
    command
}

fn clean_rust_wasm_cargo_env(command: &mut Command) {
    for key in ["RUSTC_WRAPPER", "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS"] {
        command.env_remove(key);
    }
    if let Some(rustc) = rustup_tool_path("rustc") {
        command.env("RUSTC", rustc);
    } else {
        command.env_remove("RUSTC");
    }
    if let Some(rustdoc) = rustup_tool_path("rustdoc") {
        command.env("RUSTDOC", rustdoc);
    } else {
        command.env_remove("RUSTDOC");
    }
}

fn rustup_tool_path(tool: &str) -> Option<String> {
    let output = Command::new("rustup")
        .args(["which", tool, "--toolchain", "stable"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .filter(|path| !path.is_empty())
}

fn find_wasm_artifact_path(stdout: &[u8]) -> Option<PathBuf> {
    let text = String::from_utf8_lossy(stdout);
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter(|message| {
            message.get("reason").and_then(Value::as_str) == Some("compiler-artifact")
        })
        .flat_map(|message| {
            let mut paths = message
                .get("filenames")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            if let Some(executable) = message.get("executable").cloned() {
                paths.push(executable);
            }
            paths
        })
        .filter_map(|filename| filename.as_str().map(PathBuf::from))
        .find(|path| path.extension() == Some(OsStr::new("wasm")))
}

fn copy_wasm_artifact(manifest_path: &Path, artifact_path: &Path) -> CooldisResult<PathBuf> {
    let name = manifest_path
        .parent()
        .and_then(Path::file_name)
        .and_then(OsStr::to_str)
        .unwrap_or("operation")
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    let output_dir = std::env::temp_dir().join("cooldis-wasm-builds");
    fs::create_dir_all(&output_dir).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to create stable Wasm build directory {}: {err}",
            output_dir.display()
        ))
    })?;
    let output_path = output_dir.join(format!("{name}-{}.wasm", Uuid::now_v7().simple()));
    fs::copy(artifact_path, &output_path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to copy Wasm artifact {} to {}: {err}",
            artifact_path.display(),
            output_path.display()
        ))
    })?;
    Ok(output_path)
}

#[cfg(test)]
mod tests;
