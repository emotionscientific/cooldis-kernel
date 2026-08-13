#[derive(Debug, serde::Serialize)]
struct SandboxProbe {
    host_os: String,
    host_arch: String,
    hypervisor_backend: HypervisorBackend,
    dev_kvm_exists: bool,
    macos_hvf_candidate: bool,
    libkrun_pkg_config_available: bool,
    libkrun_library_hint_present: bool,
    libkrunfw_pkg_config_available: bool,
    libkrunfw_library_hint_present: bool,
    krunvm_available: bool,
    buildah_available: bool,
    helper_process_required: bool,
    microvm_start_attempted: bool,
    private_network_probe_requested: bool,
    status: ProbeStatus,
    unavailable_reasons: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum HypervisorBackend {
    Kvm,
    Hvf,
    Missing,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum ProbeStatus {
    Ready,
    CapabilityMissing,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host_os = std::env::consts::OS.to_string();
    let host_arch = std::env::consts::ARCH.to_string();
    let dev_kvm_exists = std::path::Path::new("/dev/kvm").exists();
    let macos_hvf_candidate = host_os == "macos" && host_arch == "aarch64";
    let hypervisor_backend = if dev_kvm_exists {
        HypervisorBackend::Kvm
    } else if macos_hvf_candidate {
        HypervisorBackend::Hvf
    } else {
        HypervisorBackend::Missing
    };
    let libkrun_pkg_config_available = pkg_config_available("libkrun");
    let libkrun_library_hint_present = library_hint_present(
        "VERLET_LIBKRUN_PATH",
        "krun",
        &[
            "/opt/homebrew/lib/libkrun.dylib",
            "/usr/local/lib/libkrun.dylib",
            "/usr/lib/libkrun.so",
            "/usr/local/lib/libkrun.so",
        ],
    );
    let libkrunfw_pkg_config_available = pkg_config_available("libkrunfw");
    let libkrunfw_library_hint_present = library_hint_present(
        "VERLET_LIBKRUNFW_PATH",
        "krunfw",
        &[
            "/opt/homebrew/lib/libkrunfw.dylib",
            "/usr/local/lib/libkrunfw.dylib",
            "/usr/lib/libkrunfw.so",
            "/usr/local/lib/libkrunfw.so",
        ],
    );
    let krunvm_available = command_available("krunvm");
    let buildah_available = command_available("buildah");
    let private_network_probe_requested =
        std::env::var_os("VERLET_SANDBOX_PROBE_PRIVATE_NETWORK").is_some();

    let mut unavailable_reasons = Vec::new();
    if matches!(hypervisor_backend, HypervisorBackend::Missing) {
        unavailable_reasons.push(
            "no supported hypervisor backend was detected (/dev/kvm or macOS ARM64 HVF)"
                .to_string(),
        );
    }
    if !libkrun_pkg_config_available && !libkrun_library_hint_present {
        unavailable_reasons
            .push("libkrun was not found via pkg-config or VERLET_LIBKRUN_PATH".to_string());
    }
    if !libkrunfw_pkg_config_available && !libkrunfw_library_hint_present && !krunvm_available {
        unavailable_reasons.push(
            "libkrunfw was not found and krunvm is not available as a packaged VM path".to_string(),
        );
    }

    let probe = SandboxProbe {
        host_os,
        host_arch,
        hypervisor_backend,
        dev_kvm_exists,
        macos_hvf_candidate,
        libkrun_pkg_config_available,
        libkrun_library_hint_present,
        libkrunfw_pkg_config_available,
        libkrunfw_library_hint_present,
        krunvm_available,
        buildah_available,
        helper_process_required: true,
        microvm_start_attempted: false,
        private_network_probe_requested,
        status: if unavailable_reasons.is_empty() {
            ProbeStatus::Ready
        } else {
            ProbeStatus::CapabilityMissing
        },
        unavailable_reasons,
    };

    println!("{}", serde_json::to_string_pretty(&probe)?);
    Ok(())
}

fn pkg_config_available(name: &str) -> bool {
    std::process::Command::new("pkg-config")
        .args(["--exists", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn command_available(name: &str) -> bool {
    std::process::Command::new(name)
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn library_hint_present(env_var: &str, path_needle: &str, common_paths: &[&str]) -> bool {
    std::env::var_os(env_var).is_some()
        || std::env::var_os("DYLD_LIBRARY_PATH")
            .map(|paths| paths.to_string_lossy().contains(path_needle))
            .unwrap_or(false)
        || std::env::var_os("LD_LIBRARY_PATH")
            .map(|paths| paths.to_string_lossy().contains(path_needle))
            .unwrap_or(false)
        || common_paths
            .iter()
            .any(|path| std::path::Path::new(path).exists())
}
