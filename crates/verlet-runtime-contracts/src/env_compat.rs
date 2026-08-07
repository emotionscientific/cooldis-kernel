static WARNED_LEGACY_ENV: std::sync::OnceLock<std::sync::Mutex<std::collections::HashSet<String>>> =
    std::sync::OnceLock::new();

pub fn var(canonical: &str) -> Result<String, std::env::VarError> {
    match var_os(canonical) {
        Some(value) => value.into_string().map_err(std::env::VarError::NotUnicode),
        None => Err(std::env::VarError::NotPresent),
    }
}

pub fn var_os(canonical: &str) -> Option<std::ffi::OsString> {
    var_os_with(canonical, |name| std::env::var_os(name))
}

pub fn var_os_with(
    canonical: &str,
    get_env: impl FnMut(&str) -> Option<std::ffi::OsString>,
) -> Option<std::ffi::OsString> {
    resolve_os_with(
        canonical,
        get_env,
        |message| eprintln!("{message}"),
        WARNED_LEGACY_ENV.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new())),
    )
}

pub fn string_with(
    canonical: &str,
    mut get_env: impl FnMut(&str) -> Option<String>,
) -> Option<String> {
    var_os_with(canonical, |name| {
        get_env(name).map(std::ffi::OsString::from)
    })
    .and_then(|value| value.into_string().ok())
}

fn legacy_name(canonical: &str) -> Option<String> {
    canonical
        .strip_prefix("VERLET_")
        .map(|suffix| format!("{}{}", concat!("COOL", "DIS_"), suffix))
}

fn resolve_os_with(
    canonical: &str,
    mut get_env: impl FnMut(&str) -> Option<std::ffi::OsString>,
    mut warn: impl FnMut(&str),
    warned: &std::sync::Mutex<std::collections::HashSet<String>>,
) -> Option<std::ffi::OsString> {
    if let Some(value) = get_env(canonical) {
        return Some(value);
    }

    let legacy = legacy_name(canonical)?;
    let value = get_env(&legacy)?;
    let mut warned = warned
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if warned.insert(legacy.clone()) {
        warn(&format!(
            "warning: {legacy} is deprecated; use {canonical} (compatibility will be removed in v0.4.0)"
        ));
    }
    Some(value)
}

#[cfg(test)]
mod tests {

    fn old_name(canonical: &str) -> String {
        crate::env_compat::legacy_name(canonical).unwrap()
    }

    #[test]
    fn canonical_env_wins_without_warning() {
        let canonical = "VERLET_TEST_ENV_NEW_WINS";
        let legacy = old_name(canonical);
        let mut warnings = Vec::new();
        let warned = std::sync::Mutex::new(std::collections::HashSet::new());

        let value = crate::env_compat::resolve_os_with(
            canonical,
            |name| match name {
                name if name == canonical => Some(std::ffi::OsString::from("new")),
                name if name == legacy => Some(std::ffi::OsString::from("old")),
                _ => None,
            },
            |message| warnings.push(message.to_string()),
            &warned,
        );

        assert_eq!(value.as_deref(), Some(std::ffi::OsStr::new("new")));
        assert!(warnings.is_empty());
    }

    #[test]
    fn legacy_env_falls_back_and_warns_once() {
        let canonical = "VERLET_TEST_ENV_FALLBACK";
        let legacy = old_name(canonical);
        let mut warnings = Vec::new();
        let warned = std::sync::Mutex::new(std::collections::HashSet::new());

        let first = crate::env_compat::resolve_os_with(
            canonical,
            |name| (name == legacy).then(|| std::ffi::OsString::from("old")),
            |message| warnings.push(message.to_string()),
            &warned,
        );
        let second = crate::env_compat::resolve_os_with(
            canonical,
            |name| (name == legacy).then(|| std::ffi::OsString::from("old")),
            |message| warnings.push(message.to_string()),
            &warned,
        );

        assert_eq!(first.as_deref(), Some(std::ffi::OsStr::new("old")));
        assert_eq!(second.as_deref(), Some(std::ffi::OsStr::new("old")));
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains(&legacy));
        assert!(warnings[0].contains(canonical));
    }
}
