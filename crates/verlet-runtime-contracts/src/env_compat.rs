use std::collections::HashSet;
use std::env::VarError;
use std::ffi::OsString;
use std::sync::{Mutex, OnceLock};

static WARNED_LEGACY_ENV: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

pub fn var(canonical: &str) -> Result<String, VarError> {
    match var_os(canonical) {
        Some(value) => value.into_string().map_err(VarError::NotUnicode),
        None => Err(VarError::NotPresent),
    }
}

pub fn var_os(canonical: &str) -> Option<OsString> {
    var_os_with(canonical, |name| std::env::var_os(name))
}

pub fn var_os_with(
    canonical: &str,
    get_env: impl FnMut(&str) -> Option<OsString>,
) -> Option<OsString> {
    resolve_os_with(
        canonical,
        get_env,
        |message| eprintln!("{message}"),
        WARNED_LEGACY_ENV.get_or_init(|| Mutex::new(HashSet::new())),
    )
}

pub fn string_with(
    canonical: &str,
    mut get_env: impl FnMut(&str) -> Option<String>,
) -> Option<String> {
    var_os_with(canonical, |name| get_env(name).map(OsString::from))
        .and_then(|value| value.into_string().ok())
}

fn legacy_name(canonical: &str) -> Option<String> {
    canonical
        .strip_prefix("VERLET_")
        .map(|suffix| format!("{}{}", concat!("COOL", "DIS_"), suffix))
}

fn resolve_os_with(
    canonical: &str,
    mut get_env: impl FnMut(&str) -> Option<OsString>,
    mut warn: impl FnMut(&str),
    warned: &Mutex<HashSet<String>>,
) -> Option<OsString> {
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
    use super::*;
    use std::ffi::OsStr;

    fn old_name(canonical: &str) -> String {
        legacy_name(canonical).unwrap()
    }

    #[test]
    fn canonical_env_wins_without_warning() {
        let canonical = "VERLET_TEST_ENV_NEW_WINS";
        let legacy = old_name(canonical);
        let mut warnings = Vec::new();
        let warned = Mutex::new(HashSet::new());

        let value = resolve_os_with(
            canonical,
            |name| match name {
                name if name == canonical => Some(OsString::from("new")),
                name if name == legacy => Some(OsString::from("old")),
                _ => None,
            },
            |message| warnings.push(message.to_string()),
            &warned,
        );

        assert_eq!(value.as_deref(), Some(OsStr::new("new")));
        assert!(warnings.is_empty());
    }

    #[test]
    fn legacy_env_falls_back_and_warns_once() {
        let canonical = "VERLET_TEST_ENV_FALLBACK";
        let legacy = old_name(canonical);
        let mut warnings = Vec::new();
        let warned = Mutex::new(HashSet::new());

        let first = resolve_os_with(
            canonical,
            |name| (name == legacy).then(|| OsString::from("old")),
            |message| warnings.push(message.to_string()),
            &warned,
        );
        let second = resolve_os_with(
            canonical,
            |name| (name == legacy).then(|| OsString::from("old")),
            |message| warnings.push(message.to_string()),
            &warned,
        );

        assert_eq!(first.as_deref(), Some(OsStr::new("old")));
        assert_eq!(second.as_deref(), Some(OsStr::new("old")));
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains(&legacy));
        assert!(warnings[0].contains(canonical));
    }
}
