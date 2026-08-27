//! Raw journal inspection through the owning process or a cold Turso file.

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(crate) struct DebugJournalArgs {
    thread_id: Option<verlet_runtime_contracts::ThreadId>,
    kind: Option<verlet_history::EventKind>,
    from_sequence: Option<verlet_history::EventSequence>,
    to_sequence: Option<verlet_history::EventSequence>,
    json: bool,
    journal: Option<std::path::PathBuf>,
    endpoint: crate::cli::debug_rpc::DebugRpcEndpointArgs,
}

pub(crate) async fn run_debug_journal(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_debug_journal_help();
        return Ok(());
    }
    let options = parse_debug_journal_args(args)?;
    let records = match options.journal.as_deref() {
        Some(journal) => load_debug_journal_direct(journal, &options).await?,
        None => load_debug_journal_rpc(&options).await?,
    };
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &records).map_err(|err| {
            debug_journal_usage_error(format!("failed to encode journal records: {err}"))
        })?;
        println!();
    } else {
        print!("{}", render_debug_journal_records(&records));
    }
    Ok(())
}

pub(crate) fn parse_debug_journal_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DebugJournalArgs> {
    let mut endpoint = crate::cli::debug_rpc::DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut thread_id = None;
    let mut kind = None;
    let mut from_sequence = None;
    let mut to_sequence = None;
    let mut json = false;
    let mut journal = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--thread" => {
                let value = required_debug_journal_value(&mut iter, "--thread")?;
                let value = value.to_string_lossy();
                thread_id = Some(
                    verlet_runtime_contracts::ThreadId::parse_str(&value).map_err(|err| {
                        debug_journal_usage_error(format!(
                            "invalid --thread value {value:?}: {err}"
                        ))
                    })?,
                );
            }
            "--kind" => {
                let value = required_debug_journal_value(&mut iter, "--kind")?;
                let value = value.to_string_lossy();
                kind = Some(value.parse::<verlet_history::EventKind>().map_err(|err| {
                    debug_journal_usage_error(format!("invalid --kind value {value:?}: {err}"))
                })?);
            }
            "--from-sequence" => {
                from_sequence = Some(parse_debug_journal_sequence(&mut iter, "--from-sequence")?);
            }
            "--to-sequence" => {
                to_sequence = Some(parse_debug_journal_sequence(&mut iter, "--to-sequence")?);
            }
            "--json" => json = true,
            "--url" => {
                endpoint.url = Some(
                    required_debug_journal_value(&mut iter, "--url")?
                        .to_string_lossy()
                        .to_string(),
                );
            }
            "--config" => {
                endpoint.config = Some(std::path::PathBuf::from(required_debug_journal_value(
                    &mut iter, "--config",
                )?));
            }
            "--journal" => {
                journal = Some(std::path::PathBuf::from(required_debug_journal_value(
                    &mut iter,
                    "--journal",
                )?));
            }
            other => {
                return Err(debug_journal_usage_error(format!(
                    "unknown debug journal argument {other:?}"
                )));
            }
        }
    }
    if from_sequence
        .zip(to_sequence)
        .is_some_and(|(from, to)| from.get() > to.get())
    {
        return Err(debug_journal_usage_error(
            "--from-sequence must not exceed --to-sequence",
        ));
    }
    let endpoint_count = usize::from(endpoint.url.is_some())
        + usize::from(endpoint.config.is_some())
        + usize::from(journal.is_some());
    if endpoint_count > 1 {
        return Err(debug_journal_usage_error(
            "verlet debug journal accepts --url, --config, or --journal, not more than one",
        ));
    }
    Ok(DebugJournalArgs {
        thread_id,
        kind,
        from_sequence,
        to_sequence,
        json,
        journal,
        endpoint,
    })
}

fn required_debug_journal_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<std::ffi::OsString> {
    let value = iter
        .next()
        .ok_or_else(|| debug_journal_usage_error(format!("{flag} requires a value")))?;
    if value.to_string_lossy().starts_with('-') {
        return Err(debug_journal_usage_error(format!(
            "{flag} requires a value"
        )));
    }
    Ok(value)
}

fn parse_debug_journal_sequence(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventSequence> {
    let value = iter
        .next()
        .ok_or_else(|| debug_journal_usage_error(format!("{flag} requires a value")))?;
    let value = value.to_string_lossy();
    let sequence = value.parse::<i64>().map_err(|err| {
        debug_journal_usage_error(format!("invalid {flag} value {value:?}: {err}"))
    })?;
    if sequence < 1 {
        return Err(debug_journal_usage_error(format!(
            "{flag} sequence must be positive"
        )));
    }
    Ok(verlet_history::EventSequence::new(sequence))
}

async fn load_debug_journal_rpc(
    options: &DebugJournalArgs,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
    let endpoint = crate::cli::debug_rpc::resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = crate::cli::debug_rpc::connect_debug_rpc_client(&endpoint).await?;
    let result = client
        .request("journal/events/list", debug_journal_rpc_params(options))
        .await?;
    client.close().await?;
    serde_json::from_value(result.get("data").cloned().ok_or_else(|| {
        debug_journal_usage_error("journal/events/list response missing data array")
    })?)
    .map_err(|err| {
        debug_journal_usage_error(format!(
            "journal/events/list response contains invalid records: {err}"
        ))
    })
}

fn debug_journal_rpc_params(options: &DebugJournalArgs) -> serde_json::Value {
    let mut params = serde_json::Map::new();
    if let Some(thread_id) = options.thread_id {
        params.insert(
            "threadId".to_string(),
            serde_json::Value::String(thread_id.to_string()),
        );
    }
    if let Some(kind) = options.kind {
        params.insert(
            "kind".to_string(),
            serde_json::Value::String(kind.to_string()),
        );
    }
    if let Some(sequence) = options.from_sequence {
        params.insert(
            "fromSequence".to_string(),
            serde_json::json!(sequence.get()),
        );
    }
    if let Some(sequence) = options.to_sequence {
        params.insert("toSequence".to_string(), serde_json::json!(sequence.get()));
    }
    serde_json::Value::Object(params)
}

async fn load_debug_journal_direct(
    journal: &std::path::Path,
    options: &DebugJournalArgs,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::EventRecord>> {
    let store = verlet_history_sqlite::SqliteSessionStore::open_read_only(journal)
        .await
        .map_err(|err| {
            if crate::adapters::app_server::instance::turso_cross_process_lock_error(
                &err.to_string(),
            ) {
                debug_journal_usage_error(
                    "Turso refused direct journal access because another process owns this database; read the live journal through the owner RPC by omitting --journal and using --url or --config",
                )
            } else {
                debug_journal_usage_error(format!("failed to open journal read-only: {err}"))
            }
        })?;
    store
        .list_event_records(
            options.thread_id,
            options.kind,
            options.from_sequence,
            options.to_sequence,
        )
        .await
        .map_err(|err| debug_journal_usage_error(format!("failed to read journal: {err}")))
}

fn render_debug_journal_records(records: &[verlet_history::EventRecord]) -> String {
    let mut out = String::new();
    for record in records {
        out.push_str(&format!(
            "{}\t{}:{}\t{}\t{}\t{}\n",
            record.created_at_ms,
            record.stream_id,
            record.sequence.get(),
            record.kind,
            record.id,
            record.payload,
        ));
    }
    out
}

pub(crate) fn print_debug_journal_help() {
    println!(
        "verlet debug journal\n\
\n\
Usage:\n\
  verlet debug journal [--thread <thread-id>] [--kind <kind>] [--from-sequence <n>] [--to-sequence <n>] [--json] [--url <unix://path|ws-url> | --config <verlet.toml> | --journal <db>]\n\
\n\
List raw event records for forensic inspection. Live mode discovers the current\n\
instance endpoint and reads through its owner RPC. --journal directly opens a\n\
cold Turso store read-only and is refused\n\
while another owner process holds that store. Sequence filters are positive,\n\
inclusive, and apply to each stream's local sequence; without --thread the\n\
result can contain records from many streams with the same sequence.\n"
    );
}

fn debug_journal_usage_error(
    message: impl Into<String>,
) -> crate::kernel::runtime_host::VerletError {
    crate::cli::usage_error(format!(
        "{}\nUsage: verlet debug journal --help",
        message.into()
    ))
}
