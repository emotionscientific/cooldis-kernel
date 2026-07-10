use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use uuid::Uuid;

const ROUTE_ID: &str = "restart-smoke";
const WEBHOOK_PATH: &str = "/ingress";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let daemon_bin = daemon_binary()?;
    continuity_after_idle_kill(&daemon_bin).await?;
    continuity_after_binding_crash_cut(&daemon_bin).await?;
    println!("cooldis restart smoke ok: idle restart and binding crash cut preserved continuity");
    Ok(())
}

async fn continuity_after_idle_kill(daemon_bin: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new("idle")?;
    let mut daemon = fixture.spawn_daemon(daemon_bin, None).await?;

    fixture.post_update(1001, 1, "before process kill").await?;
    let first_thread = fixture.wait_for_single_bound_thread().await?;
    let first_receipt = fixture.wait_for_receipts(&first_thread, 1).await?.remove(0);
    let pre_kill_entries = receipt_entry_ids(&first_receipt)?;
    if pre_kill_entries.is_empty() {
        return Err("first compiled context receipt did not contain session entries".into());
    }

    daemon.sigkill().await?;
    let mut restarted = fixture.spawn_daemon(daemon_bin, None).await?;
    fixture.post_update(1002, 2, "after process kill").await?;

    let resumed_thread = fixture.wait_for_single_bound_thread().await?;
    if resumed_thread != first_thread {
        return Err(format!(
            "same routing key changed thread after restart: {first_thread} -> {resumed_thread}"
        )
        .into());
    }
    let receipts = fixture.wait_for_receipts(&resumed_thread, 2).await?;
    let resumed_entries = receipt_entry_ids(receipts.last().expect("two receipts"))?;
    if !pre_kill_entries
        .iter()
        .all(|entry_id| resumed_entries.contains(entry_id))
    {
        return Err(format!(
            "resumed compiled context omitted pre-kill entries: before={pre_kill_entries:?}, after={resumed_entries:?}"
        )
        .into());
    }

    restarted.sigkill().await?;
    Ok(())
}

async fn continuity_after_binding_crash_cut(
    daemon_bin: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new("binding-cut")?;
    let marker = fixture.root.join("binding-persisted.marker");
    let mut daemon = fixture.spawn_daemon(daemon_bin, Some(&marker)).await?;

    let addr = fixture.webhook_addr;
    let pending_request =
        tokio::spawn(async move { post_update(addr, 2001, 1, "crash before first turn").await });
    wait_for_path(&marker, Duration::from_secs(5)).await?;
    let bound_before_kill = fixture.wait_for_single_bound_thread().await?;
    if fixture.thread_event_count(&bound_before_kill, "turn.submitted")? != 0 {
        return Err("binding crash cut was reached after the first turn submission".into());
    }

    daemon.sigkill().await?;
    let _ = pending_request.await;
    let mut restarted = fixture.spawn_daemon(daemon_bin, None).await?;
    fixture
        .post_update(2002, 2, "resume after binding cut")
        .await?;

    let resumed_thread = fixture.wait_for_single_bound_thread().await?;
    if resumed_thread != bound_before_kill {
        return Err(format!(
            "binding crash cut created a duplicate thread: {bound_before_kill} -> {resumed_thread}"
        )
        .into());
    }
    fixture.wait_for_receipts(&resumed_thread, 1).await?;

    restarted.sigkill().await?;
    Ok(())
}

struct Fixture {
    root: PathBuf,
    config_path: PathBuf,
    io_db: PathBuf,
    history_db: PathBuf,
    webhook_addr: std::net::SocketAddr,
}

impl Fixture {
    fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let id = Uuid::now_v7().simple().to_string();
        let root = PathBuf::from("/tmp").join(format!("cdis-restart-{name}-{}", &id[..12]));
        std::fs::create_dir_all(&root)?;
        let webhook_addr = unused_loopback_addr()?;
        let config_path = root.join("cooldis.toml");
        let io_db = root.join("io.sqlite");
        let history_db = root.join("state/session_history.sqlite3");
        write_config(&config_path, &root, webhook_addr)?;
        Ok(Self {
            root,
            config_path,
            io_db,
            history_db,
            webhook_addr,
        })
    }

    async fn spawn_daemon(
        &self,
        daemon_bin: &Path,
        binding_marker: Option<&Path>,
    ) -> Result<DaemonChild, Box<dyn std::error::Error>> {
        let log_path = self.root.join("daemon.log");
        let stdout = append_file(&log_path)?;
        let stderr = append_file(&log_path)?;
        let mut command = Command::new(daemon_bin);
        command
            .arg("daemon")
            .arg("run")
            .arg("--config")
            .arg(&self.config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        if let Some(marker) = binding_marker {
            command.env("COOLDIS_TEST_PAUSE_AFTER_INGRESS_BINDING", marker);
        }
        let child = command.spawn()?;
        let daemon = DaemonChild {
            child: Some(child),
            log_path,
        };
        if let Err(err) = wait_for_listener(self.webhook_addr, Duration::from_secs(8)).await {
            return Err(format!("{err}; daemon log:\n{}", daemon.read_log()).into());
        }
        Ok(daemon)
    }

    async fn post_update(
        &self,
        update_id: i64,
        message_id: i64,
        text: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let response = post_update(self.webhook_addr, update_id, message_id, text)
            .await
            .map_err(|err| format!("webhook request failed: {err}"))?;
        if !response.starts_with("HTTP/1.1 200") || !response.contains("\"accepted\":true") {
            return Err(format!("unexpected webhook response: {response:?}").into());
        }
        Ok(())
    }

    async fn wait_for_single_bound_thread(&self) -> Result<String, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let threads = self.bound_threads()?;
            if threads.len() == 1 {
                return Ok(threads[0].clone());
            }
            if threads.len() > 1 {
                return Err(format!("routing key has duplicate bound threads: {threads:?}").into());
            }
            if Instant::now() >= deadline {
                return Err("timed out waiting for durable thread binding".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn bound_threads(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        if !self.io_db.exists() {
            return Ok(Vec::new());
        }
        let connection = Connection::open(&self.io_db)?;
        let mut statement = connection.prepare(
            "SELECT DISTINCT thread_id FROM cooldis_daemon_egress_threads WHERE route_id = ?1 ORDER BY thread_id",
        )?;
        let rows = statement.query_map(params![ROUTE_ID], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    async fn wait_for_receipts(
        &self,
        thread_id: &str,
        count: usize,
    ) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let receipts = self.context_receipts(thread_id)?;
            if receipts.len() >= count {
                return Ok(receipts);
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "timed out waiting for {count} compiled context receipt(s) on {thread_id}; saw {}",
                    receipts.len()
                )
                .into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn context_receipts(&self, thread_id: &str) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        if !self.history_db.exists() {
            return Ok(Vec::new());
        }
        let connection = Connection::open(&self.history_db)?;
        let mut statement = connection.prepare(
            "SELECT payload_json FROM observation_records WHERE thread_id = ?1 AND kind = 'compiled_context_receipt' ORDER BY created_at_ms, observation_id",
        )?;
        let rows = statement.query_map(params![thread_id], |row| row.get::<_, String>(0))?;
        rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
    }

    fn thread_event_count(
        &self,
        thread_id: &str,
        kind: &str,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let connection = Connection::open(&self.history_db)?;
        let count = connection.query_row(
            "SELECT COUNT(*) FROM event_records WHERE thread_id = ?1 AND kind = ?2",
            params![thread_id, kind],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count as usize)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct DaemonChild {
    child: Option<Child>,
    log_path: PathBuf,
}

impl DaemonChild {
    async fn sigkill(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut child) = self.child.take() {
            child.start_kill()?;
            tokio::time::timeout(Duration::from_secs(3), child.wait()).await??;
        }
        Ok(())
    }

    fn read_log(&self) -> String {
        let mut log = String::new();
        if let Ok(mut file) = File::open(&self.log_path) {
            let _ = file.read_to_string(&mut log);
        }
        log
    }
}

impl Drop for DaemonChild {
    fn drop(&mut self) {
        if let Some(child) = &mut self.child {
            let _ = child.start_kill();
        }
    }
}

fn daemon_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = std::env::var_os("COOLDIS_DAEMON_BIN") {
        return Ok(PathBuf::from(path));
    }
    let sibling = std::env::current_exe()?
        .parent()
        .ok_or("restart smoke executable had no parent directory")?
        .join(format!("cooldis{}", std::env::consts::EXE_SUFFIX));
    if sibling.is_file() {
        Ok(sibling)
    } else {
        Err(format!(
            "daemon binary not found at {}; build `cooldis` first or set COOLDIS_DAEMON_BIN",
            sibling.display()
        )
        .into())
    }
}

fn write_config(
    path: &Path,
    root: &Path,
    webhook_addr: std::net::SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = format!(
        r#"[daemon.runtime]
cwd = "{}"
runtime_home = "{}"
state_home = "{}"

[daemon.app_server]
listen = "unix://{}"

[daemon.provider]
provider = "local"

[daemon.io.ingress.persistence]
mode = "best_effort_direct"

[daemon.io.ingress.queue]
sqlite_path = "{}"

[[daemon.io.routes]]
id = "{ROUTE_ID}"
kind = "telegram.bot"
threading = "per_conversation"

[daemon.io.routes.telegram]
listen = "{webhook_addr}"
path = "{WEBHOOK_PATH}"
"#,
        toml_path(&std::env::current_dir()?),
        toml_path(&root.join("runtime")),
        toml_path(&root.join("state")),
        toml_path(&root.join("daemon.sock")),
        toml_path(&root.join("io.sqlite")),
    );
    std::fs::write(path, config)?;
    Ok(())
}

fn toml_path(path: &Path) -> String {
    path.display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn append_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn unused_loopback_addr() -> std::io::Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

async fn wait_for_listener(addr: std::net::SocketAddr, wait: Duration) -> Result<(), String> {
    let deadline = Instant::now() + wait;
    loop {
        if TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for daemon HTTP ingress at {addr}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_for_path(path: &Path, wait: Duration) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + wait;
    loop {
        if path.exists() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                format!("timed out waiting for crash-cut marker {}", path.display()).into(),
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn post_update(
    addr: std::net::SocketAddr,
    update_id: i64,
    message_id: i64,
    text: &str,
) -> Result<String, String> {
    let body = serde_json::to_vec(&json!({
        "update_id": update_id,
        "message": {
            "message_id": message_id,
            "from": { "id": 42, "is_bot": false, "first_name": "Restart" },
            "chat": { "id": 777, "type": "private" },
            "date": 1_700_000_000,
            "text": text,
        }
    }))
    .map_err(|err| err.to_string())?;
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|err| err.to_string())?;
    let request = format!(
        "POST {WEBHOOK_PATH} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|err| err.to_string())?;
    stream
        .write_all(&body)
        .await
        .map_err(|err| err.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(|err| err.to_string())?;
    Ok(response)
}

fn receipt_entry_ids(receipt: &Value) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let entries = receipt
        .get("session_entry_ids")
        .and_then(Value::as_array)
        .ok_or("compiled context receipt did not contain session_entry_ids")?;
    entries
        .iter()
        .map(|entry| {
            entry
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "compiled context receipt entry id was not a string".into())
        })
        .collect()
}
