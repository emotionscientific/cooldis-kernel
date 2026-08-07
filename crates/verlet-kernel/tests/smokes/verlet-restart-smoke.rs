use std::io::Read as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

const ROUTE_ID: &str = "restart-smoke";
const WEBHOOK_PATH: &str = "/ingress";
const WEBHOOK_SECRET: &str = "restart-smoke-secret";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let daemon_bin = daemon_binary()?;
    continuity_after_idle_kill(&daemon_bin).await?;
    continuity_after_binding_crash_cut(&daemon_bin).await?;
    println!("verlet restart smoke ok: idle restart and binding crash cut preserved continuity");
    Ok(())
}

async fn continuity_after_idle_kill(
    daemon_bin: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new("idle")?;
    let mut telegram = fixture.start_telegram_api().await?;
    let mut daemon = fixture.spawn_daemon(daemon_bin, None).await?;

    fixture.post_update(1001, 1, "before process kill").await?;
    let first_thread = fixture.wait_for_single_bound_thread().await?;
    telegram
        .wait_for_delivery("local:before process kill")
        .await?;
    daemon.sigkill().await?;
    let first_receipt = fixture
        .context_receipts(&first_thread)
        .await?
        .into_iter()
        .next()
        .ok_or("first turn did not compile a durable context")?;
    let pre_kill_entries = receipt_entry_ids(&first_receipt)?;
    if pre_kill_entries.is_empty() {
        return Err("first compiled context receipt did not contain session entries".into());
    }

    let mut restarted = fixture.spawn_daemon(daemon_bin, None).await?;
    fixture.post_update(1002, 2, "after process kill").await?;
    telegram
        .wait_for_delivery("local:after process kill")
        .await?;

    let resumed_thread = fixture.wait_for_single_bound_thread().await?;
    if resumed_thread != first_thread {
        return Err(format!(
            "same routing key changed thread after restart: {first_thread} -> {resumed_thread}"
        )
        .into());
    }
    restarted.sigkill().await?;
    let receipts = fixture.context_receipts(&resumed_thread).await?;
    if receipts.len() < 2 {
        return Err(format!(
            "resumed turn did not add a second durable context receipt; saw {}",
            receipts.len()
        )
        .into());
    }
    let resumed_receipt = receipts.last().expect("receipt count checked above");
    let resumed_entries = receipt_entry_ids(resumed_receipt)?;
    if !pre_kill_entries
        .iter()
        .all(|entry_id| resumed_entries.contains(entry_id))
    {
        return Err(format!(
            "resumed compiled context omitted pre-kill entries: before={pre_kill_entries:?}, after={resumed_entries:?}"
        )
        .into());
    }

    Ok(())
}

async fn continuity_after_binding_crash_cut(
    daemon_bin: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new("binding-cut")?;
    let mut telegram = fixture.start_telegram_api().await?;
    let marker = fixture.root.join("binding-persisted.marker");
    let mut daemon = fixture.spawn_daemon(daemon_bin, Some(&marker)).await?;

    let addr = fixture.webhook_addr;
    let pending_request =
        tokio::spawn(async move { post_update(addr, 2001, 1, "crash before first turn").await });
    wait_for_path(&marker, std::time::Duration::from_secs(5)).await?;
    let bound_before_kill = fixture.wait_for_single_bound_thread().await?;
    daemon.sigkill().await?;
    if fixture
        .thread_event_count(&bound_before_kill, "turn.submitted")
        .await?
        != 0
    {
        return Err("binding crash cut was reached after the first turn submission".into());
    }

    let _ = pending_request.await;
    let mut restarted = fixture.spawn_daemon(daemon_bin, None).await?;
    fixture
        .post_update(2002, 2, "resume after binding cut")
        .await?;
    telegram
        .wait_for_delivery("local:resume after binding cut")
        .await?;

    let resumed_thread = fixture.wait_for_single_bound_thread().await?;
    if resumed_thread != bound_before_kill {
        return Err(format!(
            "binding crash cut created a duplicate thread: {bound_before_kill} -> {resumed_thread}"
        )
        .into());
    }
    restarted.sigkill().await?;
    if fixture.context_receipts(&resumed_thread).await?.is_empty() {
        return Err("resumed binding did not compile a durable context".into());
    }
    Ok(())
}

struct Fixture {
    root: std::path::PathBuf,
    config_path: std::path::PathBuf,
    io_db: std::path::PathBuf,
    history_db: std::path::PathBuf,
    webhook_addr: std::net::SocketAddr,
    telegram_api_addr: std::net::SocketAddr,
}

impl Fixture {
    fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let id = uuid::Uuid::now_v7().simple().to_string();
        let root =
            std::path::PathBuf::from("/tmp").join(format!("cdis-restart-{name}-{}", &id[..12]));
        std::fs::create_dir_all(&root)?;
        let webhook_addr = unused_loopback_addr()?;
        let telegram_api_addr = unused_loopback_addr()?;
        let config_path = root.join("verlet.toml");
        let io_db = root.join("io.sqlite");
        let history_db = root.join("state/session_history.sqlite3");
        write_config(&config_path, &root, webhook_addr, telegram_api_addr)?;
        Ok(Self {
            root,
            config_path,
            io_db,
            history_db,
            webhook_addr,
            telegram_api_addr,
        })
    }

    async fn start_telegram_api(&self) -> Result<TelegramApiFixture, Box<dyn std::error::Error>> {
        TelegramApiFixture::start(self.telegram_api_addr).await
    }

    async fn spawn_daemon(
        &self,
        daemon_bin: &std::path::Path,
        binding_marker: Option<&std::path::Path>,
    ) -> Result<DaemonChild, Box<dyn std::error::Error>> {
        let log_path = self.root.join("daemon.log");
        let stdout = append_file(&log_path)?;
        let stderr = append_file(&log_path)?;
        let mut command = tokio::process::Command::new(daemon_bin);
        command
            .arg("daemon")
            .arg("run")
            .arg("--config")
            .arg(&self.config_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::from(stdout))
            .stderr(std::process::Stdio::from(stderr));
        if let Some(marker) = binding_marker {
            command.env("VERLET_TEST_PAUSE_AFTER_INGRESS_BINDING", marker);
        }
        let child = command.spawn()?;
        let daemon = DaemonChild {
            child: Some(child),
            log_path,
        };
        if let Err(err) =
            wait_for_listener(self.webhook_addr, std::time::Duration::from_secs(8)).await
        {
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
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let threads = self.bound_threads()?;
            if threads.len() == 1 {
                return Ok(threads[0].clone());
            }
            if threads.len() > 1 {
                return Err(format!("routing key has duplicate bound threads: {threads:?}").into());
            }
            if std::time::Instant::now() >= deadline {
                return Err("timed out waiting for durable thread binding".into());
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    fn bound_threads(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        if !self.io_db.exists() {
            return Ok(Vec::new());
        }
        let connection = rusqlite::Connection::open(&self.io_db)?;
        let mut statement = connection.prepare(
            "SELECT DISTINCT thread_id FROM cooldis_daemon_egress_threads WHERE route_id = ?1 ORDER BY thread_id",
        )?;
        let rows =
            statement.query_map(rusqlite::params![ROUTE_ID], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    async fn context_receipts(
        &self,
        thread_id: &str,
    ) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
        let db =
            verlet_sqlite::Db::open(&self.history_db, verlet_sqlite::DbConfig::default()).await?;
        let connection = db.connect().await?;
        let mut rows = connection
            .query(
                "SELECT payload_json FROM observation_records WHERE thread_id = ?1 AND kind = 'compiled_context_receipt' ORDER BY created_at_ms, observation_id",
                verlet_sqlite::params![thread_id],
            )
            .await?;
        let mut receipts = Vec::new();
        while let Some(row) = rows.next().await? {
            let payload_json: String = row.get(0)?;
            receipts.push(serde_json::from_str(&payload_json)?);
        }
        Ok(receipts)
    }

    async fn thread_event_count(
        &self,
        thread_id: &str,
        kind: &str,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let db =
            verlet_sqlite::Db::open(&self.history_db, verlet_sqlite::DbConfig::default()).await?;
        let connection = db.connect().await?;
        let mut rows = connection
            .query(
                "SELECT COUNT(*) FROM event_records WHERE thread_id = ?1 AND kind = ?2",
                verlet_sqlite::params![thread_id, kind],
            )
            .await?;
        let row = rows
            .next()
            .await?
            .ok_or("event count query returned no row")?;
        let count: i64 = row.get(0)?;
        Ok(count as usize)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct TelegramApiFixture {
    deliveries: tokio::sync::mpsc::UnboundedReceiver<String>,
    task: tokio::task::JoinHandle<()>,
}

impl TelegramApiFixture {
    async fn start(addr: std::net::SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let (deliveries_tx, deliveries) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                if let Ok(delivered_text) = serve_telegram_api_connection(stream).await {
                    let _ = deliveries_tx.send(delivered_text);
                }
            }
        });
        Ok(Self { deliveries, task })
    }

    async fn wait_for_delivery(
        &mut self,
        expected_text: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut observed = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let delivered_text = match tokio::time::timeout(remaining, self.deliveries.recv()).await
            {
                Ok(Some(delivered_text)) => delivered_text,
                Ok(None) => return Err("Telegram API fixture stopped before delivery".into()),
                Err(_) => {
                    return Err(format!(
                        "timed out waiting for Telegram delivery {expected_text:?}; observed {observed:?}"
                    )
                    .into());
                }
            };
            if delivered_text == expected_text {
                return Ok(());
            }
            observed.push(delivered_text);
        }
    }
}

impl Drop for TelegramApiFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_telegram_api_connection(
    mut stream: tokio::net::TcpStream,
) -> Result<String, String> {
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    let (header_end, expected_len) = loop {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|err| err.to_string())?;
        if read == 0 {
            return Err("Telegram API request closed before headers".to_string());
        }
        request.extend_from_slice(&buffer[..read]);
        if request.len() > 64 * 1024 {
            return Err("Telegram API fixture request exceeded 64 KiB".to_string());
        }
        if let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = header_end + 4;
            let headers =
                std::str::from_utf8(&request[..header_end]).map_err(|err| err.to_string())?;
            if !headers.starts_with("POST /botrestart-smoke-token/sendMessage ") {
                return Err(format!("unexpected Telegram API request: {headers:?}"));
            }
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .ok_or_else(|| "Telegram API request omitted Content-Length".to_string())?;
            break (header_end, header_end + content_length);
        }
    };
    while request.len() < expected_len {
        let read = stream
            .read(&mut buffer)
            .await
            .map_err(|err| err.to_string())?;
        if read == 0 {
            return Err("Telegram API request closed before body".to_string());
        }
        request.extend_from_slice(&buffer[..read]);
    }
    let delivered_text =
        serde_json::from_slice::<serde_json::Value>(&request[header_end..expected_len])
            .map_err(|err| format!("Telegram API request body was not JSON: {err}"))?
            .get("text")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Telegram API request body omitted text".to_string())?
            .to_string();

    let body = r#"{"ok":true,"result":{"message_id":9001,"chat":{"id":777,"type":"private"},"date":1700000000,"text":"delivered"}}"#;
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .map_err(|err| err.to_string())?;
    Ok(delivered_text)
}

struct DaemonChild {
    child: Option<tokio::process::Child>,
    log_path: std::path::PathBuf,
}

impl DaemonChild {
    async fn sigkill(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(mut child) = self.child.take() {
            child.start_kill()?;
            tokio::time::timeout(std::time::Duration::from_secs(30), child.wait()).await??;
        }
        Ok(())
    }

    fn read_log(&self) -> String {
        let mut log = String::new();
        if let Ok(mut file) = std::fs::File::open(&self.log_path) {
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

fn daemon_binary() -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    if let Some(path) = verlet_runtime_contracts::env_compat::var_os("VERLET_DAEMON_BIN") {
        return Ok(std::path::PathBuf::from(path));
    }
    let sibling = std::env::current_exe()?
        .parent()
        .ok_or("restart smoke executable had no parent directory")?
        .join(format!("verlet{}", std::env::consts::EXE_SUFFIX));
    if sibling.is_file() {
        Ok(sibling)
    } else {
        Err(format!(
            "daemon binary not found at {}; build `verlet` first or set VERLET_DAEMON_BIN",
            sibling.display()
        )
        .into())
    }
}

fn write_config(
    path: &std::path::Path,
    root: &std::path::Path,
    webhook_addr: std::net::SocketAddr,
    telegram_api_addr: std::net::SocketAddr,
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
bot_token = "restart-smoke-token"
secret_token = "{WEBHOOK_SECRET}"
api_base = "http://{telegram_api_addr}"
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

fn toml_path(path: &std::path::Path) -> String {
    path.display()
        .to_string()
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
}

fn append_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

fn unused_loopback_addr() -> std::io::Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr()
}

async fn wait_for_listener(
    addr: std::net::SocketAddr,
    wait: std::time::Duration,
) -> Result<(), String> {
    let deadline = std::time::Instant::now() + wait;
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for daemon HTTP ingress at {addr}"
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn wait_for_path(
    path: &std::path::Path,
    wait: std::time::Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = std::time::Instant::now() + wait;
    loop {
        if path.exists() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(
                format!("timed out waiting for crash-cut marker {}", path.display()).into(),
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

async fn post_update(
    addr: std::net::SocketAddr,
    update_id: i64,
    message_id: i64,
    text: &str,
) -> Result<String, String> {
    let body = serde_json::to_vec(&serde_json::json!({
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
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .map_err(|err| err.to_string())?;
    let request = format!(
        "POST {WEBHOOK_PATH} HTTP/1.1\r\nHost: {addr}\r\nX-Telegram-Bot-Api-Secret-Token: {WEBHOOK_SECRET}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
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

fn receipt_entry_ids(
    receipt: &serde_json::Value,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let entries = receipt
        .get("session_entry_ids")
        .and_then(serde_json::Value::as_array)
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
