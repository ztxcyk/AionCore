use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use aionui_api_types::{PreviewState, PreviewStatusEvent, WebSocketMessage};
use aionui_realtime::EventBroadcaster;
use aionui_runtime::Builder as CmdBuilder;
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::error::OfficeError;
use crate::officecli_runtime::{OFFICECLI_LATEST_RELEASE_URL, OfficecliPolicy, install_command, resolve_officecli_path};
use crate::port::{allocate_port, is_port_listening};
use crate::types::DocType;

const POLL_INTERVAL_MS: u64 = 100;
const POLL_MAX_ATTEMPTS: u32 = 150;
const START_PORT_MAX_ATTEMPTS: usize = 3;
const STOP_DELAY_MS: u64 = 500;
const VERSION_CHECK_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

// ---------------------------------------------------------------------------
// ProcessSpawner trait — abstraction for child process management
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
pub trait ProcessSpawner: Send + Sync {
    fn is_private_officecli(&self) -> bool {
        false
    }

    async fn spawn_officecli(
        &self,
        file_path: &str,
        port: u16,
        doc_type: DocType,
    ) -> Result<Box<dyn ProcessHandle>, OfficeError>;

    async fn install_officecli(&self) -> Result<(), OfficeError>;

    async fn is_officecli_installed(&self) -> bool;

    async fn check_update(&self, doc_type: DocType) -> Result<(), OfficeError>;
}

pub trait ProcessHandle: Send + Sync {
    fn kill(&self);
    fn is_alive(&self) -> bool;
}

// ---------------------------------------------------------------------------
// WatchSession — per-file preview session
// ---------------------------------------------------------------------------

struct WatchSession {
    user_id: String,
    port: u16,
    process: Box<dyn ProcessHandle>,
    file_path: String,
    doc_type: DocType,
    aborted: bool,
}

// ---------------------------------------------------------------------------
// OfficecliWatchManager
// ---------------------------------------------------------------------------

pub struct OfficecliWatchManager {
    sessions: DashMap<WatchSessionKey, WatchSession>,
    spawner: Arc<dyn ProcessSpawner>,
    broadcaster: Arc<dyn EventBroadcaster>,
    last_version_check: Mutex<Option<std::time::Instant>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WatchSessionKey {
    user_id: String,
    doc_type: DocType,
    resolved_path: String,
}

impl WatchSessionKey {
    fn new(user_id: &str, resolved_path: &str, doc_type: DocType) -> Self {
        Self {
            user_id: user_id.to_owned(),
            doc_type,
            resolved_path: resolved_path.to_owned(),
        }
    }
}

impl OfficecliWatchManager {
    pub fn new(spawner: Arc<dyn ProcessSpawner>, broadcaster: Arc<dyn EventBroadcaster>) -> Self {
        Self {
            sessions: DashMap::new(),
            spawner,
            broadcaster,
            last_version_check: Mutex::new(None),
        }
    }

    pub async fn start(&self, file_path: &str, doc_type: DocType) -> Result<u16, OfficeError> {
        self.start_for_user("system_default_user", file_path, doc_type).await
    }

    pub async fn start_for_user(&self, user_id: &str, file_path: &str, doc_type: DocType) -> Result<u16, OfficeError> {
        let resolved = resolve_path(file_path)?;
        let key = session_key(user_id, &resolved, doc_type);

        if let Some(entry) = self.sessions.get(&key) {
            if !entry.aborted && entry.process.is_alive() {
                return Ok(entry.port);
            }
            drop(entry);
            self.sessions.remove(&key);
        }

        self.broadcast_status_for_user(user_id, doc_type, PreviewState::Starting, None);

        let result = self.try_start(user_id, &resolved, doc_type).await;

        match &result {
            Ok(port) => {
                self.broadcast_status_for_user(user_id, doc_type, PreviewState::Ready, None);
                if doc_type == DocType::Ppt && !self.spawner.is_private_officecli() {
                    self.maybe_check_update(doc_type).await;
                }
                Ok(*port)
            }
            Err(e) => {
                self.broadcast_status_for_user(
                    user_id,
                    doc_type,
                    PreviewState::Error,
                    Some(public_preview_error_message(e)),
                );
                Err(match e {
                    OfficeError::OfficecliNotFound => OfficeError::OfficecliNotFound,
                    OfficeError::InstallFailed(m) => OfficeError::InstallFailed(m.clone()),
                    OfficeError::StartFailed(m) => OfficeError::StartFailed(m.clone()),
                    OfficeError::PortTimeout(m) => OfficeError::PortTimeout(m.clone()),
                    OfficeError::Io(io) => OfficeError::StartFailed(format!("IO error: {io}")),
                    OfficeError::Snapshot(m) => OfficeError::StartFailed(m.clone()),
                    OfficeError::Json(e) => OfficeError::StartFailed(format!("JSON error: {e}")),
                    OfficeError::Conversion(m) => OfficeError::StartFailed(m.clone()),
                    OfficeError::ToolNotFound(m) => OfficeError::StartFailed(m.clone()),
                })
            }
        }
    }

    async fn try_start(&self, user_id: &str, resolved: &str, doc_type: DocType) -> Result<u16, OfficeError> {
        for attempt in 1..=START_PORT_MAX_ATTEMPTS {
            let port = allocate_port()?;
            match self
                .spawn_officecli_with_install(user_id, resolved, port, doc_type)
                .await
            {
                Ok(process) => {
                    self.poll_port_ready(port, resolved).await?;

                    let key = session_key(user_id, resolved, doc_type);
                    self.sessions.insert(
                        key,
                        WatchSession {
                            user_id: user_id.to_owned(),
                            port,
                            process,
                            file_path: resolved.to_owned(),
                            doc_type,
                            aborted: false,
                        },
                    );

                    return Ok(port);
                }
                Err(error) if is_port_in_use_start_failure(&error) && attempt < START_PORT_MAX_ATTEMPTS => {
                    tracing::debug!(
                        port,
                        attempt,
                        max_attempts = START_PORT_MAX_ATTEMPTS,
                        "allocated preview port was already in use; retrying"
                    );
                }
                Err(error) => return Err(error),
            }
        }

        Err(OfficeError::StartFailed(
            "failed to allocate an available preview port".into(),
        ))
    }

    async fn spawn_officecli_with_install(
        &self,
        user_id: &str,
        resolved: &str,
        port: u16,
        doc_type: DocType,
    ) -> Result<Box<dyn ProcessHandle>, OfficeError> {
        match self.spawner.spawn_officecli(resolved, port, doc_type).await {
            Ok(process) => Ok(process),
            Err(OfficeError::OfficecliNotFound) if !self.spawner.is_private_officecli() => {
                self.broadcast_status_for_user(user_id, doc_type, PreviewState::Installing, None);
                self.spawner.install_officecli().await?;
                self.spawner.spawn_officecli(resolved, port, doc_type).await
            }
            Err(error) => Err(error),
        }
    }

    async fn poll_port_ready(&self, port: u16, file_path: &str) -> Result<(), OfficeError> {
        for _ in 0..POLL_MAX_ATTEMPTS {
            if is_port_listening(port).await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
        Err(OfficeError::PortTimeout(file_path.to_owned()))
    }

    pub async fn stop(&self, file_path: &str, doc_type: DocType) {
        self.stop_for_user("system_default_user", file_path, doc_type).await;
    }

    pub async fn stop_for_user(&self, user_id: &str, file_path: &str, doc_type: DocType) {
        let resolved = match resolve_path(file_path) {
            Ok(p) => p,
            Err(_) => return,
        };
        let key = session_key(user_id, &resolved, doc_type);

        tokio::time::sleep(Duration::from_millis(STOP_DELAY_MS)).await;

        if let Some((_, session)) = self.sessions.remove(&key) {
            session.process.kill();
        }
    }

    pub fn stop_all(&self) {
        for entry in self.sessions.iter() {
            tracing::debug!(
                file_path = %entry.value().file_path,
                doc_type = %entry.value().doc_type,
                "stopping preview session"
            );
            entry.value().process.kill();
        }
        self.sessions.clear();
    }

    pub fn stop_all_for_user(&self, user_id: &str) -> usize {
        let keys: Vec<WatchSessionKey> = self
            .sessions
            .iter()
            .filter(|entry| entry.key().user_id == user_id)
            .map(|entry| entry.key().clone())
            .collect();
        let stopped = keys.len();

        for key in keys {
            if let Some((_, session)) = self.sessions.remove(&key) {
                session.process.kill();
            }
        }

        if stopped > 0 {
            tracing::info!(user_id = %user_id, stopped, "stopped office preview sessions for user");
        }
        stopped
    }

    pub fn is_active_port(&self, port: u16, doc_type: DocType) -> bool {
        self.sessions
            .iter()
            .any(|entry| entry.port == port && entry.doc_type == doc_type)
    }

    pub fn is_active_port_for_user(&self, user_id: &str, port: u16, doc_type: DocType) -> bool {
        self.sessions
            .iter()
            .any(|entry| entry.user_id == user_id && entry.port == port && entry.doc_type == doc_type)
    }

    pub fn is_active_watch_port(&self, port: u16) -> bool {
        self.sessions
            .iter()
            .any(|entry| entry.port == port && matches!(entry.doc_type, DocType::Word | DocType::Excel))
    }

    pub fn is_active_watch_port_for_user(&self, user_id: &str, port: u16) -> bool {
        self.sessions.iter().any(|entry| {
            entry.user_id == user_id && entry.port == port && matches!(entry.doc_type, DocType::Word | DocType::Excel)
        })
    }

    pub fn active_session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Port of this user's live watch for `file_path`, or `None` when no such
    /// session exists (never started, already stopped, or the child died).
    ///
    /// The inverse of the `is_active_port*` family above, which asks "is this port
    /// live" — the refresh endpoint arrives with a document and needs the port to
    /// reach its server on.
    ///
    /// `file_path` goes through the same [`resolve_path`] the start path uses before
    /// keying, so a caller may pass any spelling of the path. Skipping that step
    /// would make every lookup miss whenever the caller's spelling differed from the
    /// canonical one, and the failure would look like "no session" rather than a
    /// key mismatch.
    pub(crate) fn active_port_for(&self, user_id: &str, file_path: &str, doc_type: DocType) -> Option<u16> {
        let resolved = resolve_path(file_path).ok()?;
        let entry = self.sessions.get(&session_key(user_id, &resolved, doc_type))?;
        // Same liveness test as `start_for_user`: an aborted or dead session still
        // occupies the map until its next start, and its port serves nothing.
        (!entry.aborted && entry.process.is_alive()).then_some(entry.port)
    }

    async fn maybe_check_update(&self, doc_type: DocType) {
        let mut last = self.last_version_check.lock().await;
        let should_check = match *last {
            Some(t) => t.elapsed() >= VERSION_CHECK_INTERVAL,
            None => true,
        };
        if should_check {
            *last = Some(std::time::Instant::now());
            drop(last);
            let spawner = Arc::clone(&self.spawner);
            tokio::spawn(async move {
                if let Err(e) = spawner.check_update(doc_type).await {
                    tracing::warn!("officecli version check failed: {e}");
                }
            });
        }
    }

    fn broadcast_status_for_user(
        &self,
        user_id: &str,
        doc_type: DocType,
        state: PreviewState,
        message: Option<String>,
    ) {
        let event_name = format!("{}.status", doc_type.event_prefix());
        let payload = PreviewStatusEvent { state, message };
        let mut data = match serde_json::to_value(payload) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("failed to serialize preview status: {e}");
                return;
            }
        };
        data["user_id"] = serde_json::Value::String(user_id.to_owned());
        self.broadcaster.broadcast(WebSocketMessage::new(event_name, data));
    }
}

impl Drop for OfficecliWatchManager {
    fn drop(&mut self) {
        for entry in self.sessions.iter() {
            entry.value().process.kill();
        }
        self.sessions.clear();
    }
}

// ---------------------------------------------------------------------------
// DefaultProcessSpawner — real implementation using tokio::process
// ---------------------------------------------------------------------------

pub struct DefaultProcessSpawner {
    _data_dir: PathBuf,
    policy: OfficecliPolicy,
}

impl DefaultProcessSpawner {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            _data_dir: data_dir,
            policy: OfficecliPolicy::from_env(),
        }
    }
}

struct TokioProcessHandle {
    child: Mutex<Option<tokio::process::Child>>,
}

impl ProcessHandle for TokioProcessHandle {
    fn kill(&self) {
        if let Ok(mut guard) = self.child.try_lock() {
            if let Some(ref mut child) = *guard {
                let _ = child.start_kill();
            }
            *guard = None;
        }
    }

    fn is_alive(&self) -> bool {
        if let Ok(mut guard) = self.child.try_lock()
            && let Some(ref mut child) = *guard
        {
            return child.try_wait().ok().flatten().is_none();
        }
        false
    }
}

#[async_trait::async_trait]
impl ProcessSpawner for DefaultProcessSpawner {
    fn is_private_officecli(&self) -> bool {
        self.policy.is_private()
    }

    async fn spawn_officecli(
        &self,
        file_path: &str,
        port: u16,
        _doc_type: DocType,
    ) -> Result<Box<dyn ProcessHandle>, OfficeError> {
        let officecli = self.policy.resolve_with(resolve_officecli_path)?;
        if !officecli_supports_watch(&officecli, &self.policy).await {
            return Err(if self.policy.is_private() {
                OfficeError::StartFailed(format!(
                    "private OfficeCLI is unavailable or does not support watch: {}",
                    officecli.display()
                ))
            } else {
                OfficeError::OfficecliNotFound
            });
        }

        let mut builder = CmdBuilder::new(&officecli);
        self.policy.configure_command(&mut builder);
        builder
            .arg("watch")
            .arg(file_path)
            .arg("--port")
            .arg(port.to_string())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let child = builder.spawn().map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound && !self.policy.is_private() {
                OfficeError::OfficecliNotFound
            } else {
                OfficeError::StartFailed(e.to_string())
            }
        })?;

        Ok(Box::new(TokioProcessHandle {
            child: Mutex::new(Some(child)),
        }))
    }

    async fn install_officecli(&self) -> Result<(), OfficeError> {
        if self.policy.is_private() {
            return Err(OfficeError::InstallFailed("private OfficeCLI cannot be installed or updated".into()));
        }
        let command = install_command();
        tracing::info!(
            platform = std::env::consts::OS,
            "installing officecli via official iOfficeAI/OfficeCli installer"
        );

        let mut builder = CmdBuilder::clean_cli(&command.program);
        builder.args(command.args.iter());
        let output = builder
            .output()
            .await
            .map_err(|e| OfficeError::InstallFailed(e.to_string()))?;

        if !output.status.success() {
            tracing::warn!(
                status = ?output.status.code(),
                "officecli official installer failed"
            );
            return Err(OfficeError::InstallFailed("official OfficeCLI installer failed".into()));
        }
        Ok(())
    }

    async fn is_officecli_installed(&self) -> bool {
        let Ok(officecli) = self.policy.resolve_with(resolve_officecli_path) else {
            return false;
        };

        officecli_supports_watch(&officecli, &self.policy).await
    }

    async fn check_update(&self, _doc_type: DocType) -> Result<(), OfficeError> {
        if self.policy.is_private() {
            return Ok(());
        }
        let officecli = self.policy.resolve_with(resolve_officecli_path)?;
        if !officecli_supports_watch(&officecli, &self.policy).await {
            return Err(OfficeError::OfficecliNotFound);
        }

        let mut builder = CmdBuilder::clean_cli(&officecli);
        builder.arg("--version");
        let output = builder
            .output()
            .await
            .map_err(|e| OfficeError::StartFailed(e.to_string()))?;

        if !output.status.success() {
            return Ok(());
        }

        let local_version = normalize_officecli_version(&String::from_utf8_lossy(&output.stdout));
        let response = reqwest::Client::new()
            .get(OFFICECLI_LATEST_RELEASE_URL)
            .send()
            .await
            .map_err(|e| OfficeError::StartFailed(e.to_string()))?;
        let remote_version = response
            .url()
            .path_segments()
            .and_then(|mut segments| segments.next_back())
            .map(normalize_officecli_version)
            .unwrap_or_default();

        if !remote_version.is_empty() && remote_version != local_version {
            tracing::info!(
                local_version = %local_version,
                remote_version = %remote_version,
                "officecli update available, installing via official installer"
            );
            self.install_officecli().await?;
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn resolve_path(file_path: &str) -> Result<String, OfficeError> {
    let path = Path::new(file_path);
    let resolved = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    Ok(resolved.to_string_lossy().into_owned())
}

fn session_key(user_id: &str, resolved_path: &str, doc_type: DocType) -> WatchSessionKey {
    WatchSessionKey::new(user_id, resolved_path, doc_type)
}

fn is_port_in_use_start_failure(error: &OfficeError) -> bool {
    matches!(error, OfficeError::StartFailed(message) if is_port_in_use_message(message))
}

fn is_port_in_use_message(message: &str) -> bool {
    message.contains("Address already in use")
        || message.contains("Only one usage of each socket address")
        || message.contains("os error 98")
        || message.contains("os error 48")
        || message.contains("os error 10048")
}

fn normalize_officecli_version(raw: &str) -> String {
    raw.split_whitespace()
        .last()
        .unwrap_or_default()
        .trim_start_matches('v')
        .to_owned()
}

async fn officecli_supports_watch(officecli: &Path, policy: &OfficecliPolicy) -> bool {
    let mut version = CmdBuilder::clean_cli(officecli);
    policy.configure_command(&mut version);
    version.arg("--version");
    if !version.output().await.is_ok_and(|o| o.status.success()) {
        return false;
    }

    let mut watch_help = CmdBuilder::clean_cli(officecli);
    policy.configure_command(&mut watch_help);
    watch_help.args(["watch", "--help"]);
    let ok = watch_help.output().await.is_ok_and(|o| o.status.success());
    if !ok {
        tracing::warn!("officecli exists but does not expose watch command");
    }
    ok
}

fn public_preview_error_message(error: &OfficeError) -> String {
    match error {
        OfficeError::InstallFailed(_) => "officecli install failed".to_owned(),
        OfficeError::OfficecliNotFound => "officecli not found".to_owned(),
        _ => error.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    struct MockProcessHandle {
        alive: AtomicBool,
        killed: AtomicBool,
    }

    impl MockProcessHandle {
        fn new() -> Self {
            Self {
                alive: AtomicBool::new(true),
                killed: AtomicBool::new(false),
            }
        }
    }

    impl ProcessHandle for MockProcessHandle {
        fn kill(&self) {
            self.alive.store(false, Ordering::SeqCst);
            self.killed.store(true, Ordering::SeqCst);
        }

        fn is_alive(&self) -> bool {
            self.alive.load(Ordering::SeqCst)
        }
    }

    struct MockSpawner {
        installed: AtomicBool,
        private: bool,
        spawn_count: AtomicU32,
        install_count: AtomicU32,
        update_count: AtomicU32,
        fail_spawn: AtomicBool,
        fail_with_address_in_use_once: AtomicBool,
        start_listener: AtomicBool,
    }

    impl MockSpawner {
        fn new() -> Self {
            Self {
                installed: AtomicBool::new(true),
                private: false,
                spawn_count: AtomicU32::new(0),
                install_count: AtomicU32::new(0),
                update_count: AtomicU32::new(0),
                fail_spawn: AtomicBool::new(false),
                fail_with_address_in_use_once: AtomicBool::new(false),
                start_listener: AtomicBool::new(true),
            }
        }
    }

    #[async_trait::async_trait]
    impl ProcessSpawner for MockSpawner {
        fn is_private_officecli(&self) -> bool {
            self.private
        }

        async fn spawn_officecli(
            &self,
            _file_path: &str,
            port: u16,
            _doc_type: DocType,
        ) -> Result<Box<dyn ProcessHandle>, OfficeError> {
            self.spawn_count.fetch_add(1, Ordering::SeqCst);

            if self.fail_spawn.load(Ordering::SeqCst) {
                return Err(OfficeError::StartFailed("mock spawn failure".into()));
            }

            if self.fail_with_address_in_use_once.swap(false, Ordering::SeqCst) {
                return Err(OfficeError::StartFailed("Address already in use (os error 98)".into()));
            }

            if !self.installed.load(Ordering::SeqCst) {
                return Err(OfficeError::OfficecliNotFound);
            }

            if self.start_listener.load(Ordering::SeqCst) {
                let listener = std::net::TcpListener::bind(format!("127.0.0.1:{port}"))
                    .map_err(|e| OfficeError::StartFailed(e.to_string()))?;
                std::mem::forget(listener);
            }

            Ok(Box::new(MockProcessHandle::new()))
        }

        async fn install_officecli(&self) -> Result<(), OfficeError> {
            self.install_count.fetch_add(1, Ordering::SeqCst);
            self.installed.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn is_officecli_installed(&self) -> bool {
            self.installed.load(Ordering::SeqCst)
        }

        async fn check_update(&self, _doc_type: DocType) -> Result<(), OfficeError> {
            self.update_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct RecordingBroadcaster {
        events: std::sync::Mutex<Vec<WebSocketMessage<serde_json::Value>>>,
    }

    impl RecordingBroadcaster {
        fn new() -> Self {
            Self {
                events: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<WebSocketMessage<serde_json::Value>> {
            self.events.lock().unwrap().clone()
        }
    }

    impl EventBroadcaster for RecordingBroadcaster {
        fn broadcast(&self, event: WebSocketMessage<serde_json::Value>) {
            self.events.lock().unwrap().push(event);
        }
    }

    fn make_manager(spawner: Arc<MockSpawner>, broadcaster: Arc<RecordingBroadcaster>) -> OfficecliWatchManager {
        OfficecliWatchManager::new(spawner, broadcaster)
    }

    #[tokio::test]
    async fn private_preview_does_not_install_on_missing_binary() {
        let mut mock = MockSpawner::new();
        mock.private = true;
        mock.installed.store(false, Ordering::SeqCst);
        let spawner = Arc::new(mock);
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));
        let result = mgr.start("missing.docx", DocType::Word).await;
        assert!(result.is_err());
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 1);
        assert_eq!(spawner.install_count.load(Ordering::SeqCst), 0);
        assert!(!broadcaster.events().iter().any(|e| e.data["state"] == "installing"));
    }

    #[tokio::test]
    async fn private_ppt_preview_never_checks_for_updates() {
        let mut mock = MockSpawner::new();
        mock.private = true;
        let spawner = Arc::new(mock);
        let mgr = make_manager(Arc::clone(&spawner), Arc::new(RecordingBroadcaster::new()));
        mgr.start("test.pptx", DocType::Ppt).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spawner.update_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn private_spawner_direct_install_and_update_are_blocked() {
        let spawner = DefaultProcessSpawner {
            _data_dir: PathBuf::new(),
            policy: OfficecliPolicy::from_private_path(Some(std::ffi::OsString::new())),
        };
        assert!(spawner.install_officecli().await.is_err());
        assert!(spawner.check_update(DocType::Ppt).await.is_ok());
    }

    #[tokio::test]
    async fn private_spawner_reports_invalid_path_without_installing() {
        let spawner = DefaultProcessSpawner {
            _data_dir: PathBuf::new(),
            policy: OfficecliPolicy::from_private_path(Some(std::ffi::OsString::new())),
        };
        let result = spawner.spawn_officecli("test.docx", 12345, DocType::Word).await;
        assert!(matches!(result, Err(OfficeError::ToolNotFound(message)) if message.contains("AIONUI_OFFICECLI_PRIVATE_PATH")));
        assert!(!spawner.is_officecli_installed().await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn officecli_child_processes_receive_private_update_policy() {
        use std::os::unix::fs::PermissionsExt;

        if std::env::var_os("AIONUI_TEST_OFFICECLI_CHILD").is_some() {
            use aionui_api_types::ConversionTarget;

            let private = std::env::var_os("AIONUI_OFFICECLI_PRIVATE_PATH").is_some();
            let expected = if private { "1" } else { "parent-value" };
            let exe = PathBuf::from(std::env::var_os("AIONUI_TEST_OFFICECLI").unwrap());
            let log = PathBuf::from(std::env::var_os("AIONUI_TEST_LOG").unwrap());
            let file = std::env::var("AIONUI_TEST_DOCUMENT").unwrap();
            let spawner = DefaultProcessSpawner::new(PathBuf::new());

            assert!(spawner.is_officecli_installed().await);
            let process = spawner.spawn_officecli(&file, 12345, DocType::Ppt).await.unwrap();
            for _ in 0..100 {
                if std::fs::read_to_string(&log)
                    .unwrap_or_default()
                    .lines()
                    .any(|line| line.starts_with("watch|") && !line.starts_with("watch|--help|"))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            process.kill();
            let service = crate::conversion::ConversionService::new(Some(exe.to_string_lossy().into_owned()));
            let result = service.convert(&file, ConversionTarget::PptJson).await.unwrap();
            assert!(result.result.success);

            let entries = std::fs::read_to_string(log).unwrap();
            let lines: Vec<_> = entries.lines().collect();
            assert_eq!(lines.len(), 6, "{entries}");
            assert_eq!(lines.iter().filter(|line| line.starts_with("--version|")).count(), 2);
            assert_eq!(lines.iter().filter(|line| line.starts_with("watch|--help|")).count(), 2);
            assert_eq!(
                lines
                    .iter()
                    .filter(|line| line.starts_with("watch|") && !line.starts_with("watch|--help|"))
                    .count(),
                1
            );
            assert_eq!(lines.iter().filter(|line| line.starts_with("ppt2json|")).count(), 1);
            assert!(lines.iter().all(|line| line.ends_with(&format!("|{expected}"))), "{entries}");
            assert_eq!(std::env::var("OFFICECLI_SKIP_UPDATE").unwrap(), "parent-value");
            return;
        }

        let tmp = tempfile::tempdir().unwrap();
        let exe = tmp.path().join("officecli");
        let document = tmp.path().join("test.pptx");
        std::fs::write(
            &exe,
            "#!/bin/sh\nprintf '%s|%s|%s\\n' \"$1\" \"$2\" \"$OFFICECLI_SKIP_UPDATE\" >> \"$AIONUI_TEST_LOG\"\nif [ \"$1\" = ppt2json ]; then printf '{\"ok\":true}'; fi\n",
        )
        .unwrap();
        std::fs::set_permissions(&exe, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(&document, b"ppt").unwrap();
        let mut paths = vec![tmp.path().to_path_buf()];
        paths.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default()));
        for private in [true, false] {
            let log = tmp.path().join(if private { "private.log" } else { "default.log" });
            let mut child = std::process::Command::new(std::env::current_exe().unwrap());
            child
                .args([
                    "--exact",
                    "watch_manager::tests::officecli_child_processes_receive_private_update_policy",
                    "--nocapture",
                ])
                .env("AIONUI_TEST_OFFICECLI_CHILD", "1")
                .env("AIONUI_TEST_OFFICECLI", &exe)
                .env("AIONUI_TEST_LOG", &log)
                .env("AIONUI_TEST_DOCUMENT", &document)
                .env("OFFICECLI_SKIP_UPDATE", "parent-value")
                .env("PATH", std::env::join_paths(&paths).unwrap());
            if private {
                child.env("AIONUI_OFFICECLI_PRIVATE_PATH", &exe);
            } else {
                child.env_remove("AIONUI_OFFICECLI_PRIVATE_PATH");
            }
            let output = child.output().unwrap();
            let stdout = String::from_utf8_lossy(&output.stdout);
            assert!(
                output.status.success() && stdout.contains("running 1 test"),
                "{stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn private_spawner_rejects_binary_without_watch_capability() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("officecli");
        std::fs::write(&path, "#!/bin/sh\n[ \"$1\" = \"--version\" ]\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let spawner = DefaultProcessSpawner {
            _data_dir: PathBuf::new(),
            policy: OfficecliPolicy::from_private_path(Some(path.into_os_string())),
        };
        let result = spawner.spawn_officecli("test.docx", 12345, DocType::Word).await;
        assert!(matches!(result, Err(OfficeError::StartFailed(message)) if message.contains("does not support watch")));
        assert!(!spawner.is_officecli_installed().await);
    }

    #[test]
    fn session_key_preserves_fields() {
        let key = session_key("user-1", "/path/to/doc.docx", DocType::Word);
        assert_eq!(key.user_id, "user-1");
        assert_eq!(key.doc_type, DocType::Word);
        assert_eq!(key.resolved_path, "/path/to/doc.docx");
    }

    #[test]
    fn session_key_different_doc_types() {
        let k1 = session_key("user-1", "/a.docx", DocType::Word);
        let k2 = session_key("user-1", "/a.docx", DocType::Excel);
        assert_ne!(k1, k2);
    }

    #[test]
    fn session_key_different_users() {
        let k1 = session_key("user-1", "/a.docx", DocType::Word);
        let k2 = session_key("user-2", "/a.docx", DocType::Word);
        assert_ne!(k1, k2);
    }

    #[test]
    fn session_key_keeps_user_type_and_path_boundaries() {
        let first = session_key("user:word", "/a.docx", DocType::Excel);
        let second = session_key("user", "word:/a.docx", DocType::Excel);
        assert_ne!(first, second);
    }

    #[test]
    fn install_failed_public_preview_message_is_sanitized() {
        let err = OfficeError::InstallFailed("installer stderr".into());
        assert_eq!(public_preview_error_message(&err), "officecli install failed");
    }

    #[test]
    fn port_in_use_start_failure_detects_platform_messages() {
        for message in [
            "Address already in use (os error 98)",
            "Address already in use (os error 48)",
            "Only one usage of each socket address (protocol/network address/port) is normally permitted. (os error 10048)",
        ] {
            assert!(is_port_in_use_start_failure(&OfficeError::StartFailed(message.into())));
        }
    }

    #[test]
    fn port_in_use_start_failure_ignores_other_errors() {
        assert!(!is_port_in_use_start_failure(&OfficeError::StartFailed(
            "mock spawn failure".into()
        )));
        assert!(!is_port_in_use_start_failure(&OfficeError::OfficecliNotFound));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn officecli_capability_probe_requires_watch_command() {
        let tmp = tempfile::tempdir().unwrap();
        let officecli = tmp.path().join("officecli");
        std::fs::write(
            &officecli,
            "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then exit 0; fi\nif [ \"$1\" = \"watch\" ] && [ \"$2\" = \"--help\" ]; then exit 1; fi\nexit 0\n",
        )
        .unwrap();
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&officecli).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&officecli, perms).unwrap();

        assert!(!officecli_supports_watch(&officecli, &OfficecliPolicy::from_private_path(None)).await);
    }

    #[tokio::test]
    async fn start_creates_session() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let port = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();
        assert!(port > 0);
        assert_eq!(mgr.active_session_count(), 1);
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn start_reuses_existing_session() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let p1 = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();
        let p2 = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();

        assert_eq!(p1, p2);
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn start_same_file_for_different_users_creates_independent_sessions() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();
        let path = file.to_str().unwrap();

        let user_a_port = mgr.start_for_user("user-a", path, DocType::Word).await.unwrap();
        let user_b_port = mgr.start_for_user("user-b", path, DocType::Word).await.unwrap();

        assert_ne!(user_a_port, user_b_port);
        assert_eq!(mgr.active_session_count(), 2);
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 2);

        mgr.stop_for_user("user-a", path, DocType::Word).await;
        assert_eq!(mgr.active_session_count(), 1);
        assert!(!mgr.is_active_port(user_a_port, DocType::Word));
        assert!(!mgr.is_active_port_for_user("user-a", user_a_port, DocType::Word));
        assert!(mgr.is_active_port(user_b_port, DocType::Word));
        assert!(mgr.is_active_port_for_user("user-b", user_b_port, DocType::Word));
        assert!(!mgr.is_active_port_for_user("user-a", user_b_port, DocType::Word));

        mgr.stop_for_user("user-b", path, DocType::Word).await;
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[tokio::test]
    async fn start_retries_when_allocated_port_is_taken() {
        let spawner = Arc::new(MockSpawner::new());
        spawner.fail_with_address_in_use_once.store(true, Ordering::SeqCst);
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let port = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();

        assert!(port > 0);
        assert!(mgr.is_active_port(port, DocType::Word));
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn start_different_doc_types_independent() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let p1 = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();
        let p2 = mgr.start(file.to_str().unwrap(), DocType::Excel).await.unwrap();

        assert_ne!(p1, p2);
        assert_eq!(mgr.active_session_count(), 2);
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn stop_removes_session() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();
        let path = file.to_str().unwrap();

        mgr.start(path, DocType::Word).await.unwrap();
        assert_eq!(mgr.active_session_count(), 1);

        mgr.stop(path, DocType::Word).await;
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[tokio::test]
    async fn stop_all_clears_everything() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let f1 = dir.path().join("a.docx");
        let f2 = dir.path().join("b.xlsx");
        std::fs::write(&f1, b"a").unwrap();
        std::fs::write(&f2, b"b").unwrap();

        mgr.start(f1.to_str().unwrap(), DocType::Word).await.unwrap();
        mgr.start(f2.to_str().unwrap(), DocType::Excel).await.unwrap();
        assert_eq!(mgr.active_session_count(), 2);

        mgr.stop_all();
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[tokio::test]
    async fn stop_all_for_user_keeps_other_user_sessions() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("shared.docx");
        std::fs::write(&file, b"test").unwrap();
        let path = file.to_str().unwrap();

        let user_a_port = mgr.start_for_user("user-a", path, DocType::Word).await.unwrap();
        let user_b_port = mgr.start_for_user("user-b", path, DocType::Word).await.unwrap();
        assert_eq!(mgr.active_session_count(), 2);

        assert_eq!(mgr.stop_all_for_user("user-a"), 1);

        assert_eq!(mgr.active_session_count(), 1);
        assert!(!mgr.is_active_port_for_user("user-a", user_a_port, DocType::Word));
        assert!(mgr.is_active_port_for_user("user-b", user_b_port, DocType::Word));
    }

    #[tokio::test]
    async fn is_active_port_returns_true_for_active() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let port = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();
        assert!(mgr.is_active_port(port, DocType::Word));
        assert!(mgr.is_active_port_for_user("system_default_user", port, DocType::Word));
        assert!(!mgr.is_active_port_for_user("other-user", port, DocType::Word));
        assert!(!mgr.is_active_port(port, DocType::Ppt));
        assert!(!mgr.is_active_port(12345, DocType::Word));
    }

    #[tokio::test]
    async fn is_active_watch_port_accepts_word_and_excel() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let word_file = dir.path().join("test.docx");
        let excel_file = dir.path().join("test.xlsx");
        let ppt_file = dir.path().join("test.pptx");
        std::fs::write(&word_file, b"w").unwrap();
        std::fs::write(&excel_file, b"e").unwrap();
        std::fs::write(&ppt_file, b"p").unwrap();

        let word_port = mgr.start(word_file.to_str().unwrap(), DocType::Word).await.unwrap();
        let excel_port = mgr.start(excel_file.to_str().unwrap(), DocType::Excel).await.unwrap();
        let ppt_port = mgr.start(ppt_file.to_str().unwrap(), DocType::Ppt).await.unwrap();

        assert!(mgr.is_active_watch_port(word_port));
        assert!(mgr.is_active_watch_port(excel_port));
        assert!(mgr.is_active_watch_port_for_user("system_default_user", word_port));
        assert!(mgr.is_active_watch_port_for_user("system_default_user", excel_port));
        assert!(!mgr.is_active_watch_port_for_user("other-user", word_port));
        assert!(!mgr.is_active_watch_port(ppt_port));
        assert!(!mgr.is_active_watch_port(12345));
    }

    #[tokio::test]
    async fn auto_install_when_not_found() {
        let spawner = Arc::new(MockSpawner::new());
        spawner.installed.store(false, Ordering::SeqCst);
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let port = mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();
        assert!(port > 0);
        assert_eq!(spawner.install_count.load(Ordering::SeqCst), 1);
        // First spawn fails (not installed), then install, then second spawn succeeds
        assert_eq!(spawner.spawn_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn broadcasts_starting_and_ready() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();

        let events = broadcaster.events();
        assert!(events.len() >= 2);
        assert_eq!(events[0].name, "word-preview.status");
        assert_eq!(events[0].data["state"], "starting");
        assert_eq!(events[1].name, "word-preview.status");
        assert_eq!(events[1].data["state"], "ready");
    }

    #[tokio::test]
    async fn broadcasts_installing_on_auto_install() {
        let spawner = Arc::new(MockSpawner::new());
        spawner.installed.store(false, Ordering::SeqCst);
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();

        let events = broadcaster.events();
        let states: Vec<&str> = events.iter().filter_map(|e| e.data["state"].as_str()).collect();
        assert!(states.contains(&"starting"));
        assert!(states.contains(&"installing"));
        assert!(states.contains(&"ready"));
    }

    #[tokio::test]
    async fn broadcasts_error_on_failure() {
        let spawner = Arc::new(MockSpawner::new());
        spawner.fail_spawn.store(true, Ordering::SeqCst);
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let result = mgr.start(file.to_str().unwrap(), DocType::Word).await;
        assert!(result.is_err());

        let events = broadcaster.events();
        let last = events.last().unwrap();
        assert_eq!(last.data["state"], "error");
    }

    #[tokio::test(start_paused = true)]
    async fn port_timeout_on_no_listener() {
        let spawner = Arc::new(MockSpawner::new());
        spawner.start_listener.store(false, Ordering::SeqCst);
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let resolved = resolve_path(file.to_str().unwrap()).unwrap();
        let port = allocate_port().unwrap();
        let result = mgr.poll_port_ready(port, &resolved).await;
        assert!(matches!(result, Err(OfficeError::PortTimeout(_))));
    }

    #[tokio::test]
    async fn ppt_triggers_version_check() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.pptx");
        std::fs::write(&file, b"test").unwrap();

        mgr.start(file.to_str().unwrap(), DocType::Ppt).await.unwrap();

        // Give the spawned task a moment
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spawner.update_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn word_does_not_trigger_version_check() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        mgr.start(file.to_str().unwrap(), DocType::Word).await.unwrap();

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(spawner.update_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn stop_nonexistent_is_no_op() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(spawner, broadcaster);

        mgr.stop("/nonexistent/file.docx", DocType::Word).await;
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[test]
    fn resolve_path_normalizes() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("test.docx");
        std::fs::write(&file, b"test").unwrap();

        let resolved = resolve_path(file.to_str().unwrap()).unwrap();
        assert!(!resolved.is_empty());
    }

    #[test]
    fn resolve_path_nonexistent_returns_original() {
        let result = resolve_path("/nonexistent/path/test.docx").unwrap();
        assert_eq!(result, "/nonexistent/path/test.docx");
    }
    // -- active_port_for: document → serving port ---------------------------

    #[tokio::test]
    async fn active_port_for_finds_a_live_session() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("doc.docx");
        std::fs::write(&file, b"x").unwrap();
        let path = file.to_str().unwrap();

        let port = mgr.start_for_user("u1", path, DocType::Word).await.unwrap();

        assert_eq!(
            mgr.active_port_for("u1", path, DocType::Word),
            Some(port),
            "the port serving this document must be reachable from its path"
        );
    }

    /// The lookup has to normalize the path the same way `start_for_user` does before
    /// keying. Without that, a caller spelling the path differently — which is the
    /// normal case, since the refresh request carries whatever the tab holds — would
    /// miss, and the miss would read as "no session" rather than a key mismatch.
    #[tokio::test]
    async fn active_port_for_normalizes_the_path_like_start_does() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("doc.docx");
        std::fs::write(&file, b"x").unwrap();

        let port = mgr
            .start_for_user("u1", file.to_str().unwrap(), DocType::Word)
            .await
            .unwrap();

        // Same file, spelled with a redundant `.` segment.
        let indirect = dir.path().join(".").join("doc.docx");
        assert_eq!(
            mgr.active_port_for("u1", indirect.to_str().unwrap(), DocType::Word),
            Some(port)
        );
    }

    #[tokio::test]
    async fn active_port_for_is_none_for_other_users_doc_types_and_unknown_paths() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("doc.docx");
        std::fs::write(&file, b"x").unwrap();
        let path = file.to_str().unwrap();

        mgr.start_for_user("u1", path, DocType::Word).await.unwrap();

        // Another user's session must not be reachable — the port would let them
        // drive a document they never opened.
        assert_eq!(mgr.active_port_for("u2", path, DocType::Word), None);
        // Same path, different doc type is a different session.
        assert_eq!(mgr.active_port_for("u1", path, DocType::Excel), None);
        // Never-started document.
        let other = dir.path().join("absent.docx");
        assert_eq!(mgr.active_port_for("u1", other.to_str().unwrap(), DocType::Word), None);
    }

    /// A stopped session leaves nothing to refresh. Returning its old port would
    /// point the switch call at a port serving nothing — or, worse, at whatever took
    /// it over.
    #[tokio::test]
    async fn active_port_for_is_none_after_stop() {
        let spawner = Arc::new(MockSpawner::new());
        let broadcaster = Arc::new(RecordingBroadcaster::new());
        let mgr = make_manager(Arc::clone(&spawner), Arc::clone(&broadcaster));
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("doc.docx");
        std::fs::write(&file, b"x").unwrap();
        let path = file.to_str().unwrap();

        mgr.start_for_user("u1", path, DocType::Word).await.unwrap();
        mgr.stop_for_user("u1", path, DocType::Word).await;

        assert_eq!(mgr.active_port_for("u1", path, DocType::Word), None);
    }
}
