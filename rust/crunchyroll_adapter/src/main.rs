use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Serialize)]
struct SeriesRef {
    provider_series_id: String,
    title: String,
    season_title: Option<String>,
    season_number: Option<i64>,
}

#[derive(Serialize)]
struct EpisodeProgress {
    provider_episode_id: String,
    provider_series_id: String,
    episode_number: Option<i64>,
    episode_title: Option<String>,
    playback_position_ms: Option<i64>,
    duration_ms: Option<i64>,
    completion_ratio: Option<f64>,
    last_watched_at: Option<String>,
    audio_locale: Option<String>,
    subtitle_locale: Option<String>,
    rating: Option<String>,
}

#[derive(Serialize)]
struct WatchlistEntry {
    provider_series_id: String,
    added_at: Option<String>,
    status: Option<String>,
}

#[derive(Serialize)]
struct Snapshot {
    contract_version: String,
    generated_at: String,
    provider: String,
    account_id_hint: Option<String>,
    series: Vec<SeriesRef>,
    progress: Vec<EpisodeProgress>,
    watchlist: Vec<WatchlistEntry>,
    raw: serde_json::Value,
}

struct SnapshotArgs {
    contract_version: String,
    profile: String,
    state_dir: Option<PathBuf>,
}

struct AuthArgs {
    profile: String,
    state_dir: Option<PathBuf>,
}

struct AuthSaveRefreshTokenArgs {
    profile: String,
    state_dir: Option<PathBuf>,
    refresh_token_file: PathBuf,
    device_id_file: Option<PathBuf>,
    locale: String,
}

#[derive(Serialize, Deserialize, Default)]
struct SessionState {
    profile: String,
    locale: String,
    refresh_token_present: bool,
    device_id_present: bool,
    last_login_attempt_at: Option<String>,
    last_login_success_at: Option<String>,
    last_account_id_hint: Option<String>,
    last_error: Option<String>,
    adapter_phase: Option<String>,
}

struct StatePaths {
    root: PathBuf,
    refresh_token_path: PathBuf,
    device_id_path: PathBuf,
    session_state_path: PathBuf,
}

fn print_usage(mut writer: impl Write) -> io::Result<()> {
    writeln!(writer, "Usage:")?;
    writeln!(writer, "  crunchyroll-adapter snapshot [--contract-version 1.0] [--profile default] [--state-dir PATH]")?;
    writeln!(writer, "  crunchyroll-adapter auth status [--profile default] [--state-dir PATH]")?;
    writeln!(writer, "  crunchyroll-adapter auth save-refresh-token --refresh-token-file PATH [--device-id-file PATH] [--profile default] [--state-dir PATH] [--locale en-US]")
}

fn main() {
    if let Err(err) = real_main() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage(io::stderr()).map_err(|e| e.to_string())?;
        return Err("missing command".into());
    };

    match command.as_str() {
        "snapshot" => handle_snapshot(parse_snapshot_args(args.collect())?),
        "auth" => handle_auth(args.collect()),
        "help" | "--help" | "-h" => {
            print_usage(io::stdout()).map_err(|e| e.to_string())?;
            Ok(())
        }
        _ => {
            print_usage(io::stderr()).map_err(|e| e.to_string())?;
            Err(format!("unknown command: {command}"))
        }
    }
}

fn parse_snapshot_args(args: Vec<String>) -> Result<SnapshotArgs, String> {
    let mut contract_version = String::from("1.0");
    let mut profile = String::from("default");
    let mut state_dir = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--contract-version" => {
                contract_version = iter
                    .next()
                    .ok_or_else(|| "--contract-version requires a value".to_string())?;
            }
            "--profile" => {
                profile = iter
                    .next()
                    .ok_or_else(|| "--profile requires a value".to_string())?;
            }
            "--state-dir" => {
                state_dir = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| "--state-dir requires a value".to_string())?,
                ));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(SnapshotArgs {
        contract_version,
        profile,
        state_dir,
    })
}

fn handle_snapshot(args: SnapshotArgs) -> Result<(), String> {
    if args.contract_version != "1.0" {
        return Err(format!("unsupported contract version: {}", args.contract_version));
    }

    let state_paths = resolve_state_paths(args.state_dir.as_deref(), &args.profile)?;
    let mut state = load_session_state(&state_paths, &args.profile)?;
    state.refresh_token_present = state_paths.refresh_token_path.exists();
    state.device_id_present = state_paths.device_id_path.exists();
    state.adapter_phase = Some("auth_material_staged".into());
    save_session_state(&state_paths, &state)?;

    let snapshot = Snapshot {
        contract_version: args.contract_version,
        generated_at: now_string(),
        provider: "crunchyroll".into(),
        account_id_hint: state.last_account_id_hint.clone(),
        series: Vec::new(),
        progress: Vec::new(),
        watchlist: Vec::new(),
        raw: serde_json::json!({
            "status": "auth_material_staged",
            "note": "Crunchyroll auth/session file conventions are now wired, but live crunchyroll-rs login/fetch is blocked on the host Rust toolchain and is not being faked.",
            "profile": args.profile,
            "state_root": state_paths.root.display().to_string(),
            "refresh_token_present": state.refresh_token_present,
            "device_id_present": state.device_id_present,
            "session_state_path": state_paths.session_state_path.display().to_string(),
        }),
    };

    serde_json::to_writer_pretty(io::stdout(), &snapshot).map_err(|e| e.to_string())?;
    println!();
    Ok(())
}

fn handle_auth(args: Vec<String>) -> Result<(), String> {
    let mut iter = args.into_iter();
    let Some(subcommand) = iter.next() else {
        print_usage(io::stderr()).map_err(|e| e.to_string())?;
        return Err("missing auth subcommand".into());
    };

    match subcommand.as_str() {
        "status" => {
            let parsed = parse_auth_args(iter.collect())?;
            handle_auth_status(parsed)
        }
        "save-refresh-token" => {
            let parsed = parse_auth_save_refresh_token_args(iter.collect())?;
            handle_auth_save_refresh_token(parsed)
        }
        _ => {
            print_usage(io::stderr()).map_err(|e| e.to_string())?;
            Err(format!("unknown auth subcommand: {subcommand}"))
        }
    }
}

fn parse_auth_args(args: Vec<String>) -> Result<AuthArgs, String> {
    let mut profile = String::from("default");
    let mut state_dir = None;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--profile" => {
                profile = iter
                    .next()
                    .ok_or_else(|| "--profile requires a value".to_string())?;
            }
            "--state-dir" => {
                state_dir = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| "--state-dir requires a value".to_string())?,
                ));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(AuthArgs { profile, state_dir })
}

fn parse_auth_save_refresh_token_args(args: Vec<String>) -> Result<AuthSaveRefreshTokenArgs, String> {
    let mut profile = String::from("default");
    let mut state_dir = None;
    let mut refresh_token_file = None;
    let mut device_id_file = None;
    let mut locale = String::from("en-US");
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--profile" => {
                profile = iter
                    .next()
                    .ok_or_else(|| "--profile requires a value".to_string())?;
            }
            "--state-dir" => {
                state_dir = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| "--state-dir requires a value".to_string())?,
                ));
            }
            "--refresh-token-file" => {
                refresh_token_file = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| "--refresh-token-file requires a value".to_string())?,
                ));
            }
            "--device-id-file" => {
                device_id_file = Some(PathBuf::from(
                    iter.next()
                        .ok_or_else(|| "--device-id-file requires a value".to_string())?,
                ));
            }
            "--locale" => {
                locale = iter
                    .next()
                    .ok_or_else(|| "--locale requires a value".to_string())?;
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    Ok(AuthSaveRefreshTokenArgs {
        profile,
        state_dir,
        refresh_token_file: refresh_token_file
            .ok_or_else(|| "--refresh-token-file is required".to_string())?,
        device_id_file,
        locale,
    })
}

fn handle_auth_status(args: AuthArgs) -> Result<(), String> {
    let state_paths = resolve_state_paths(args.state_dir.as_deref(), &args.profile)?;
    let state = load_session_state(&state_paths, &args.profile)?;
    println!("provider=crunchyroll");
    println!("profile={}", args.profile);
    println!("state_root={}", state_paths.root.display());
    println!("refresh_token_path={}", state_paths.refresh_token_path.display());
    println!("device_id_path={}", state_paths.device_id_path.display());
    println!("session_state_path={}", state_paths.session_state_path.display());
    println!("refresh_token_present={}", state_paths.refresh_token_path.exists());
    println!("device_id_present={}", state_paths.device_id_path.exists());
    println!("locale={}", state.locale);
    println!("adapter_phase={}", state.adapter_phase.unwrap_or_else(|| "<unset>".into()));
    println!("last_login_attempt_at={}", state.last_login_attempt_at.unwrap_or_else(|| "<never>".into()));
    println!("last_login_success_at={}", state.last_login_success_at.unwrap_or_else(|| "<never>".into()));
    println!("last_account_id_hint={}", state.last_account_id_hint.unwrap_or_else(|| "<unknown>".into()));
    println!("last_error={}", state.last_error.unwrap_or_else(|| "<none>".into()));
    Ok(())
}

fn handle_auth_save_refresh_token(args: AuthSaveRefreshTokenArgs) -> Result<(), String> {
    let state_paths = resolve_state_paths(args.state_dir.as_deref(), &args.profile)?;
    fs::create_dir_all(&state_paths.root).map_err(|e| e.to_string())?;

    let refresh_token = read_trimmed_file(&args.refresh_token_file)?;
    if refresh_token.is_empty() {
        return Err(format!(
            "refresh token file is empty: {}",
            args.refresh_token_file.display()
        ));
    }
    write_secret_file(&state_paths.refresh_token_path, &refresh_token)?;

    let device_id_present = if let Some(device_id_file) = args.device_id_file.as_ref() {
        let device_id = read_trimmed_file(device_id_file)?;
        if device_id.is_empty() {
            return Err(format!("device id file is empty: {}", device_id_file.display()));
        }
        write_secret_file(&state_paths.device_id_path, &device_id)?;
        true
    } else {
        false
    };

    let mut state = load_session_state(&state_paths, &args.profile)?;
    state.locale = args.locale;
    state.refresh_token_present = true;
    state.device_id_present = device_id_present || state_paths.device_id_path.exists();
    state.adapter_phase = Some("auth_material_staged".into());
    save_session_state(&state_paths, &state)?;

    println!("Saved refresh token to {}", state_paths.refresh_token_path.display());
    if device_id_present {
        println!("Saved device id to {}", state_paths.device_id_path.display());
    }
    println!("Locale set to {}", state.locale);
    Ok(())
}

fn resolve_state_paths(state_dir: Option<&Path>, profile: &str) -> Result<StatePaths, String> {
    let base = match state_dir {
        Some(path) => path.to_path_buf(),
        None => env::current_dir()
            .map_err(|e| e.to_string())?
            .join("state"),
    };
    let root = base.join("crunchyroll").join(profile);
    Ok(StatePaths {
        refresh_token_path: root.join("refresh_token.txt"),
        device_id_path: root.join("device_id.txt"),
        session_state_path: root.join("session.json"),
        root,
    })
}

fn load_session_state(state_paths: &StatePaths, profile: &str) -> Result<SessionState, String> {
    if !state_paths.session_state_path.exists() {
        return Ok(SessionState {
            profile: profile.to_string(),
            locale: "en-US".into(),
            refresh_token_present: state_paths.refresh_token_path.exists(),
            device_id_present: state_paths.device_id_path.exists(),
            adapter_phase: Some("scaffold".into()),
            ..SessionState::default()
        });
    }
    let text = fs::read_to_string(&state_paths.session_state_path).map_err(|e| e.to_string())?;
    let mut state: SessionState = serde_json::from_str(&text).map_err(|e| e.to_string())?;
    if state.profile.is_empty() {
        state.profile = profile.to_string();
    }
    if state.locale.is_empty() {
        state.locale = "en-US".into();
    }
    state.refresh_token_present = state_paths.refresh_token_path.exists();
    state.device_id_present = state_paths.device_id_path.exists();
    Ok(state)
}

fn save_session_state(state_paths: &StatePaths, state: &SessionState) -> Result<(), String> {
    fs::create_dir_all(&state_paths.root).map_err(|e| e.to_string())?;
    let body = serde_json::to_string_pretty(state).map_err(|e| e.to_string())?;
    fs::write(&state_paths.session_state_path, body).map_err(|e| e.to_string())
}

fn read_trimmed_file(path: &Path) -> Result<String, String> {
    Ok(fs::read_to_string(path)
        .map_err(|e| format!("unable to read {}: {e}", path.display()))?
        .trim()
        .to_string())
}

fn write_secret_file(path: &Path, value: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    fs::write(path, format!("{}\n", value.trim())).map_err(|e| e.to_string())?;
    Ok(())
}

fn now_string() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}
