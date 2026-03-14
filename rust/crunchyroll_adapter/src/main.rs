use chrono::{SecondsFormat, Utc};
use crunchyroll_rs::crunchyroll::DeviceIdentifier;
use crunchyroll_rs::list::{WatchHistoryEntry, WatchlistEntry as CrunchyrollWatchlistEntry, WatchlistOptions};
use crunchyroll_rs::media::MediaCollection;
use crunchyroll_rs::{Crunchyroll, Locale};
use futures_util::StreamExt;
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
struct WatchlistRecord {
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
    watchlist: Vec<WatchlistRecord>,
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
    device_type_hint: Option<String>,
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

struct CrunchyrollFetch {
    account_id: String,
    email_hint: Option<String>,
    progress: Vec<EpisodeProgress>,
    series: Vec<SeriesRef>,
    watchlist: Vec<WatchlistRecord>,
}

fn print_usage(mut writer: impl Write) -> io::Result<()> {
    writeln!(writer, "Usage:")?;
    writeln!(writer, "  crunchyroll-adapter snapshot [--contract-version 1.0] [--profile default] [--state-dir PATH]")?;
    writeln!(writer, "  crunchyroll-adapter auth status [--profile default] [--state-dir PATH]")?;
    writeln!(writer, "  crunchyroll-adapter auth save-refresh-token --refresh-token-file PATH [--device-id-file PATH] [--profile default] [--state-dir PATH] [--locale en-US]")
}

#[tokio::main]
async fn main() {
    if let Err(err) = real_main().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn real_main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage(io::stderr()).map_err(|e| e.to_string())?;
        return Err("missing command".into());
    };

    match command.as_str() {
        "snapshot" => handle_snapshot(parse_snapshot_args(args.collect())?).await,
        "auth" => handle_auth(args.collect()).await,
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

async fn handle_snapshot(args: SnapshotArgs) -> Result<(), String> {
    if args.contract_version != "1.0" {
        return Err(format!(
            "unsupported contract version: {}",
            args.contract_version
        ));
    }

    let state_paths = resolve_state_paths(args.state_dir.as_deref(), &args.profile)?;
    let mut state = load_session_state(&state_paths, &args.profile)?;
    state.refresh_token_present = state_paths.refresh_token_path.exists();
    state.device_id_present = state_paths.device_id_path.exists();
    state.last_login_attempt_at = Some(now_string());

    let snapshot = if !state.refresh_token_present {
        state.adapter_phase = Some("auth_material_missing".into());
        state.last_error = Some(format!(
            "missing refresh token at {}",
            state_paths.refresh_token_path.display()
        ));
        build_snapshot(
            args.contract_version,
            state.last_account_id_hint.clone(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            serde_json::json!({
                "status": "auth_material_missing",
                "note": "No Crunchyroll refresh token is staged yet.",
                "profile": args.profile,
                "state_root": state_paths.root.display().to_string(),
                "refresh_token_present": state.refresh_token_present,
                "device_id_present": state.device_id_present,
                "session_state_path": state_paths.session_state_path.display().to_string(),
            }),
        )
    } else {
        match fetch_live_snapshot(&state_paths, &state).await {
            Ok(fetch) => {
                state.adapter_phase = Some("live_snapshot".into());
                state.last_login_success_at = Some(now_string());
                state.last_account_id_hint = Some(fetch.account_id.clone());
                state.last_error = None;
                build_snapshot(
                    args.contract_version,
                    Some(fetch.account_id.clone()),
                    fetch.series,
                    fetch.progress,
                    fetch.watchlist,
                    serde_json::json!({
                        "status": "ok",
                        "profile": args.profile,
                        "state_root": state_paths.root.display().to_string(),
                        "session_state_path": state_paths.session_state_path.display().to_string(),
                        "refresh_token_present": state.refresh_token_present,
                        "device_id_present": state.device_id_present,
                        "device_type_hint": state.device_type_hint,
                        "email_hint": fetch.email_hint,
                    }),
                )
            }
            Err(err) => {
                state.adapter_phase = Some("auth_failed".into());
                state.last_error = Some(err.clone());
                build_snapshot(
                    args.contract_version,
                    state.last_account_id_hint.clone(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    serde_json::json!({
                        "status": "auth_failed",
                        "profile": args.profile,
                        "state_root": state_paths.root.display().to_string(),
                        "session_state_path": state_paths.session_state_path.display().to_string(),
                        "refresh_token_present": state.refresh_token_present,
                        "device_id_present": state.device_id_present,
                        "device_type_hint": state.device_type_hint,
                        "error": err,
                        "note": "Live crunchyroll-rs login was attempted against staged local auth material and failed honestly.",
                    }),
                )
            }
        }
    };

    save_session_state(&state_paths, &state)?;
    serde_json::to_writer_pretty(io::stdout(), &snapshot).map_err(|e| e.to_string())?;
    println!();
    Ok(())
}

async fn handle_auth(args: Vec<String>) -> Result<(), String> {
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
    println!("device_type_hint={}", state.device_type_hint.unwrap_or_else(|| "ANDROIDTV (default)".into()));
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
    state.device_type_hint.get_or_insert_with(|| "ANDROIDTV".into());
    state.adapter_phase = Some("auth_material_staged".into());
    state.last_error = None;
    save_session_state(&state_paths, &state)?;

    println!("Saved refresh token to {}", state_paths.refresh_token_path.display());
    if device_id_present {
        println!("Saved device id to {}", state_paths.device_id_path.display());
    }
    println!("Locale set to {}", state.locale);
    println!("Device type hint set to {}", state.device_type_hint.unwrap_or_else(|| "ANDROIDTV".into()));
    Ok(())
}

async fn fetch_live_snapshot(state_paths: &StatePaths, state: &SessionState) -> Result<CrunchyrollFetch, String> {
    let refresh_token = read_trimmed_file(&state_paths.refresh_token_path)?;
    let device_id = if state_paths.device_id_path.exists() {
        read_trimmed_file(&state_paths.device_id_path)?
    } else {
        "0000-0000-0000-0000".to_string()
    };
    let device_identifier = DeviceIdentifier {
        device_id,
        device_type: state
            .device_type_hint
            .clone()
            .unwrap_or_else(|| "ANDROIDTV".to_string()),
        device_name: None,
    };

    let crunchy = Crunchyroll::builder()
        .locale(Locale::Custom(state.locale.clone()))
        .login_with_refresh_token(refresh_token, device_identifier)
        .await
        .map_err(|e| format!("refresh-token login failed: {e}"))?;

    let account = crunchy
        .account()
        .await
        .map_err(|e| format!("account fetch failed after login: {e}"))?;

    let history = collect_watch_history(&crunchy).await?;
    let progress = history.iter().filter_map(map_watch_history_entry).collect::<Vec<_>>();
    let mut series = Vec::new();
    for entry in &progress {
        push_series_once(
            &mut series,
            SeriesRef {
                provider_series_id: entry.provider_series_id.clone(),
                title: entry.provider_series_id.clone(),
                season_title: None,
                season_number: None,
            },
        );
    }

    let watchlist_entries = crunchy
        .watchlist(WatchlistOptions::default())
        .await
        .map_err(|e| format!("watchlist fetch failed: {e}"))?;
    let mut watchlist = Vec::new();
    for entry in watchlist_entries {
        if let Some((series_ref, watchlist_entry)) = map_watchlist_entry(entry) {
            push_series_once(&mut series, series_ref);
            watchlist.push(watchlist_entry);
        }
    }

    Ok(CrunchyrollFetch {
        account_id: account.account_id,
        email_hint: (!account.email.is_empty()).then_some(account.email),
        progress,
        series,
        watchlist,
    })
}

async fn collect_watch_history(crunchy: &Crunchyroll) -> Result<Vec<WatchHistoryEntry>, String> {
    let mut history = crunchy.watch_history();
    history.page_size(100);
    let mut entries = Vec::new();
    while let Some(item) = history.next().await {
        entries.push(item.map_err(|e| format!("watch-history fetch failed: {e}"))?);
    }
    Ok(entries)
}

fn map_watch_history_entry(entry: &WatchHistoryEntry) -> Option<EpisodeProgress> {
    let panel = entry.panel.as_ref()?;
    match panel {
        MediaCollection::Episode(episode) => {
            let duration_ms = episode.duration.num_milliseconds();
            let playback_position_ms = i64::from(entry.playhead);
            Some(EpisodeProgress {
                provider_episode_id: episode.id.clone(),
                provider_series_id: episode.series_id.clone(),
                episode_number: episode.episode_number.map(i64::from),
                episode_title: (!episode.title.is_empty()).then_some(episode.title.clone()),
                playback_position_ms: Some(playback_position_ms),
                duration_ms: Some(duration_ms),
                completion_ratio: if duration_ms > 0 {
                    Some((playback_position_ms as f64 / duration_ms as f64).clamp(0.0, 1.0))
                } else if entry.fully_watched {
                    Some(1.0)
                } else {
                    None
                },
                last_watched_at: Some(entry.date_played.to_rfc3339()),
                audio_locale: Some(episode.audio_locale.to_string()),
                subtitle_locale: episode.subtitle_locales.first().map(|locale| locale.to_string()),
                rating: None,
            })
        }
        MediaCollection::Movie(movie) => {
            let duration_ms = movie.duration.num_milliseconds();
            let playback_position_ms = i64::from(entry.playhead);
            Some(EpisodeProgress {
                provider_episode_id: movie.id.clone(),
                provider_series_id: movie.movie_listing_id.clone(),
                episode_number: None,
                episode_title: (!movie.title.is_empty()).then_some(movie.title.clone()),
                playback_position_ms: Some(playback_position_ms),
                duration_ms: Some(duration_ms),
                completion_ratio: if duration_ms > 0 {
                    Some((playback_position_ms as f64 / duration_ms as f64).clamp(0.0, 1.0))
                } else if entry.fully_watched {
                    Some(1.0)
                } else {
                    None
                },
                last_watched_at: Some(entry.date_played.to_rfc3339()),
                audio_locale: None,
                subtitle_locale: None,
                rating: Some("movie".into()),
            })
        }
        _ => None,
    }
}

fn map_watchlist_entry(entry: CrunchyrollWatchlistEntry) -> Option<(SeriesRef, WatchlistRecord)> {
    match entry.panel {
        MediaCollection::Series(series) => Some((
            SeriesRef {
                provider_series_id: series.id.clone(),
                title: series.title.clone(),
                season_title: None,
                season_number: None,
            },
            WatchlistRecord {
                provider_series_id: series.id,
                added_at: None,
                status: Some(if entry.fully_watched {
                    "fully_watched"
                } else if entry.never_watched {
                    "never_watched"
                } else {
                    "in_progress"
                }
                .into()),
            },
        )),
        MediaCollection::MovieListing(movie_listing) => Some((
            SeriesRef {
                provider_series_id: movie_listing.id.clone(),
                title: movie_listing.title.clone(),
                season_title: None,
                season_number: None,
            },
            WatchlistRecord {
                provider_series_id: movie_listing.id,
                added_at: None,
                status: Some(if entry.fully_watched {
                    "fully_watched"
                } else if entry.never_watched {
                    "never_watched"
                } else {
                    "in_progress"
                }
                .into()),
            },
        )),
        _ => None,
    }
}

fn push_series_once(series: &mut Vec<SeriesRef>, candidate: SeriesRef) {
    if series
        .iter()
        .any(|existing| existing.provider_series_id == candidate.provider_series_id)
    {
        return;
    }
    series.push(candidate);
}

fn build_snapshot(
    contract_version: String,
    account_id_hint: Option<String>,
    series: Vec<SeriesRef>,
    progress: Vec<EpisodeProgress>,
    watchlist: Vec<WatchlistRecord>,
    raw: serde_json::Value,
) -> Snapshot {
    Snapshot {
        contract_version,
        generated_at: now_string(),
        provider: "crunchyroll".into(),
        account_id_hint,
        series,
        progress,
        watchlist,
        raw,
    }
}

fn resolve_state_paths(state_dir: Option<&Path>, profile: &str) -> Result<StatePaths, String> {
    let base = match state_dir {
        Some(path) => path.to_path_buf(),
        None => env::current_dir().map_err(|e| e.to_string())?.join("state"),
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
            device_type_hint: Some("ANDROIDTV".into()),
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
    state.device_type_hint.get_or_insert_with(|| "ANDROIDTV".into());
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
