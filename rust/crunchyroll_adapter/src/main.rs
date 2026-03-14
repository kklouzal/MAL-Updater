use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use std::env;
use std::io::{self, Write};
use std::path::PathBuf;

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

struct AuthLoginArgs {
    profile: String,
    state_dir: Option<PathBuf>,
}

fn print_usage(mut writer: impl Write) -> io::Result<()> {
    writeln!(writer, "Usage:")?;
    writeln!(writer, "  crunchyroll-adapter snapshot [--contract-version 1.0] [--profile default] [--state-dir PATH]")?;
    writeln!(writer, "  crunchyroll-adapter auth login [--profile default] [--state-dir PATH]")?;
    writeln!(writer, "  crunchyroll-adapter auth status [--profile default] [--state-dir PATH]")
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

    let snapshot = Snapshot {
        contract_version: args.contract_version,
        generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
        provider: "crunchyroll".into(),
        account_id_hint: None,
        series: Vec::new(),
        progress: Vec::new(),
        watchlist: Vec::new(),
        raw: serde_json::json!({
            "status": "scaffold",
            "note": "Crunchyroll fetching is not implemented yet",
            "profile": args.profile,
            "state_dir": args.state_dir.as_ref().map(|p| p.display().to_string()),
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
        "login" => {
            let parsed = parse_auth_args(iter.collect())?;
            Err(format!(
                "auth login not implemented yet (profile={}, state_dir={})",
                parsed.profile,
                parsed
                    .state_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<default>".into())
            ))
        }
        "status" => {
            let parsed = parse_auth_args(iter.collect())?;
            println!("provider=crunchyroll");
            println!("profile={}", parsed.profile);
            println!(
                "state_dir={}",
                parsed
                    .state_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<default>".into())
            );
            println!("auth_status=not_implemented");
            Ok(())
        }
        _ => {
            print_usage(io::stderr()).map_err(|e| e.to_string())?;
            Err(format!("unknown auth subcommand: {subcommand}"))
        }
    }
}

fn parse_auth_args(args: Vec<String>) -> Result<AuthLoginArgs, String> {
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

    Ok(AuthLoginArgs { profile, state_dir })
}
