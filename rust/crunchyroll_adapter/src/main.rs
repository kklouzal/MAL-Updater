use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use std::env;
use std::io::{self, Write};

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

fn print_usage(mut writer: impl Write) -> io::Result<()> {
    writeln!(writer, "Usage: crunchyroll-adapter snapshot [--contract-version 1.0]")
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
        "snapshot" => {
            let mut contract_version = String::from("1.0");
            while let Some(arg) = args.next() {
                match arg.as_str() {
                    "--contract-version" => {
                        let Some(value) = args.next() else {
                            return Err("--contract-version requires a value".into());
                        };
                        contract_version = value;
                    }
                    other => return Err(format!("unknown argument: {other}")),
                }
            }

            if contract_version != "1.0" {
                return Err(format!("unsupported contract version: {contract_version}"));
            }

            let snapshot = Snapshot {
                contract_version,
                generated_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                provider: "crunchyroll".into(),
                account_id_hint: None,
                series: Vec::new(),
                progress: Vec::new(),
                watchlist: Vec::new(),
                raw: serde_json::json!({
                    "status": "scaffold",
                    "note": "Crunchyroll fetching is not implemented yet"
                }),
            };

            serde_json::to_writer_pretty(io::stdout(), &snapshot).map_err(|e| e.to_string())?;
            println!();
            Ok(())
        }
        _ => {
            print_usage(io::stderr()).map_err(|e| e.to_string())?;
            Err(format!("unknown command: {command}"))
        }
    }
}
