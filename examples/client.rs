// This demonstrates the use of a rust client for the log server

use eigenlog::{self, db, ParseLogTreeInfoError};
use std::{
    io::{self, Write},
    path, result, str,
};

#[cfg(not(feature = "wasm"))]
use std::time;

// CLI command to get info
// CLI command to query (takes params)
// can connect to a local sled db file or over http to a server
// Print options:
// 	- CSV
//	- JSON
//	- text-table

#[derive(structopt::StructOpt, strum_macros::EnumString)]
#[strum(serialize_all = "lowercase")]
enum PrintOptions {
    Csv,
    Json,
    Table,
}

#[derive(structopt::StructOpt)]
struct App {
    /// Which format the log messages should be formatted in when printing output
    #[structopt(
        short = "f",
        long = "format",
        default_value = "PrintOptions::TextTable"
    )]
    output_format: PrintOptions,

    /// Location of the sled db file to use (mutually exclusive with -u/--url)
    #[structopt(short = "d", long = "database")]
    db_file: Option<path::PathBuf>,

    /// Location of the eigenlog server to use (mutually exclusive with -d/--database)
    /// -> note: this will require the API key to be sent via stdin.
    /// -> note: this is the base url, the standard paths will be appended
    #[structopt(short = "u", long = "url")]
    server_url: Option<reqwest::Url>,

    #[structopt(subcommand)]
    cmd: Cmd,
}

impl App {
    fn data_source(&self) -> anyhow::Result<DataSource> {
        match (&self.db_file, &self.server_url) {
            (Some(_), Some(_)) => {
                Err(anyhow::anyhow!("-d/--database and -u/--url are mutually exclusive, please specify only one of them"))
            }
            (None, None) => {
                Err(anyhow::anyhow!("at least one of -d/--database and -u/--url must be specified"))
            }
            (None, Some(url)) => {
                Ok(DataSource::Remote(url.clone()))
            }
            (Some(db_loc), None) => {
                Ok(DataSource::Local(db_loc.clone()))
            }
        }
    }
}

enum DataSource {
    Remote(reqwest::Url),
    Local(path::PathBuf),
}

impl DataSource {
    async fn run_cmd(self, cmd: Cmd) -> anyhow::Result<CmdResult> {
        match self {
            DataSource::Local(db_file) => {
                let db_handle = sled::open(db_file)?;
                match cmd {
                    Cmd::Info => {
                        let info = db::info(&db_handle)?;
                        Ok(info.into())
                    }
                    Cmd::Detail { host, app, level } => {
                        let detail = db::detail(&host, &app, level, &db_handle)?;
                        Ok(detail.into())
                    }
                    Cmd::Query {
                        max_log_level,
                        start_timestamp,
                        end_timestamp,
                        host_contains,
                        app_contains,
                        message_matches,
                        message_not_matches,
                        max_results,
                    } => {
                        let query = db::query(
                            eigenlog::QueryParams {
                                max_log_level,
                                start_timestamp,
                                end_timestamp,
                                host_contains,
                                app_contains,
                                message_matches,
                                message_not_matches,
                                max_results,
                            },
                            &db_handle,
                        )?;

                        Ok(query.into())
                    }
                }
            }
            DataSource::Remote(base_url) => {
                let stdin = io::stdin();
                let mut api_key = String::new();
                stdin.read_line(&mut api_key)?;
                let api_config = eigenlog::ApiConfig {
                    client: reqwest::Client::new(),
                    proxy: eigenlog::BasicProxy::init("123".to_string()),
                    base_url,
                    serialization_format: eigenlog::SerializationFormat::Bincode,
                };
                let client = reqwest::Client::new();
                match cmd {
                    Cmd::Info => {
                        let info = api_config.info(&client).await?;
                        Ok(info.into())
                    }
                    Cmd::Detail { host, app, level } => {
                        let detail = api_config.detail(&client, &host, &app, level).await?;
                        Ok(detail.into())
                    }
                    Cmd::Query {
                        max_log_level,
                        start_timestamp,
                        end_timestamp,
                        host_contains,
                        app_contains,
                        message_matches,
                        message_not_matches,
                        max_results,
                    } => {
                        let query = api_config
                            .query(
                                &client,
                                &eigenlog::QueryParams {
                                    max_log_level,
                                    start_timestamp,
                                    end_timestamp,
                                    host_contains,
                                    app_contains,
                                    message_matches,
                                    message_not_matches,
                                    max_results,
                                },
                                #[cfg(not(feature = "wasm"))]
                                time::Duration::from_secs(15),
                            )
                            .await?;
                        Ok(query.into())
                    }
                }
            }
        }
    }
}

enum CmdResult {
    Info(Vec<result::Result<eigenlog::LogTreeInfo, ParseLogTreeInfoError>>),
    Query(Vec<eigenlog::QueryResponse>),
    Detail(eigenlog::LogTreeDetail),
}

impl From<Vec<result::Result<eigenlog::LogTreeInfo, ParseLogTreeInfoError>>> for CmdResult {
    fn from(i: Vec<result::Result<eigenlog::LogTreeInfo, ParseLogTreeInfoError>>) -> CmdResult {
        CmdResult::Info(i)
    }
}

impl From<Vec<eigenlog::QueryResponse>> for CmdResult {
    fn from(i: Vec<eigenlog::QueryResponse>) -> CmdResult {
        CmdResult::Query(i)
    }
}

impl From<eigenlog::LogTreeDetail> for CmdResult {
    fn from(i: eigenlog::LogTreeDetail) -> CmdResult {
        CmdResult::Detail(i)
    }
}

impl CmdResult {
    fn write_out(self, output_format: PrintOptions) -> anyhow::Result<()> {
        let stdout = io::stdout();
        let mut handle = stdout.lock();
        match output_format {
            PrintOptions::Csv => {
                let mut writer = csv::Writer::from_writer(handle);
                match self {
                    CmdResult::Info(info) => {
                        for row in info {
                            writer.serialize(row)?;
                        }
                    }
                    CmdResult::Detail(det) => {
                        for (date, rows) in det.row_detail {
                            writer.write_byte_record(
                                &csv::StringRecord::from(vec![date.to_string(), rows.to_string()])
                                    .into_byte_record(),
                            )?;
                        }
                    }
                    CmdResult::Query(query) => {
                        for row in query {
                            writer.serialize(row)?;
                        }
                    }
                }
                writer.flush()?;
            }
            PrintOptions::Json => match self {
                CmdResult::Info(info) => {
                    serde_json::to_writer_pretty(handle, &info)?;
                }
                CmdResult::Detail(detail) => {
                    serde_json::to_writer_pretty(handle, &detail)?;
                }
                CmdResult::Query(query) => {
                    serde_json::to_writer_pretty(handle, &query)?;
                }
            },
            PrintOptions::Table => match self {
                CmdResult::Info(i) => {
                    let (table, errors) = info_to_table(i);
                    println!("List of all trees in the database: ");
                    for row in table.lines() {
                        handle.write_all(row.as_bytes())?;
                        handle.write_all("\n".as_bytes())?;
                    }
                    println!("Errors during listing of trees: ");
                    for row in errors.lines() {
                        handle.write_all(row.as_bytes())?;
                        handle.write_all("\n".as_bytes())?;
                    }
                }
                CmdResult::Query(q) => {
                    for row in data_to_table(q).lines() {
                        handle.write_all(row.as_bytes())?;
                        handle.write_all("\n".as_bytes())?;
                    }
                }
                CmdResult::Detail(d) => {
                    println!(
                        "Detail for {}/{}/{} ({} total rows)",
                        d.host, d.app, d.level, d.rows
                    );
                    for row in detail_to_table(d).lines() {
                        handle.write_all(row.as_bytes())?;
                        handle.write_all("\n".as_bytes())?;
                    }
                }
            },
        }
        Ok(())
    }
}

#[derive(structopt::StructOpt)]
enum Cmd {
    Info,
    Query {
        #[structopt(short = "l", long = "level")]
        max_log_level: Option<eigenlog::Level>,
        #[structopt(short = "s", long = "start")]
        start_timestamp: Option<chrono::DateTime<chrono::Utc>>,
        #[structopt(short = "e", long = "end")]
        end_timestamp: Option<chrono::DateTime<chrono::Utc>>,
        #[structopt(short = "h", long = "host")]
        host_contains: Option<eigenlog::Host>,
        #[structopt(short = "a", long = "app")]
        app_contains: Option<eigenlog::App>,
        #[structopt(short = "m", long = "matches")]
        message_matches: Option<String>,
        #[structopt(short = "n", long = "not_matches")]
        message_not_matches: Option<String>,
        #[structopt(short = "r", long = "rows")]
        max_results: Option<usize>,
    },
    Detail {
        #[structopt(short = "h", long = "host")]
        host: eigenlog::Host,
        #[structopt(short = "a", long = "app")]
        app: eigenlog::App,
        #[structopt(short = "l", long = "level")]
        level: eigenlog::Level,
    },
}

fn info_to_table(
    info: Vec<result::Result<eigenlog::LogTreeInfo, ParseLogTreeInfoError>>,
) -> (comfy_table::Table, comfy_table::Table) {
    let mut table = comfy_table::Table::new();
    table.set_header(vec!["Host", "App", "Level", "Min", "Max"]);

    let mut errors = comfy_table::Table::new();
    errors.set_header(vec!["Message"]);

    for row in info {
        match row {
            Ok(row) => {
                table.add_row(vec![
                    row.host.to_string(),
                    row.app.to_string(),
                    row.level.to_string(),
                    row.min.to_string(),
                    row.max.to_string(),
                ]);
            }
            Err(e) => {
                errors.add_row(vec![e.to_string()]);
            }
        }
    }
    (table, errors)
}

fn detail_to_table(data: eigenlog::LogTreeDetail) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table.set_header(vec!["Date", "Rows"]);
    for (date, rows) in data.row_detail {
        table.add_row(vec![date.to_string(), rows.to_string()]);
    }
    table
}

fn data_to_table(data: Vec<eigenlog::QueryResponse>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table.set_header(vec!["Host", "App", "Level", "ID", "ID Time", "Message"]);
    for row in data {
        table.add_row(vec![
            row.host.to_string(),
            row.app.to_string(),
            row.level.to_string(),
            row.id.to_string(),
            row.id.datetime().to_string(),
            row.data.message.to_string(),
        ]);
    }
    table
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let app_config: App = structopt::StructOpt::from_args();

    let data_source = app_config.data_source()?;

    let result = data_source.run_cmd(app_config.cmd).await?;

    result.write_out(app_config.output_format)?;

    Ok(())
}
