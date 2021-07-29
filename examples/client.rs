// This demonstrates the use of a rust client for the log server

use std::{str, path};
use eigenlog::{self, db};

// CLI command to get info
// CLI command to query (takes params)
// can connect to a local sled db file or over http to a server
// Print options:
// 	- CSV
//	- JSON
//	- text-table

#[derive(structopt::StructOpt)]
enum PrintOptions {
	Csv,
	Json,
	TextTable,
}

impl str::FromStr for PrintOptions {
    type Err = String;
    fn from_str(s: &str) -> Result<PrintOptions, Self::Err> {
        todo!()
    }
}

#[derive(structopt::StructOpt)]
struct App {
    /// Which format the log messages should be formatted in when printing output
    #[structopt(short = "f", long = "format", default_value = "PrintOptions::TextTable")]
    output_format: PrintOptions,

    /// Location of the sled db file to use (mutually exclusive with -u/--url)
    #[structopt(short = "d", long = "database")]
    db_file: Option<path::PathBuf>,

    /// Location of the eigenlog server to use (mutually exclusive with -d/--database)
    /// -> note: this will require the API key to be sent via stdin.
    #[structopt(short = "u", long = "url")]
    server_url: Option<reqwest::Url>,

    #[structopt(subcommand)]
    cmd: Cmd,
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
    }
}

fn info_to_table(info: Vec<eigenlog::LogTreeInfo>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table
        .set_header(vec!["Host", "App", "Level", "Min", "Max"]);
    for row in info {
        table.add_row(vec![
            row.host.to_string(),
            row.app.to_string(),
            row.level.to_string(),
            row.min.to_string(),
            row.max.to_string(),
        ]);
    }
    table
}

fn data_to_table(data: Vec<eigenlog::QueryResponse>) -> comfy_table::Table {
    let mut table = comfy_table::Table::new();
    table
        .set_header(vec!["Host", "App", "Level", "ID", "Message"]);
    for row in data {
        table.add_row(vec![
            row.host.to_string(),
            row.app.to_string(),
            row.level.to_string(),
            row.id.to_string(),
            row.data.message.to_string(),
        ]);
    }
    table
}

fn main() -> anyhow::Result<()> {
    let app_config: App = structopt::StructOpt::from_args();

    match (&app_config.db_file, &app_config.server_url) {
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!("-d/--database and -u/--url are mutually exclusive, please specify only one of them"))
        }
        (None, None) => {
            return Err(anyhow::anyhow!("at least one of -d/--database and -u/--url must be specified"))
        }
        (None, Some(_)) => {
            return Err(anyhow::anyhow!("-u/--url is not yet supported"))
        }
        _ => {}
    }

    let db_handle = sled::open(app_config.db_file.unwrap())?;

    match app_config.cmd {
        Cmd::Info => {
            let info = db::info(&db_handle)?;
            println!("{}", info_to_table(info));
        }
        Cmd::Query { max_log_level, start_timestamp, end_timestamp, host_contains, app_contains } => {

            
        }
    }

    Ok(())
}
