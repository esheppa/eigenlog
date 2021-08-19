// this should be run simultaneously with the `server` example
// and this demonstrates the use of a remote subscriber

use eigenlog::subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let error_handler = Box::new(|err| eprintln!("Error from logging subscriber: {}", err));
    let sender_error_handler = Box::new(|e| eprintln!("Error from data sender {}", e));
    let api_config = eigenlog::ApiConfig {
        base_url: reqwest::Url::parse("http://127.0.0.1/log")?,
        api_key: "123".to_string(),
        serialization_format: eigenlog::SerializationFormat::Bincode,
    };
    let host = "local"
        .parse::<eigenlog::Host>()
        .map_err(|e| anyhow::anyhow!(e))?;
    let app = "remoteSubscriberExample"
        .parse::<eigenlog::App>()
        .map_err(|e| anyhow::anyhow!(e))?;

    let (subscriber, data_sender) = subscriber::Subscriber::new_remote(
        error_handler,
        api_config,
        host,
        app,
        log::Level::Debug,
        subscriber::CacheLimit::default(),
    );

    subscriber.set_logger(log::LevelFilter::Trace)?;

    let log_generator = async {
        let mut generator = names::Generator::default();

        for i in 1..10000 {
            log::info!("{}: {}", i, generator.next().unwrap());
        }
        println!("Generated 10000 logs");
    };

    tokio::select! {
        _ = data_sender.run_forever(sender_error_handler) => {
            eprintln!("Data sender exited");
        }
        _ = log_generator => {
            eprintln!("Log generator exited");
        }
    };

    Ok(())
}
