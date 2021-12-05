// this should be run simultaneously with the `server` example
// and this demonstrates the use of a remote subscriber

use eigenlog::subscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let error_handler = Box::new(|err| eprintln!("Error from logging subscriber: {}", err));
    let api_config = eigenlog::ApiConfig {
        client: reqwest::Client::new(),
        base_url: reqwest::Url::parse("http://127.0.0.1:8080/log")?,
        proxy: eigenlog::BasicProxy::init("123".to_string()),
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

    subscriber.set_logger(log::LevelFilter::Info)?;

    let log_generator = async {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let mut generator = names::Generator::default();

        for i in 1..10000 {
            println!("Doing log {}", i);
            log::info!("{}: {}", i, generator.next().unwrap());
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        println!("Generated 10000 logs");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    };

    tokio::select! {
        _ = data_sender => {
            eprintln!("Data sender exited");
        }
        _ = log_generator => {
            eprintln!("Log generator exited");
        }
    };

    Ok(())
}
