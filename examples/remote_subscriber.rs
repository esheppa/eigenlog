// this should be run simultaneously with the `server` example
// and this demonstrates the use of a remote subscriber

use eigenlog::subscriber;
use futures_util::future;
use std::time;

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
        log::LevelFilter::Debug,
        subscriber::CacheLimit {
            debug: 1000,
            trace: 1000,
            info: 100,
            error: 1,
            warn: 1,
        },
        time::Duration::from_secs(30),
    );

    subscriber.set_logger()?;

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

    match future::select(Box::pin(log_generator), Box::pin(data_sender.run())).await {
        future::Either::Left((_, _)) => {
            eprintln!("Log generator exited");
        }
        future::Either::Right((_, _)) => {
            eprintln!("Data sender exited");
        }
    }

    // tokio::spawn(async move {
    //     log_generator.await;
    //     // process::exit(1);
    // });

    // data_sender.run().await;

    Ok(())
}
