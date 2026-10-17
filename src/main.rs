use clap::Parser as _;
use thermia2mqtt::commands;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[derive(clap::Parser)]
#[clap(version, about, author)]
enum Commands {
    Registers(commands::registers::Args),
    Mqtt(commands::mqtt::Args),
}

fn end<E: std::error::Error>(r: Result<(), E>) {
    std::process::exit(match r {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("error: {e}");
            let mut cause = e.source();
            while let Some(e) = cause {
                eprintln!("  because: {e}");
                cause = e.source();
            }
            1
        }
    });
}

fn main() {
    let filter_description = std::env::var("THERMIA2MQTT_LOG");
    let filter = filter_description
        .as_deref()
        .unwrap_or("info")
        .parse::<tracing_subscriber::filter::targets::Targets>();
    match filter {
        Ok(f) => tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(f)
            .init(),
        Err(e) => end(Err(e)),
    }
    match Commands::parse() {
        Commands::Registers(args) => end(commands::registers::run(args)),
        Commands::Mqtt(args) => end(commands::mqtt::run(args)),
    }
}
