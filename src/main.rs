#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(clippy::needless_return)]

use std::{io::IsTerminal, path::PathBuf};

use beggar::{PostgresDatastore, Result, StorageBackend};
use clap::{CommandFactory, Parser};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder as ConnBuilder,
    service::TowerToHyperService,
};
use s3s::{auth::SimpleAuth, service::S3ServiceBuilder};
use tokio::net::TcpListener;
use tracing::{error, info};

#[derive(Debug, Parser)]
#[command(version)]
struct Opt {
    /// Host name to listen on.
    #[arg(long, default_value = "localhost")]
    host: String,

    /// Port number to listen on.
    #[arg(long, default_value = "8014")] // The original design was finished on 2020-08-14.
    port: u16,

    /// Access key used for authentication.
    #[arg(long)]
    access_key: Option<String>,

    /// Secret key used for authentication.
    #[arg(long)]
    secret_key: Option<String>,

    /// Domain names used for virtual-hosted-style requests.
    #[arg(long)]
    domain: Vec<String>,

    /// Root directory of stored data.
    root: PathBuf,
}

fn settings() -> Result<beggar::Settings, config::ConfigError> {
    let s = config::Config::builder()
        .add_source(config::File::with_name("config/default.yaml").required(false))
        .add_source(config::File::with_name("config/local.yaml").required(false))
        .add_source(config::File::with_name("config/application.yaml").required(true))
        .build()?;

    s.try_deserialize()
}

fn setup_tracing() {
    use tracing_subscriber::EnvFilter;

    let env_filter = EnvFilter::from_default_env();
    let enable_color = std::io::stdout().is_terminal();

    tracing_subscriber::fmt()
        .pretty()
        .with_env_filter(env_filter)
        .with_ansi(enable_color)
        .init();
}

fn check_cli_args(opt: &Opt) {
    use clap::error::ErrorKind;

    let mut cmd = Opt::command();

    // TODO: how to specify the requirements with clap derive API?
    if let (Some(_), None) | (None, Some(_)) = (&opt.access_key, &opt.secret_key) {
        let msg = "access key and secret key must be specified together";
        cmd.error(ErrorKind::MissingRequiredArgument, msg).exit();
    }

    for s in &opt.domain {
        if s.contains('/') {
            let msg = format!("expected domain name, found URL-like string: {s:?}");
            cmd.error(ErrorKind::InvalidValue, msg).exit();
        }
    }
}

fn main() -> Result {
    let opt = Opt::parse();
    check_cli_args(&opt);

    setup_tracing();
    run(opt)
}

#[tokio::main]
async fn run(opt: Opt) -> Result {
    // load application settings / configuration
    let s = match settings() {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to load settings: {}", e);
            return Err(beggar::Error::from_string(format!(
                "Failed to load settings: {e}"
            )));
        }
    };

    info!(host = ?s.datasource.host, port = s.datasource.port, "settings loaded");

    // Use the asynchronous connect method instead of new
    let ds = match PostgresDatastore::connect(&s).await {
        Ok(ds) => ds,
        Err(e) => {
            error!("Failed to connect to database: {}", e);
            return Err(e);
        }
    };

    // Run migrations after successful connection
    if let Err(e) = ds.migrate().await {
        error!("Failed to run database migrations: {}", e);
        return Err(e);
    }

    // Setup S3 provider
    let fs = match StorageBackend::new(opt.root, ds) {
        Ok(fs) => fs,
        Err(e) => {
            error!("Failed to initialize storage backend: {}", e);
            return Err(e);
        }
    };

    // Setup S3 service
    let service = {
        let mut b = S3ServiceBuilder::new(fs);

        // Enable authentication
        if let (Some(ak), Some(sk)) = (opt.access_key, opt.secret_key) {
            b.set_auth(SimpleAuth::from_single(ak, sk));
            info!("authentication is enabled");
        }

        b.build().into_shared()
    };

    // Run server
    let listener = TcpListener::bind((opt.host.as_str(), opt.port)).await?;
    let local_addr = listener.local_addr()?;

    let http_server = ConnBuilder::new(TokioExecutor::new());
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();

    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    info!("server is running at http://{local_addr}");

    loop {
        let (stream, _) = tokio::select! {
            res =  listener.accept() => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };

        let io = TokioIo::new(stream);

        // let svc = ServiceBuilder::new().layer_fn(Logger::new).service(hyper_service.
        // clone());

        let conn = http_server.serve_connection(
            io,
            TowerToHyperService::new(
                tower::ServiceBuilder::new()
                    // .layer(CorsLayer::very_permissive())
                    // .layer(ConcurrencyLimitLayer::new(2))
                    // .layer(RequestBodyLimitLayer::new(4096))
                    .service(service.clone()),
            ),
        );
        let conn = graceful.watch(conn.into_owned());
        tokio::spawn(async move {
            let _ = conn.await;
        });
    }

    tokio::select! {
        () = graceful.shutdown() => {
             tracing::debug!("Gracefully shut down!");
        },
        () = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
             tracing::debug!("Waited 10 seconds for graceful shutdown, aborting...");
        }
    }

    info!("server is stopped");
    Ok(())
}
