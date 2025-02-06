#![forbid(unsafe_code)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(clippy::needless_return)]

use config::ConfigError;
use s3s_fs::FileSystem;
use s3s_fs::PostgresDatastore;
use s3s_fs::Result;

use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_fs::Settings;
use s3s_fs::StorageBackend;

use std::io::IsTerminal;
// use std::ops::Not;
use std::path::PathBuf;

use tokio::net::TcpListener;

use clap::{CommandFactory, Parser};
use tracing::{debug, info};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::service::TowerToHyperService;

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

fn settings() -> Result<s3s_fs::Settings, ConfigError> {
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
    // let opt = Opt::parse();
    // check_cli_args(&opt);

    setup_tracing();
    let s = settings();

    if let Ok(s) = s {
        debug!("settings: {:?}", s);
        return test_db(s);
    }
    Ok(())
    //run(opt)
}

#[tokio::main]
async fn test_db(s: Settings) -> Result<()> {
    debug!("settings: {:?}", s);
    let backend = StorageBackend::new(PostgresDatastore::new(&s));
    backend.save_s3_item_detail().await?;
    Ok(())
}

// #[tokio::main]
async fn run(opt: Opt) -> Result {
    // Setup S3 provider
    let fs = FileSystem::new(opt.root)?;

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
                        tracing::error!("error accepting connection: {err}");
                        continue;
                    }
                }
            }
            _ = ctrl_c.as_mut() => {
                break;
            }
        };

        let io = TokioIo::new(stream);

        // let svc = ServiceBuilder::new().layer_fn(Logger::new).service(hyper_service.clone());

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
