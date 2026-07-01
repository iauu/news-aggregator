mod plugins;
mod value_enum;
mod config;
mod model;
mod routes;

use std::collections::HashMap;
use axum::{body::Bytes, extract::ws::{Message, WebSocketUpgrade}, response::{IntoResponse, Response}, routing::any, Json, Router};
use axum_extra::TypedHeader;
use indexmap::IndexMap;

use crate::plugins::source::{remap, RSSSource, RSSSourceType};
use crate::value_enum::EnumFromStr;
use axum::body::Body;
use axum::extract::ws::Utf8Bytes;
use axum::extract::{ConnectInfo, Query, State};
use common::unify::{UnifyOutput, UnifyOutputRaw};
use plugins::net::rss_fetch::get_raw;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use crate::config::Config;
use ahash::RandomState;
use rand::Rng;

use std::{env, thread};
use std::fmt::format;
use std::path::PathBuf;
use cfg_if::cfg_if;
use sqlx::{query, Error, Executor, PgPool, Postgres};
use tracing::log::warn;
use model::{get_model, Model};
use crate::model::{embedding_thread, Task};
use pgvector::Vector;

struct ServerState {
    // conns: Arc<Mutex<Vec<Arc<Mutex<WebSocket>>>>>,
    receiver: tokio::sync::broadcast::Receiver<UnifyOutputRaw>,
    hash_data: Arc<RwLock<HashMap<String, u64, RandomState>>>,
    sender: tokio::sync::mpsc::UnboundedSender<Task>,
    pool: PgPool,
}

impl ServerState {
    pub fn new(receiver: tokio::sync::broadcast::Receiver<UnifyOutputRaw>, sender: tokio::sync::mpsc::UnboundedSender<Task>, pool: PgPool) -> Self {
        Self {
            receiver,
            hash_data: Arc::new(RwLock::new(HashMap::default())),
            sender,
            pool
        }
    }
}


#[derive(Error, Debug)]
pub(crate) enum ApplicationError {
    #[error("Missing Rss Type {0}")]
    MissingRssType(String),
    #[error("Failed to obtain RSS URL (rss_type: {0}, query: {1}) ")]
    FailedToObtainRssUrl(String, String),
    #[error("Invalid URL {0}")]
    InvalidUrl(String),
    #[error("RssFetchError")]
    RssFetchError(#[from] plugins::source::RssFetchError),
}

pub async fn fetch_with_config(config: &Config) -> Result<Vec<UnifyOutput>, ApplicationError> {
    let kind = RSSSourceType::enum_str(&config.rss_type).map_err(
        |e| {
            tracing::warn!("Missing RSS type: {}", e);
            ApplicationError::MissingRssType(config.rss_type.clone())
        }
    )?;
    let source = remap(kind);
    let url = source.get_url(&config.query).ok_or_else(|| {
        tracing::warn!("Failed to obtain RSS URL (rss_type: {}, query: {})", config.rss_type, config.query);
        ApplicationError::FailedToObtainRssUrl(config.rss_type.clone(), config.query.clone())
    })?;
    let content = get_raw(url.parse().map_err(|e| {
        tracing::warn!("Failed to parse url: {}", e);
        ApplicationError::InvalidUrl(url.clone())

    })?).await.map_err(|e| {
        tracing::warn!("Request failed: {}", e);
        ApplicationError::RssFetchError(e)
    })?;
    source.get_unify(&content).map_err(|e| {
        tracing::warn!("Failed to deserialize: {}", e);
        ApplicationError::RssFetchError(e)
    })
}

pub async fn background_fetching(
    config: &Config,
    sender: tokio::sync::broadcast::Sender<UnifyOutputRaw>,
    state: Arc<Mutex<ServerState>>,
) -> () {
    // testing currently - should be reading config file in future instead
    let pool = state.lock().await.pool.clone();
    loop {
        let span = tracing::info_span!("background_fetching");

        span.in_scope(async || {
            let outputs = fetch_with_config(config).await;
            if let Err(err) = outputs {
                warn!("Error fetching the {}, error: {}", config, err);
                return;
            }
            let Ok(outputs) = outputs else {unreachable!()};
            info!("Pushing {} outputs from {}", outputs.len(), config);
            let mut proc_id: Vec<(i64, UnifyOutput)> = Vec::new();
            for output in outputs {
                match sqlx::query!("SELECT 1 as f FROM unify
        WHERE hash_key && $1 ", output.hash_key.as_slice())
                    .fetch_one(&pool).await {
                    Ok(_) => {
                        continue;
                    },
                    Err(Error::RowNotFound) => {}
                    Err(e) => {
                        warn!("Fail to check duplicate: {}", e);
                    }
                }
                let result = sqlx::query!("INSERT INTO
    public.unify(id, organisation, title, description, time, source, score, link, hash_key)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT DO NOTHING
RETURNING idx",
                    output.id,
                    output.organisation,
                    output.title,
                    output.description,
                    output.time.naive_utc(),
                    serde_json::to_string(&output.source).ok(),
                    output.score.map(|x| x as f64),
                    output.link,
                    output.hash_key.as_slice(),
                ).fetch_one(&pool).await; // Ignore idx field because it is not known yet - the config will just give 0
                match result {
                    Ok(record) => {
                        proc_id.push((record.idx, output));
                    },
                    Err(Error::RowNotFound) => {}
                    Err(err) => {
                        if err.to_string().contains(&" One of the strings in hash_key is already assigned to another record") {
                            continue;
                        }
                        warn!("Database failure on creating record: {}", err);
                    }
                }
            }
            if proc_id.len() == 0 {
                return;
            }
            let task = Task::create(proc_id.iter().map(|x| x.1.clone()).clone().collect());
            state.lock().await.sender.send(task.0).unwrap();
            let results = match task.1.await {
                Ok(v) => v,
                Err(e) => vec![None; proc_id.len()]
            };
            for (i, result) in results.iter().enumerate() {
                let pair = &proc_id[i];
                if let Some(embedding) = result {
                    let query = sqlx::query::<Postgres>(
                        "UPDATE public.unify
SET embedding = $2
WHERE idx = $1")
                        .bind(pair.0)
                        .bind(Vector::from(embedding.clone()))
                        .execute(&pool).await;
                    if let Err(err) = query {
                        warn!("Database failure on updating embedding: {}", err);
                    }
                }
            }
            let ids = proc_id.iter().map(|x| x.0).collect::<Vec<i64>>();
            let multi = match sqlx::query_as::<Postgres, UnifyOutput>(
                "SELECT * FROM public.unify WHERE idx = ANY($1)"
            )
                .bind(ids.as_slice()) // You use .bind() for runtime queries
                .fetch_all(&pool)
                .await {
                Ok(r) => r,
                Err(e) => {
                    warn!("Fail to read from db: {}", e);
                    return;
                }
            };
            for item in multi {
                let recv_count = sender.send(item.to_raw()).unwrap_or(0);
                info!("Pushed document {} to {} receivers", item.id, recv_count);
            }
        }).await;
        tokio::time::sleep(Duration::from_secs(config.update_interval as u64)).await;
    }
}

#[tokio::main]
async fn main() {
    let postgres_username = env::var("POSTGRES_USER").unwrap_or("postgres".to_string());
    let postgres_password = env::var("POSTGRES_PASSWORD").unwrap_or("please-change-7a9ebb7fc05ac78b8cb04bf8".to_string());
    let url = env::var("DATABASE_URL").unwrap_or("database:5432".to_string());
    let pool: PgPool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!("postgresql://{}:{}@{}", postgres_username, postgres_password, url))
        .await.unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();

    let (sender, receiver) = tokio::sync::broadcast::channel::<UnifyOutputRaw>(1024);
    let (model_sender, model_receiver) = tokio::sync::mpsc::unbounded_channel::<Task>();
    let state = Arc::new(Mutex::new(ServerState::new(receiver, model_sender, pool.clone())));


    let model = get_model();

    thread::spawn(move || { embedding_thread(model, model_receiver); });

    let config_str = String::from_utf8(std::fs::read("config.toml").unwrap()).unwrap();
    let configs: crate::config::Configs = toml::from_str(&config_str).unwrap();

    for config in configs.configs {
        let _clone = state.clone();
        let sender_clone = sender.clone();
        let conf_clone = config.clone();

        tokio::spawn(async move {
            background_fetching(&conf_clone, sender_clone, _clone).await;
        });
    }

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();


    let _clone = state.clone();
    let app = Router::new()
        .merge(routes::routes())
        .with_state(_clone)
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(DefaultMakeSpan::default().include_headers(true)),
        );
    let listener = tokio::net::TcpListener::bind("[::]:3000").await.unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    let _ = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await;
}




