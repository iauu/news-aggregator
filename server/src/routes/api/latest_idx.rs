use std::sync::Arc;
use axum::body::Body;
use axum::extract::{Query, State};
use axum::Json;
use axum::response::Response;
use axum::routing::get;
use sqlx::Postgres;
use tokio::sync::Mutex;
use tracing::log::warn;
use common::unify::UnifyOutput;
use crate::routes::Router;
use crate::ServerState;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(latest_idx_handler))
}

#[axum::debug_handler]
async fn latest_idx_handler(
    State(state): State<Arc<Mutex<ServerState>>>,
) -> Json<i64> {
    let pool = state.lock().await.pool.clone();
    let result = sqlx::query!("SELECT idx FROM public.unify ORDER BY idx DESC LIMIT 1")
        .fetch_one(&pool).await;
    match result {
        Ok(record) => Json(record.idx),
        Err(e) => {
            warn!("Error fetching latest records idx for unity: {}", e);
            Json(0)
        }
    }

}
