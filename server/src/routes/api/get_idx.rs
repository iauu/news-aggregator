use std::sync::Arc;
use axum::extract::{Query, State};
use axum::Json;
use axum::routing::get;
use serde::Deserialize;
use tokio::sync::Mutex;
use tracing::log::warn;
use crate::routes::Router;
use crate::ServerState;

pub fn routes() -> Router {
    Router::new()
        .route("/", get(get_idx_handler))
}

#[derive(Deserialize)]
struct Offset {
    #[serde(default = "default_offset")]
    t_offset: u64,
}

fn default_offset() -> u64 {
    0
}

#[axum::debug_handler]
async fn get_idx_handler(
    State(state): State<Arc<Mutex<ServerState>>>,
    Query(query): Query<Offset>,
) -> Json<i64> {
    let pool = state.lock().await.pool.clone();
    let result = sqlx::query!("SELECT idx FROM public.unify WHERE time > NOW() - ($1 * INTERVAL '1 second') ORDER BY idx DESC LIMIT 1", query.t_offset as f64)
        .fetch_one(&pool).await;
    match result {
        Ok(record) => Json(record.idx),
        Err(e) => {
            warn!("Error fetching latest records idx for unity: {}", e);
            Json(0)
        }
    }

}
