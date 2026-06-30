use axum::routing::any;
use crate::routes::Router;

mod history;
mod latest_idx;
mod get_new;
mod get_idx;

pub fn routes() -> Router {
    Router::new()
        .nest("/history", history::routes())
        .nest("/latest_idx", latest_idx::routes())
        .nest("/get_new", get_new::routes())
        .nest("/get_idx", get_idx::routes())
}
