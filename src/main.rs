use actix_web::{web, App, HttpServer};
use log::{error, info};
use qlib_rs::data::AsyncStoreProxy;
use std::sync::Arc;
use tokio::sync::RwLock;

mod handlers;
mod models;
mod websocket;

pub struct AppState {
    store_proxy: Arc<RwLock<AsyncStoreProxy>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let store_address = std::env::var("QCORE_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string());
    
    info!("Starting qweb server");
    info!("Connecting to qcore-rs at: {}", store_address);

    // Connect to qcore-rs on startup
    let store_proxy = match AsyncStoreProxy::connect(&store_address).await {
        Ok(proxy) => {
            info!("Successfully connected to qcore-rs");
            proxy
        }
        Err(e) => {
            error!("Failed to connect to qcore-rs at {}: {:?}", store_address, e);
            return Err(std::io::Error::other(
                format!("Failed to connect to qcore-rs: {:?}", e),
            ));
        }
    };

    let app_state = web::Data::new(AppState {
        store_proxy: Arc::new(RwLock::new(store_proxy)),
    });

    let bind_address = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    info!("Binding web server to: {}", bind_address);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/api/read", web::post().to(handlers::read))
            .route("/api/write", web::post().to(handlers::write))
            .route("/api/create", web::post().to(handlers::create))
            .route("/api/delete", web::post().to(handlers::delete))
            .route("/api/find", web::post().to(handlers::find))
            .route("/api/schema", web::post().to(handlers::schema))
            .route("/ws", web::get().to(websocket::ws_handler))
    })
    .bind(bind_address)?
    .run()
    .await
}
