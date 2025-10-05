use actix_web::{web, App, HttpServer};
use log::{error, info};
use qlib_rs::StoreProxy;

mod handlers;
mod models;
mod store_service;
mod websocket;

use store_service::{StoreHandle, StoreService};

pub struct AppState {
    store_handle: StoreHandle,
    jwt_secret: String,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let store_address = std::env::var("QCORE_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:9100".to_string());
    
    info!("Starting qweb server");
    info!("Connecting to qcore-rs at: {}", store_address);

    // Connect to qcore-rs on startup using non-async StoreProxy
    let store_proxy = match StoreProxy::connect(&store_address) {
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

    let jwt_secret = std::env::var("JWT_SECRET")
        .unwrap_or_else(|_| "default_secret".to_string());

    info!("JWT secret configured");

    // Create StoreService and get handle
    let (store_handle, mut store_service) = StoreService::new(store_proxy);

    // Spawn the StoreService in a separate task
    tokio::spawn(async move {
        store_service.run().await;
    });

    let app_state = web::Data::new(AppState { store_handle, jwt_secret });

    let bind_address = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    info!("Binding web server to: {}", bind_address);

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .route("/api/login", web::post().to(handlers::login))
            .route("/api/read", web::post().to(handlers::read))
            .route("/api/write", web::post().to(handlers::write))
            .route("/api/create", web::post().to(handlers::create))
            .route("/api/delete", web::post().to(handlers::delete))
            .route("/api/find", web::post().to(handlers::find))
            .route("/api/schema", web::post().to(handlers::schema))
            .route("/api/complete_schema", web::post().to(handlers::complete_schema))
            .route("/api/resolve_entity_type", web::post().to(handlers::resolve_entity_type))
            .route("/api/resolve_field_type", web::post().to(handlers::resolve_field_type))
            .route("/api/get_field_schema", web::post().to(handlers::get_field_schema))
            .route("/api/entity_exists", web::post().to(handlers::entity_exists))
            .route("/api/field_exists", web::post().to(handlers::field_exists))
            .route("/api/resolve_indirection", web::post().to(handlers::resolve_indirection))
            .route("/ws", web::get().to(websocket::ws_handler))
    })
    .bind(bind_address)?
    .run()
    .await
}
