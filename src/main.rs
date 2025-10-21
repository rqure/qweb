use actix_web::{web, App, HttpServer};
use actix_cors::Cors;
use log::{error, info};
use qlib_rs::{StoreProxy, EntityId};

mod handlers;
mod models;
mod store_service;
mod websocket;
mod session_cleanup;
mod session_service;

use store_service::{StoreHandle, StoreService};

pub struct AppState {
    store_handle: StoreHandle,
    jwt_secret: String,
    qweb_service_id: EntityId,
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

    // Create StoreService and get handle. StoreService will initialize a ServiceState
    // using the same StoreProxy connection so qweb can write periodic heartbeats.
    let (store_handle, mut store_service) = StoreService::new(store_proxy);

    // Try to obtain the Service entity id from the initialized ServiceState. This avoids
    // performing an extra lookup here; if ServiceState initialization failed we'll fall
    // back to the asynchronous handle-based discovery as before.
    let qweb_service_id = if let Some(id) = store_service.get_service_id() {
        info!("Determined qweb service ID from ServiceState: {:?}", id);
        id
    } else {
        error!("Failed to determine qweb service ID from ServiceState");
        std::process::exit(1);
    };

    // Spawn the StoreService in a separate task
    tokio::spawn(async move {
        store_service.run().await;
    });

    let app_state = web::Data::new(AppState { 
        store_handle: store_handle.clone(), 
        jwt_secret,
        qweb_service_id,
    });

        // Start session cleanup task
    let cleanup_handle = store_handle.clone();
    tokio::spawn(async move {
        session_cleanup::session_cleanup_task(cleanup_handle, qweb_service_id).await;
    });

    let bind_address = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    info!("Binding web server to: {}", bind_address);

    // Get CORS allowed origins from environment variable
    let cors_origins = std::env::var("CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:5173,http://localhost:3000".to_string());
    
    info!("CORS allowed origins: {}", cors_origins);
    
    // Parse origins into a Vec that can be moved into the closure
    let allowed_origins: Vec<String> = cors_origins
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    HttpServer::new(move || {
        // Clone allowed origins for this instance of the app
        let origins = allowed_origins.clone();
        
        // Configure CORS
        let mut cors = Cors::default()
            .allowed_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
            .allowed_headers(vec![
                actix_web::http::header::AUTHORIZATION,
                actix_web::http::header::ACCEPT,
                actix_web::http::header::CONTENT_TYPE,
            ])
            .supports_credentials()
            .max_age(3600);
        
        // Add each allowed origin
        for origin in &origins {
            cors = cors.allowed_origin(origin);
        }

        App::new()
            .wrap(cors)
            .app_data(app_state.clone())
            .route("/api/login", web::post().to(handlers::login))
            .route("/api/logout", web::post().to(handlers::logout))
            .route("/api/refresh", web::post().to(handlers::refresh))
            .route("/api/read", web::post().to(handlers::read))
            .route("/api/write", web::post().to(handlers::write))
            .route("/api/create", web::post().to(handlers::create))
            .route("/api/delete", web::post().to(handlers::delete))
            .route("/api/find", web::post().to(handlers::find))
            .route("/api/schema", web::post().to(handlers::schema))
            .route("/api/complete_schema", web::post().to(handlers::complete_schema))
            .route("/api/update_schema", web::post().to(handlers::update_schema))
            .route("/api/get_entity_type", web::post().to(handlers::get_entity_type))
            .route("/api/get_field_type", web::post().to(handlers::get_field_type))
            .route("/api/resolve_entity_type", web::post().to(handlers::resolve_entity_type))
            .route("/api/resolve_field_type", web::post().to(handlers::resolve_field_type))
            .route("/api/get_field_schema", web::post().to(handlers::get_field_schema))
            .route("/api/entity_exists", web::post().to(handlers::entity_exists))
            .route("/api/field_exists", web::post().to(handlers::field_exists))
            .route("/api/resolve_indirection", web::post().to(handlers::resolve_indirection))
            .route("/api/pipeline", web::post().to(handlers::pipeline))
            .route("/ws", web::get().to(websocket::ws_handler))
    })
    .bind(bind_address)?
    .run()
    .await
}
