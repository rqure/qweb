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

/// Periodic task to clean up expired sessions
async fn session_cleanup_task(handle: StoreHandle) {
    use qlib_rs::Value;
    use std::time::Duration;
    
    info!("Starting session cleanup task");
    
    loop {
        // Sleep for 30 seconds between cleanup cycles
        tokio::time::sleep(Duration::from_secs(30)).await;
        
        // Get Session entity type
        let session_entity_type = match handle.get_entity_type("Session").await {
            Ok(et) => et,
            Err(e) => {
                error!("Failed to get Session entity type: {:?}", e);
                continue;
            }
        };
        
        // Find all Session entities
        let sessions = match handle.find_entities(session_entity_type, None).await {
            Ok(entities) => entities,
            Err(e) => {
                error!("Failed to find Sessions: {:?}", e);
                continue;
            }
        };
        
        // Get field types
        let current_user_ft = match handle.get_field_type("CurrentUser").await {
            Ok(ft) => ft,
            Err(e) => {
                error!("Failed to get CurrentUser field type: {:?}", e);
                continue;
            }
        };
        
        let previous_user_ft = match handle.get_field_type("PreviousUser").await {
            Ok(ft) => ft,
            Err(e) => {
                error!("Failed to get PreviousUser field type: {:?}", e);
                continue;
            }
        };
        
        let expires_at_ft = match handle.get_field_type("ExpiresAt").await {
            Ok(ft) => ft,
            Err(e) => {
                error!("Failed to get ExpiresAt field type: {:?}", e);
                continue;
            }
        };
        
        let token_ft = match handle.get_field_type("Token").await {
            Ok(ft) => ft,
            Err(e) => {
                error!("Failed to get Token field type: {:?}", e);
                continue;
            }
        };
        
        let now = qlib_rs::now();
        let mut cleaned_count = 0;
        
        // Check each session for expiration
        for session_id in sessions {
            // Read CurrentUser and ExpiresAt
            let (current_user, _, _) = match handle.read(session_id, &[current_user_ft]).await {
                Ok(result) => result,
                Err(_) => continue,
            };
            
            let (expires_at, _, _) = match handle.read(session_id, &[expires_at_ft]).await {
                Ok(result) => result,
                Err(_) => continue,
            };
            
            // Skip if session is not in use
            if let Value::EntityReference(None) = current_user {
                continue;
            }
            
            // Check if expired
            if let Value::Timestamp(expiration) = expires_at {
                if now > expiration {
                    // Session is expired, clean it up
                    info!("Cleaning up expired session: {:?}", session_id);
                    
                    // Save CurrentUser to PreviousUser
                    if let Value::EntityReference(Some(user_id)) = current_user {
                        if let Err(e) = handle.write(session_id, &[previous_user_ft], Value::EntityReference(Some(user_id)), None, None, None, None).await {
                            error!("Failed to save previous user during cleanup: {:?}", e);
                        }
                    }
                    
                    // Clear CurrentUser
                    if let Err(e) = handle.write(session_id, &[current_user_ft], Value::EntityReference(None), None, None, None, None).await {
                        error!("Failed to clear current user during cleanup: {:?}", e);
                        continue;
                    }
                    
                    // Clear token
                    if let Err(e) = handle.write(session_id, &[token_ft], Value::String("".to_string()), None, None, None, None).await {
                        error!("Failed to clear token during cleanup: {:?}", e);
                    }
                    
                    // Reset expiration
                    if let Err(e) = handle.write(session_id, &[expires_at_ft], Value::Timestamp(qlib_rs::epoch()), None, None, None, None).await {
                        error!("Failed to reset expiration during cleanup: {:?}", e);
                    }
                    
                    cleaned_count += 1;
                }
            }
        }
        
        if cleaned_count > 0 {
            info!("Cleaned up {} expired session(s)", cleaned_count);
        }
    }
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

    let app_state = web::Data::new(AppState { store_handle: store_handle.clone(), jwt_secret });

    // Spawn periodic session cleanup task
    let cleanup_handle = store_handle.clone();
    tokio::spawn(async move {
        session_cleanup_task(cleanup_handle).await;
    });

    let bind_address = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0:3000".to_string());

    info!("Binding web server to: {}", bind_address);

    HttpServer::new(move || {
        App::new()
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
