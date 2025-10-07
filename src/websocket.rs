use actix_web::{rt, web, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crossbeam::channel;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::store_service::StoreHandle;
use crate::AppState;

use qlib_rs::{auth::AuthorizationScope, EntityId, EntityType, FieldType, NotifyConfig, Value};

/// Check session ownership for websocket connections
async fn check_websocket_session_ownership(
    handle: &StoreHandle,
    session_id: EntityId,
    qweb_service_id: EntityId,
) -> Result<bool, String> {
    let parent_field_type = handle.get_field_type("Parent").await
        .map_err(|e| format!("Failed to get Parent field type: {:?}", e))?;
    
    // Check if session belongs to this qweb instance
    let (parent_value, _, _) = handle.read(session_id, &[parent_field_type]).await
        .map_err(|e| format!("Failed to read session parent: {:?}", e))?;
    
    if let qlib_rs::Value::EntityReference(Some(parent_id)) = parent_value {
        Ok(parent_id == qweb_service_id)
    } else {
        Err("Session has no parent".to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum WsRequest {
    Read { entity_id: EntityId, fields: Vec<FieldType> },
    Write { entity_id: EntityId, field: FieldType, value: Value },
    Create { entity_type: EntityType, name: String },
    Delete { entity_id: EntityId },
    Find { entity_type: EntityType, filter: Option<String> },
    RegisterNotification { config: NotifyConfig },
    UnregisterNotification { config: NotifyConfig },
    Schema { entity_type: EntityType },
    CompleteSchema { entity_type: EntityType },
    GetEntityType { name: String },
    GetFieldType { name: String },
    ResolveEntityType { entity_type: EntityType },
    ResolveFieldType { field_type: FieldType },
    GetFieldSchema { entity_type: EntityType, field_type: FieldType },
    EntityExists { entity_id: EntityId },
    FieldExists { entity_type: EntityType, field_type: FieldType },
    ResolveIndirection { entity_id: EntityId, fields: Vec<FieldType> },
    Pipeline { commands: Vec<crate::models::PipelineCommand> },
    Refresh,
    Ping,
}

#[derive(Debug, Serialize)]
struct WsResponse {
    success: bool,
    data: Option<serde_json::Value>,
    error: Option<String>,
}

impl WsResponse {
    fn success(data: serde_json::Value) -> Self {
        WsResponse {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn error(error: String) -> Self {
        WsResponse {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}

pub async fn ws_handler(
    req: HttpRequest,
    body: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, body)?;

    let handle = state.store_handle.clone();
    let (subject_id, ws_session_id) = match crate::handlers::get_subject_and_session_from_request(&req, &state.jwt_secret, &handle).await {
        Ok((uid, sid)) => {
            // Verify session ownership
            match check_websocket_session_ownership(&handle, sid, state.qweb_service_id).await {
                Ok(true) => (Some(uid), Some(sid)),
                Ok(false) => {
                    error!("Session belongs to different qweb instance");
                    (None, None)
                }
                Err(e) => {
                    error!("Failed to verify session ownership: {}", e);
                    (None, None)
                }
            }
        }
        Err(_) => (None, None),
    };
    
    rt::spawn(async move {
        info!("WebSocket connection established");

        // Create channels for notifications
        let (crossbeam_sender, crossbeam_receiver) = channel::unbounded::<qlib_rs::Notification>();
        let (tokio_sender, mut tokio_receiver) = mpsc::unbounded_channel::<qlib_rs::Notification>();

        // Track registered configs by config_hash
        let registered_configs: Arc<RwLock<HashMap<u64, qlib_rs::NotifyConfig>>> = Arc::new(RwLock::new(HashMap::new()));

        // Spawn task to forward notifications to the WebSocket
        let mut session_clone = session.clone();
        tokio::spawn(async move {
            while let Some(notification) = tokio_receiver.recv().await {
                let notification_json = serde_json::to_string(&WsResponse::success(serde_json::json!({
                    "type": "notification",
                    "notification": notification
                }))).unwrap();
                if let Err(e) = session_clone.text(notification_json).await {
                    error!("Failed to send notification: {}", e);
                    break;
                }
            }
        });

        // Spawn task to poll for qlib notifications from crossbeam
        tokio::spawn(async move {
            loop {
                match crossbeam_receiver.try_recv() {
                    Ok(notification) => {
                        if tokio_sender.send(notification).is_err() {
                            break;
                        }
                    }
                    Err(crossbeam::channel::TryRecvError::Empty) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Err(crossbeam::channel::TryRecvError::Disconnected) => {
                        break;
                    }
                }
            }
        });

        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                Message::Text(text) => {
                    let response = match serde_json::from_str::<WsRequest>(&text) {
                        Ok(request) => {
                            handle_ws_request(request, &handle, &crossbeam_sender, &registered_configs, subject_id, ws_session_id).await
                        }
                        Err(e) => WsResponse::error(format!("Invalid JSON: {}", e)),
                    };

                    let response_json = serde_json::to_string(&response).unwrap();
                    if let Err(e) = session.text(response_json).await {
                        error!("Failed to send WebSocket response: {}", e);
                        break;
                    }
                }
                Message::Ping(bytes) => {
                    if let Err(e) = session.pong(&bytes).await {
                        error!("Failed to send pong: {}", e);
                        break;
                    }
                }
                Message::Close(_) => {
                    info!("WebSocket connection closed by client");
                    break;
                }
                _ => {}
            }
        }

        // Unregister all notifications on disconnect
        {
            let map = registered_configs.read().await;
            for (_hash, qlib_config) in map.iter() {
                let _ = handle.unregister_notification(qlib_config.clone(), crossbeam_sender.clone()).await;
            }
        }

        // Auto-logout: Clear session on disconnect
        if let Some(ws_session_id) = ws_session_id {
            info!("WebSocket disconnected, auto-logging out session: {:?}", ws_session_id);
            
            // Get field types
            if let (Ok(current_user_ft), Ok(previous_user_ft), Ok(token_ft), Ok(expires_at_ft)) = (
                handle.get_field_type("CurrentUser").await,
                handle.get_field_type("PreviousUser").await,
                handle.get_field_type("Token").await,
                handle.get_field_type("ExpiresAt").await,
            ) {
                // Save CurrentUser to PreviousUser
                if let Ok((qlib_rs::Value::EntityReference(Some(user_id)), _, _)) = handle.read(ws_session_id, &[current_user_ft]).await {
                    let _ = handle.write(ws_session_id, &[previous_user_ft], qlib_rs::Value::EntityReference(Some(user_id)), None, None, None, None).await;
                }
                
                // Clear the session
                let _ = handle.write(ws_session_id, &[current_user_ft], qlib_rs::Value::EntityReference(None), None, None, None, None).await;
                let _ = handle.write(ws_session_id, &[token_ft], qlib_rs::Value::String("".to_string()), None, None, None, None).await;
                let _ = handle.write(ws_session_id, &[expires_at_ft], qlib_rs::Value::Timestamp(qlib_rs::epoch()), None, None, None, None).await;
            }
        }

        info!("WebSocket connection terminated");
    });

    Ok(response)
}

async fn handle_ws_request(
    request: WsRequest,
    handle: &StoreHandle,
    notification_sender: &channel::Sender<qlib_rs::Notification>,
    registered_configs: &Arc<RwLock<HashMap<u64, qlib_rs::NotifyConfig>>>,
    subject_id: Option<qlib_rs::EntityId>,
    session_id: Option<qlib_rs::EntityId>,
) -> WsResponse {
    match request {
        WsRequest::Ping => WsResponse::success(serde_json::json!({ "message": "pong" })),
        
        WsRequest::Refresh => {
            // Extract and validate the existing subject_id and session_id
            let user_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Not authenticated".to_string()),
            };

            let session_id = match session_id {
                Some(id) => id,
                None => return WsResponse::error("No session".to_string()),
            };

            // Issue a new token with extended expiration
            use jsonwebtoken::{encode, Header, EncodingKey};
            
            let jwt_secret = std::env::var("JWT_SECRET")
                .unwrap_or_else(|_| "default_secret".to_string());
            
            let expiration = chrono::Utc::now() + chrono::Duration::minutes(1);
            let claims = serde_json::json!({
                "sub": user_id.0.to_string(),
                "session_id": session_id.0.to_string(),
                "exp": expiration.timestamp() as usize,
            });
            
            let token = match encode(&Header::default(), &claims, &EncodingKey::from_secret(jwt_secret.as_bytes())) {
                Ok(t) => t,
                Err(e) => return WsResponse::error(format!("Failed to generate token: {:?}", e)),
            };

            // Update Session with new token and expiration
            let token_field_type = match handle.get_field_type("Token").await {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(format!("Failed to get Token field type: {:?}", e)),
            };

            let expires_at_field_type = match handle.get_field_type("ExpiresAt").await {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(format!("Failed to get ExpiresAt field type: {:?}", e)),
            };

            if let Err(e) = handle.write(session_id, &[token_field_type], qlib_rs::Value::String(token.clone()), None, None, None, None).await {
                return WsResponse::error(format!("Failed to update token: {:?}", e));
            }

            let expiration_timestamp = qlib_rs::millis_to_timestamp(expiration.timestamp_millis() as u64);
            if let Err(e) = handle.write(session_id, &[expires_at_field_type], qlib_rs::Value::Timestamp(expiration_timestamp), None, None, None, None).await {
                return WsResponse::error(format!("Failed to update expiration: {:?}", e));
            }

            WsResponse::success(serde_json::json!({
                "token": token
            }))
        }
        
        WsRequest::Pipeline { commands } => {
            match handle.execute_pipeline(commands).await {
                Ok(results) => {
                    WsResponse::success(serde_json::json!({
                        "results": results
                    }))
                }
                Err(e) => WsResponse::error(format!("{:?}", e)),
            }
        }
        
        WsRequest::Read { entity_id, fields } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            for &ft in &fields {
                let scope = match handle.get_scope(subject_id, entity_id, ft).await {
                    Ok(s) => s,
                    Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
                };
                if scope == AuthorizationScope::None {
                    return WsResponse::error("Access denied".to_string());
                }
            }

            match handle.read(entity_id, &fields).await {
                Ok((value, timestamp, writer_id)) => WsResponse::success(serde_json::json!({
                    "value": value,
                    "timestamp": timestamp,
                    "writer_id": writer_id
                })),
                Err(e) => WsResponse::error(format!("Failed to read entity: {:?}", e)),
            }
        }

        WsRequest::Write { entity_id, field, value } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            let scope = match handle.get_scope(subject_id, entity_id, field).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
            };
            if scope != AuthorizationScope::ReadWrite {
                return WsResponse::error("Access denied".to_string());
            }

            match handle.write(entity_id, &[field], value, None, None, None, None).await {
                Ok(_) => WsResponse::success(serde_json::json!({
                    "message": "Successfully wrote value"
                })),
                Err(e) => WsResponse::error(format!("Failed to write: {:?}", e)),
            }
        }

        WsRequest::Create { entity_type, name } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            let scope = match handle.get_scope(subject_id, qlib_rs::EntityId::new(entity_type, 0), qlib_rs::FieldType(0)).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
            };

            if scope != qlib_rs::auth::AuthorizationScope::ReadWrite {
                return WsResponse::error("Access denied".to_string());
            }

            match handle.create_entity(entity_type, None, &name).await {
                Ok(entity_id) => WsResponse::success(serde_json::json!({
                    "entity_id": entity_id,
                    "name": name
                })),
                Err(e) => WsResponse::error(format!("Failed to create entity: {:?}", e)),
            }
        }

        WsRequest::Delete { entity_id } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            let scope = match handle.get_scope(subject_id, entity_id, qlib_rs::FieldType(0)).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
            };

            if scope != qlib_rs::auth::AuthorizationScope::ReadWrite {
                return WsResponse::error("Access denied".to_string());
            }

            match handle.delete_entity(entity_id).await {
                Ok(_) => WsResponse::success(serde_json::json!({
                    "message": "Successfully deleted entity"
                })),
                Err(e) => WsResponse::error(format!("Failed to delete entity: {:?}", e)),
            }
        }

        WsRequest::Find { entity_type, filter } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            match handle.find_entities(entity_type, filter.as_deref()).await {
                Ok(entity_ids) => WsResponse::success(serde_json::json!({
                    "entities": entity_ids
                })),
                Err(e) => WsResponse::error(format!("Failed to find entities: {:?}", e)),
            }
        }
        WsRequest::RegisterNotification { config } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };
            let config_hash = qlib_rs::hash_notify_config(&config);
            {
                let map = registered_configs.read().await;
                if map.contains_key(&config_hash) {
                    return WsResponse::error("Notification already registered".to_string());
                }
            }

            match handle.register_notification(config.clone(), notification_sender.clone()).await {
                Ok(_) => {
                    let mut map = registered_configs.write().await;
                    map.insert(config_hash, config);
                    WsResponse::success(serde_json::json!({
                        "message": "Notification registered"
                    }))
                }
                Err(e) => WsResponse::error(format!("Failed to register notification: {:?}", e)),
            }
        }
        WsRequest::UnregisterNotification { config } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };
            let config_hash = qlib_rs::hash_notify_config(&config);
            {
                let map = registered_configs.read().await;
                if !map.contains_key(&config_hash) {
                    return WsResponse::error("Notification not registered".to_string());
                }
            }

            match handle.unregister_notification(config.clone(), notification_sender.clone()).await {
                Ok(_) => {
                    let mut map = registered_configs.write().await;
                    map.remove(&config_hash);
                    WsResponse::success(serde_json::json!({
                        "message": "Notification unregistered"
                    }))
                }
                Err(e) => WsResponse::error(format!("Failed to unregister notification: {:?}", e)),
            }
        }
        WsRequest::Schema { entity_type } => {
            match handle.get_entity_schema(entity_type).await {
                Ok(schema) => {
                    WsResponse::success(serde_json::json!({
                        "schema": schema
                    }))
                }
                Err(e) => WsResponse::error(format!("Failed to get schema: {:?}", e)),
            }
        }
        WsRequest::CompleteSchema { entity_type } => {
            match handle.get_complete_entity_schema(entity_type).await {
                Ok(schema) => {
                    WsResponse::success(serde_json::json!({
                        "schema": schema
                    }))
                }
                Err(e) => WsResponse::error(format!("Failed to get complete schema: {:?}", e)),
            }
        }
        WsRequest::GetEntityType { name } => {
            match handle.get_entity_type(&name).await {
                Ok(entity_type) => WsResponse::success(serde_json::json!({
                    "entity_type": entity_type
                })),
                Err(e) => WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            }
        }
        WsRequest::GetFieldType { name } => {
            match handle.get_field_type(&name).await {
                Ok(field_type) => WsResponse::success(serde_json::json!({
                    "field_type": field_type
                })),
                Err(e) => WsResponse::error(format!("Failed to get field type: {:?}", e)),
            }
        }
        WsRequest::ResolveEntityType { entity_type } => {
            match handle.resolve_entity_type(entity_type).await {
                Ok(name) => WsResponse::success(serde_json::json!({
                    "name": name
                })),
                Err(e) => WsResponse::error(format!("Failed to resolve entity type: {:?}", e)),
            }
        }
        WsRequest::ResolveFieldType { field_type } => {
            match handle.resolve_field_type(field_type).await {
                Ok(name) => WsResponse::success(serde_json::json!({
                    "name": name
                })),
                Err(e) => WsResponse::error(format!("Failed to resolve field type: {:?}", e)),
            }
        }
        WsRequest::GetFieldSchema { entity_type, field_type } => {
            match handle.get_field_schema(entity_type, field_type).await {
                Ok(schema) => {
                    WsResponse::success(serde_json::json!({
                        "schema": schema
                    }))
                }
                Err(e) => WsResponse::error(format!("Failed to get field schema: {:?}", e)),
            }
        }
        WsRequest::EntityExists { entity_id } => {
            let exists = handle.entity_exists(entity_id).await;
            WsResponse::success(serde_json::json!({
                "exists": exists
            }))
        }
        WsRequest::FieldExists { entity_type, field_type } => {
            let exists = handle.field_exists(entity_type, field_type).await;
            WsResponse::success(serde_json::json!({
                "exists": exists
            }))
        }
        WsRequest::ResolveIndirection { entity_id, fields } => {
            match handle.resolve_indirection(entity_id, &fields).await {
                Ok((resolved_entity_id, resolved_field_type)) => WsResponse::success(serde_json::json!({
                    "resolved_entity_id": resolved_entity_id,
                    "resolved_field_type": resolved_field_type
                })),
                Err(e) => WsResponse::error(format!("Failed to resolve indirection: {:?}", e)),
            }
        }
    }
}


