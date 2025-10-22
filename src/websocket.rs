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

#[derive(Debug, Serialize, Deserialize)]
struct WsRequestWrapper {
    request_id: Option<String>,
    #[serde(flatten)]
    request: WsRequest,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum WsRequest {
    Read { entity_id: EntityId, fields: Vec<FieldType> },
    Write { entity_id: EntityId, field: FieldType, value: Value },
    Create { entity_type: EntityType, name: String, parent_id: Option<EntityId> },
    Delete { entity_id: EntityId },
    Find { entity_type: EntityType, filter: Option<String> },
    RegisterNotification { config: NotifyConfig },
    UnregisterNotification { config: NotifyConfig },
    Schema { entity_type: EntityType },
    CompleteSchema { entity_type: EntityType },
    UpdateSchema { entity_type: EntityType, inherit: Vec<EntityType>, fields: std::collections::HashMap<FieldType, qlib_rs::FieldSchema> },
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
    request_id: Option<String>,
    data: Option<serde_json::Value>,
    error: Option<String>,
}

impl WsResponse {
    fn success(request_id: Option<String>, data: serde_json::Value) -> Self {
        WsResponse {
            success: true,
            request_id,
            data: Some(data),
            error: None,
        }
    }

    fn error(request_id: Option<String>, error: String) -> Self {
        WsResponse {
            success: false,
            request_id,
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
    let session_handle = state.session_handle.clone();
    
    // Get JWT secret from SessionService
    let jwt_secret = state.session_handle.get_jwt_secret().await;
    
    let (subject_id, ws_session_id) = match crate::handlers::get_subject_and_session_from_request(&req, &jwt_secret, &handle).await {
        Ok((uid, sid)) => (Some(uid), Some(sid)),
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
                // Serialize notification and convert config_hash to string for JavaScript
                let mut notification_value = serde_json::to_value(&notification).unwrap();
                if let Some(obj) = notification_value.as_object_mut() {
                    if let Some(config_hash) = obj.get("config_hash") {
                        if let Some(hash_num) = config_hash.as_u64() {
                            obj.insert("config_hash".to_string(), serde_json::Value::String(hash_num.to_string()));
                        }
                    }
                }
                
                // Notifications don't have request_id
                let notification_json = serde_json::to_string(&WsResponse::success(None, serde_json::json!({
                    "type": "notification",
                    "notification": notification_value
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
                    let response = match serde_json::from_str::<WsRequestWrapper>(&text) {
                        Ok(wrapper) => {
                            handle_ws_request(wrapper.request, wrapper.request_id, &handle, &session_handle, &crossbeam_sender, &registered_configs, subject_id, ws_session_id).await
                        }
                        Err(e) => WsResponse::error(None, format!("Invalid JSON: {}", e)),
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

        // Logout to trigger immediate session cleanup in qsession_manager
        if let Some(user_id) = subject_id {
            if let Err(e) = session_handle.logout(user_id).await {
                error!("Failed to logout on WebSocket disconnect: {:?}", e);
            }
        }
        
        info!("WebSocket connection terminated");
    });

    Ok(response)
}

async fn handle_ws_request(
    request: WsRequest,
    request_id: Option<String>,
    handle: &StoreHandle,
    session_handle: &crate::session_service::SessionHandle,
    notification_sender: &channel::Sender<qlib_rs::Notification>,
    registered_configs: &Arc<RwLock<HashMap<u64, qlib_rs::NotifyConfig>>>,
    subject_id: Option<qlib_rs::EntityId>,
    session_id: Option<qlib_rs::EntityId>,
) -> WsResponse {
    match request {
        WsRequest::Ping => WsResponse::success(request_id, serde_json::json!({ "message": "pong" })),
        
        WsRequest::Refresh => {
            // Extract and validate the existing subject_id and session_id
            let user_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Not authenticated".to_string()),
            };

            let _session_id = match session_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "No session".to_string()),
            };

            // Request refresh via SessionController (qsession_manager will generate new JWT)
            let token = match session_handle.refresh_session(user_id).await {
                Ok(tok) => tok,
                Err(e) => return WsResponse::error(request_id.clone(), format!("Session refresh failed: {:?}", e)),
            };

            WsResponse::success(request_id.clone(), serde_json::json!({
                "token": token
            }))
        }
        
        WsRequest::Pipeline { commands } => {
            let subject = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            match handle.execute_pipeline(commands, Some(subject)).await {
                Ok(results) => {
                    WsResponse::success(request_id.clone(), serde_json::json!({
                        "results": results
                    }))
                }
                Err(e) => WsResponse::error(request_id.clone(), format!("{:?}", e)),
            }
        }
        
        WsRequest::Read { entity_id, fields } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            for &ft in &fields {
                let scope = match handle.get_scope(subject_id, entity_id, ft).await {
                    Ok(s) => s,
                    Err(e) => return WsResponse::error(request_id.clone(), format!("Authorization check failed: {:?}", e)),
                };
                if scope == AuthorizationScope::None {
                    return WsResponse::error(request_id.clone(), "Access denied".to_string());
                }
            }

            match handle.read(entity_id, &fields).await {
                Ok((value, timestamp, writer_id)) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "value": value,
                    "timestamp": timestamp,
                    "writer_id": writer_id
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to read entity: {:?}", e)),
            }
        }

        WsRequest::Write { entity_id, field, value } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            let scope = match handle.get_scope(subject_id, entity_id, field).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(request_id.clone(), format!("Authorization check failed: {:?}", e)),
            };
            if scope != AuthorizationScope::ReadWrite {
                return WsResponse::error(request_id.clone(), "Access denied".to_string());
            }

            match handle.write(entity_id, &[field], value, Some(subject_id), None, None, None).await {
                Ok(_) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "message": "Successfully wrote value"
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to write: {:?}", e)),
            }
        }

        WsRequest::Create { entity_type, name, parent_id } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            let scope = match handle.get_scope(subject_id, qlib_rs::EntityId::new(entity_type, 0), qlib_rs::FieldType(0)).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(request_id.clone(), format!("Authorization check failed: {:?}", e)),
            };

            if scope != qlib_rs::auth::AuthorizationScope::ReadWrite {
                return WsResponse::error(request_id.clone(), "Access denied".to_string());
            }

            match handle.create_entity(entity_type, parent_id, &name).await {
                Ok(entity_id) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "entity_id": entity_id,
                    "name": name
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to create entity: {:?}", e)),
            }
        }

        WsRequest::Delete { entity_id } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            let scope = match handle.get_scope(subject_id, entity_id, qlib_rs::FieldType(0)).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(request_id.clone(), format!("Authorization check failed: {:?}", e)),
            };

            if scope != qlib_rs::auth::AuthorizationScope::ReadWrite {
                return WsResponse::error(request_id.clone(), "Access denied".to_string());
            }

            match handle.delete_entity(entity_id).await {
                Ok(_) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "message": "Successfully deleted entity"
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to delete entity: {:?}", e)),
            }
        }

        WsRequest::Find { entity_type, filter } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            match handle.find_entities(entity_type, filter.as_deref()).await {
                Ok(entity_ids) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "entities": entity_ids
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to find entities: {:?}", e)),
            }
        }
        WsRequest::RegisterNotification { config } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };
            let config_hash = qlib_rs::hash_notify_config(&config);
            {
                let map = registered_configs.read().await;
                if map.contains_key(&config_hash) {
                    return WsResponse::error(request_id.clone(), "Notification already registered".to_string());
                }
            }

            match handle.register_notification(config.clone(), notification_sender.clone()).await {
                Ok(_) => {
                    let mut map = registered_configs.write().await;
                    map.insert(config_hash, config);
                    WsResponse::success(request_id.clone(), serde_json::json!({
                        "message": "Notification registered",
                        // Send as string to avoid JavaScript number precision loss (53-bit vs 64-bit)
                        "config_hash": config_hash.to_string()
                    }))
                }
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to register notification: {:?}", e)),
            }
        }
        WsRequest::UnregisterNotification { config } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };
            let config_hash = qlib_rs::hash_notify_config(&config);
            {
                let map = registered_configs.read().await;
                if !map.contains_key(&config_hash) {
                    return WsResponse::error(request_id.clone(), "Notification not registered".to_string());
                }
            }

            match handle.unregister_notification(config.clone(), notification_sender.clone()).await {
                Ok(_) => {
                    let mut map = registered_configs.write().await;
                    map.remove(&config_hash);
                    WsResponse::success(request_id.clone(), serde_json::json!({
                        "message": "Notification unregistered"
                    }))
                }
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to unregister notification: {:?}", e)),
            }
        }
        WsRequest::Schema { entity_type } => {
            match handle.get_entity_schema(entity_type).await {
                Ok(schema) => {
                    WsResponse::success(request_id.clone(), serde_json::json!({
                        "schema": schema
                    }))
                }
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to get schema: {:?}", e)),
            }
        }
        WsRequest::CompleteSchema { entity_type } => {
            match handle.get_complete_entity_schema(entity_type).await {
                Ok(schema) => {
                    WsResponse::success(request_id.clone(), serde_json::json!({
                        "schema": schema
                    }))
                }
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to get complete schema: {:?}", e)),
            }
        }
        WsRequest::UpdateSchema { entity_type, inherit, fields } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error(request_id.clone(), "Authentication required".to_string()),
            };

            match handle.update_schema(entity_type, inherit, fields).await {
                Ok(_) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "message": "Schema updated successfully"
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to update schema: {:?}", e)),
            }
        }
        WsRequest::GetEntityType { name } => {
            match handle.get_entity_type(&name).await {
                Ok(entity_type) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "entity_type": entity_type
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to get entity type: {:?}", e)),
            }
        }
        WsRequest::GetFieldType { name } => {
            match handle.get_field_type(&name).await {
                Ok(field_type) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "field_type": field_type
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to get field type: {:?}", e)),
            }
        }
        WsRequest::ResolveEntityType { entity_type } => {
            match handle.resolve_entity_type(entity_type).await {
                Ok(name) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "name": name
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to resolve entity type: {:?}", e)),
            }
        }
        WsRequest::ResolveFieldType { field_type } => {
            match handle.resolve_field_type(field_type).await {
                Ok(name) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "name": name
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to resolve field type: {:?}", e)),
            }
        }
        WsRequest::GetFieldSchema { entity_type, field_type } => {
            match handle.get_field_schema(entity_type, field_type).await {
                Ok(schema) => {
                    WsResponse::success(request_id.clone(), serde_json::json!({
                        "schema": schema
                    }))
                }
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to get field schema: {:?}", e)),
            }
        }
        WsRequest::EntityExists { entity_id } => {
            let exists = handle.entity_exists(entity_id).await;
            WsResponse::success(request_id.clone(), serde_json::json!({
                "exists": exists
            }))
        }
        WsRequest::FieldExists { entity_type, field_type } => {
            let exists = handle.field_exists(entity_type, field_type).await;
            WsResponse::success(request_id.clone(), serde_json::json!({
                "exists": exists
            }))
        }
        WsRequest::ResolveIndirection { entity_id, fields } => {
            match handle.resolve_indirection(entity_id, &fields).await {
                Ok((resolved_entity_id, resolved_field_type)) => WsResponse::success(request_id.clone(), serde_json::json!({
                    "resolved_entity_id": resolved_entity_id,
                    "resolved_field_type": resolved_field_type
                })),
                Err(e) => WsResponse::error(request_id.clone(), format!("Failed to resolve indirection: {:?}", e)),
            }
        }
    }
}


