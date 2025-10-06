use actix_web::{rt, web, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::sync::mpsc;
use crossbeam::channel;

use crate::store_service::StoreHandle;
use crate::AppState;

use qlib_rs::{auth::AuthorizationScope, EntityId, FieldType};

/// Helper function to resolve a field identifier (either name or numeric ID)
async fn resolve_field_identifier(handle: &StoreHandle, field_identifier: &str) -> Result<FieldType, String> {
    // Try to parse as a numeric field type ID first, otherwise look up by name
    if let Ok(field_type_id) = field_identifier.parse::<u64>() {
        Ok(FieldType(field_type_id))
    } else {
        handle.get_field_type(field_identifier).await
            .map_err(|e| format!("Failed to get field type for '{}': {:?}", field_identifier, e))
    }
}

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
pub struct NotifyConfigJson {
    pub entity_id: Option<String>, // For EntityId variant
    pub entity_type: Option<String>, // For EntityType variant
    pub field: String,
    pub trigger_on_change: bool,
    pub context: Vec<Vec<String>>, // Context fields as field names
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum WsRequest {
    Read { entity_id: String, fields: Vec<String> },
    Write { entity_id: String, field: String, value: serde_json::Value },
    Create { entity_type: String, name: String },
    Delete { entity_id: String },
    Find { entity_type: String, filter: Option<String> },
    RegisterNotification { config: NotifyConfigJson },
    UnregisterNotification { config: NotifyConfigJson },
    Schema { entity_type: String },
    CompleteSchema { entity_type: String },
    ResolveEntityType { entity_type: String },
    ResolveFieldType { field_type: String },
    GetFieldSchema { entity_type: String, field_type: String },
    EntityExists { entity_id: String },
    FieldExists { entity_type: String, field_type: String },
    ResolveIndirection { entity_id: String, fields: Vec<String> },
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

        // Track registered configs
        let mut registered_configs: HashMap<qlib_rs::NotifyConfig, ()> = HashMap::new();

        // Spawn task to forward notifications to WebSocket
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

        // Spawn task to poll for notifications from crossbeam and send to tokio
        tokio::spawn(async move {
            loop {
                match crossbeam_receiver.try_recv() {
                    Ok(notification) => {
                        if tokio_sender.send(notification).is_err() {
                            break;
                        }
                    }
                    Err(crossbeam::channel::TryRecvError::Empty) => {
                        // No notification available, sleep briefly
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                    Err(crossbeam::channel::TryRecvError::Disconnected) => {
                        // Channel disconnected, exit
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
                            handle_ws_request(request, &handle, &crossbeam_sender, &mut registered_configs, subject_id, ws_session_id).await
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
        for config in registered_configs.keys() {
            let _ = handle.unregister_notification(config.clone(), crossbeam_sender.clone()).await;
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
    registered_configs: &mut HashMap<qlib_rs::NotifyConfig, ()>,
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

            let entity_id = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let mut field_types = Vec::new();
            for field_name in &fields {
                match resolve_field_identifier(handle, field_name).await {
                    Ok(ft) => field_types.push(ft),
                    Err(e) => {
                        return WsResponse::error(e)
                    }
                }
            }

            for &ft in &field_types {
                let scope = match handle.get_scope(subject_id, entity_id, ft).await {
                    Ok(s) => s,
                    Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
                };
                if scope == AuthorizationScope::None {
                    return WsResponse::error("Access denied".to_string());
                }
            }

            match handle.read(entity_id, &field_types).await {
                Ok((value, timestamp, writer_id)) => WsResponse::success(serde_json::json!({
                    "entity_id": entity_id.0.to_string(),
                    "value": format!("{:?}", value),
                    "timestamp": timestamp.to_string(),
                    "writer_id": writer_id.map(|id| id.0.to_string())
                })),
                Err(e) => WsResponse::error(format!("Failed to read entity: {:?}", e)),
            }
        }

        WsRequest::Write { entity_id, field, value } => {
            let subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            let entity_id = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let field_type = match resolve_field_identifier(handle, &field).await {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(e),
            };

            let scope = match handle.get_scope(subject_id, entity_id, field_type).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
            };
            if scope != AuthorizationScope::ReadWrite {
                return WsResponse::error("Access denied".to_string());
            }

            let qlib_value = match json_to_qlib_value(&value) {
                Ok(v) => v,
                Err(e) => return WsResponse::error(format!("Invalid value: {}", e)),
            };

            match handle.write(entity_id, &[field_type], qlib_value, None, None, None, None).await {
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

            let et = match handle.get_entity_type(&entity_type).await {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            };

            let scope = match handle.get_scope(subject_id, qlib_rs::EntityId::new(et, 0), qlib_rs::FieldType(0)).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
            };

            if scope != qlib_rs::auth::AuthorizationScope::ReadWrite {
                return WsResponse::error("Access denied".to_string());
            }

            match handle.create_entity(et, None, &name).await {
                Ok(entity_id) => WsResponse::success(serde_json::json!({
                    "entity_id": entity_id.0.to_string(),
                    "entity_type": entity_type,
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

            let entity_id_parsed = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let scope = match handle.get_scope(subject_id, entity_id_parsed, qlib_rs::FieldType(0)).await {
                Ok(s) => s,
                Err(e) => return WsResponse::error(format!("Authorization check failed: {:?}", e)),
            };

            if scope != qlib_rs::auth::AuthorizationScope::ReadWrite {
                return WsResponse::error("Access denied".to_string());
            }

            match handle.delete_entity(entity_id_parsed).await {
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

            let et = match handle.get_entity_type(&entity_type).await {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            };

            match handle.find_entities(et, filter.as_deref()).await {
                Ok(entity_ids) => WsResponse::success(serde_json::json!({
                    "entities": entity_ids.iter().map(|id| id.0.to_string()).collect::<Vec<_>>()
                })),
                Err(e) => WsResponse::error(format!("Failed to find entities: {:?}", e)),
            }
        }
        WsRequest::RegisterNotification { config } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            match config_to_notify_config(&config, handle).await {
                Ok(notify_config) => {
                    if registered_configs.contains_key(&notify_config) {
                        return WsResponse::error("Notification already registered".to_string());
                    }
                    match handle.register_notification(notify_config.clone(), notification_sender.clone()).await {
                        Ok(_) => {
                            registered_configs.insert(notify_config, ());
                            WsResponse::success(serde_json::json!({
                                "message": "Notification registered"
                            }))
                        }
                        Err(e) => WsResponse::error(format!("Failed to register notification: {:?}", e)),
                    }
                }
                Err(e) => WsResponse::error(format!("Invalid config: {}", e)),
            }
        }
        WsRequest::UnregisterNotification { config } => {
            let _subject_id = match subject_id {
                Some(id) => id,
                None => return WsResponse::error("Authentication required".to_string()),
            };

            match config_to_notify_config(&config, handle).await {
                Ok(notify_config) => {
                    if !registered_configs.contains_key(&notify_config) {
                        return WsResponse::error("Notification not registered".to_string());
                    }
                    match handle.unregister_notification(notify_config.clone(), notification_sender.clone()).await {
                        Ok(_) => {
                            registered_configs.remove(&notify_config);
                            WsResponse::success(serde_json::json!({
                                "message": "Notification unregistered"
                            }))
                        }
                        Err(e) => WsResponse::error(format!("Failed to unregister notification: {:?}", e)),
                    }
                }
                Err(e) => WsResponse::error(format!("Invalid config: {}", e)),
            }
        }
        WsRequest::Schema { entity_type } => {
            let et = match handle.get_entity_type(&entity_type).await {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            };

            match handle.get_entity_schema(et).await {
                Ok(schema) => {
                    match serde_json::to_value(&schema) {
                        Ok(schema_json) => WsResponse::success(serde_json::json!({
                            "entity_type": entity_type,
                            "schema": schema_json
                        })),
                        Err(e) => WsResponse::error(format!("Failed to serialize schema: {:?}", e)),
                    }
                }
                Err(e) => WsResponse::error(format!("Failed to get schema: {:?}", e)),
            }
        }
        WsRequest::CompleteSchema { entity_type } => {
            let et = match handle.get_entity_type(&entity_type).await {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            };

            match handle.get_complete_entity_schema(et).await {
                Ok(schema) => {
                    match serde_json::to_value(&schema) {
                        Ok(schema_json) => WsResponse::success(serde_json::json!({
                            "entity_type": entity_type,
                            "schema": schema_json
                        })),
                        Err(e) => WsResponse::error(format!("Failed to serialize schema: {:?}", e)),
                    }
                }
                Err(e) => WsResponse::error(format!("Failed to get complete schema: {:?}", e)),
            }
        }
        WsRequest::ResolveEntityType { entity_type } => {
            let et = match entity_type.parse::<u32>() {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Invalid entity type: {}", e)),
            };

            match handle.resolve_entity_type(qlib_rs::EntityType(et)).await {
                Ok(name) => WsResponse::success(serde_json::json!({
                    "entity_type": entity_type,
                    "name": name
                })),
                Err(e) => WsResponse::error(format!("Failed to resolve entity type: {:?}", e)),
            }
        }
        WsRequest::ResolveFieldType { field_type } => {
            let ft = match field_type.parse::<u64>() {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(format!("Invalid field type: {}", e)),
            };

            match handle.resolve_field_type(qlib_rs::FieldType(ft)).await {
                Ok(name) => WsResponse::success(serde_json::json!({
                    "field_type": field_type,
                    "name": name
                })),
                Err(e) => WsResponse::error(format!("Failed to resolve field type: {:?}", e)),
            }
        }
        WsRequest::GetFieldSchema { entity_type, field_type } => {
            let et = match entity_type.parse::<u32>() {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Invalid entity type: {}", e)),
            };

            let ft = match field_type.parse::<u64>() {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(format!("Invalid field type: {}", e)),
            };

            match handle.get_field_schema(qlib_rs::EntityType(et), qlib_rs::FieldType(ft)).await {
                Ok(schema) => WsResponse::success(serde_json::json!({
                    "entity_type": entity_type,
                    "field_type": field_type,
                    "schema": format!("{:?}", schema)
                })),
                Err(e) => WsResponse::error(format!("Failed to get field schema: {:?}", e)),
            }
        }
        WsRequest::EntityExists { entity_id } => {
            let eid = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let exists = handle.entity_exists(eid).await;

            WsResponse::success(serde_json::json!({
                "entity_id": entity_id,
                "exists": exists
            }))
        }
        WsRequest::FieldExists { entity_type, field_type } => {
            let et = match entity_type.parse::<u32>() {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Invalid entity type: {}", e)),
            };

            let ft = match field_type.parse::<u64>() {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(format!("Invalid field type: {}", e)),
            };

            let exists = handle.field_exists(qlib_rs::EntityType(et), qlib_rs::FieldType(ft)).await;

            WsResponse::success(serde_json::json!({
                "entity_type": entity_type,
                "field_type": field_type,
                "exists": exists
            }))
        }
        WsRequest::ResolveIndirection { entity_id, fields } => {
            let eid = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let mut field_types = Vec::new();
            for field_name in &fields {
                match resolve_field_identifier(handle, field_name).await {
                    Ok(ft) => field_types.push(ft),
                    Err(e) => return WsResponse::error(e),
                }
            }

            match handle.resolve_indirection(eid, &field_types).await {
                Ok((resolved_entity_id, resolved_field_type)) => WsResponse::success(serde_json::json!({
                    "entity_id": entity_id,
                    "resolved_entity_id": resolved_entity_id.0.to_string(),
                    "resolved_field_type": resolved_field_type.0.to_string()
                })),
                Err(e) => WsResponse::error(format!("Failed to resolve indirection: {:?}", e)),
            }
        }
    }
}

fn json_to_qlib_value(json: &serde_json::Value) -> Result<qlib_rs::Value, String> {
    use qlib_rs::Value;
    
    match json {
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err("Invalid number".to_string())
            }
        }
        serde_json::Value::String(s) => {
            if let Ok(id) = s.parse::<u64>() {
                Ok(Value::EntityReference(Some(qlib_rs::EntityId(id))))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        serde_json::Value::Array(arr) => {
            let ids: Result<Vec<qlib_rs::EntityId>, _> = arr
                .iter()
                .map(|v| {
                    if let serde_json::Value::String(s) = v {
                        s.parse::<u64>().map(qlib_rs::EntityId).map_err(|_| "Invalid entity ID")
                    } else {
                        Err("Invalid entity ID")
                    }
                })
                .collect();
            match ids {
                Ok(entity_ids) => Ok(Value::EntityList(entity_ids)),
                Err(e) => Err(format!("Array must contain valid entity IDs: {}", e)),
            }
        }
        serde_json::Value::Null => Ok(Value::EntityReference(None)),
        _ => Err("Unsupported JSON type".to_string()),
    }
}

async fn config_to_notify_config(config: &NotifyConfigJson, handle: &StoreHandle) -> Result<qlib_rs::NotifyConfig, String> {
    let field_type = resolve_field_identifier(handle, &config.field).await?;

    let mut context = Vec::new();
    for path in &config.context {
        let mut path_types = Vec::new();
        for field_name in path {
            let ft = resolve_field_identifier(handle, field_name).await?;
            path_types.push(ft);
        }
        context.push(path_types);
    }

    if let Some(entity_id_str) = &config.entity_id {
        let entity_id = entity_id_str.parse::<u64>()
            .map(qlib_rs::EntityId)
            .map_err(|_| "Invalid entity ID")?;
        Ok(qlib_rs::NotifyConfig::EntityId {
            entity_id,
            field_type,
            trigger_on_change: config.trigger_on_change,
            context,
        })
    } else if let Some(entity_type_str) = &config.entity_type {
        let entity_type = handle.get_entity_type(entity_type_str).await
            .map_err(|e| format!("Failed to get entity type: {:?}", e))?;
        Ok(qlib_rs::NotifyConfig::EntityType {
            entity_type,
            field_type,
            trigger_on_change: config.trigger_on_change,
            context,
        })
    } else {
        Err("Either entity_id or entity_type must be provided".to_string())
    }
}
