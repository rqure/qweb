use actix_web::{web, HttpRequest, HttpResponse};
use actix_ws::Message;
use futures_util::StreamExt;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use qlib_rs::data::AsyncStoreProxy;

use crate::AppState;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
enum WsRequest {
    Read { entity_id: String, fields: Vec<String> },
    Write { entity_id: String, field: String, value: serde_json::Value },
    Create { entity_type: String, name: String },
    Delete { entity_id: String },
    Find { entity_type: String, filter: Option<String> },
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
    
    info!("WebSocket connection established");

    let proxy = state.store_proxy.clone();

    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            match msg {
                Message::Text(text) => {
                    let response = match serde_json::from_str::<WsRequest>(&text) {
                        Ok(request) => handle_ws_request(request, &proxy).await,
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

        info!("WebSocket connection terminated");
    });

    Ok(response)
}

async fn handle_ws_request(
    request: WsRequest,
    proxy: &Arc<RwLock<AsyncStoreProxy>>,
) -> WsResponse {
    match request {
        WsRequest::Ping => WsResponse::success(serde_json::json!({ "message": "pong" })),
        
        WsRequest::Read { entity_id, fields } => {
            let p = proxy.read().await;

            let entity_id = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::data::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let mut field_types = Vec::new();
            for field_name in &fields {
                match p.get_field_type(field_name).await {
                    Ok(ft) => field_types.push(ft),
                    Err(e) => {
                        return WsResponse::error(format!(
                            "Failed to get field type '{}': {:?}",
                            field_name, e
                        ))
                    }
                }
            }

            match p.read(entity_id, &field_types).await {
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
            let p = proxy.read().await;

            let entity_id = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::data::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            let field_type = match p.get_field_type(&field).await {
                Ok(ft) => ft,
                Err(e) => return WsResponse::error(format!("Failed to get field type: {:?}", e)),
            };

            let qlib_value = match json_to_qlib_value(&value) {
                Ok(v) => v,
                Err(e) => return WsResponse::error(format!("Invalid value: {}", e)),
            };

            match p.write(entity_id, &[field_type], qlib_value, None, None, None, None).await {
                Ok(_) => WsResponse::success(serde_json::json!({
                    "message": "Successfully wrote value"
                })),
                Err(e) => WsResponse::error(format!("Failed to write: {:?}", e)),
            }
        }

        WsRequest::Create { entity_type, name } => {
            let p = proxy.read().await;

            let et = match p.get_entity_type(&entity_type).await {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            };

            match p.create_entity(et, None, &name).await {
                Ok(entity_id) => WsResponse::success(serde_json::json!({
                    "entity_id": entity_id.0.to_string(),
                    "entity_type": entity_type,
                    "name": name
                })),
                Err(e) => WsResponse::error(format!("Failed to create entity: {:?}", e)),
            }
        }

        WsRequest::Delete { entity_id } => {
            let p = proxy.read().await;

            let entity_id = match entity_id.parse::<u64>() {
                Ok(id) => qlib_rs::data::EntityId(id),
                Err(e) => return WsResponse::error(format!("Invalid entity ID: {}", e)),
            };

            match p.delete_entity(entity_id).await {
                Ok(_) => WsResponse::success(serde_json::json!({
                    "message": "Successfully deleted entity"
                })),
                Err(e) => WsResponse::error(format!("Failed to delete entity: {:?}", e)),
            }
        }

        WsRequest::Find { entity_type, filter } => {
            let p = proxy.read().await;

            let et = match p.get_entity_type(&entity_type).await {
                Ok(et) => et,
                Err(e) => return WsResponse::error(format!("Failed to get entity type: {:?}", e)),
            };

            match p.find_entities(et, filter.as_deref()).await {
                Ok(entity_ids) => WsResponse::success(serde_json::json!({
                    "entities": entity_ids.iter().map(|id| id.0.to_string()).collect::<Vec<_>>()
                })),
                Err(e) => WsResponse::error(format!("Failed to find entities: {:?}", e)),
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
                Ok(Value::EntityReference(Some(qlib_rs::data::EntityId(id))))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        serde_json::Value::Array(arr) => {
            let ids: Result<Vec<qlib_rs::data::EntityId>, _> = arr
                .iter()
                .map(|v| {
                    if let serde_json::Value::String(s) = v {
                        s.parse::<u64>().map(qlib_rs::data::EntityId).map_err(|_| "Invalid entity ID")
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
