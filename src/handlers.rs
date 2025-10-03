use actix_web::{web, HttpResponse, Responder};
use qlib_rs::{EntityId, PageOpts};

use crate::models::{
    ApiResponse, CreateRequest, DeleteRequest, FindRequest, ReadRequest,
    SchemaRequest, WriteRequest,
};
use crate::AppState;

pub async fn read(state: web::Data<AppState>, req: web::Json<ReadRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let entity_id = match req.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity ID: {}", e)))
        }
    };

    let mut field_types = Vec::new();
    for field_name in &req.fields {
        match handle.get_field_type(field_name).await {
            Ok(ft) => field_types.push(ft),
            Err(e) => {
                return HttpResponse::BadRequest().json(ApiResponse::<()>::error(
                    format!("Failed to get field type '{}': {:?}", field_name, e),
                ))
            }
        }
    }

    match handle.read(entity_id, &field_types).await {
        Ok((value, timestamp, writer_id)) => {
            let result: serde_json::Value = serde_json::json!({
                "entity_id": req.entity_id,
                "value": format!("{:?}", value),
                "timestamp": timestamp.to_string(),
                "writer_id": writer_id.map(|id| id.0.to_string())
            });
            HttpResponse::Ok().json(ApiResponse::success(result))
        }
        Err(e) => HttpResponse::InternalServerError().json(ApiResponse::<()>::error(
            format!("Failed to read entity: {:?}", e),
        )),
    }
}

pub async fn write(state: web::Data<AppState>, req: web::Json<WriteRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let entity_id = match req.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity ID: {}", e)))
        }
    };

    let field_type = match handle.get_field_type(&req.field).await {
        Ok(ft) => ft,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get field type: {:?}",
                e
            )))
        }
    };

    let value = match json_to_value(&req.value) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid value: {}", e)))
        }
    };

    match handle.write(entity_id, &[field_type], value, None, None, None, None).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "message": "Successfully wrote value"
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to write: {:?}", e))),
    }
}

pub async fn create(state: web::Data<AppState>, req: web::Json<CreateRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let entity_type = match handle.get_entity_type(&req.entity_type).await {
        Ok(et) => et,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get entity type: {:?}",
                e
            )))
        }
    };

    match handle.create_entity(entity_type, None, &req.name).await {
        Ok(entity_id) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_id": entity_id.0.to_string(),
            "entity_type": req.entity_type,
            "name": req.name
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to create entity: {:?}", e))),
    }
}

pub async fn delete(state: web::Data<AppState>, req: web::Json<DeleteRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let entity_id = match req.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity ID: {}", e)))
        }
    };

    match handle.delete_entity(entity_id).await {
        Ok(_) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "message": "Successfully deleted entity"
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to delete entity: {:?}", e))),
    }
}

pub async fn find(state: web::Data<AppState>, req: web::Json<FindRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let entity_type = match handle.get_entity_type(&req.entity_type).await {
        Ok(et) => et,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get entity type: {:?}",
                e
            )))
        }
    };

    let page_opts = if req.page_size.is_some() || req.page_number.is_some() {
        Some(PageOpts {
            limit: req.page_size.unwrap_or(100),
            cursor: req.page_number.map(|n| if n > 0 { n - 1 } else { 0 }),
        })
    } else {
        None
    };

    match handle.find_entities_paginated(entity_type, page_opts.as_ref(), req.filter.as_deref()).await {
        Ok(result) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entities": result.items.iter().map(|id| id.0.to_string()).collect::<Vec<_>>(),
            "total": result.total,
            "next_cursor": result.next_cursor
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to find entities: {:?}", e))),
    }
}

pub async fn schema(state: web::Data<AppState>, req: web::Json<SchemaRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let entity_type = match handle.get_entity_type(&req.entity_type).await {
        Ok(et) => et,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get entity type: {:?}",
                e
            )))
        }
    };

    match handle.get_entity_schema(entity_type).await {
        Ok(schema) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_type": req.entity_type,
            "schema": format!("{:?}", schema)
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to get schema: {:?}", e))),
    }
}

fn json_to_value(json: &serde_json::Value) -> Result<qlib_rs::Value, String> {
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
                Ok(Value::EntityReference(Some(EntityId(id))))
            } else {
                Ok(Value::String(s.clone()))
            }
        }
        serde_json::Value::Array(arr) => {
            let ids: Result<Vec<EntityId>, _> = arr
                .iter()
                .map(|v| {
                    if let serde_json::Value::String(s) = v {
                        s.parse::<u64>().map(EntityId).map_err(|_| "Invalid entity ID")
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
