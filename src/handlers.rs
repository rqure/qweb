use actix_web::{web, HttpResponse, Responder, HttpRequest};
use qlib_rs::{EntityId, PageOpts, EntityType, FieldType};
use qlib_rs::auth::AuthorizationScope;

use crate::models::{
    ApiResponse, CreateRequest, DeleteRequest, FindRequest, ReadRequest,
    SchemaRequest, WriteRequest, LoginRequest, LoginResponse,
    ResolveEntityTypeRequest, ResolveFieldTypeRequest, GetFieldSchemaRequest,
    EntityExistsRequest, FieldExistsRequest, ResolveIndirectionRequest,
};
use crate::AppState;

pub async fn login(state: web::Data<AppState>, req: web::Json<LoginRequest>) -> impl Responder {
    let handle = &state.store_handle;

    match handle.authenticate_user(&req.username, &req.password).await {
        Ok(entity_id) => {
            // Create JWT
            use jsonwebtoken::{encode, Header, EncodingKey};
            let claims = serde_json::json!({
                "sub": entity_id.0.to_string(),
                "exp": (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp() as usize,
            });
            let token = encode(&Header::default(), &claims, &EncodingKey::from_secret(state.jwt_secret.as_bytes())).unwrap();
            HttpResponse::Ok().json(ApiResponse::success(LoginResponse { token }))
        }
        Err(e) => HttpResponse::Unauthorized().json(ApiResponse::<()>::error(format!("Authentication failed: {:?}", e))),
    }
}

pub async fn read(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ReadRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = match body.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_id: {}", e)));
        }
    };

    let mut field_types = Vec::new();
    for field_name in &body.fields {
        match handle.get_field_type(field_name).await {
            Ok(ft) => {
                let scope = match handle.get_scope(subject_id, entity_id, ft).await {
                    Ok(s) => s,
                    Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Authorization check failed: {:?}", e))),
                };
                if scope == AuthorizationScope::None {
                    return HttpResponse::Forbidden().json(ApiResponse::<()>::error("Access denied".to_string()));
                }
                field_types.push(ft);
            }
            Err(e) => {
                return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                    "Failed to get field type for '{}': {:?}",
                    field_name, e
                )));
            }
        }
    }

    match handle.read(entity_id, &field_types).await {
        Ok((value, timestamp, writer_id)) => {
            let result: serde_json::Value = serde_json::json!({
                "value": value,
                "timestamp": timestamp,
                "writer_id": writer_id.map(|id| id.0.to_string())
            });
            HttpResponse::Ok().json(ApiResponse::success(result))
        }
        Err(e) => HttpResponse::InternalServerError().json(ApiResponse::<()>::error(
            format!("Failed to read entity: {:?}", e),
        )),
    }
}

pub async fn write(req: HttpRequest, state: web::Data<AppState>, body: web::Json<WriteRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = match body.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_id: {}", e)));
        }
    };

    let field_type = match handle.get_field_type(&body.field).await {
        Ok(ft) => ft,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get field type: {:?}",
                e
            )));
        }
    };

    let scope = match handle.get_scope(subject_id, entity_id, field_type).await {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Authorization check failed: {:?}", e))),
    };
    if scope != AuthorizationScope::ReadWrite {
        return HttpResponse::Forbidden().json(ApiResponse::<()>::error("Access denied".to_string()));
    }

    let value = match json_to_value(&body.value) {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid value: {}", e)));
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

pub async fn create(req: HttpRequest, state: web::Data<AppState>, body: web::Json<CreateRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = match handle.get_entity_type(&body.entity_type).await {
        Ok(et) => et,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get entity type: {:?}",
                e
            )));
        }
    };

    match handle.create_entity(entity_type, None, &body.name).await {
        Ok(entity_id) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_id": entity_id.0.to_string(),
            "entity_type": body.entity_type,
            "name": body.name
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to create entity: {:?}", e))),
    }
}

pub async fn delete(req: HttpRequest, state: web::Data<AppState>, body: web::Json<DeleteRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = match body.entity_id.parse::<u64>() {
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

pub async fn find(req: HttpRequest, state: web::Data<AppState>, body: web::Json<FindRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = match handle.get_entity_type(&body.entity_type).await {
        Ok(et) => et,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                "Failed to get entity type: {:?}",
                e
            )))
        }
    };

    let page_opts = if body.page_size.is_some() || body.page_number.is_some() {
        Some(PageOpts {
            limit: body.page_size.unwrap_or(100),
            cursor: body.page_number.map(|n| if n > 0 { n - 1 } else { 0 }),
        })
    } else {
        None
    };

    match handle.find_entities_paginated(entity_type, page_opts.as_ref(), body.filter.as_deref()).await {
        Ok(result) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entities": result.items.iter().map(|id| id.0.to_string()).collect::<Vec<_>>(),
            "total": result.total,
            "next_cursor": result.next_cursor
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to find entities: {:?}", e))),
    }
}

pub async fn schema(req: HttpRequest, state: web::Data<AppState>, body: web::Json<SchemaRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = match handle.get_entity_type(&body.entity_type).await {
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
            "entity_type": body.entity_type,
            "schema": format!("{:?}", schema)
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to get schema: {:?}", e))),
    }
}

pub async fn resolve_entity_type(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ResolveEntityTypeRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = match body.entity_type.parse::<u32>() {
        Ok(id) => EntityType(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_type: {}", e)));
        }
    };

    match handle.resolve_entity_type(entity_type).await {
        Ok(name) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_type": body.entity_type,
            "name": name
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to resolve entity type: {:?}", e))),
    }
}

pub async fn resolve_field_type(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ResolveFieldTypeRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let field_type = match body.field_type.parse::<u64>() {
        Ok(id) => FieldType(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid field_type: {}", e)));
        }
    };

    match handle.resolve_field_type(field_type).await {
        Ok(name) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "field_type": body.field_type,
            "name": name
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to resolve field type: {:?}", e))),
    }
}

pub async fn get_field_schema(req: HttpRequest, state: web::Data<AppState>, body: web::Json<GetFieldSchemaRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = match body.entity_type.parse::<u32>() {
        Ok(id) => EntityType(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_type: {}", e)));
        }
    };

    let field_type = match body.field_type.parse::<u64>() {
        Ok(id) => FieldType(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid field_type: {}", e)));
        }
    };

    match handle.get_field_schema(entity_type, field_type).await {
        Ok(schema) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_type": body.entity_type,
            "field_type": body.field_type,
            "schema": format!("{:?}", schema)
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to get field schema: {:?}", e))),
    }
}

pub async fn entity_exists(req: HttpRequest, state: web::Data<AppState>, body: web::Json<EntityExistsRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = match body.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_id: {}", e)));
        }
    };

    let exists = handle.entity_exists(entity_id).await;
    HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
        "entity_id": body.entity_id,
        "exists": exists
    })))
}

pub async fn field_exists(req: HttpRequest, state: web::Data<AppState>, body: web::Json<FieldExistsRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = match body.entity_type.parse::<u32>() {
        Ok(id) => EntityType(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_type: {}", e)));
        }
    };

    let field_type = match body.field_type.parse::<u64>() {
        Ok(id) => FieldType(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid field_type: {}", e)));
        }
    };

    let exists = handle.field_exists(entity_type, field_type).await;
    HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
        "entity_type": body.entity_type,
        "field_type": body.field_type,
        "exists": exists
    })))
}

pub async fn resolve_indirection(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ResolveIndirectionRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = match body.entity_id.parse::<u64>() {
        Ok(id) => EntityId(id),
        Err(e) => {
            return HttpResponse::BadRequest()
                .json(ApiResponse::<()>::error(format!("Invalid entity_id: {}", e)));
        }
    };

    let mut field_types = Vec::new();
    for field_name in &body.fields {
        match handle.get_field_type(field_name).await {
            Ok(ft) => field_types.push(ft),
            Err(e) => {
                return HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!(
                    "Failed to get field type for '{}': {:?}",
                    field_name, e
                )));
            }
        }
    }

    match handle.resolve_indirection(entity_id, &field_types).await {
        Ok((resolved_entity_id, resolved_field_type)) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_id": body.entity_id,
            "fields": body.fields,
            "resolved_entity_id": resolved_entity_id.0.to_string(),
            "resolved_field_type": resolved_field_type.0.to_string()
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to resolve indirection: {:?}", e))),
    }
}

fn json_to_value(json: &serde_json::Value) -> Result<qlib_rs::Value, String> {
    match json {
        serde_json::Value::Null => Ok(qlib_rs::Value::EntityReference(None)),
        serde_json::Value::Bool(b) => Ok(qlib_rs::Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(qlib_rs::Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(qlib_rs::Value::Float(f))
            } else {
                Err("Number out of range".to_string())
            }
        }
        serde_json::Value::String(s) => Ok(qlib_rs::Value::String(s.clone())),
        serde_json::Value::Array(arr) => {
            let mut ids = Vec::new();
            for item in arr {
                if let serde_json::Value::Number(n) = item {
                    if let Some(id) = n.as_u64() {
                        ids.push(qlib_rs::EntityId(id));
                    } else {
                        return Err("Array contains non-integer entity ID".to_string());
                    }
                } else {
                    return Err("Array must contain entity IDs (numbers)".to_string());
                }
            }
            Ok(qlib_rs::Value::EntityList(ids))
        }
        serde_json::Value::Object(_) => Err("Object values not supported".to_string()),
    }
}

fn get_subject_from_request(req: &HttpRequest, jwt_secret: &str) -> Result<EntityId, String> {
    let auth_header = req.headers().get("Authorization").ok_or("No Authorization header")?;

    let auth_str = auth_header.to_str().map_err(|_| "Invalid header")?;

    if !auth_str.starts_with("Bearer ") {
        return Err("Invalid token format".into());
    }

    let token = &auth_str[7..];

    use jsonwebtoken::{decode, Validation, DecodingKey};

    let token_data = decode::<serde_json::Value>(&token, &DecodingKey::from_secret(jwt_secret.as_bytes()), &Validation::default()).map_err(|_| "Invalid token")?;

    let sub = token_data.claims["sub"].as_str().ok_or("No sub")?;

    let entity_id = sub.parse::<u64>().map_err(|_| "Invalid sub")?;

    Ok(EntityId(entity_id))
}
