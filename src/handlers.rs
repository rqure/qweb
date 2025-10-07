use actix_web::{web, HttpResponse, Responder, HttpRequest};
use qlib_rs::{EntityId, PageOpts, FieldType};
use qlib_rs::auth::AuthorizationScope;

use crate::models::{
    ApiResponse, CreateRequest, DeleteRequest, FindRequest, ReadRequest,
    SchemaRequest, CompleteSchemaRequest, WriteRequest, LoginRequest, LoginResponse,
    GetEntityTypeRequest, GetFieldTypeRequest, ResolveEntityTypeRequest, ResolveFieldTypeRequest, GetFieldSchemaRequest,
    EntityExistsRequest, FieldExistsRequest, ResolveIndirectionRequest, RefreshRequest,
    LogoutRequest,
};
use crate::AppState;

pub async fn login(state: web::Data<AppState>, req: web::Json<LoginRequest>) -> impl Responder {
    let handle = &state.store_handle;

    // Authenticate the user
    let user_id = match handle.authenticate_user(&req.username, &req.password).await {
        Ok(entity_id) => entity_id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(format!("Authentication failed: {:?}", e))),
    };

    // Find Session entity type
    let session_entity_type = match handle.get_entity_type("Session").await {
        Ok(et) => et,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get Session entity type: {:?}", e))),
    };

    // Get field types
    let current_user_field_type = match handle.get_field_type("CurrentUser").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get CurrentUser field type: {:?}", e))),
    };

    let previous_user_field_type = match handle.get_field_type("PreviousUser").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get PreviousUser field type: {:?}", e))),
    };

    let token_field_type = match handle.get_field_type("Token").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get Token field type: {:?}", e))),
    };

    let expires_at_field_type = match handle.get_field_type("ExpiresAt").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get ExpiresAt field type: {:?}", e))),
    };

    let created_at_field_type = match handle.get_field_type("CreatedAt").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get CreatedAt field type: {:?}", e))),
    };

    // Find an available Session owned by this qweb instance (CurrentUser field is None and Parent matches this qweb service)
    let filter = format!("CurrentUser == 0 && Parent == {}", state.qweb_service_id.0);
    let available_sessions = match handle.find_entities(session_entity_type, Some(&filter)).await {
        Ok(entities) => entities,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to find available sessions: {:?}", e))),
    };

    log::info!("Login attempt - qweb_service_id: {:?}, filter: '{}', found {} sessions", state.qweb_service_id, filter, available_sessions.len());
    if !available_sessions.is_empty() {
        log::info!("Available session IDs: {:?}", available_sessions.iter().map(|id| id.0).collect::<Vec<_>>());
    }

    let session_id = if let Some(&id) = available_sessions.first() {
        id
    } else {
        return HttpResponse::ServiceUnavailable().json(ApiResponse::<()>::error("No available sessions. Maximum concurrent users reached.".to_string()))
    };

    // Store PreviousUser before assigning new user
    let previous_user = match handle.read(session_id, &[current_user_field_type]).await {
        Ok((qlib_rs::Value::EntityReference(prev_user), _, _)) => prev_user,
        _ => None,
    };

    if let Some(prev_user_id) = previous_user {
        if let Err(e) = handle.write(session_id, &[previous_user_field_type], qlib_rs::Value::EntityReference(Some(prev_user_id)), None, None, None, None).await {
            return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to store previous user: {:?}", e)));
        }
    }

    // Create JWT with session_id in claims - 1 minute timeout
    use jsonwebtoken::{encode, Header, EncodingKey};
    let expiration = chrono::Utc::now() + chrono::Duration::minutes(1);
    let claims = serde_json::json!({
        "sub": user_id.0.to_string(),
        "session_id": session_id.0.to_string(),
        "exp": expiration.timestamp() as usize,
    });
    let token = match encode(&Header::default(), &claims, &EncodingKey::from_secret(state.jwt_secret.as_bytes())) {
        Ok(t) => t,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to generate token: {:?}", e))),
    };

    // Read PreviousUser to preserve it
    let previous_user_value = match handle.read(session_id, &[previous_user_field_type]).await {
        Ok((value, _, _)) => value,
        Err(_) => qlib_rs::Value::EntityReference(None),
    };

    // Update Session entity with CurrentUser reference, preserve or set PreviousUser, token, and timestamps
    if let Err(e) = handle.write(session_id, &[current_user_field_type], qlib_rs::Value::EntityReference(Some(user_id)), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to assign user to session: {:?}", e)));
    }

    // Set PreviousUser to the previous CurrentUser if there was one
    let prev_user = match previous_user_value {
        qlib_rs::Value::EntityReference(Some(prev_id)) => Some(prev_id),
        _ => None,
    };
    if let Err(e) = handle.write(session_id, &[previous_user_field_type], qlib_rs::Value::EntityReference(prev_user), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to set previous user: {:?}", e)));
    }

    if let Err(e) = handle.write(session_id, &[token_field_type], qlib_rs::Value::String(token.clone()), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to store token: {:?}", e)));
    }

    let now = qlib_rs::epoch();
    if let Err(e) = handle.write(session_id, &[created_at_field_type], qlib_rs::Value::Timestamp(now), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to store created timestamp: {:?}", e)));
    }

    let expiration_timestamp = qlib_rs::millis_to_timestamp(expiration.timestamp_millis() as u64);
    if let Err(e) = handle.write(session_id, &[expires_at_field_type], qlib_rs::Value::Timestamp(expiration_timestamp), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to store expiration: {:?}", e)));
    }

    HttpResponse::Ok().json(ApiResponse::success(LoginResponse { token }))
}

pub async fn logout(req: HttpRequest, state: web::Data<AppState>, _body: web::Json<LogoutRequest>) -> impl Responder {
    let handle = &state.store_handle;

    // Extract session_id from token
    let session_id = match get_session_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(format!("Invalid token: {}", e))),
    };

    // Get field types
    let current_user_field_type = match handle.get_field_type("CurrentUser").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get CurrentUser field type: {:?}", e))),
    };

    let previous_user_field_type = match handle.get_field_type("PreviousUser").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get PreviousUser field type: {:?}", e))),
    };

    let token_field_type = match handle.get_field_type("Token").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get Token field type: {:?}", e))),
    };

    let expires_at_field_type = match handle.get_field_type("ExpiresAt").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get ExpiresAt field type: {:?}", e))),
    };

    // Save CurrentUser to PreviousUser before logout
    let current_user = match handle.read(session_id, &[current_user_field_type]).await {
        Ok((qlib_rs::Value::EntityReference(user), _, _)) => user,
        _ => None,
    };

    if let Some(user_id) = current_user {
        if let Err(e) = handle.write(session_id, &[previous_user_field_type], qlib_rs::Value::EntityReference(Some(user_id)), None, None, None, None).await {
            return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to save previous user: {:?}", e)));
        }
    }

    // Clear the Session by setting CurrentUser to None, clearing token and resetting expiration
    if let Err(e) = handle.write(session_id, &[current_user_field_type], qlib_rs::Value::EntityReference(None), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to clear session user: {:?}", e)));
    }

    if let Err(e) = handle.write(session_id, &[token_field_type], qlib_rs::Value::String("".to_string()), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to clear token: {:?}", e)));
    }

    if let Err(e) = handle.write(session_id, &[expires_at_field_type], qlib_rs::Value::Timestamp(qlib_rs::epoch()), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to clear expiration: {:?}", e)));
    }

    HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
        "message": "Successfully logged out"
    })))
}

pub async fn refresh(req: HttpRequest, state: web::Data<AppState>, _body: web::Json<RefreshRequest>) -> impl Responder {
    let handle = &state.store_handle;

    // Extract and validate the existing token
    let (user_id, session_id) = match get_subject_and_session_from_request(&req, &state.jwt_secret, handle).await {
        Ok((uid, sid)) => (uid, sid),
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(format!("Invalid token: {}", e))),
    };

    // Verify this qweb instance owns the session
    let parent_field_type = match handle.get_field_type("Parent").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get Parent field type: {:?}", e))),
    };

    match check_session_ownership(handle, session_id, state.qweb_service_id, parent_field_type).await {
        Ok(true) => {}, // Session is owned by this instance
        Ok(false) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error("Session belongs to different qweb instance".to_string())),
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to verify session ownership: {}", e))),
    }

    let token_field_type = match handle.get_field_type("Token").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get Token field type: {:?}", e))),
    };

    let expires_at_field_type = match handle.get_field_type("ExpiresAt").await {
        Ok(ft) => ft,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to get ExpiresAt field type: {:?}", e))),
    };

    // Issue a new token with extended expiration (1 minute)
    use jsonwebtoken::{encode, Header, EncodingKey};
    let expiration = chrono::Utc::now() + chrono::Duration::minutes(1);
    let claims = serde_json::json!({
        "sub": user_id.0.to_string(),
        "session_id": session_id.0.to_string(),
        "exp": expiration.timestamp() as usize,
    });
    
    let token = match encode(&Header::default(), &claims, &EncodingKey::from_secret(state.jwt_secret.as_bytes())) {
        Ok(t) => t,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to generate token: {:?}", e))),
    };

    // Update Session with new token and expiration
    if let Err(e) = handle.write(session_id, &[token_field_type], qlib_rs::Value::String(token.clone()), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to update token: {:?}", e)));
    }

    let expiration_timestamp = qlib_rs::millis_to_timestamp(expiration.timestamp_millis() as u64);
    if let Err(e) = handle.write(session_id, &[expires_at_field_type], qlib_rs::Value::Timestamp(expiration_timestamp), None, None, None, None).await {
        return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Failed to update expiration: {:?}", e)));
    }

    HttpResponse::Ok().json(ApiResponse::success(LoginResponse { token }))
}

pub async fn read(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ReadRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = body.entity_id;

    let mut field_types = Vec::new();
    for ft in &body.fields {
        let scope = match handle.get_scope(subject_id, entity_id, *ft).await {
            Ok(s) => s,
            Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Authorization check failed: {:?}", e))),
        };
        if scope == AuthorizationScope::None {
            return HttpResponse::Forbidden().json(ApiResponse::<()>::error("Access denied".to_string()));
        }
        field_types.push(*ft);
    }

    match handle.read(entity_id, &field_types).await {
        Ok((value, timestamp, writer_id)) => {
            // Serialize value into a JSON-friendly format; convert EntityReference/EntityList to structured objects
            let serialized_value = match value {
                qlib_rs::Value::EntityReference(opt) => match opt {
                    Some(eid) => {
                        let et = eid.extract_type();
                        let et_name = handle.resolve_entity_type(et).await.unwrap_or_else(|_| et.0.to_string());
                        serde_json::json!({"EntityReference": {"id": eid.0.to_string(), "entity_type": {"id": et.0.to_string(), "name": et_name}}})
                    }
                    None => serde_json::json!({"EntityReference": null}),
                },
                qlib_rs::Value::EntityList(list) => {
                    let mut arr = Vec::new();
                    for eid in list.iter() {
                        let et = eid.extract_type();
                        let et_name = handle.resolve_entity_type(et).await.unwrap_or_else(|_| et.0.to_string());
                        arr.push(serde_json::json!({"id": eid.0.to_string(), "entity_type": {"id": et.0.to_string(), "name": et_name}}));
                    }
                    serde_json::json!({"EntityList": arr})
                }
                _ => serde_json::to_value(&value).unwrap_or(serde_json::Value::Null),
            };

            // Resolve writer name and build writer JSON if present
            let writer_json = if let Some(wid) = writer_id {
                let et = wid.extract_type();
                let et_name = match handle.resolve_entity_type(et).await {
                    Ok(n) => n,
                    Err(_) => et.0.to_string(),
                };
                Some(serde_json::json!({"id": wid.0.to_string(), "entity_type": {"id": et.0.to_string(), "name": et_name}}))
            } else { None };

            let result: serde_json::Value = serde_json::json!({
                "value": serialized_value,
                "timestamp": timestamp,
                "writer_id": writer_json
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

    let entity_id = body.entity_id;
    let field_type = body.field;

    let scope = match handle.get_scope(subject_id, entity_id, field_type).await {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Authorization check failed: {:?}", e))),
    };
    if scope != AuthorizationScope::ReadWrite {
        return HttpResponse::Forbidden().json(ApiResponse::<()>::error("Access denied".to_string()));
    }

    let value = body.value.clone();

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

    let subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = body.entity_type;

    let scope = match handle.get_scope(subject_id, EntityId::new(entity_type, 0), FieldType(0)).await {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Authorization check failed: {:?}", e))),
    };

    if scope != AuthorizationScope::ReadWrite {
        return HttpResponse::Forbidden().json(ApiResponse::<()>::error("Access denied".to_string()));
    }

    match handle.create_entity(entity_type, None, &body.name).await {
        Ok(entity_id) => {
            HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
                "entity_id": entity_id,
                "name": body.name
            })))
        }
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to create entity: {:?}", e))),
    }
}

pub async fn delete(req: HttpRequest, state: web::Data<AppState>, body: web::Json<DeleteRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = body.entity_id;

    let scope = match handle.get_scope(subject_id, entity_id, FieldType(0)).await {
        Ok(s) => s,
        Err(e) => return HttpResponse::InternalServerError().json(ApiResponse::<()>::error(format!("Authorization check failed: {:?}", e))),
    };

    if scope != AuthorizationScope::ReadWrite {
        return HttpResponse::Forbidden().json(ApiResponse::<()>::error("Access denied".to_string()));
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

    let entity_type = body.entity_type;

    let page_opts = if body.page_size.is_some() || body.page_number.is_some() {
        Some(PageOpts {
            limit: body.page_size.unwrap_or(100),
            cursor: body.page_number.map(|n| if n > 0 { n - 1 } else { 0 }),
        })
    } else {
        None
    };

    match handle.find_entities_paginated(entity_type, page_opts.as_ref(), body.filter.as_deref()).await {
        Ok(result) => {
            HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
                "entities": result.items,
                "total": result.total,
                "next_cursor": result.next_cursor
            })))
        }
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

    let entity_type = body.entity_type;

    match handle.get_entity_schema(entity_type).await {
        Ok(schema) => {
            HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
                "entity_type": entity_type,
                "schema": schema
            })))
        }
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to get schema: {:?}", e))),
    }
}

pub async fn complete_schema(req: HttpRequest, state: web::Data<AppState>, body: web::Json<CompleteSchemaRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = body.entity_type;

    match handle.get_complete_entity_schema(entity_type).await {
        Ok(schema) => {
            HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
                "entity_type": entity_type,
                "schema": schema
            })))
        }
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to get complete schema: {:?}", e))),
    }
}

pub async fn get_entity_type(req: HttpRequest, state: web::Data<AppState>, body: web::Json<GetEntityTypeRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _user_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(uid) => uid,
        Err(_) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error("Unauthorized".to_string())),
    };

    match handle.get_entity_type(&body.name).await {
        Ok(entity_type) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "entity_type": entity_type
        }))),
        Err(e) => HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!("{:?}", e))),
    }
}

pub async fn resolve_entity_type(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ResolveEntityTypeRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = body.entity_type;

    match handle.resolve_entity_type(entity_type).await {
        Ok(name) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "name": name
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to resolve entity type: {:?}", e))),
    }
}

pub async fn get_field_type(req: HttpRequest, state: web::Data<AppState>, body: web::Json<GetFieldTypeRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _user_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(uid) => uid,
        Err(_) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error("Unauthorized".to_string())),
    };

    match handle.get_field_type(&body.name).await {
        Ok(field_type) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "field_type": field_type
        }))),
        Err(e) => HttpResponse::BadRequest().json(ApiResponse::<()>::error(format!("{:?}", e))),
    }
}

pub async fn resolve_field_type(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ResolveFieldTypeRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let field_type = body.field_type;

    match handle.resolve_field_type(field_type).await {
        Ok(name) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
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

    let entity_type = body.entity_type;
    let field_type = body.field_type;

    match handle.get_field_schema(entity_type, field_type).await {
        Ok(schema) => {
            HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
                "entity_type": entity_type,
                "field_type": field_type,
                "schema": schema
            })))
        }
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

    let entity_id = body.entity_id;

    let exists = handle.entity_exists(entity_id).await;
    HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
        "entity_id": entity_id,
        "exists": exists
    })))
}

pub async fn field_exists(req: HttpRequest, state: web::Data<AppState>, body: web::Json<FieldExistsRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_type = body.entity_type;
    let field_type = body.field_type;

    let exists = handle.field_exists(entity_type, field_type).await;
    HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
        "entity_type": entity_type,
        "field_type": field_type,
        "exists": exists
    })))
}

pub async fn resolve_indirection(req: HttpRequest, state: web::Data<AppState>, body: web::Json<ResolveIndirectionRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => return HttpResponse::Unauthorized().json(ApiResponse::<()>::error(e)),
    };

    let entity_id = body.entity_id;
    let field_types = &body.fields;

    match handle.resolve_indirection(entity_id, field_types).await {
        Ok((resolved_entity_id, resolved_field_type)) => HttpResponse::Ok().json(ApiResponse::success(serde_json::json!({
            "resolved_entity_id": resolved_entity_id,
            "resolved_field_type": resolved_field_type
        }))),
        Err(e) => HttpResponse::InternalServerError()
            .json(ApiResponse::<()>::error(format!("Failed to resolve indirection: {:?}", e))),
    }
}

pub async fn pipeline(req: HttpRequest, state: web::Data<AppState>, body: web::Json<crate::models::PipelineRequest>) -> impl Responder {
    let handle = &state.store_handle;

    let _subject_id = match get_subject_from_request(&req, &state.jwt_secret) {
        Ok(id) => id,
        Err(e) => {
            return HttpResponse::Unauthorized().json(crate::models::ApiResponse::<()>::error(e));
        }
    };

    match handle.execute_pipeline(body.commands.clone()).await {
        Ok(results) => HttpResponse::Ok().json(crate::models::ApiResponse::success(crate::models::PipelineResponse { results })),
        Err(e) => HttpResponse::InternalServerError().json(crate::models::ApiResponse::<()>::error(format!("{:?}", e))),
    }
}

/// Check if a session belongs to this qweb instance
/// Sessions always belong to their parent qweb service (never transfer Parent)
/// Returns true if owned by this instance, false otherwise
async fn check_session_ownership(
    handle: &crate::store_service::StoreHandle,
    session_id: EntityId,
    qweb_service_id: EntityId,
    parent_field_type: FieldType,
) -> Result<bool, String> {
    // Read the session's parent to check ownership
    let (parent_value, _, _) = handle.read(session_id, &[parent_field_type]).await
        .map_err(|e| format!("Failed to read session parent: {:?}", e))?;
    
    if let qlib_rs::Value::EntityReference(Some(parent_id)) = parent_value {
        Ok(parent_id == qweb_service_id)
    } else {
        Err("Session has no parent".to_string())
    }
}

pub fn get_subject_from_request(req: &HttpRequest, jwt_secret: &str) -> Result<EntityId, String> {
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

pub fn get_session_from_request(req: &HttpRequest, jwt_secret: &str) -> Result<EntityId, String> {
    let auth_header = req.headers().get("Authorization").ok_or("No Authorization header")?;

    let auth_str = auth_header.to_str().map_err(|_| "Invalid header")?;

    if !auth_str.starts_with("Bearer ") {
        return Err("Invalid token format".into());
    }

    let token = &auth_str[7..];

    use jsonwebtoken::{decode, Validation, DecodingKey};

    let token_data = decode::<serde_json::Value>(&token, &DecodingKey::from_secret(jwt_secret.as_bytes()), &Validation::default()).map_err(|_| "Invalid token")?;

    let session_id_str = token_data.claims["session_id"].as_str().ok_or("No session_id")?;

    let session_id = session_id_str.parse::<u64>().map_err(|_| "Invalid session_id")?;

    Ok(EntityId(session_id))
}

pub async fn get_subject_and_session_from_request(
    req: &HttpRequest,
    jwt_secret: &str,
    handle: &crate::store_service::StoreHandle,
) -> Result<(EntityId, EntityId), String> {
    // First try to get token from Authorization header
    let token = if let Some(auth_header) = req.headers().get("Authorization") {
        let auth_str = auth_header.to_str().map_err(|_| "Invalid header")?;
        if !auth_str.starts_with("Bearer ") {
            return Err("Invalid token format".into());
        }
        auth_str[7..].to_string()
    } else {
        // For WebSocket connections, try to get token from query parameter
        req.uri().query()
            .and_then(|query| {
                url::form_urlencoded::parse(query.as_bytes())
                    .find(|(key, _)| key == "token")
                    .map(|(_, value)| value.to_string())
            })
            .ok_or("No Authorization header or token query parameter")?
    };

    use jsonwebtoken::{decode, Validation, DecodingKey};

    let token_data = decode::<serde_json::Value>(&token, &DecodingKey::from_secret(jwt_secret.as_bytes()), &Validation::default()).map_err(|_| "Invalid token")?;

    let sub = token_data.claims["sub"].as_str().ok_or("No sub")?;
    let session_id_str = token_data.claims["session_id"].as_str().ok_or("No session_id")?;

    let user_id = EntityId(sub.parse::<u64>().map_err(|_| "Invalid sub")?);
    let session_id = EntityId(session_id_str.parse::<u64>().map_err(|_| "Invalid session_id")?);

    // Verify the Session is still valid
    let current_user_field_type = handle.get_field_type("CurrentUser").await.map_err(|e| format!("Failed to get CurrentUser field type: {:?}", e))?;
    let expires_at_field_type = handle.get_field_type("ExpiresAt").await.map_err(|e| format!("Failed to get ExpiresAt field type: {:?}", e))?;

    // Check that the Session still has the CurrentUser assigned
    let (user_value, _, _) = handle.read(session_id, &[current_user_field_type]).await.map_err(|e| format!("Failed to read session: {:?}", e))?;
    match user_value {
        qlib_rs::Value::EntityReference(Some(stored_user_id)) if stored_user_id == user_id => {},
        _ => return Err("Session is no longer valid".to_string()),
    }

    // Check that the Session hasn't expired
    let (expires_value, _, _) = handle.read(session_id, &[expires_at_field_type]).await.map_err(|e| format!("Failed to read expiration: {:?}", e))?;
    if let qlib_rs::Value::Timestamp(expires_at) = expires_value {
        let now = qlib_rs::now();
        if now > expires_at {
            return Err("Session has expired".to_string());
        }
    } else {
        return Err("Invalid expiration value".to_string());
    }

    Ok((user_id, session_id))
}
