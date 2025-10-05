use log::{error, info};
use qlib_rs::{NotifyConfig, Value};
use std::collections::HashMap;
use std::time::Duration;
use crossbeam::channel;

use crate::store_service::StoreHandle;

/// Periodic task to clean up expired sessions using notifications
pub async fn session_cleanup_task(handle: StoreHandle) {
    info!("Starting session cleanup task");
    
    // Get field types
    let session_entity_type = match handle.get_entity_type("Session").await {
        Ok(et) => et,
        Err(e) => {
            error!("Failed to get Session entity type: {:?}", e);
            return;
        }
    };
    
    let current_user_ft = match handle.get_field_type("CurrentUser").await {
        Ok(ft) => ft,
        Err(e) => {
            error!("Failed to get CurrentUser field type: {:?}", e);
            return;
        }
    };
    
    let previous_user_ft = match handle.get_field_type("PreviousUser").await {
        Ok(ft) => ft,
        Err(e) => {
            error!("Failed to get PreviousUser field type: {:?}", e);
            return;
        }
    };
    
    let expires_at_ft = match handle.get_field_type("ExpiresAt").await {
        Ok(ft) => ft,
        Err(e) => {
            error!("Failed to get ExpiresAt field type: {:?}", e);
            return;
        }
    };
    
    let token_ft = match handle.get_field_type("Token").await {
        Ok(ft) => ft,
        Err(e) => {
            error!("Failed to get Token field type: {:?}", e);
            return;
        }
    };
    
    // Track session expiration times
    // Map: session_id -> (expiration_timestamp, current_user)
    let mut session_expirations: HashMap<qlib_rs::EntityId, (qlib_rs::Timestamp, Option<qlib_rs::EntityId>)> = HashMap::new();
    
    // Register for ExpiresAt field notifications on all Session entities
    // We'll include CurrentUser in the context to track who's using the session
    let (notification_sender, notification_receiver) = channel::unbounded::<qlib_rs::Notification>();
    let notify_config = NotifyConfig::EntityType {
        entity_type: session_entity_type,
        field_type: expires_at_ft,
        trigger_on_change: true,
        context: vec![
            vec![current_user_ft],
        ],
    };
    
    if let Err(e) = handle.register_notification(notify_config, notification_sender).await {
        error!("Failed to register session notification: {:?}", e);
        return;
    }
    
    info!("Session cleanup task registered for notifications");
    
    // Initial load: read all sessions and their expiration times
    let sessions = match handle.find_entities(session_entity_type, None).await {
        Ok(entities) => entities,
        Err(e) => {
            error!("Failed to find Sessions for initial load: {:?}", e);
            vec![]
        }
    };
    
    for session_id in sessions {
        match handle.read(session_id, &[expires_at_ft]).await {
            Ok((Value::Timestamp(expires_at), _, _)) => {
                match handle.read(session_id, &[current_user_ft]).await {
                    Ok((Value::EntityReference(user_id), _, _)) => {
                        session_expirations.insert(session_id, (expires_at, user_id));
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    
    info!("Loaded {} sessions for expiration tracking", session_expirations.len());
    
    loop {
        // Check for expirations every 1 second
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        // Process all pending notifications to update our expiration tracking
        while let Ok(notification) = notification_receiver.try_recv() {
            let session_id = notification.current.entity_id;
            
            // Extract ExpiresAt from the notification
            if let Some(Value::Timestamp(expires_at)) = &notification.current.value {
                // Extract CurrentUser from context
                let current_user = notification.context.get(&vec![current_user_ft])
                    .and_then(|info| {
                        if let Some(Value::EntityReference(user)) = &info.value {
                            user.clone()
                        } else {
                            None
                        }
                    });
                
                // Update our tracking map
                session_expirations.insert(session_id, (*expires_at, current_user));
            }
        }
        
        // Check all tracked sessions for expiration
        let now = qlib_rs::now();
        let mut expired_sessions = Vec::new();
        
        for (session_id, (expires_at, current_user)) in &session_expirations {
            // Only check sessions that are in use (have a CurrentUser)
            if current_user.is_some() && now > *expires_at {
                expired_sessions.push(*session_id);
            }
        }
        
        // Clean up expired sessions
        for session_id in expired_sessions {
            // Remove from tracking map
            if let Some((_, user_id)) = session_expirations.remove(&session_id) {
                info!("Cleaning up expired session: {:?} (user: {:?})", session_id, user_id);
                
                // Spawn cleanup task
                tokio::spawn(cleanup_expired_session(
                    handle.clone(),
                    session_id,
                    current_user_ft,
                    previous_user_ft,
                    expires_at_ft,
                    token_ft,
                ));
            }
        }
    }
}

/// Helper function to check and cleanup a single expired session
async fn cleanup_expired_session(
    handle: StoreHandle,
    session_id: qlib_rs::EntityId,
    current_user_ft: qlib_rs::FieldType,
    previous_user_ft: qlib_rs::FieldType,
    expires_at_ft: qlib_rs::FieldType,
    token_ft: qlib_rs::FieldType,
) {
    // Read CurrentUser and ExpiresAt
    let (current_user, _, _) = match handle.read(session_id, &[current_user_ft]).await {
        Ok(result) => result,
        Err(_) => return,
    };
    
    let (expires_at, _, _) = match handle.read(session_id, &[expires_at_ft]).await {
        Ok(result) => result,
        Err(_) => return,
    };
    
    // Skip if session is not in use
    if let Value::EntityReference(None) = current_user {
        return;
    }
    
    // Check if expired
    if let Value::Timestamp(expiration) = expires_at {
        let now = qlib_rs::now();
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
                return;
            }
            
            // Clear token
            if let Err(e) = handle.write(session_id, &[token_ft], Value::String("".to_string()), None, None, None, None).await {
                error!("Failed to clear token during cleanup: {:?}", e);
            }
            
            // Reset expiration
            if let Err(e) = handle.write(session_id, &[expires_at_ft], Value::Timestamp(qlib_rs::epoch()), None, None, None, None).await {
                error!("Failed to reset expiration during cleanup: {:?}", e);
            }
        }
    }
}
