use crossbeam::channel::{unbounded as crossbeam_unbounded, Receiver, Sender};
use qlib_rs::{EntityId, EntityType, FieldType, NotifyConfig, Result, Value};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use crate::models::PipelineCommand;
use crate::store_service::StoreHandle;

struct ET {
    pub session_controller: EntityType,
}

struct FT {
    pub request_login_user: FieldType,
    pub response_login_user: FieldType,
    pub response_login_session: FieldType,
    pub request_logout_user: FieldType,
    pub response_logout_user: FieldType,
    pub response_logout_session: FieldType,
    pub request_refresh_user: FieldType,
    pub response_refresh_user: FieldType,
    pub response_refresh_session: FieldType,
    pub token: FieldType,
    pub jwt_secret: FieldType,
}

pub enum SessionCommand {
    Login {
        user_id: EntityId,
        respond_to: tokio::sync::oneshot::Sender<Result<(EntityId, String)>>,
    },
    Logout {
        user_id: EntityId,
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
    },
    RefreshSession {
        user_id: EntityId,
        respond_to: tokio::sync::oneshot::Sender<Result<String>>,
    },
    GetJwtSecret {
        respond_to: tokio::sync::oneshot::Sender<String>,
    },
}

pub struct SessionService {
    store: StoreHandle,
    receiver: mpsc::UnboundedReceiver<SessionCommand>,
    et: ET,
    ft: FT,
    session_controller_id: EntityId,
    notify_sender: Sender<qlib_rs::Notification>,
    notify_receiver: Receiver<qlib_rs::Notification>,
    pending_logins: HashMap<EntityId, oneshot::Sender<Result<(EntityId, String)>>>,
    pending_logouts: HashMap<EntityId, oneshot::Sender<Result<()>>>,
    pending_refreshes: HashMap<EntityId, oneshot::Sender<Result<String>>>,
    jwt_secret_cache: String,
}

#[derive(Clone)]
pub struct SessionHandle {
    sender: tokio::sync::mpsc::UnboundedSender<SessionCommand>,
}

impl SessionHandle {
    pub async fn login(&self, user_id: EntityId) -> Result<(EntityId, String)> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SessionCommand::Login {
                user_id,
                respond_to: sender,
            })
            .map_err(|_| qlib_rs::Error::InvalidRequest("Failed to send login command".to_string()))?;
        receiver
            .await
            .map_err(|_| qlib_rs::Error::InvalidRequest("Failed to receive login response".to_string()))?
    }

    pub async fn logout(&self, user_id: EntityId) -> Result<()> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SessionCommand::Logout {
                user_id,
                respond_to: sender,
            })
            .map_err(|_| qlib_rs::Error::InvalidRequest("Failed to send logout command".to_string()))?;
        receiver
            .await
            .map_err(|_| qlib_rs::Error::InvalidRequest("Failed to receive logout response".to_string()))?
    }

    pub async fn refresh_session(&self, user_id: EntityId) -> Result<String> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SessionCommand::RefreshSession {
                user_id,
                respond_to: sender,
            })
            .map_err(|_| qlib_rs::Error::InvalidRequest("Failed to send refresh command".to_string()))?;
        receiver
            .await
            .map_err(|_| qlib_rs::Error::InvalidRequest("Failed to receive refresh response".to_string()))?
    }

    pub async fn get_jwt_secret(&self) -> String {
        let (sender, receiver) = oneshot::channel();
        // If send fails, return default (service probably shut down)
        if self.sender.send(SessionCommand::GetJwtSecret { respond_to: sender }).is_err() {
            return "default_jwt_secret_change_in_production".to_string();
        }
        // If receive fails, return default
        receiver.await.unwrap_or_else(|_| "default_jwt_secret_change_in_production".to_string())
    }
}

impl SessionService {
    pub fn new(store: StoreHandle) -> (SessionHandle, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let (notify_sender, notify_receiver) = crossbeam_unbounded();
        let service = Self {
            store,
            receiver,
            et: ET {
                session_controller: EntityType(0),
            }, // placeholder
            ft: FT {
                request_login_user: FieldType(0),
                response_login_user: FieldType(0),
                response_login_session: FieldType(0),
                request_logout_user: FieldType(0),
                response_logout_user: FieldType(0),
                response_logout_session: FieldType(0),
                request_refresh_user: FieldType(0),
                response_refresh_user: FieldType(0),
                response_refresh_session: FieldType(0),
                token: FieldType(0),
                jwt_secret: FieldType(0),
            }, // placeholder
            session_controller_id: EntityId(0), // placeholder
            notify_sender,
            notify_receiver,
            pending_logins: HashMap::new(),
            pending_logouts: HashMap::new(),
            pending_refreshes: HashMap::new(),
            jwt_secret_cache: "default_jwt_secret_change_in_production".to_string(),
        };
        (SessionHandle { sender }, service)
    }

    pub async fn run(&mut self) {
        // Initialize ET and FT
        self.et = ET {
            session_controller: self
                .store
                .get_entity_type("SessionController")
                .await
                .unwrap(),
        };
        self.ft = FT {
            request_login_user: self.store.get_field_type("RequestLoginUser").await.unwrap(),
            response_login_user: self
                .store
                .get_field_type("ResponseLoginUser")
                .await
                .unwrap(),
            response_login_session: self
                .store
                .get_field_type("ResponseLoginSession")
                .await
                .unwrap(),
            request_logout_user: self
                .store
                .get_field_type("RequestLogoutUser")
                .await
                .unwrap(),
            response_logout_user: self
                .store
                .get_field_type("ResponseLogoutUser")
                .await
                .unwrap(),
            response_logout_session: self
                .store
                .get_field_type("ResponseLogoutSession")
                .await
                .unwrap(),
            request_refresh_user: self
                .store
                .get_field_type("RequestRefreshUser")
                .await
                .unwrap(),
            response_refresh_user: self
                .store
                .get_field_type("ResponseRefreshUser")
                .await
                .unwrap(),
            response_refresh_session: self
                .store
                .get_field_type("ResponseRefreshSession")
                .await
                .unwrap(),
            token: self.store.get_field_type("Token").await.unwrap(),
            jwt_secret: self.store.get_field_type("JWTSecret").await.unwrap(),
        };

        // Find SessionController entity
        let controllers = self
            .store
            .find_entities(self.et.session_controller, None)
            .await
            .unwrap();
        self.session_controller_id = controllers[0];

        // Load initial JWT secret
        match self.store.read(self.session_controller_id, &[self.ft.jwt_secret]).await {
            Ok((Value::String(secret), _, _)) => {
                self.jwt_secret_cache = secret;
                log::info!("Loaded JWT secret from SessionController");
            }
            _ => {
                log::warn!("Failed to read JWT secret, using default");
            }
        }

        // Register notification for JWT secret changes
        self.store
            .register_notification(
                NotifyConfig::EntityId {
                    entity_id: self.session_controller_id,
                    field_type: self.ft.jwt_secret,
                    trigger_on_change: true,
                    context: vec![],
                },
                self.notify_sender.clone(),
            )
            .await
            .unwrap();

        // Register notifications
        self.store
            .register_notification(
                NotifyConfig::EntityType {
                    entity_type: self.et.session_controller,
                    field_type: self.ft.response_login_session,
                    trigger_on_change: false,
                    context: vec![vec![self.ft.response_login_user], vec![self.ft.response_login_session, self.ft.token]],
                },
                self.notify_sender.clone(),
            )
            .await
            .unwrap();

        self.store
            .register_notification(
                NotifyConfig::EntityType {
                    entity_type: self.et.session_controller,
                    field_type: self.ft.response_logout_session,
                    trigger_on_change: false,
                    context: vec![vec![self.ft.response_logout_user]],
                },
                self.notify_sender.clone(),
            )
            .await
            .unwrap();

        self.store
            .register_notification(
                NotifyConfig::EntityType {
                    entity_type: self.et.session_controller,
                    field_type: self.ft.response_refresh_session,
                    trigger_on_change: false,
                    context: vec![vec![self.ft.response_refresh_user], vec![self.ft.response_refresh_session, self.ft.token]],
                },
                self.notify_sender.clone(),
            )
            .await
            .unwrap();

        loop {
            // Process all available commands
            while let Ok(command) = self.receiver.try_recv() {
                self.handle_command(command).await;
            }

            // Process notifications
            while let Ok(notification) = self.notify_receiver.try_recv() {
                self.handle_notification(notification);
            }

            // Sleep briefly to prevent busy-waiting
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    async fn handle_command(&mut self, command: SessionCommand) {
        match command {
            SessionCommand::Login {
                user_id,
                respond_to,
            } => {
                let commands = vec![PipelineCommand::Write {
                    entity_id: self.session_controller_id,
                    field: self.ft.request_login_user,
                    value: Value::EntityReference(Some(user_id)),
                }];
                self.store.execute_pipeline(commands, None).await.unwrap();
                self.pending_logins.insert(user_id, respond_to);
            }
            SessionCommand::Logout {
                user_id,
                respond_to,
            } => {
                let commands = vec![PipelineCommand::Write {
                    entity_id: self.session_controller_id,
                    field: self.ft.request_logout_user,
                    value: Value::EntityReference(Some(user_id)),
                }];
                self.store.execute_pipeline(commands, None).await.unwrap();
                self.pending_logouts.insert(user_id, respond_to);
            }
            SessionCommand::RefreshSession {
                user_id,
                respond_to,
            } => {
                let commands = vec![PipelineCommand::Write {
                    entity_id: self.session_controller_id,
                    field: self.ft.request_refresh_user,
                    value: Value::EntityReference(Some(user_id)),
                }];
                self.store.execute_pipeline(commands, None).await.unwrap();
                self.pending_refreshes.insert(user_id, respond_to);
            }
            SessionCommand::GetJwtSecret { respond_to } => {
                let _ = respond_to.send(self.jwt_secret_cache.clone());
            }
        }
    }

    fn handle_notification(&mut self, notification: qlib_rs::Notification) {
        let field = notification.current.field_path[0];
        if field == self.ft.jwt_secret {
            // Update cached JWT secret
            if let Some(Value::String(secret)) = &notification.current.value {
                self.jwt_secret_cache = secret.clone();
                log::info!("JWT secret updated in cache");
            }
        } else if field == self.ft.response_login_session {
            if let Some(value) = &notification.current.value {
                if let Value::EntityReference(Some(session_id)) = value {
                    // Extract user_id from context
                    if let Some(info) = notification.context.get(&vec![self.ft.response_login_user])
                    {
                        if let Some(val) = &info.value {
                            if let Value::EntityReference(Some(user_id)) = val {
                                // Extract token from context via indirection
                                if let Some(token_info) = notification.context.get(&vec![self.ft.response_login_session, self.ft.token]) {
                                    if let Some(Value::String(token)) = &token_info.value {
                                        if let Some(sender) = self.pending_logins.remove(user_id) {
                                            let _ = sender.send(Ok((*session_id, token.clone())));
                                        }
                                    } else {
                                        // Token exists but wrong type
                                        if let Some(sender) = self.pending_logins.remove(user_id) {
                                            let _ = sender.send(Err(qlib_rs::Error::InvalidRequest("Token has invalid type".to_string())));
                                        }
                                    }
                                } else {
                                    // Token not found in context
                                    if let Some(sender) = self.pending_logins.remove(user_id) {
                                        let _ = sender.send(Err(qlib_rs::Error::InvalidRequest("Token not found in notification context".to_string())));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } else if field == self.ft.response_logout_session {
            if let Some(info) = notification
                .context
                .get(&vec![self.ft.response_logout_user])
            {
                if let Some(val) = &info.value {
                    if let Value::EntityReference(Some(user_id)) = val {
                        if let Some(sender) = self.pending_logouts.remove(user_id) {
                            let _ = sender.send(Ok(()));
                        }
                    }
                }
            }
        } else if field == self.ft.response_refresh_session {
            // Extract user_id from context
            if let Some(info) = notification
                .context
                .get(&vec![self.ft.response_refresh_user])
            {
                if let Some(val) = &info.value {
                    if let Value::EntityReference(Some(user_id)) = val {
                        // Extract token from context via indirection
                        if let Some(token_info) = notification.context.get(&vec![self.ft.response_refresh_session, self.ft.token]) {
                            if let Some(Value::String(token)) = &token_info.value {
                                if let Some(sender) = self.pending_refreshes.remove(user_id) {
                                    let _ = sender.send(Ok(token.clone()));
                                }
                            } else {
                                // Token exists but wrong type
                                if let Some(sender) = self.pending_refreshes.remove(user_id) {
                                    let _ = sender.send(Err(qlib_rs::Error::InvalidRequest("Token has invalid type".to_string())));
                                }
                            }
                        } else {
                            // Token not found in context
                            if let Some(sender) = self.pending_refreshes.remove(user_id) {
                                let _ = sender.send(Err(qlib_rs::Error::InvalidRequest("Token not found in notification context".to_string())));
                            }
                        }
                    }
                }
            }
        }
    }
}
