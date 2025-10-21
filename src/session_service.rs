use crossbeam::channel::{unbounded as crossbeam_unbounded, Receiver, Sender};
use qlib_rs::{EntityId, EntityType, FieldType, NotifyConfig, Result, Value};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

use crate::models::PipelineCommand;
use crate::store_service::StoreHandle;

struct ET {
    pub session_controller: EntityType,
    pub session: EntityType,
    pub user: EntityType,
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
}

pub enum SessionCommand {
    Login {
        user_id: EntityId,
        respond_to: tokio::sync::oneshot::Sender<Result<EntityId>>,
    },
    Logout {
        user_id: EntityId,
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
    },
    RefreshSession {
        user_id: EntityId,
        respond_to: tokio::sync::oneshot::Sender<Result<()>>,
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
    pending_logins: HashMap<EntityId, oneshot::Sender<Result<EntityId>>>,
    pending_logouts: HashMap<EntityId, oneshot::Sender<Result<()>>>,
    pending_refreshes: HashMap<EntityId, oneshot::Sender<Result<()>>>,
}

pub struct SessionHandle {
    sender: tokio::sync::mpsc::UnboundedSender<SessionCommand>,
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
                session: EntityType(0),
                user: EntityType(0),
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
            }, // placeholder
            session_controller_id: EntityId(0), // placeholder
            notify_sender,
            notify_receiver,
            pending_logins: HashMap::new(),
            pending_logouts: HashMap::new(),
            pending_refreshes: HashMap::new(),
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
            session: self.store.get_entity_type("Session").await.unwrap(),
            user: self.store.get_entity_type("User").await.unwrap(),
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
        };

        // Find SessionController entity
        let controllers = self
            .store
            .find_entities(self.et.session_controller, None)
            .await
            .unwrap();
        self.session_controller_id = controllers[0];

        // Register notifications
        self.store
            .register_notification(
                NotifyConfig::EntityType {
                    entity_type: self.et.session_controller,
                    field_type: self.ft.response_login_session,
                    trigger_on_change: false,
                    context: vec![vec![self.ft.response_login_user]],
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
                    context: vec![vec![self.ft.response_refresh_user]],
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
        }
    }

    fn handle_notification(&mut self, notification: qlib_rs::Notification) {
        let field = notification.current.field_path[0];
        if field == self.ft.response_login_session {
            if let Some(value) = &notification.current.value {
                if let Value::EntityReference(Some(session_id)) = value {
                    if let Some(info) = notification.context.get(&vec![self.ft.response_login_user])
                    {
                        if let Some(val) = &info.value {
                            if let Value::EntityReference(Some(user_id)) = val {
                                if let Some(sender) = self.pending_logins.remove(user_id) {
                                    let _ = sender.send(Ok(*session_id));
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
            if let Some(info) = notification
                .context
                .get(&vec![self.ft.response_refresh_user])
            {
                if let Some(val) = &info.value {
                    if let Value::EntityReference(Some(user_id)) = val {
                        if let Some(sender) = self.pending_refreshes.remove(user_id) {
                            let _ = sender.send(Ok(()));
                        }
                    }
                }
            }
        }
    }
}
