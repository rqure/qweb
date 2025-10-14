use qlib_rs::{
    AdjustBehavior, Complete, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, PageOpts,
    PageResult, PushCondition, Result, Single, StoreProxy, Timestamp, Value,
    auth::AuthorizationScope, Notification, NotifyConfig,
};
use qlib_rs::app::ServiceState;
use tokio::sync::{mpsc, oneshot};
use crossbeam::channel::Sender;

/// Commands that can be sent to the StoreService
pub enum StoreCommand {
    GetEntityType {
        name: String,
        respond_to: oneshot::Sender<Result<EntityType>>,
    },
    ResolveEntityType {
        entity_type: EntityType,
        respond_to: oneshot::Sender<Result<String>>,
    },
    GetFieldType {
        name: String,
        respond_to: oneshot::Sender<Result<FieldType>>,
    },
    ResolveFieldType {
        field_type: FieldType,
        respond_to: oneshot::Sender<Result<String>>,
    },
    GetEntitySchema {
        entity_type: EntityType,
        respond_to: oneshot::Sender<Result<EntitySchema<Single>>>,
    },
    GetCompleteEntitySchema {
        entity_type: EntityType,
        respond_to: oneshot::Sender<Result<EntitySchema<Complete>>>,
    },
    GetFieldSchema {
        entity_type: EntityType,
        field_type: FieldType,
        respond_to: oneshot::Sender<Result<FieldSchema>>,
    },
    UpdateSchema {
        entity_type: EntityType,
        inherit: Vec<EntityType>,
        fields: std::collections::HashMap<FieldType, FieldSchema>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    EntityExists {
        entity_id: EntityId,
        respond_to: oneshot::Sender<bool>,
    },
    FieldExists {
        entity_type: EntityType,
        field_type: FieldType,
        respond_to: oneshot::Sender<bool>,
    },
    ResolveIndirection {
        entity_id: EntityId,
        fields: Vec<FieldType>,
        respond_to: oneshot::Sender<Result<(EntityId, FieldType)>>,
    },
    Read {
        entity_id: EntityId,
        field_path: Vec<FieldType>,
        respond_to: oneshot::Sender<Result<(Value, Timestamp, Option<EntityId>)>>,
    },
    Write {
        entity_id: EntityId,
        field_path: Vec<FieldType>,
        value: Value,
        writer_id: Option<EntityId>,
        write_time: Option<Timestamp>,
        push_condition: Option<PushCondition>,
        adjust_behavior: Option<AdjustBehavior>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    CreateEntity {
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: String,
        respond_to: oneshot::Sender<Result<EntityId>>,
    },
    DeleteEntity {
        entity_id: EntityId,
        respond_to: oneshot::Sender<Result<()>>,
    },
    FindEntitiesPaginated {
        entity_type: EntityType,
        page_opts: Option<PageOpts>,
        filter: Option<String>,
        respond_to: oneshot::Sender<Result<PageResult<EntityId>>>,
    },
    FindEntities {
        entity_type: EntityType,
        filter: Option<String>,
        respond_to: oneshot::Sender<Result<Vec<EntityId>>>,
    },
    AuthenticateUser {
        name: String,
        password: String,
        respond_to: oneshot::Sender<Result<EntityId>>,
    },
    GetScope {
        subject_id: EntityId,
        resource_id: EntityId,
        field: FieldType,
        respond_to: oneshot::Sender<Result<AuthorizationScope>>,
    },
    RegisterNotification {
        config: NotifyConfig,
        sender: Sender<Notification>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    UnregisterNotification {
        config: NotifyConfig,
        sender: Sender<Notification>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    ExecutePipeline {
        commands: Vec<crate::models::PipelineCommand>,
        /// The authenticated subject on whose behalf the pipeline is executed.
        /// If None, pipeline write commands will not set a writer id.
        subject_id: Option<EntityId>,
        respond_to: oneshot::Sender<Result<Vec<crate::models::PipelineResult>>>,
    },
}

/// Handle for communicating with the StoreService
#[derive(Clone)]
pub struct StoreHandle {
    sender: mpsc::UnboundedSender<StoreCommand>,
}

impl StoreHandle {
    pub async fn get_entity_type(&self, name: &str) -> Result<EntityType> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::GetEntityType {
                name: name.to_string(),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn resolve_entity_type(&self, entity_type: EntityType) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::ResolveEntityType {
                entity_type,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn get_field_type(&self, name: &str) -> Result<FieldType> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::GetFieldType {
                name: name.to_string(),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn resolve_field_type(&self, field_type: FieldType) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::ResolveFieldType {
                field_type,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn get_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Single>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::GetEntitySchema {
                entity_type,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service dropped".to_string()))?
    }

    pub async fn get_complete_entity_schema(&self, entity_type: EntityType) -> Result<EntitySchema<Complete>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::GetCompleteEntitySchema {
                entity_type,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service dropped".to_string()))?
    }

    pub async fn get_field_schema(
        &self,
        entity_type: EntityType,
        field_type: FieldType,
    ) -> Result<FieldSchema> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::GetFieldSchema {
                entity_type,
                field_type,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?
    }

    pub async fn update_schema(
        &self,
        entity_type: EntityType,
        inherit: Vec<EntityType>,
        fields: std::collections::HashMap<FieldType, FieldSchema>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::UpdateSchema {
                entity_type,
                inherit,
                fields,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?
    }

    pub async fn entity_exists(&self, entity_id: EntityId) -> bool {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(StoreCommand::EntityExists {
                entity_id,
                respond_to: tx,
            })
            .is_err()
        {
            return false;
        }
        rx.await.unwrap_or(false)
    }

    pub async fn field_exists(&self, entity_type: EntityType, field_type: FieldType) -> bool {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(StoreCommand::FieldExists {
                entity_type,
                field_type,
                respond_to: tx,
            })
            .is_err()
        {
            return false;
        }
        rx.await.unwrap_or(false)
    }

    pub async fn resolve_indirection(
        &self,
        entity_id: EntityId,
        fields: &[FieldType],
    ) -> Result<(EntityId, FieldType)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::ResolveIndirection {
                entity_id,
                fields: fields.to_vec(),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn read(
        &self,
        entity_id: EntityId,
        field_path: &[FieldType],
    ) -> Result<(Value, Timestamp, Option<EntityId>)> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::Read {
                entity_id,
                field_path: field_path.to_vec(),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn write(
        &self,
        entity_id: EntityId,
        field_path: &[FieldType],
        value: Value,
        writer_id: Option<EntityId>,
        write_time: Option<Timestamp>,
        push_condition: Option<PushCondition>,
        adjust_behavior: Option<AdjustBehavior>,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::Write {
                entity_id,
                field_path: field_path.to_vec(),
                value,
                writer_id,
                write_time,
                push_condition,
                adjust_behavior,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn create_entity(
        &self,
        entity_type: EntityType,
        parent_id: Option<EntityId>,
        name: &str,
    ) -> Result<EntityId> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::CreateEntity {
                entity_type,
                parent_id,
                name: name.to_string(),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn delete_entity(&self, entity_id: EntityId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::DeleteEntity {
                entity_id,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn find_entities_paginated(
        &self,
        entity_type: EntityType,
        page_opts: Option<&PageOpts>,
        filter: Option<&str>,
    ) -> Result<PageResult<EntityId>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::FindEntitiesPaginated {
                entity_type,
                page_opts: page_opts.cloned(),
                filter: filter.map(|s| s.to_string()),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn find_entities(
        &self,
        entity_type: EntityType,
        filter: Option<&str>,
    ) -> Result<Vec<EntityId>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::FindEntities {
                entity_type,
                filter: filter.map(|s| s.to_string()),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn authenticate_user(&self, name: &str, password: &str) -> Result<EntityId> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::AuthenticateUser {
                name: name.to_string(),
                password: password.to_string(),
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn get_scope(&self, subject_id: EntityId, resource_id: EntityId, field: FieldType) -> Result<AuthorizationScope> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::GetScope {
                subject_id,
                resource_id,
                field,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn register_notification(&self, config: NotifyConfig, sender: Sender<Notification>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::RegisterNotification {
                config,
                sender,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn unregister_notification(&self, config: NotifyConfig, sender: Sender<Notification>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::UnregisterNotification {
                config,
                sender,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn execute_pipeline(&self, commands: Vec<crate::models::PipelineCommand>, subject_id: Option<EntityId>) -> Result<Vec<crate::models::PipelineResult>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::ExecutePipeline {
                commands,
                subject_id,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }
}

/// Create permission cache
fn create_permission_cache(store: &StoreProxy) -> Option<qlib_rs::Cache> {
    // Get values needed for cache creation
    let permission_entity_type = match store.get_entity_type("Permission") {
        Ok(et) => et,
        Err(e) => {
            log::debug!("Failed to get permission entity type: {}", e);
            return None;
        }
    };
    
    let resource_type_field = match store.get_field_type("ResourceType") {
        Ok(ft) => ft,
        Err(e) => {
            log::debug!("Failed to get resource type field: {}", e);
            return None;
        }
    };
    
    let resource_field_field = match store.get_field_type("ResourceField") {
        Ok(ft) => ft,
        Err(e) => {
            log::debug!("Failed to get resource field field: {}", e);
            return None;
        }
    };
    
    let scope_field = match store.get_field_type("Scope") {
        Ok(ft) => ft,
        Err(e) => {
            log::debug!("Failed to get scope field: {}", e);
            return None;
        }
    };
    
    let condition_field = match store.get_field_type("Condition") {
        Ok(ft) => ft,
        Err(e) => {
            log::debug!("Failed to get condition field: {}", e);
            return None;
        }
    };
    
    match qlib_rs::Cache::new(
        store,
        permission_entity_type,
        vec![resource_type_field, resource_field_field],
        vec![scope_field, condition_field]
    ) {
        Ok((cache, _notification_queue)) => {
            log::debug!("Successfully created permission cache");
            Some(cache)
        }
        Err(e) => {
            log::debug!("Failed to create permission cache: {}", e);
            None
        }
    }
}

/// Service that wraps StoreProxy and runs in its own task
pub struct StoreService {
    proxy: StoreProxy,
    receiver: mpsc::UnboundedReceiver<StoreCommand>,
    auth_config: qlib_rs::AuthConfig,
    permission_cache: Option<qlib_rs::Cache>,
    cel_executor: qlib_rs::CelExecutor,
    // Optional ServiceState used to write periodic heartbeats for this service
    service_state: Option<ServiceState>,
}

impl StoreService {
    /// Create a new StoreService and return a handle to communicate with it
    pub fn new(proxy: StoreProxy) -> (StoreHandle, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = StoreHandle { sender };
        let auth_config = qlib_rs::AuthConfig::default();
        // We need a mutable proxy reference to initialize ServiceState, so make proxy mutable here
        let mut proxy = proxy;
        let permission_cache = create_permission_cache(&proxy);
        let cel_executor = qlib_rs::CelExecutor::new();

        // Initialize ServiceState so qweb can write regular heartbeats. This uses the
        // same StoreProxy connection owned by the StoreService.
        let service_name = std::env::var("SERVICE_NAME").unwrap_or_else(|_| "qweb".to_string());
        let heartbeat_interval_msecs: u64 = std::env::var("SERVICE_HEARTBEAT_INTERVAL_MSECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        // ServiceState::new() may call expect() internally and panic if the topology
        // isn't present; protect against that so the whole process doesn't unwind.
        let service_state = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            qlib_rs::app::ServiceState::new(&mut proxy, service_name.clone(), false, heartbeat_interval_msecs)
        })) {
            Ok(Ok(s)) => {
                log::info!("ServiceState initialized (heartbeats enabled, fault_tolerant=false)");
                Some(s)
            }
            Ok(Err(e)) => {
                log::warn!("Failed to initialize ServiceState, service heartbeats disabled: {:?}", e);
                None
            }
            Err(_) => {
                log::warn!("Panic occurred while initializing ServiceState; service heartbeats disabled");
                None
            }
        };

        let service = StoreService { proxy, receiver, auth_config, permission_cache, cel_executor, service_state };
        (handle, service)
    }

    /// Return the EntityId for the initialized Service instance, if available.
    pub fn get_service_id(&self) -> Option<EntityId> {
        self.service_state.as_ref().map(|s| s.service_id)
    }

    fn send_result<T>(&self, result: Result<T>, respond_to: oneshot::Sender<Result<T>>)
    where
        T: Send + 'static,
    {
        if let Err(qlib_rs::Error::ConnectionLost) = &result {
            log::error!("Connection to store lost, exiting");
            std::process::exit(1);
        }
        let _ = respond_to.send(result);
    }

    /// Run the service, processing commands until the channel is closed
    pub async fn run(&mut self) {
        loop {
            // Process all available commands
            while let Ok(command) = self.receiver.try_recv() {
                self.handle_command(command);
            }
            // Process any pending notifications
            if let Err(e) = self.proxy.process_notifications() {
                if let qlib_rs::Error::ConnectionLost = e {
                    log::error!("Connection to store lost, exiting");
                    std::process::exit(1);
                }
                log::warn!("Failed to process notifications: {:?}", e);
            }
            // Tick the ServiceState (handles heartbeat writes). If it reports connection lost,
            // exit similarly to notification processing.
            if let Some(ref mut svc) = self.service_state {
                if let Err(e) = svc.tick(&mut self.proxy) {
                    if let qlib_rs::Error::ConnectionLost = e {
                        log::error!("Connection to store lost during ServiceState tick, exiting");
                        std::process::exit(1);
                    }
                    log::warn!("ServiceState tick failed: {:?}", e);
                }
            }
            // Sleep for 10ms
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    fn handle_command(&mut self, command: StoreCommand) {
        match command {
            StoreCommand::GetEntityType { name, respond_to } => {
                let result = self.proxy.get_entity_type(&name);
                self.send_result(result, respond_to);
            }
            StoreCommand::ResolveEntityType {
                entity_type,
                respond_to,
            } => {
                let result = self.proxy.resolve_entity_type(entity_type);
                self.send_result(result, respond_to);
            }
            StoreCommand::GetFieldType { name, respond_to } => {
                let result = self.proxy.get_field_type(&name);
                self.send_result(result, respond_to);
            }
            StoreCommand::ResolveFieldType {
                field_type,
                respond_to,
            } => {
                let result = self.proxy.resolve_field_type(field_type);
                self.send_result(result, respond_to);
            }
            StoreCommand::GetEntitySchema {
                entity_type,
                respond_to,
            } => {
                let result = self.proxy.get_entity_schema(entity_type);
                self.send_result(result, respond_to);
            }
            StoreCommand::GetCompleteEntitySchema {
                entity_type,
                respond_to,
            } => {
                let result = self.proxy.get_complete_entity_schema(entity_type);
                self.send_result(result, respond_to);
            }
            StoreCommand::GetFieldSchema {
                entity_type,
                field_type,
                respond_to,
            } => {
                let result = self.proxy.get_field_schema(entity_type, field_type);
                self.send_result(result, respond_to);
            }
            StoreCommand::UpdateSchema { entity_type, inherit, fields, respond_to } => {
                // Convert to String-based schema for StoreProxy API
                let entity_type_name = match self.proxy.resolve_entity_type(entity_type) {
                    Ok(name) => name,
                    Err(e) => {
                        self.send_result(Err(e), respond_to);
                        return;
                    }
                };

                let mut inherit_names = Vec::new();
                for et in inherit {
                    match self.proxy.resolve_entity_type(et) {
                        Ok(name) => inherit_names.push(name),
                        Err(e) => {
                            self.send_result(Err(e), respond_to);
                            return;
                        }
                    }
                }

                // Build schema with String types
                let mut schema = qlib_rs::EntitySchema::<qlib_rs::Single, String, String>::new(entity_type_name, inherit_names);
                
                // Convert fields to String-keyed fields by converting each FieldSchema
                for (field_type, field_schema) in fields {
                    let field_name = match self.proxy.resolve_field_type(field_type) {
                        Ok(name) => name,
                        Err(e) => {
                            self.send_result(Err(e), respond_to);
                            return;
                        }
                    };
                    
                    // Convert FieldSchema<FieldType> to FieldSchema<String>
                    let string_field_schema = field_schema.to_string_schema(&self.proxy);
                    schema.fields.insert(field_name, string_field_schema);
                }

                let result = self.proxy.update_schema(schema);
                self.send_result(result, respond_to);
            }
            StoreCommand::EntityExists {
                entity_id,
                respond_to,
            } => {
                let result = self.proxy.entity_exists(entity_id);
                let _ = respond_to.send(result);
            }
            StoreCommand::FieldExists {
                entity_type,
                field_type,
                respond_to,
            } => {
                let result = self.proxy.field_exists(entity_type, field_type);
                let _ = respond_to.send(result);
            }
            StoreCommand::ResolveIndirection {
                entity_id,
                fields,
                respond_to,
            } => {
                let result = self.proxy.resolve_indirection(entity_id, &fields);
                self.send_result(result, respond_to);
            }
            StoreCommand::Read {
                entity_id,
                field_path,
                respond_to,
            } => {
                let result = self.proxy.read(entity_id, &field_path);
                self.send_result(result, respond_to);
            }
            StoreCommand::Write {
                entity_id,
                field_path,
                value,
                writer_id,
                write_time,
                push_condition,
                adjust_behavior,
                respond_to,
            } => {
                let result = self.proxy.write(
                    entity_id,
                    &field_path,
                    value,
                    writer_id,
                    write_time,
                    push_condition,
                    adjust_behavior,
                );
                self.send_result(result, respond_to);
            }
            StoreCommand::CreateEntity {
                entity_type,
                parent_id,
                name,
                respond_to,
            } => {
                let result = self.proxy.create_entity(entity_type, parent_id, &name);
                self.send_result(result, respond_to);
            }
            StoreCommand::DeleteEntity {
                entity_id,
                respond_to,
            } => {
                let result = self.proxy.delete_entity(entity_id);
                self.send_result(result, respond_to);
            }
            StoreCommand::FindEntitiesPaginated {
                entity_type,
                page_opts,
                filter,
                respond_to,
            } => {
                let result = self.proxy.find_entities_paginated(
                    entity_type,
                    page_opts.as_ref(),
                    filter.as_deref(),
                );
                self.send_result(result, respond_to);
            }
            StoreCommand::FindEntities {
                entity_type,
                filter,
                respond_to,
            } => {
                let result = self.proxy.find_entities(entity_type, filter.as_deref());
                self.send_result(result, respond_to);
            }
            StoreCommand::AuthenticateUser { name, password, respond_to } => {
                let result = qlib_rs::auth::authenticate_user(&mut self.proxy, &name, &password, &self.auth_config);
                self.send_result(result, respond_to);
            }
            StoreCommand::GetScope { subject_id, resource_id, field, respond_to } => {
                if let Some(cache) = &self.permission_cache {
                    let result = qlib_rs::auth::get_scope(&self.proxy, &mut self.cel_executor, cache, subject_id, resource_id, field);
                    self.send_result(result, respond_to);
                } else {
                    let _ = respond_to.send(Err(qlib_rs::Error::StoreProxyError("Permission cache not available".to_string())));
                }
            }
            StoreCommand::RegisterNotification { config, sender, respond_to } => {
                let result = self.proxy.register_notification(config, sender);
                self.send_result(result, respond_to);
            }
            StoreCommand::UnregisterNotification { config, sender, respond_to } => {
                self.proxy.unregister_notification(&config, &sender);
                let _ = respond_to.send(Ok(()));
            }
            StoreCommand::ExecutePipeline { commands, subject_id, respond_to } => {
                let result = self.execute_pipeline_commands(commands, subject_id);
                self.send_result(result, respond_to);
            }
        }
    }

    fn execute_pipeline_commands(&mut self, commands: Vec<crate::models::PipelineCommand>, subject_id: Option<EntityId>) -> Result<Vec<crate::models::PipelineResult>> {
        use crate::models::{PipelineCommand, PipelineResult};

        let mut pipeline = self.proxy.pipeline();
        // store tuple of (command type, optional id metadata)
        let mut command_types: Vec<(&str, Option<String>)> = Vec::new();

        // Queue all commands
        for cmd in commands {
            match cmd {
                PipelineCommand::Read { entity_id, fields } => {
                    pipeline.read(entity_id, &fields)?;
                    command_types.push(("Read", None));
                }
                PipelineCommand::Write { entity_id, field, value } => {
                    // Use subject_id as the writer when available
                    pipeline.write(entity_id, &[field], value.clone(), subject_id, None, None, None)?;
                    command_types.push(("Write", None));
                }
                PipelineCommand::Create { entity_type, name } => {
                    pipeline.create_entity(entity_type, None, &name)?;
                    command_types.push(("Create", None));
                }
                PipelineCommand::Delete { entity_id } => {
                    pipeline.delete_entity(entity_id)?;
                    command_types.push(("Delete", None));
                }
                PipelineCommand::GetEntityType { name } => {
                    pipeline.get_entity_type(&name)?;
                    command_types.push(("GetEntityType", None));
                }
                PipelineCommand::ResolveEntityType { entity_type } => {
                    pipeline.resolve_entity_type(entity_type)?;
                    command_types.push(("ResolveEntityType", None));
                }
                PipelineCommand::GetFieldType { name } => {
                    pipeline.get_field_type(&name)?;
                    command_types.push(("GetFieldType", None));
                }
                PipelineCommand::ResolveFieldType { field_type } => {
                    pipeline.resolve_field_type(field_type)?;
                    command_types.push(("ResolveFieldType", None));
                }
                PipelineCommand::EntityExists { entity_id } => {
                    pipeline.entity_exists(entity_id)?;
                    command_types.push(("EntityExists", None));
                }
                PipelineCommand::FieldExists { entity_type, field_type } => {
                    pipeline.field_exists(entity_type, field_type)?;
                    command_types.push(("FieldExists", None));
                }
                PipelineCommand::FindEntities { entity_type, filter } => {
                    pipeline.find_entities(entity_type, filter.as_deref())?;
                    command_types.push(("FindEntities", None));
                }
                PipelineCommand::GetEntityTypes => {
                    pipeline.get_entity_types()?;
                    command_types.push(("GetEntityTypes", None));
                }
                PipelineCommand::ResolveIndirection { entity_id, fields } => {
                    // Pipeline doesn't support resolve_indirection, run it immediately and return
                    let result = self.proxy.resolve_indirection(entity_id, &fields);
                    return Ok(vec![match result {
                        Ok((entity_id, field_type)) => PipelineResult::ResolveIndirection {
                            entity_id,
                            field_type,
                        },
                        Err(e) => PipelineResult::Error { message: format!("{:?}", e) },
                    }]);
                }
            }
        }

        // Execute pipeline
        let results = pipeline.execute()?;

        // Convert results to PipelineResult using the recorded command_types metadata
        let mut output = Vec::new();
        for (i, (cmd_name, _meta)) in command_types.iter().enumerate() {
            let result = match *cmd_name {
                "Read" => {
                    let (value, timestamp, writer_id): (Value, Timestamp, Option<EntityId>) = results.get(i)?;
                    PipelineResult::Read {
                        value,
                        timestamp,
                        writer_id,
                    }
                }
                "Write" => {
                    let _: () = results.get(i)?;
                    PipelineResult::Write
                }
                "Create" => {
                    let entity_id: EntityId = results.get(i)?;
                    PipelineResult::Create { entity_id }
                }
                "Delete" => {
                    let _: () = results.get(i)?;
                    PipelineResult::Delete
                }
                "GetEntityType" => {
                    let entity_type: EntityType = results.get(i)?;
                    PipelineResult::GetEntityType { entity_type }
                }
                "ResolveEntityType" => {
                    let name: String = results.get(i)?;
                    PipelineResult::ResolveEntityType { name }
                }
                "GetFieldType" => {
                    let field_type: FieldType = results.get(i)?;
                    PipelineResult::GetFieldType { field_type }
                }
                "ResolveFieldType" => {
                    let name: String = results.get(i)?;
                    PipelineResult::ResolveFieldType { name }
                }
                "EntityExists" => {
                    let exists: bool = results.get(i)?;
                    PipelineResult::EntityExists { exists }
                }
                "FieldExists" => {
                    let exists: bool = results.get(i)?;
                    PipelineResult::FieldExists { exists }
                }
                "FindEntities" => {
                    let entities: Vec<EntityId> = results.get(i)?;
                    PipelineResult::FindEntities { entities }
                }
                "GetEntityTypes" => {
                    let entity_types: Vec<EntityType> = results.get(i)?;
                    PipelineResult::GetEntityTypes { entity_types }
                }
                _ => PipelineResult::Error { message: "Unknown command type".to_string() },
            };
            output.push(result);
        }

        Ok(output)
    }
}


