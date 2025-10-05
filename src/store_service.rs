use qlib_rs::{
    AdjustBehavior, Complete, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, PageOpts,
    PageResult, PushCondition, Result, Single, StoreProxy, Timestamp, Value,
    auth::AuthorizationScope, Notification, NotifyConfig,
};
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
        respond_to: oneshot::Sender<Result<Vec<crate::models::PipelineResult>>>,
    },
    MachineInfo {
        respond_to: oneshot::Sender<Result<String>>,
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
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
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

    pub async fn execute_pipeline(&self, commands: Vec<crate::models::PipelineCommand>) -> Result<Vec<crate::models::PipelineResult>> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::ExecutePipeline {
                commands,
                respond_to: tx,
            })
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service unavailable".to_string()))?;
        rx.await
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
    }

    pub async fn machine_info(&self) -> Result<String> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(StoreCommand::MachineInfo {
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
}

impl StoreService {
    /// Create a new StoreService and return a handle to communicate with it
    pub fn new(proxy: StoreProxy) -> (StoreHandle, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = StoreHandle { sender };
        let auth_config = qlib_rs::AuthConfig::default();
        let permission_cache = create_permission_cache(&proxy);
        let cel_executor = qlib_rs::CelExecutor::new();
        let service = StoreService { proxy, receiver, auth_config, permission_cache, cel_executor };
        (handle, service)
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
                log::warn!("Failed to process notifications: {:?}", e);
            }
            // Sleep for 10ms
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    fn handle_command(&mut self, command: StoreCommand) {
        match command {
            StoreCommand::GetEntityType { name, respond_to } => {
                let result = self.proxy.get_entity_type(&name);
                let _ = respond_to.send(result);
            }
            StoreCommand::ResolveEntityType {
                entity_type,
                respond_to,
            } => {
                let result = self.proxy.resolve_entity_type(entity_type);
                let _ = respond_to.send(result);
            }
            StoreCommand::GetFieldType { name, respond_to } => {
                let result = self.proxy.get_field_type(&name);
                let _ = respond_to.send(result);
            }
            StoreCommand::ResolveFieldType {
                field_type,
                respond_to,
            } => {
                let result = self.proxy.resolve_field_type(field_type);
                let _ = respond_to.send(result);
            }
            StoreCommand::GetEntitySchema {
                entity_type,
                respond_to,
            } => {
                let result = self.proxy.get_entity_schema(entity_type);
                let _ = respond_to.send(result);
            }
            StoreCommand::GetCompleteEntitySchema {
                entity_type,
                respond_to,
            } => {
                let result = self.proxy.get_complete_entity_schema(entity_type);
                let _ = respond_to.send(result);
            }
            StoreCommand::GetFieldSchema {
                entity_type,
                field_type,
                respond_to,
            } => {
                let result = self.proxy.get_field_schema(entity_type, field_type);
                let _ = respond_to.send(result);
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
                let _ = respond_to.send(result);
            }
            StoreCommand::Read {
                entity_id,
                field_path,
                respond_to,
            } => {
                let result = self.proxy.read(entity_id, &field_path);
                let _ = respond_to.send(result);
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
                let _ = respond_to.send(result);
            }
            StoreCommand::CreateEntity {
                entity_type,
                parent_id,
                name,
                respond_to,
            } => {
                let result = self.proxy.create_entity(entity_type, parent_id, &name);
                let _ = respond_to.send(result);
            }
            StoreCommand::DeleteEntity {
                entity_id,
                respond_to,
            } => {
                let result = self.proxy.delete_entity(entity_id);
                let _ = respond_to.send(result);
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
                let _ = respond_to.send(result);
            }
            StoreCommand::FindEntities {
                entity_type,
                filter,
                respond_to,
            } => {
                let result = self.proxy.find_entities(entity_type, filter.as_deref());
                let _ = respond_to.send(result);
            }
            StoreCommand::AuthenticateUser { name, password, respond_to } => {
                let result = qlib_rs::auth::authenticate_user(&mut self.proxy, &name, &password, &self.auth_config);
                let _ = respond_to.send(result);
            }
            StoreCommand::GetScope { subject_id, resource_id, field, respond_to } => {
                if let Some(cache) = &self.permission_cache {
                    let result = qlib_rs::auth::get_scope(&self.proxy, &mut self.cel_executor, cache, subject_id, resource_id, field);
                    let _ = respond_to.send(result);
                } else {
                    let _ = respond_to.send(Err(qlib_rs::Error::StoreProxyError("Permission cache not available".to_string())));
                }
            }
            StoreCommand::RegisterNotification { config, sender, respond_to } => {
                let result = self.proxy.register_notification(config, sender);
                let _ = respond_to.send(result);
            }
            StoreCommand::UnregisterNotification { config, sender, respond_to } => {
                self.proxy.unregister_notification(&config, &sender);
                let _ = respond_to.send(Ok(()));
            }
            StoreCommand::ExecutePipeline { commands, respond_to } => {
                let result = self.execute_pipeline_commands(commands);
                let _ = respond_to.send(result);
            }
            StoreCommand::MachineInfo { respond_to } => {
                let result = self.proxy.machine_info();
                let _ = respond_to.send(result);
            }
        }
    }

    fn execute_pipeline_commands(&mut self, commands: Vec<crate::models::PipelineCommand>) -> Result<Vec<crate::models::PipelineResult>> {
        use crate::models::{PipelineCommand, PipelineResult};
        
        let mut pipeline = self.proxy.pipeline();
        let mut command_types = Vec::new();
        
        // Queue all commands
        for cmd in commands {
            match cmd {
                PipelineCommand::Read { entity_id, fields } => {
                    let entity_id = entity_id.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity ID".to_string()))?;
                    let entity_id = EntityId(entity_id);
                    
                    let mut field_types = Vec::new();
                    for field_name in &fields {
                        let field_type = self.proxy.get_field_type(field_name)?;
                        field_types.push(field_type);
                    }
                    
                    pipeline.read(entity_id, &field_types)?;
                    command_types.push("Read");
                }
                PipelineCommand::Write { entity_id, field, value } => {
                    let entity_id = entity_id.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity ID".to_string()))?;
                    let entity_id = EntityId(entity_id);
                    
                    let field_type = self.proxy.get_field_type(&field)?;
                    let qlib_value = json_to_qlib_value(&value)
                        .map_err(|e| qlib_rs::Error::StoreProxyError(e))?;
                    
                    pipeline.write(entity_id, &[field_type], qlib_value, None, None, None, None)?;
                    command_types.push("Write");
                }
                PipelineCommand::Create { entity_type, name } => {
                    let et = self.proxy.get_entity_type(&entity_type)?;
                    pipeline.create_entity(et, None, &name)?;
                    command_types.push("Create");
                }
                PipelineCommand::Delete { entity_id } => {
                    let entity_id = entity_id.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity ID".to_string()))?;
                    pipeline.delete_entity(EntityId(entity_id))?;
                    command_types.push("Delete");
                }
                PipelineCommand::GetEntityType { name } => {
                    pipeline.get_entity_type(&name)?;
                    command_types.push("GetEntityType");
                }
                PipelineCommand::ResolveEntityType { entity_type } => {
                    let et = entity_type.parse::<u32>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity type".to_string()))?;
                    pipeline.resolve_entity_type(EntityType(et))?;
                    command_types.push("ResolveEntityType");
                }
                PipelineCommand::GetFieldType { name } => {
                    pipeline.get_field_type(&name)?;
                    command_types.push("GetFieldType");
                }
                PipelineCommand::ResolveFieldType { field_type } => {
                    let ft = field_type.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid field type".to_string()))?;
                    pipeline.resolve_field_type(FieldType(ft))?;
                    command_types.push("ResolveFieldType");
                }
                PipelineCommand::EntityExists { entity_id } => {
                    let entity_id = entity_id.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity ID".to_string()))?;
                    pipeline.entity_exists(EntityId(entity_id))?;
                    command_types.push("EntityExists");
                }
                PipelineCommand::FieldExists { entity_type, field_type } => {
                    let et = entity_type.parse::<u32>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity type".to_string()))?;
                    let ft = field_type.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid field type".to_string()))?;
                    pipeline.field_exists(EntityType(et), FieldType(ft))?;
                    command_types.push("FieldExists");
                }
                PipelineCommand::FindEntities { entity_type, filter } => {
                    let et = self.proxy.get_entity_type(&entity_type)?;
                    pipeline.find_entities(et, filter.as_deref())?;
                    command_types.push("FindEntities");
                }
                PipelineCommand::GetEntityTypes => {
                    pipeline.get_entity_types()?;
                    command_types.push("GetEntityTypes");
                }
                PipelineCommand::ResolveIndirection { entity_id, fields } => {
                    let entity_id = entity_id.parse::<u64>()
                        .map_err(|_| qlib_rs::Error::StoreProxyError("Invalid entity ID".to_string()))?;
                    let entity_id = EntityId(entity_id);
                    
                    let mut field_types = Vec::new();
                    for field_name in &fields {
                        let field_type = self.proxy.get_field_type(field_name)?;
                        field_types.push(field_type);
                    }
                    
                    // Note: Pipeline doesn't have resolve_indirection yet, we need to handle this differently
                    // For now, execute it immediately
                    let result = self.proxy.resolve_indirection(entity_id, &field_types);
                    return Ok(vec![match result {
                        Ok((eid, ft)) => PipelineResult::ResolveIndirection {
                            entity_id: eid.0.to_string(),
                            field_type: ft.0.to_string(),
                        },
                        Err(e) => PipelineResult::Error { message: format!("{:?}", e) },
                    }]);
                }
            }
        }
        
        // Execute pipeline
        let results = pipeline.execute()?;
        
        // Convert results to PipelineResult
        let mut output = Vec::new();
        for (i, cmd_type) in command_types.iter().enumerate() {
            let result = match *cmd_type {
                "Read" => {
                    let (value, timestamp, writer_id): (Value, Timestamp, Option<EntityId>) = results.get(i)?;
                    PipelineResult::Read {
                        value: value_to_json_value(&value),
                        timestamp: timestamp.to_string(),
                        writer_id: writer_id.map(|id| id.0.to_string()),
                    }
                }
                "Write" => {
                    let _: () = results.get(i)?;
                    PipelineResult::Write
                }
                "Create" => {
                    let entity_id: EntityId = results.get(i)?;
                    PipelineResult::Create {
                        entity_id: entity_id.0.to_string(),
                    }
                }
                "Delete" => {
                    let _: () = results.get(i)?;
                    PipelineResult::Delete
                }
                "GetEntityType" => {
                    let entity_type: EntityType = results.get(i)?;
                    PipelineResult::GetEntityType {
                        entity_type: entity_type.0.to_string(),
                    }
                }
                "ResolveEntityType" => {
                    let name: String = results.get(i)?;
                    PipelineResult::ResolveEntityType { name }
                }
                "GetFieldType" => {
                    let field_type: FieldType = results.get(i)?;
                    PipelineResult::GetFieldType {
                        field_type: field_type.0.to_string(),
                    }
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
                    PipelineResult::FindEntities {
                        entities: entities.iter().map(|e| e.0.to_string()).collect(),
                    }
                }
                "GetEntityTypes" => {
                    let entity_types: Vec<EntityType> = results.get(i)?;
                    PipelineResult::GetEntityTypes {
                        entity_types: entity_types.iter().map(|et| et.0.to_string()).collect(),
                    }
                }
                _ => PipelineResult::Error { message: "Unknown command type".to_string() },
            };
            output.push(result);
        }
        
        Ok(output)
    }
}

fn json_to_qlib_value(json: &serde_json::Value) -> std::result::Result<Value, String> {
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
            let entity_ids: std::result::Result<Vec<EntityId>, String> = arr.iter().map(|item| {
                if let serde_json::Value::String(s) = item {
                    s.parse::<u64>().map(EntityId).map_err(|e| e.to_string())
                } else if let serde_json::Value::Number(n) = item {
                    n.as_u64().map(EntityId).ok_or_else(|| "Invalid number".to_string())
                } else {
                    Err("Array must contain entity IDs".to_string())
                }
            }).collect();
            Ok(Value::EntityList(entity_ids?))
        }
        serde_json::Value::Null => Ok(Value::EntityReference(None)),
        _ => Err("Unsupported JSON type".to_string()),
    }
}

fn value_to_json_value(value: &Value) -> serde_json::Value {
    match value {
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::json!(f),
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::EntityReference(Some(id)) => serde_json::Value::String(id.0.to_string()),
        Value::EntityReference(None) => serde_json::Value::Null,
        Value::EntityList(list) => {
            serde_json::Value::Array(list.iter().map(|id| serde_json::Value::String(id.0.to_string())).collect())
        }
        Value::Blob(b) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        },
        Value::Choice(c) => serde_json::Value::Number((*c).into()),
        Value::Timestamp(t) => serde_json::Value::String(t.to_string()),
    }
}
