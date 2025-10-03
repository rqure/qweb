use qlib_rs::{
    AdjustBehavior, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, PageOpts,
    PageResult, PushCondition, Result, Single, StoreProxy, Timestamp, Value,
    auth::AuthorizationScope,
};
use tokio::sync::{mpsc, oneshot};

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
            .map_err(|_| qlib_rs::Error::StoreProxyError("Service closed".to_string()))?
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
    pub async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            self.handle_command(command);
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
        }
    }
}
