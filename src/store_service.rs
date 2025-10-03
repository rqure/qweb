use qlib_rs::{
    AdjustBehavior, EntityId, EntitySchema, EntityType, FieldSchema, FieldType, PageOpts,
    PageResult, PushCondition, Result, Single, StoreProxy, Timestamp, Value,
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
}

/// Service that wraps StoreProxy and runs in its own task
pub struct StoreService {
    proxy: StoreProxy,
    receiver: mpsc::UnboundedReceiver<StoreCommand>,
}

impl StoreService {
    /// Create a new StoreService and return a handle to communicate with it
    pub fn new(proxy: StoreProxy) -> (StoreHandle, Self) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let handle = StoreHandle { sender };
        let service = StoreService { proxy, receiver };
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
        }
    }
}
