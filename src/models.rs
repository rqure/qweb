use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntityTypeModel {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FieldTypeModel {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EntityIdModel {
    pub id: String,
    pub entity_type: EntityTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRequest {
    pub entity_id: EntityIdModel,
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRequest {
    pub entity_id: EntityIdModel,
    pub field: FieldTypeModel,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRequest {
    pub entity_type: EntityTypeModel,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub entity_id: EntityIdModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FindRequest {
    pub entity_type: EntityTypeModel,
    pub filter: Option<String>,
    pub page_size: Option<usize>,
    pub page_number: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LoginResponse {
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RefreshRequest {
    // No body needed - token comes from Authorization header
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogoutRequest {
    // No body needed - token comes from Authorization header
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaRequest {
    pub entity_type: EntityTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteSchemaRequest {
    pub entity_type: EntityTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveEntityTypeRequest {
    pub entity_type: EntityTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveFieldTypeRequest {
    pub field_type: FieldTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetFieldSchemaRequest {
    pub entity_type: EntityTypeModel,
    pub field_type: FieldTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntityExistsRequest {
    pub entity_id: EntityIdModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldExistsRequest {
    pub entity_type: EntityTypeModel,
    pub field_type: FieldTypeModel,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveIndirectionRequest {
    pub entity_id: EntityIdModel,
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineRequest {
    pub commands: Vec<PipelineCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PipelineCommand {
    Read { entity_id: EntityIdModel, fields: Vec<String> },
    Write { entity_id: EntityIdModel, field: FieldTypeModel, value: serde_json::Value },
    Create { entity_type: EntityTypeModel, name: String },
    Delete { entity_id: EntityIdModel },
    GetEntityType { name: String },
    ResolveEntityType { entity_type: EntityTypeModel },
    GetFieldType { name: String },
    ResolveFieldType { field_type: FieldTypeModel },
    EntityExists { entity_id: EntityIdModel },
    FieldExists { entity_type: EntityTypeModel, field_type: FieldTypeModel },
    FindEntities { entity_type: EntityTypeModel, filter: Option<String> },
    GetEntityTypes,
    ResolveIndirection { entity_id: EntityIdModel, fields: Vec<String> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineResponse {
    pub results: Vec<PipelineResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PipelineResult {
    Read { value: serde_json::Value, timestamp: String, writer_id: Option<EntityIdModel> },
    Write,
    Create { entity_id: EntityIdModel },
    Delete,
    GetEntityType { entity_type: EntityTypeModel },
    ResolveEntityType { entity_type: EntityTypeModel },
    GetFieldType { field_type: FieldTypeModel },
    ResolveFieldType { field_type: FieldTypeModel },
    EntityExists { exists: bool },
    FieldExists { exists: bool },
    FindEntities { entities: Vec<EntityIdModel> },
    GetEntityTypes { entity_types: Vec<EntityTypeModel> },
    ResolveIndirection { entity_id: EntityIdModel, field_type: FieldTypeModel },
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        ApiResponse {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn error(error: String) -> Self {
        ApiResponse {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}

    // ---------------------------------------------------------------------------
    // Notification models
    // ---------------------------------------------------------------------------

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct NotifyConfigModel {
        pub entity_id: Option<String>,
        pub entity_type: Option<String>,
        pub field: String,
        pub trigger_on_change: bool,
        pub context: Vec<Vec<String>>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct NotifyInfoModel {
        pub entity_id: Option<String>,
        /// Field path represented as a list of field type IDs (as strings) for indirection
        pub field_path: Vec<String>,
        /// Value serialized the same way qweb sends values (e.g. {"Int": 42} / {"String": "x"})
        pub value: Option<serde_json::Value>,
        /// Timestamp may be represented as either an ISO string or an object with secs/nanos
        pub timestamp: Option<serde_json::Value>,
        pub writer_id: Option<String>,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct NotificationModel {
        pub current: NotifyInfoModel,
        pub previous: NotifyInfoModel,
        /// Context map where the key is a comma-separated list of field type IDs
        pub context: std::collections::BTreeMap<String, NotifyInfoModel>,
        pub config_hash: u64,
        /// Optionally include the original notify config so clients can easily match callbacks
        pub config: Option<NotifyConfigModel>,
    }

    // ---------------------------------------------------------------------------
    // Schema models
    // ---------------------------------------------------------------------------

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct FieldSchemaModel {
        pub field_type: FieldTypeModel,
        pub rank: i64,
        /// Default value encoded as qweb values (variant object form)
        pub default_value: serde_json::Value,
    }

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct EntitySchemaModel {
        pub entity_type: EntityTypeModel,
        pub inherit: Vec<EntityTypeModel>,
        pub fields: Vec<FieldSchemaModel>,
    }
