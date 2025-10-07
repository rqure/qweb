use serde::{Deserialize, Serialize};
use qlib_rs::{EntityId, EntityType, FieldType, Value};

// API Request models - use qlib_rs types directly
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRequest {
    pub entity_id: EntityId,
    pub fields: Vec<FieldType>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRequest {
    pub entity_id: EntityId,
    pub field: FieldType,
    pub value: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRequest {
    pub entity_type: EntityType,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub entity_id: EntityId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FindRequest {
    pub entity_type: EntityType,
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
    pub entity_type: EntityType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteSchemaRequest {
    pub entity_type: EntityType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetEntityTypeRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetFieldTypeRequest {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveEntityTypeRequest {
    pub entity_type: EntityType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveFieldTypeRequest {
    pub field_type: FieldType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetFieldSchemaRequest {
    pub entity_type: EntityType,
    pub field_type: FieldType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntityExistsRequest {
    pub entity_id: EntityId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldExistsRequest {
    pub entity_type: EntityType,
    pub field_type: FieldType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveIndirectionRequest {
    pub entity_id: EntityId,
    pub fields: Vec<FieldType>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineRequest {
    pub commands: Vec<PipelineCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PipelineCommand {
    Read { entity_id: EntityId, fields: Vec<FieldType> },
    Write { entity_id: EntityId, field: FieldType, value: Value },
    Create { entity_type: EntityType, name: String },
    Delete { entity_id: EntityId },
    GetEntityType { name: String },
    ResolveEntityType { entity_type: EntityType },
    GetFieldType { name: String },
    ResolveFieldType { field_type: FieldType },
    EntityExists { entity_id: EntityId },
    FieldExists { entity_type: EntityType, field_type: FieldType },
    FindEntities { entity_type: EntityType, filter: Option<String> },
    GetEntityTypes,
    ResolveIndirection { entity_id: EntityId, fields: Vec<FieldType> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineResponse {
    pub results: Vec<PipelineResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PipelineResult {
    Read { value: Value, timestamp: qlib_rs::Timestamp, writer_id: Option<EntityId> },
    Write,
    Create { entity_id: EntityId },
    Delete,
    GetEntityType { entity_type: EntityType },
    ResolveEntityType { name: String },
    GetFieldType { field_type: FieldType },
    ResolveFieldType { name: String },
    EntityExists { exists: bool },
    FieldExists { exists: bool },
    FindEntities { entities: Vec<EntityId> },
    GetEntityTypes { entity_types: Vec<EntityType> },
    ResolveIndirection { entity_id: EntityId, field_type: FieldType },
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
// Notification models - these are kept for frontend serialization compatibility
// but can be replaced with qlib_rs types in the future
// ---------------------------------------------------------------------------
