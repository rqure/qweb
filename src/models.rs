use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReadRequest {
    pub entity_id: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WriteRequest {
    pub entity_id: String,
    pub field: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateRequest {
    pub entity_type: String,
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub entity_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FindRequest {
    pub entity_type: String,
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
pub struct SchemaRequest {
    pub entity_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CompleteSchemaRequest {
    pub entity_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveEntityTypeRequest {
    pub entity_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveFieldTypeRequest {
    pub field_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetFieldSchemaRequest {
    pub entity_type: String,
    pub field_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EntityExistsRequest {
    pub entity_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FieldExistsRequest {
    pub entity_type: String,
    pub field_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResolveIndirectionRequest {
    pub entity_id: String,
    pub fields: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineRequest {
    pub commands: Vec<PipelineCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PipelineCommand {
    Read { entity_id: String, fields: Vec<String> },
    Write { entity_id: String, field: String, value: serde_json::Value },
    Create { entity_type: String, name: String },
    Delete { entity_id: String },
    GetEntityType { name: String },
    ResolveEntityType { entity_type: String },
    GetFieldType { name: String },
    ResolveFieldType { field_type: String },
    EntityExists { entity_id: String },
    FieldExists { entity_type: String, field_type: String },
    FindEntities { entity_type: String, filter: Option<String> },
    GetEntityTypes,
    ResolveIndirection { entity_id: String, fields: Vec<String> },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PipelineResponse {
    pub results: Vec<PipelineResult>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PipelineResult {
    Read { value: serde_json::Value, timestamp: String, writer_id: Option<String> },
    Write,
    Create { entity_id: String },
    Delete,
    GetEntityType { entity_type: String },
    ResolveEntityType { name: String },
    GetFieldType { field_type: String },
    ResolveFieldType { name: String },
    EntityExists { exists: bool },
    FieldExists { exists: bool },
    FindEntities { entities: Vec<String> },
    GetEntityTypes { entity_types: Vec<String> },
    ResolveIndirection { entity_id: String, field_type: String },
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
