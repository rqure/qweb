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
pub struct SchemaRequest {
    pub entity_type: String,
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
