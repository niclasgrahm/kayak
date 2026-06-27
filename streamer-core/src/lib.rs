use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamerDto {
    pub id: String,
    pub config: serde_json::Value,
}
