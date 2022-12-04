use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Payload {
    pub id: u32,
    pub data: Vec<u8>,
}
