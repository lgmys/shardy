use serde::{Deserialize, Serialize};

use crate::shards::Shard;

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageLog {
    pub log: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageSearchRequest {
    pub query: String,
    pub id: String,
    pub shard: Shard,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageSearchResponse {
    pub id: String,
    pub payload: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Log(MessageLog),
    SearchRequest(MessageSearchRequest),
    SearchResponse(MessageSearchResponse),
}
