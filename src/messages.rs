use serde::{Deserialize, Serialize};

use crate::shards::{QueryResults, Shard};

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MessageSearchResponse {
    pub id: String,
    pub payload: QueryResults,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Log(MessageLog),
    SearchRequest(MessageSearchRequest),
    SearchResponse(MessageSearchResponse),
}
