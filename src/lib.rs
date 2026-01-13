use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositMessage {
    pub sender: String,
    pub amount: String,
    pub block_number: u64,
    pub tx_hash: String,
    pub log_index: u64,
}