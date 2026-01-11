use alloy::{
    primitives::{keccak256, Address},
    providers::{Provider, ProviderBuilder},
    rpc::types::Filter,
};
use serde_json::Value;
use std::{fs, str::FromStr};
use tokio::time::{sleep, Duration};

const CHAIN_A_URL: &str = "http://localhost:8545";
const CHECKPOINT_PATH: &str = "data/subscriber_checkpoint.json";

struct ProcessedTransaction{
    block_number: u64,
    tx_hash: String,
    log_index: u64,
}

#[tokio::main]
async fn main(){
    let deposit_abi_path: &str = "../relayerContracts/data/Deposit.abi.json";
    let abi_string: String = fs::read_to_string(deposit_abi_path).expect("Can not read Deposit ABI");

    let abi_json: Value =
        serde_json::from_str(&abi_string).expect("Could not turn ABI string to JSON");

    let abi_items: Vec<Value> = match abi_json {
        Value::Array(items) => items,
        _other => panic!("ABI JSON is of invalid form"),
    };

    let deposited_event = abi_items
        .iter()
        .find(|item| {
            item.get("type").and_then(|v| v.as_str()) == Some("event")
                && item.get("name").and_then(|v| v.as_str()) == Some("Deposited")
        })
        .expect("ABI does not contain event Deposited");

    let name = deposited_event
        .get("name")
        .and_then(|v| v.as_str())
        .expect("event ABI missing 'name'");

    let inputs = deposited_event
        .get("inputs")
        .and_then(|v| v.as_array())
        .expect("event ABI missing 'inputs' array");

    let types: Vec<&str> = inputs
        .iter()
        .map(|inp| {
            inp.get("type")
                .and_then(|v| v.as_str())
                .expect("event input missing 'type'")
        })
        .collect();

    let signature = format!("{}({})", name, types.join(","));
    println!("Derived event signature: {}", signature);         

    let topic_to_look_for = keccak256(signature.as_bytes());

    let deployments_json_path: &str = "../relayerContracts/data/deployments.json";

    let deployments_json_string: String = fs::read_to_string(deployments_json_path)
        .expect("Deployments Json could not be read");
    
    let deployments_json: Value = serde_json::from_str(&deployments_json_string)
        .expect("Could not parse deployments.json");

    let deposit_addr_str: &str = deployments_json
        .get("deposit")
        .and_then(|v| v.as_str())
        .expect("deployments.json missing string field 'deposit'");

    let deposit_address: Address =
        Address::from_str(deposit_addr_str).expect("Invalid deposit address");

    println!("Deposit address: {:?}", deposit_address);

    let provider = ProviderBuilder::new().connect_http(CHAIN_A_URL.parse().expect("bad CHAINA_URL"));
   
    let mut last_scanned_block: u64 = provider
        .get_block_number()
        .await
        .expect("get_block_number failed");

    println!("Starting at head block {}", last_scanned_block);

    let mut last_seen: Option<ProcessedTransaction> = None;

    loop{
        let head: u64 = match provider.get_block_number().await {
        Ok(h) => h,
        Err(e) => {
            eprintln!("get_block_number error: {e:?}");
            sleep(Duration::from_secs(1)).await;
            continue;
        }
    };

        if head <= last_scanned_block {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let from_block = last_scanned_block + 1;
        let to_block = head;

         let filter = Filter::new()
            .address(deposit_address)
            .from_block(from_block)
            .to_block(to_block);

        let logs = match provider.get_logs(&filter).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("get_logs error: {e}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        for log in logs{
            if log.topics().first() != Some(&topic_to_look_for) {
                println!("Current topic is not of interest");
                continue;
            }

            let Some(block_number) = log.block_number else { continue; };
            let Some(tx_hash) = log.transaction_hash else { continue; };
            let Some(log_index) = log.log_index else { continue; };

            let current_tx = ProcessedTransaction {
                block_number,
                tx_hash: format!("{tx_hash:?}"),
                log_index,
            };

            println!("Found transaction {}", current_tx.tx_hash);
            last_seen = Some(current_tx);
            

        }

    }


    





    

    


}