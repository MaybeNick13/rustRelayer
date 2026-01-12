use alloy::{
    primitives::{keccak256, Address},
    providers::{Provider, ProviderBuilder},
    rpc::types::Filter,
};
use serde_json::Value;
use std::{fs, str::FromStr};
use tokio::time::{sleep, Duration};

const CHAIN_B_URL: &str = "http://localhost:8546";
const CHECKPOINT_PATH: &str = "data/subscriber_checkpoint.json";


#[tokio::main]
async fn main(){
    let token_abi_path: &str = "../relayerContracts/data/token.abi.json";
    let abi_string: String = fs::read_to_string(token_abi_path).expect("Can not read token ABI");

    let abi_json: Value =
        serde_json::from_str(&abi_string).expect("Could not turn ABI string to JSON");

    let abi_items: Vec<Value> = match abi_json {
        Value::Array(items) => items,
        _other => panic!("ABI JSON is of invalid form"),
    };

    let tokened_event = abi_items
        .iter()
        .find(|item| {
            item.get("type").and_then(|v| v.as_str()) == Some("event")
                && item.get("name").and_then(|v| v.as_str()) == Some("tokened")
        })
        .expect("ABI does not contain event tokened");

    let name = tokened_event
        .get("name")
        .and_then(|v| v.as_str())
        .expect("event ABI missing 'name'");

    let inputs = tokened_event
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

    let token_addr_str: &str = deployments_json
        .get("token")
        .and_then(|v| v.as_str())
        .expect("deployments.json missing string field 'token'");

    let token_address: Address =
        Address::from_str(token_addr_str).expect("Invalid token address");

    println!("token address: {:?}", token_address);

    let provider = ProviderBuilder::new().connect_http(CHAIN_A_URL.parse().expect("bad CHAINA_URL"));
}