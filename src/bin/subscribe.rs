
use alloy::{primitives::{keccak256,Address}, providers::{Provider, ProviderBuilder}, rpc::types::Filter, dyn_abi::{DynSolType, DynSolValue}};
use serde_json::Value;
use serde::{Deserialize, Serialize};
use std::{fs, str::FromStr};
use tokio::time::{sleep, Duration};
use rustRelayer::DepositMessage;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,options::*,
    types::FieldTable,
};


static CHAINA_URL: &str = "http://localhost:8545";
const CHECKPOINT_PATH: &str = "data/subscriber_checkpoint.json";


const RABBITMQ_URL: &str = "amqp://guest:guest@localhost:5672";
const QUEUE_NAME: &str = "deposit_events";

struct ProcessedTransaction{
    block_number: u64,
    tx_hash: String,
    log_index: u64,
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        


#[tokio::main]
async fn main(){

    let conn = Connection::connect(RABBITMQ_URL, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = conn.create_channel()
        .await
        .expect("Failed to create channel");

    channel
        .queue_declare(
            QUEUE_NAME,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    println!("Connected to RabbitMQ, queue '{}'", QUEUE_NAME);
     
    let deposit_abi_path: &str = "../relayerContracts/data/Deposit.abi.json";
    let abiString: String = fs::read_to_string(deposit_abi_path).expect("Can not read Deposit ABI");

    let abiJson: Value = serde_json::from_str(&abiString).expect("Could not turn ABI string to JSON");

     let abi_items: Vec<Value> = match abiJson {
        Value::Array(items) => items,
        other => panic!("ABI JSON is of invalid form"),
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

    let topicToLookFor= keccak256(signature.as_bytes());

    let addressesJson: &str = "../relayerContracts/data/deployments.json";

    let addressesString: String = fs::read_to_string(addressesJson).expect("Deployments Json could not be read");
    
    let addressesJsonVal: Value =serde_json::from_str(&addressesString).expect("Could not parse deployments.json");

    let deposit_addr_str: &str = addressesJsonVal
        .get("deposit")
        .and_then(|v| v.as_str())
        .expect("deployments.json missing string field 'deposit'");

    let deposit_address: Address =
        Address::from_str(deposit_addr_str).expect("Invalid deposit address");

    println!("Deposit address: {:?}", deposit_address);

    let provider = ProviderBuilder::new().connect_http(CHAINA_URL.parse().expect("bad CHAINA_URL"));
   
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
            if log.topics()[0] != topicToLookFor{
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

            let senderBytes =  Address::from_slice(&log.topics()[1].as_slice()[12..]); //first 12 bytes are filler
            let sender = format!("{senderBytes:?}");

            let log_data = log.data().data.as_ref();

            let amount = match DynSolType::String
                .abi_decode(log_data)
                .expect("failed to decode amount from log.data")
            {
                DynSolValue::String(s) => s,
                other => panic!("expected string amount, got {other:?}"),
            };


            

            let messageForIncluder = DepositMessage{
                sender,
                amount,
                block_number,
                tx_hash: format!("{tx_hash:?}"),
                log_index
            };

            let payload = serde_json::to_vec(&messageForIncluder).expect("Payload error");
            
             channel.basic_publish(
                    "",
                    QUEUE_NAME,
                    BasicPublishOptions::default(),
                    payload.as_slice(),
                    BasicProperties::default(),
                )
                .await
                .expect("Message could not be sent");



            println!("Found transaction {}", current_tx.tx_hash);
            last_seen = Some(current_tx);
            

        }
        last_scanned_block = head;

    }


    





    

    


}