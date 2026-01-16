use alloy::{primitives::{keccak256,Address}, providers::{Provider, ProviderBuilder}, rpc::types::Filter, dyn_abi::{DynSolType, DynSolValue}};
use serde_json::Value;
use std::{env, fs, str::FromStr};
use tokio::time::{sleep, Duration};
use rustRelayer::DepositMessage;
use lapin::{
    BasicProperties, Connection, ConnectionProperties,options::*,
    types::FieldTable,
};
use sqlx::postgres::PgPoolOptions;


struct ProcessedTransaction{
    block_number: u64,
    tx_hash: String,
    log_index: u64,
}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        


#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let service_name = env::var("SERVICE_NAME").expect("SERVICE_NAME not set");
    let database_client = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");

    let chain_a_url = env::var("CHAINA_URL").expect("CHAINA_URL not set");
    let rabbitmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL not set");
    let queue_name = env::var("QUEUE_NAME").expect("QUEUE_NAME not set");
    let _checkpoint_path = env::var("CHECKPOINT_PATH").expect("CHECKPOINT_PATH not set");

    let reorg_value: Option<String> = sqlx::query_scalar(
        "select value from process_settings where setting = 'reorg_buffer_blocks'",
    )
    .fetch_optional(&database_client)
    .await
    .expect("Failed to read process_settings");

    let reorg_buffer_blocks: u64 = reorg_value
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);

    let conn = Connection::connect(&rabbitmq_url, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = conn.create_channel()
        .await
        .expect("Failed to create channel");

    channel
        .queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    println!("Connected to RabbitMQ, queue '{}'", queue_name);
     
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

    let provider = ProviderBuilder::new().connect_http(chain_a_url.parse().expect("bad CHAINA_URL"));
   
    let head_now: u64 = provider
        .get_block_number()
        .await
        .expect("get_block_number failed");

    let saved_last_scanned_block: Option<i64> = sqlx::query_scalar(
        "select last_scanned_block from subscriber_state where service_name = $1",
    )
    .bind(&service_name)
    .fetch_optional(&database_client)
    .await
    .expect("Failed to read subscriber_state");

    let saved_last_scanned_block = saved_last_scanned_block.map(|v| v as u64);

    let mut last_scanned_block = head_now;
    if let Some(saved) = saved_last_scanned_block {
        if saved <= head_now {
            last_scanned_block = saved;
        } else {
            println!("Chain was restarted (cursor ahead of head); starting at head {head_now}");
        }
    } else {
        println!("Chain was restarted (no cursor); starting at head {head_now}");
    }

    last_scanned_block = last_scanned_block.saturating_sub(reorg_buffer_blocks);

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
                sender: sender.clone(),
                amount: amount.clone(),
                block_number,
                tx_hash: format!("{tx_hash:?}"),
                log_index
            };

            let payload_json = sqlx::types::Json(messageForIncluder.clone());
            let rows = sqlx::query(
                "insert into subscriber_published_deposits \
                (source_tx_hash, source_log_index, source_block, sender, amount, queue_name, payload) \
                values ($1,$2,$3,$4,$5,$6,$7) \
                on conflict (source_tx_hash, source_log_index) do nothing",
            )
            .bind(&messageForIncluder.tx_hash)
            .bind(messageForIncluder.log_index as i64)
            .bind(messageForIncluder.block_number as i64)
            .bind(&sender)
            .bind(&amount)
            .bind(&queue_name)
            .bind(payload_json)
            .execute(&database_client)
            .await
            .expect("Failed to insert subscriber_published_deposits")
            .rows_affected();

            let inserted = rows == 1;

            if !inserted {
                continue;
            }

            let payload = serde_json::to_vec(&messageForIncluder).expect("Payload error");
            
             channel.basic_publish(
                    "",
                      &queue_name,
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
        sqlx::query(
            "insert into subscriber_state (service_name, last_scanned_block) values ($1,$2) \
             on conflict (service_name) do update set last_scanned_block = excluded.last_scanned_block, updated_at = now()",
        )
        .bind(&service_name)
        .bind(last_scanned_block as i64)
        .execute(&database_client)
        .await
        .expect("Failed to update subscriber_state");

    }


    





    

    
}