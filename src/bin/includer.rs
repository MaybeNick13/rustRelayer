use alloy::{
    network::EthereumWallet,
    primitives::Address,
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};
use futures_util::StreamExt;
use lapin::{
    options::*,
    types::FieldTable,
    Connection, ConnectionProperties,
};
use rustRelayer::DepositMessage;
use serde_json::Value;
use std::{env, fs, str::FromStr};
use sqlx::postgres::PgPoolOptions;

sol! {
    #[sol(rpc)]
    contract Token {
        function mint(string amount) external;
    }
}


#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let database_client = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to Postgres");


    let chain_b_url = env::var("CHAIN_B_URL").expect("CHAIN_B_URL not set");
    let private_key = env::var("PRIVATE_KEY").expect("PRIVATE_KEY not set");
    let rabbitmq_url = env::var("RABBITMQ_URL").expect("RABBITMQ_URL not set");
    let queue_name = env::var("QUEUE_NAME").expect("QUEUE_NAME not set");

    let conn = Connection::connect(&rabbitmq_url, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = conn.create_channel()
        .await
        .expect("Failed to create channel");

    channel.queue_declare(
            &queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    println!("Connected to RabbitMQ, queue '{}'", queue_name);

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


 
    let signer = PrivateKeySigner::from_str(&private_key).expect("bad PRIVATE_KEY");
    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(chain_b_url.parse().expect("bad CHAIN_B_URL"));
    
    let token = Token::new(token_address, provider.clone());

    let mut consumer = channel
        .basic_consume(
            &queue_name,
            "includer_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to create consumer");

    println!("Includer is waiting for messages...");
    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.expect("consumer delivery error");

        let msg: DepositMessage =
            serde_json::from_slice(&delivery.data).expect("invalid DepositMessage JSON");

        println!(
            "Received deposit: sender={}, amount={}, tx_hash={}, log_index={}",
            msg.sender, msg.amount, msg.tx_hash, msg.log_index
        );

        // If we've already processed this (tx_hash, log_index), just ack and skip.
        let rows = sqlx::query(
            "insert into includer_mint_calls (source_tx_hash, source_log_index, token_address, amount) \
             values ($1,$2,$3,$4) \
             on conflict (source_tx_hash, source_log_index) do nothing",
        )
        .bind(&msg.tx_hash)
        .bind(msg.log_index as i64)
        .bind(format!("{token_address:?}"))
        .bind(&msg.amount)
        .execute(&database_client)
        .await
        .expect("Failed to insert includer_mint_calls")
        .rows_affected();

        let inserted = rows == 1;

        if !inserted {
            delivery
                .ack(BasicAckOptions::default())
                .await
                .expect("ack failed");
            continue;
        }

        let mint_res = token
            .mint(msg.amount.clone())
            .send()
            .await;

        match mint_res {
            Ok(pending) => {
                let tx_hash = pending
                    .watch()
                    .await
                    .expect("mint watch failed");

                println!("Mint tx mined: {tx_hash:?}");

                sqlx::query(
                    "update includer_mint_calls set dest_tx_hash = $3, success = true, updated_at = now() \
                     where source_tx_hash = $1 and source_log_index = $2",
                )
                .bind(&msg.tx_hash)
                .bind(msg.log_index as i64)
                .bind(format!("{tx_hash:?}"))
                .execute(&database_client)
                .await
                .expect("Failed to update includer_mint_calls");
            }
            Err(e) => {
                sqlx::query(
                    "update includer_mint_calls set success = false, error = $3, updated_at = now() \
                     where source_tx_hash = $1 and source_log_index = $2",
                )
                .bind(&msg.tx_hash)
                .bind(msg.log_index as i64)
                .bind(format!("{e:?}"))
                .execute(&database_client)
                .await
                .expect("Failed to update includer_mint_calls error");
            }
        }


        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("ack failed");
    }
}