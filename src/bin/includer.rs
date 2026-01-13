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
    BasicProperties, Connection, ConnectionProperties,
};
use rustRelayer::DepositMessage;
use serde_json::Value;
use std::{fs, str::FromStr};

static CHAIN_B_URL: &str = "http://localhost:8546";
static PRIVATE_KEY: &str =
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d";

const RABBITMQ_URL: &str = "amqp://guest:guest@localhost:5672";
const QUEUE_NAME: &str = "deposit_events";




#[tokio::main]
async fn main(){
    let conn = Connection::connect(RABBITMQ_URL, ConnectionProperties::default())
        .await
        .expect("Failed to connect to RabbitMQ");

    let channel = conn.create_channel()
        .await
        .expect("Failed to create channel");

    channel.queue_declare(
            QUEUE_NAME,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("Failed to declare queue");

    println!("Connected to RabbitMQ, queue '{}'", QUEUE_NAME);

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


 
    let signer = PrivateKeySigner::from_str(PRIVATE_KEY).expect("bad PRIVATE_KEY");
    let wallet = EthereumWallet::from(signer);

    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(CHAIN_B_URL.parse().expect("bad CHAIN_B_URL"));
    let token_contract = Token::new(token_address, &provider);


    let mut consumer = channel
        .basic_consume(
            QUEUE_NAME,
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

        token_contract
            .mint(msg.amount)
            .send()
            .await
            .expect("mint tx failed to send");

        delivery
            .ack(BasicAckOptions::default())
            .await
            .expect("ack failed");
    }
}