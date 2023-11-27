use async_channel::{Receiver, Sender};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedMessage;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::signal::ctrl_c;
const NUM_WORKERS: usize = 128;

#[tokio::main]
async fn main() {
    let clicks_consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "stats_service_clicks_consumer")
        .set(
            "bootstrap.servers",
            std::env::var("KAFKA_BROKERS").unwrap(),
        )
        .create()
        .expect("failed to create consumer");


    spawn_consumers(clicks_consumer).await;
}


async fn spawn_consumers(consumer: StreamConsumer) {
    consumer
        .subscribe(&["clicks"])
        .expect("cannot subscribe to topic");

    let (tx, rx): (Sender<OwnedMessage>, Receiver<OwnedMessage>) =
        async_channel::bounded(NUM_WORKERS);

    let _done: JoinHandle<()> = tokio::spawn(async move {
        'b: loop {
            select! {
                _ = ctrl_c() => break 'b,
                msg = consumer.recv() => {
                    match msg {
                        Ok(msg) => {
                            // send message to channel
                            tx.send(msg.detach()).await.unwrap();
                        },
                        Err(e) => {
                            panic!("cannot receive message: {:?}", e)
                        },
                    }
                }
            }
        }

        // unsubscribe (also commit offsets)
        consumer.unsubscribe();
        tx.close();
    });

    let mut futures = FuturesUnordered::new();

    for i in 0..NUM_WORKERS {
        let _rx = rx.clone();
        futures.push(tokio::spawn(async move {
            worker(_rx).await;
        }));
    }

    while let Some(_) = futures.next().await {

    }
}

async fn worker(rx: Receiver<OwnedMessage>) {
    while let Ok(_) = rx.recv().await {
        println!("received message for future process");
    }
}