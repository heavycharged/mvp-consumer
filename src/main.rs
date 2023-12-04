use derive_more::{Display, Error, From};
use futures::{FutureExt as _, TryFutureExt as _, TryStreamExt as _};
use rdkafka::{
    config::ClientConfig,
    consumer::{CommitMode, Consumer as _, StreamConsumer},
    error::KafkaError,
    message::OwnedMessage,
};
use stream_cancel::StreamExt as _;
use tokio::{signal, task};

const NUM_WORKERS: usize = 128;

#[tokio::main]
async fn main() {
    let clicks_consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", "stats_service_clicks_consumer")
        .set("bootstrap.servers", std::env::var("KAFKA_BROKERS").unwrap())
        .create()
        .expect("failed to create consumer");

    // Or you can do `tokio::spawn(run_consumers(clicks_consumer))` if you need fast abortability
    // here.
    if let Err(e) = run_consumers(clicks_consumer).await {
        eprintln!("consuming failed: {e}");
    }
}

async fn run_consumers(consumer: StreamConsumer) -> Result<(), ConsumingError> {
    let consumer = &consumer; // to capture reference in closures instead of an owned value

    consumer.subscribe(&["clicks"])?;
    consumer
        .stream()
        // Once `ctrl-c` is received, we stop receiving new messages from topic and finish this
        // `Stream`, allowing already received messages to process gracefully.
        .take_until_if(signal::ctrl_c().map(|_| true))
        .map_err(ConsumingError::Kafka)
        // For each received message we perform the actual job, ideally as a separate `tokio::task`
        // for the sake of lighter polling of the whole pipeline.
        .map_ok(|msg| do_work(msg.detach()).map_ok(move |_| msg))
        // This is what handles backpressure here, instead of a bounded channel. It will process
        // `NUM_WORKERS` `Stream::Item`s at most and poll new ones only when there is a free slot in
        // its buffer.
        // Important thing here, that while `Stream::Item`s are processed concurrently, after being
        // processed they're still emitted in the same order as have been received from Kafka.
        // This is required for consistency: we should not overcommit offset if the 2nd received
        // message is processed earlier than 1st one.
        .try_buffered(NUM_WORKERS)
        .try_for_each(|msg| async move {
            // We commit each message after processing it successfully.
            // This allows us to start exactly were we have finished, even if the process or
            // pipeline was aborted forcefully.
            consumer
                .commit_message(&msg, CommitMode::Sync)
                .map_err(ConsumingError::Kafka)
        })
        .await
}

async fn do_work(msg: OwnedMessage) -> Result<(), ConsumingError> {
    // This `tokio::spawn` here is not abort-safe. If someone will do
    // `let pipeline = tokio::spawn(run_consumers(clicks_consumer));`
    // and then
    // `pipeline.abort()`
    // then the whole `run_consumers` pipeline will be dropped and aborted, while tasks, spawned
    // here won't.
    // To fix that, we need to introduce an `AbortGuard` type wrapping `task::JoinHandle` and
    // performing `handle.abort()` on its `Drop`.
    tokio::spawn(async move {
        // Actual performed work on the provided `OwnedMessage` here.
        // If this doesn't require some long-lived state, then it could be just a
        // `Future`/`async fn`.
        // If some long-lived state is required, then here should be a negotiation with the worker
        // via channel or some other mechanism, instead of `tokio::spawn`.
        println!("received message for future process: {msg:?}");
    })
    .await
    .map_err(ConsumingError::from)
}

#[derive(Debug, Display, Error, From)]
enum ConsumingError {
    Kafka(KafkaError),
    Tokio(task::JoinError),
}
