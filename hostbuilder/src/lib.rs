use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use dotenv::dotenv;
use mockall::automock;
use publisher::Publisher;
use subscriber::{Subscriber, SubscriberTrait};
use std::{env, error::Error};

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

pub struct HostedObject {
    subscriber: Subscriber,
    publisher: Publisher
}

impl HostedObject {
    pub fn subscriber(&self) -> &Subscriber {
        &self.subscriber
    }

    pub fn publisher(&self) -> &Publisher {
        &self.publisher
    }

    pub async fn build() -> Result<Self> {
        dotenv().ok();
        let config_path = env::var("CONFIG_PATH").expect("CONFIG_PATH must be set");
        let config = Config::new(&config_path)
            .expect("Failed to load config");
        let addr = format!(
            "{}:{}",
            config.message_broker.address,
            config.message_broker.port
        );
        let topics = config.topics.clone();
        let subscriber = Subscriber::new(&addr, &topics).await.expect("Failed to create subscriber");
        subscriber.subscribe()
            .await
            .expect("Failed to subscribe to topics");
        let publisher = Publisher::new(&addr).await.expect("Failed to create publisher");
        Ok(Self {
            subscriber,
            publisher,
        })
    }
}

#[async_trait]
impl HostedObjectTrait for HostedObject {
    async fn run(&self) -> Result<(), Box<dyn Error>> {
        self.subscriber.listen(|message| {
            println!("Received message: {:?}", message);
        }).await?;
        Ok(())
    }
}