use serde::{Serialize, Deserialize};
use serde_yaml;
use std::fs;
use anyhow::{
    Context,
    Result
};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct Config {
    pub message_broker: MessageBroker,
    pub publish_topics: Vec<String>,  // Changed from publish_topic to publish_topics
    pub subscribe_topics: Vec<String>
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct MessageBroker {
    pub address: String,
    pub port: u16,
}

impl Config {
    pub fn new(file_path: &str) -> Result<Self> {
        let config_data = fs::read_to_string(file_path)
            .with_context(|| format!("Unable to read file: {}", file_path))?;
        let config: Config = serde_yaml::from_str(&config_data)
            .context("YAML was not well-formatted")?;
        Ok(config)
    }
}