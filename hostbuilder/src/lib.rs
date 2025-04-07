use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use datahandler::DataHandler;
use dotenv::dotenv;
use mockall::automock;
use portfoliohandler::PortfolioHandler;
use signaldispatcher::SignalDispatcher;
use std::{env, error::Error};

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&self) -> Result<(), Box<dyn Error>>;
}

pub struct HostedObject {
    datahandler: DataHandler,
    portfoliohandler: PortfolioHandler,
    signaldispatcher: SignalDispatcher,
}

impl HostedObject {

}