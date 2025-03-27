use anyhow::Result;

pub trait Hostbuilder {
    async fn run(&mut self) -> Result<()>;
}

pub struct HostedObject {
}