use hostbuilder::{
    HostedObject,
    HostedObjectTrait
};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut engine = HostedObject::build().await?;
    Ok(())
}