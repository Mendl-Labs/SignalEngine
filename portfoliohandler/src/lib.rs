use std::fmt::Error;

pub trait PortfolioHandlerTrait {
    fn listen(&self, data: String) -> Result<(), Error>;
}

pub struct PortfolioHandler {
    
}
