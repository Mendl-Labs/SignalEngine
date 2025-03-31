use std::fmt::Error;

pub trait DataHandlerTrait {
    fn listen(&self, data: String) -> Result<(), Error>;
}

