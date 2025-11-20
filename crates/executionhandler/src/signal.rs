/// Temporary Signal definition for compilation
#[derive(Debug, Clone)]
pub struct Signal {
    pub id: String,
    pub strategy_id: String,
    pub symbol: String,
    pub exchange: String,
    pub action: SignalAction,
    pub quantity: f64,
    pub price: Option<f64>,
    pub confidence: f64,
    pub timestamp: u64,
    pub metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SignalAction {
    Buy,
    Sell,
    BuyLimit,
    SellLimit,
    BuyStop,
    SellStop,
}
