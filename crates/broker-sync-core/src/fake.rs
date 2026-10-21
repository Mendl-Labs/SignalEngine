//! In-memory reader for tests: scripted answers, no network.

use crate::reader::{AccountSummary, BrokerReader, ReadError};
use crate::snapshot::{Broker, OpenOrder, Position};
use std::sync::Mutex;

pub struct FakeReader {
    broker: Broker,
    summary: Mutex<Result<AccountSummary, ReadError>>,
    positions: Mutex<Result<Vec<Position>, ReadError>>,
    open_orders: Mutex<Result<Vec<OpenOrder>, ReadError>>,
    reads: Mutex<u32>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl FakeReader {
    pub fn new(broker: Broker, summary: AccountSummary, positions: Vec<Position>, open_orders: Vec<OpenOrder>) -> Self {
        Self {
            broker,
            summary: Mutex::new(Ok(summary)),
            positions: Mutex::new(Ok(positions)),
            open_orders: Mutex::new(Ok(open_orders)),
            reads: Mutex::new(0),
        }
    }

    /// A reader whose every read fails with `err`.
    pub fn failing(broker: Broker, err: ReadError) -> Self {
        Self {
            broker,
            summary: Mutex::new(Err(err.clone())),
            positions: Mutex::new(Err(err.clone())),
            open_orders: Mutex::new(Err(err)),
            reads: Mutex::new(0),
        }
    }

    pub fn set_positions(&self, positions: Vec<Position>) {
        *lock(&self.positions) = Ok(positions);
    }

    pub fn fail_positions(&self, err: ReadError) {
        *lock(&self.positions) = Err(err);
    }

    /// Total number of read calls made so far.
    pub fn read_count(&self) -> u32 {
        *lock(&self.reads)
    }

    fn bump(&self) {
        *lock(&self.reads) += 1;
    }
}

impl BrokerReader for FakeReader {
    fn broker(&self) -> Broker {
        self.broker
    }
    fn read_account_summary(&self) -> Result<AccountSummary, ReadError> {
        self.bump();
        lock(&self.summary).clone()
    }
    fn read_positions(&self) -> Result<Vec<Position>, ReadError> {
        self.bump();
        lock(&self.positions).clone()
    }
    fn read_open_orders(&self) -> Result<Vec<OpenOrder>, ReadError> {
        self.bump();
        lock(&self.open_orders).clone()
    }
}
