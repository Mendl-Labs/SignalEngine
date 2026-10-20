//! Readers for venues whose adapters cannot yet be wrapped read-only without new work. They answer `Unsupported`
//! and make no network call. Each names the build-order step (ruling R54-E1) that replaces it.

use crate::reader::{AccountSummary, BrokerReader, ReadError};
use crate::snapshot::{Broker, OpenOrder, Position};

macro_rules! stub_reader {
    ($name:ident, $broker:expr, $label:literal, $pr:literal) => {
        #[derive(Debug, Default, Clone, Copy)]
        pub struct $name;

        impl $name {
            fn unsupported() -> ReadError {
                ReadError::Unsupported { broker: $label, pr: $pr }
            }
        }

        impl BrokerReader for $name {
            fn broker(&self) -> Broker {
                $broker
            }
            fn read_account_summary(&self) -> Result<AccountSummary, ReadError> {
                Err(Self::unsupported())
            }
            fn read_positions(&self) -> Result<Vec<Position>, ReadError> {
                Err(Self::unsupported())
            }
            fn read_open_orders(&self) -> Result<Vec<OpenOrder>, ReadError> {
                Err(Self::unsupported())
            }
        }
    };
}

// TODO(R54 B-PR4): Kraken needs the balance, trade-balance and ticker reads assembled into a snapshot (the
// rebalancer's `view::kraken_snapshot` builder); wrap it when the worker lands.
stub_reader!(KrakenReader, Broker::Kraken, "kraken", "R54 B-PR4");
// TODO(R54 B-PR4): OANDA needs summary, positions and pricing reads (`view::oanda_snapshot`); the adapter has only
// ever run offline, so this also waits for a practice-account smoke test.
stub_reader!(OandaReader, Broker::Oanda, "oanda", "R54 B-PR4");
// TODO(R54 B-PR4): Bitstamp is live-only and live reads are gated on owner decision D1.
stub_reader!(BitstampReader, Broker::Bitstamp, "bitstamp", "R54 B-PR4 and owner decision D1");
