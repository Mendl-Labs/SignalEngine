//! Nonce monotonicity: restarts, backwards clocks, concurrent callers, failing persistence, and
//! arrival-order at the transport.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use broker_adapters::kraken::auth::KrakenCredentials;
use broker_adapters::kraken::pairs::PairTable;
use broker_adapters::kraken::{KrakenAdapter, KrakenConfig};
use broker_adapters::nonce::{
    next_after, FileNonceStore, InMemoryNonceStore, NonceError, NonceGenerator, NonceStore, SystemClock,
};
use broker_adapters::testing::{FakeTransport, ManualClock};
use broker_adapters::transport::HttpResponse;
use broker_adapters::BrokerAdapter;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

fn temp_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let d = std::env::temp_dir().join(format!("broker-adapters-test-{}-{name}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn rule_is_max_of_clock_and_last_plus_one() {
    assert_eq!(next_after(100, None).unwrap(), 100);
    assert_eq!(next_after(100, Some(50)).unwrap(), 100);
    assert_eq!(next_after(100, Some(100)).unwrap(), 101);
    assert_eq!(next_after(100, Some(500)).unwrap(), 501);
    assert_eq!(next_after(0, None).unwrap(), 1);
    assert_eq!(next_after(5, Some(u64::MAX)), Err(NonceError::Exhausted));
}

#[test]
fn strictly_increasing_with_a_frozen_clock() {
    let clock = Arc::new(ManualClock::new(1_000));
    let g = NonceGenerator::new(Arc::new(InMemoryNonceStore::new()), clock);
    let seq: Vec<u64> = (0..5).map(|_| g.next().unwrap()).collect();
    assert_eq!(seq, vec![1000, 1001, 1002, 1003, 1004]);
}

#[test]
fn clock_going_backwards_never_produces_a_lower_nonce() {
    let clock = Arc::new(ManualClock::new(10_000));
    let g = NonceGenerator::new(Arc::new(InMemoryNonceStore::new()), clock.clone());
    let a = g.next().unwrap();
    clock.set(5_000);
    let b = g.next().unwrap();
    clock.set(0);
    let c = g.next().unwrap();
    assert!(a < b && b < c, "{a} {b} {c}");
}

#[test]
fn survives_simulated_restarts_even_when_the_clock_is_behind() {
    let dir = temp_dir("restart");
    let path = dir.join("nonce.txt");
    let clock = Arc::new(ManualClock::new(1_000_000));
    let mut last = 0u64;
    for round in 0..4 {
        // "process restart": brand new store + generator over the same file
        let store = Arc::new(FileNonceStore::open(&path).unwrap());
        let g = NonceGenerator::new(store, clock.clone());
        for _ in 0..3 {
            let n = g.next().unwrap();
            assert!(n > last, "round {round}: {n} not > {last}");
            last = n;
        }
        // The clock does NOT advance across restarts here: this is the process-uptime failure mode.
    }
    // And a restart with the clock jumping far back still goes forward.
    clock.set(1);
    let g = NonceGenerator::new(Arc::new(FileNonceStore::open(&path).unwrap()), clock);
    assert!(g.next().unwrap() > last);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn system_clock_nonce_is_epoch_nanoseconds_scale() {
    let g = NonceGenerator::with_system_clock(Arc::new(InMemoryNonceStore::new()));
    let n = g.next().unwrap();
    // 2020-01-01 in ns is 1.577e18; any current time is far above and below u64::MAX.
    assert!(n > 1_577_000_000_000_000_000, "{n}");
    let _ = SystemClock;
}

#[test]
fn concurrent_callers_get_unique_increasing_nonces_in_memory() {
    let g = NonceGenerator::new(Arc::new(InMemoryNonceStore::new()), Arc::new(ManualClock::new(42)));
    let all = run_threads(&g, 8, 250);
    assert_all_unique(&all, 8 * 250);
}

#[test]
fn concurrent_callers_through_two_file_store_instances_on_one_path() {
    let dir = temp_dir("concurrent-file");
    let path = dir.join("nonce.txt");
    let clock = Arc::new(ManualClock::new(7));
    // Two independent store instances (as two components in one process would create).
    let g1 = NonceGenerator::new(Arc::new(FileNonceStore::open(&path).unwrap()), clock.clone());
    let g2 = NonceGenerator::new(Arc::new(FileNonceStore::open(&path).unwrap()), clock);
    let (g1c, g2c) = (g1.clone(), g2.clone());
    let h1 = std::thread::spawn(move || run_threads(&g1c, 3, 40));
    let h2 = std::thread::spawn(move || run_threads(&g2c, 3, 40));
    let mut all = h1.join().unwrap();
    all.extend(h2.join().unwrap());
    assert_all_unique(&all, 6 * 40);
    let _ = std::fs::remove_dir_all(dir);
}

fn run_threads(g: &NonceGenerator, threads: usize, per_thread: usize) -> Vec<u64> {
    let handles: Vec<_> = (0..threads)
        .map(|_| {
            let g = g.clone();
            std::thread::spawn(move || {
                let mut mine = Vec::with_capacity(per_thread);
                for _ in 0..per_thread {
                    mine.push(g.next().unwrap());
                }
                // Within one thread the sequence must be strictly increasing.
                assert!(mine.windows(2).all(|w| w[0] < w[1]));
                mine
            })
        })
        .collect();
    handles.into_iter().flat_map(|h| h.join().unwrap()).collect()
}

fn assert_all_unique(all: &[u64], expected: usize) {
    let set: BTreeSet<u64> = all.iter().copied().collect();
    assert_eq!(all.len(), expected);
    assert_eq!(set.len(), expected, "duplicate nonces handed out");
}

#[test]
fn corrupt_store_fails_closed_instead_of_resetting() {
    let dir = temp_dir("corrupt");
    let path = dir.join("nonce.txt");
    std::fs::write(&path, "not-a-number").unwrap();
    let g = NonceGenerator::new(Arc::new(FileNonceStore::open(&path).unwrap()), Arc::new(ManualClock::new(5)));
    assert!(matches!(g.next(), Err(NonceError::Corrupt(_))));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn persistence_failure_means_no_nonce_is_returned() {
    struct FailingStore;
    impl NonceStore for FailingStore {
        fn advance(&self, _c: u64) -> Result<u64, NonceError> {
            Err(NonceError::Io("disk full".into()))
        }
        fn last(&self) -> Result<Option<u64>, NonceError> {
            Ok(None)
        }
    }
    let g = NonceGenerator::new(Arc::new(FailingStore), Arc::new(ManualClock::new(5)));
    assert!(g.next().is_err());

    // And through the adapter: nothing is sent when the nonce cannot be persisted.
    let transport = Arc::new(FakeTransport::new());
    let creds = KrakenCredentials::new("k", &B64.encode(b"secret")).unwrap();
    let adapter =
        KrakenAdapter::new(KrakenConfig::default(), creds, transport.clone(), g, PairTable::builtin());
    assert!(adapter.get_balances().is_err());
    assert_eq!(transport.request_count(), 0);
}

#[test]
fn file_store_writes_the_high_water_mark() {
    let dir = temp_dir("hwm");
    let store = FileNonceStore::for_key(&dir, "abcd1234").unwrap();
    assert_eq!(store.last().unwrap(), None);
    assert_eq!(store.advance(10).unwrap(), 10);
    assert_eq!(store.advance(5).unwrap(), 11);
    assert_eq!(store.last().unwrap(), Some(11));
    let text = std::fs::read_to_string(dir.join("kraken-nonce-abcd1234.txt")).unwrap();
    assert_eq!(text.trim(), "11");
    let _ = std::fs::remove_dir_all(dir);
}

/// Requests must reach the transport in nonce order even with concurrent callers, because Kraken
/// rejects a nonce lower than the last one it has seen for the key.
#[test]
fn concurrent_adapter_calls_arrive_in_strictly_increasing_nonce_order() {
    let transport = Arc::new(FakeTransport::new());
    let seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_h = seen.clone();
    transport.set_handler(move |req| {
        let body = req.body.clone().unwrap();
        let nonce: u64 = body.split('&').next().unwrap().strip_prefix("nonce=").unwrap().parse().unwrap();
        // Widen the race window between nonce allocation and arrival.
        std::thread::sleep(std::time::Duration::from_micros(200));
        seen_h.lock().unwrap().push(nonce);
        Ok(HttpResponse { status: 200, body: r#"{"error":[],"result":{"ZUSD":"1.0"}}"#.to_string() })
    });
    let creds = KrakenCredentials::new("k", &B64.encode(b"secret")).unwrap();
    let nonces = NonceGenerator::new(Arc::new(InMemoryNonceStore::new()), Arc::new(ManualClock::new(1)));
    let adapter = Arc::new(KrakenAdapter::new(KrakenConfig::default(), creds, transport, nonces, PairTable::builtin()));
    let handles: Vec<_> = (0..6)
        .map(|_| {
            let a = adapter.clone();
            std::thread::spawn(move || {
                for _ in 0..15 {
                    a.get_balances().unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 90);
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "nonces arrived out of order: {:?}", &seen[..10]);
}
