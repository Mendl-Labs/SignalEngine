//! Per-deployment signal handles.
//!
//! A strategy's signals travel through a fixed-size `ultra_signal::Signal`, whose `strategy_id` is only a
//! `u16`. That value is how the signal loop finds the deployment a signal belongs to, so it MUST identify
//! exactly one live deployment. It used to be `strategy_id & 0xFFFF` -- the low 16 bits of the strategy's
//! (backtest result's) id -- which is not unique: two deployments of one strategy share it, and any two
//! strategies collide 1 time in 65,536. Either case routed orders and fills to whichever deployment the
//! registry scan found first.
//!
//! This allocator hands every ACTIVE deployment its own handle instead:
//! * unique among live deployments (never zero, so 0 can mean "unassigned");
//! * idempotent per deployment (a re-deploy of the same instance keeps its handle);
//! * NOT reused right away: handles are taken from a rotating cursor, so a handle released by a stopped
//!   deployment is not given to a new one until the whole space has been cycled -- signals still in flight
//!   from the old deployment cannot be attributed to the new one.
//!
//! The handle is in-memory only (signals, the registry and log labels); nothing derives meaning from its
//! value, so it does not need to be stable across restarts.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex};

use uuid::Uuid;

/// Handles are `1..=u16::MAX`.
const MAX_HANDLES: usize = u16::MAX as usize;

#[derive(Debug, PartialEq, Eq)]
pub enum HandleError {
    /// Every one of the 65,535 handles belongs to a live deployment.
    Exhausted,
}

impl std::fmt::Display for HandleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandleError::Exhausted => write!(f, "all {MAX_HANDLES} deployment handles are in use"),
        }
    }
}

#[derive(Default)]
struct Inner {
    by_instance: HashMap<Uuid, u16>,
    in_use: HashSet<u16>,
    /// Next candidate; 0 means "start at 1".
    cursor: u16,
}

pub struct HandleAllocator {
    inner: Mutex<Inner>,
}

impl Default for HandleAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl HandleAllocator {
    pub fn new() -> Self {
        Self { inner: Mutex::new(Inner::default()) }
    }

    /// The handle for `instance`, allocating one if it has none. The bool is true when it was newly allocated.
    pub fn allocate(&self, instance: Uuid) -> Result<(u16, bool), HandleError> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(h) = g.by_instance.get(&instance) {
            return Ok((*h, false));
        }
        if g.in_use.len() >= MAX_HANDLES {
            return Err(HandleError::Exhausted);
        }
        loop {
            let candidate = if g.cursor == 0 { 1 } else { g.cursor };
            g.cursor = candidate.wrapping_add(1);
            if !g.in_use.contains(&candidate) {
                g.in_use.insert(candidate);
                g.by_instance.insert(instance, candidate);
                return Ok((candidate, true));
            }
        }
    }

    /// Frees `instance`'s handle. Returns true if it had one.
    pub fn release(&self, instance: &Uuid) -> bool {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        match g.by_instance.remove(instance) {
            Some(h) => {
                g.in_use.remove(&h);
                true
            }
            None => false,
        }
    }

    pub fn handle_of(&self, instance: &Uuid) -> Option<u16> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).by_instance.get(instance).copied()
    }

    pub fn active(&self) -> usize {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).in_use.len()
    }

    /// Allocate for the duration of a deployment attempt: a handle allocated by this lease is released when the
    /// lease is dropped unless [`HandleLease::commit`] was called. This is what keeps a rejected deployment (any
    /// early `continue` in the deploy handler) from leaking its handle.
    pub fn lease(&self, instance: Uuid) -> Result<HandleLease<'_>, HandleError> {
        let (handle, fresh) = self.allocate(instance)?;
        Ok(HandleLease { alloc: self, instance, handle, fresh, committed: false })
    }
}

pub struct HandleLease<'a> {
    alloc: &'a HandleAllocator,
    instance: Uuid,
    handle: u16,
    fresh: bool,
    committed: bool,
}

impl HandleLease<'_> {
    pub fn handle(&self) -> u16 {
        self.handle
    }

    /// The deployment is registered; keep the handle.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for HandleLease<'_> {
    fn drop(&mut self) {
        // A lease on an instance that ALREADY had a handle (a re-deploy of a live instance) never releases it.
        if self.fresh && !self.committed {
            self.alloc.release(&self.instance);
        }
    }
}

/// The process-wide allocator used by the deploy handler and the signal loop.
pub static DEPLOYMENT_HANDLES: LazyLock<HandleAllocator> = LazyLock::new(HandleAllocator::new);

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(n: usize) -> Vec<Uuid> {
        (0..n).map(|_| Uuid::new_v4()).collect()
    }

    #[test]
    fn every_live_deployment_gets_a_unique_nonzero_handle() {
        let a = HandleAllocator::new();
        let mut seen = HashSet::new();
        for id in ids(500) {
            let (h, fresh) = a.allocate(id).unwrap();
            assert!(fresh && h != 0 && seen.insert(h), "duplicate or zero handle {h}");
        }
        assert_eq!(a.active(), 500);
    }

    #[test]
    fn two_deployments_of_the_same_strategy_no_longer_share_a_handle() {
        // The defect this fixes: the old handle was the low 16 bits of the STRATEGY id, identical here.
        let strategy_id = Uuid::new_v4();
        let old_style = |s: &Uuid| (s.as_u128() & 0xFFFF) as u16;
        let (d1, d2) = (Uuid::new_v4(), Uuid::new_v4());
        assert_eq!(old_style(&strategy_id), old_style(&strategy_id), "old scheme: same strategy, same handle");
        let a = HandleAllocator::new();
        let (h1, _) = a.allocate(d1).unwrap();
        let (h2, _) = a.allocate(d2).unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn allocation_is_idempotent_per_instance() {
        let a = HandleAllocator::new();
        let id = Uuid::new_v4();
        let (h, fresh) = a.allocate(id).unwrap();
        let (h2, fresh2) = a.allocate(id).unwrap();
        assert!(fresh && !fresh2 && h == h2);
        assert_eq!(a.active(), 1);
    }

    #[test]
    fn a_released_handle_is_not_reused_immediately() {
        let a = HandleAllocator::new();
        let first = Uuid::new_v4();
        let (h_old, _) = a.allocate(first).unwrap();
        assert!(a.release(&first));
        let (h_new, _) = a.allocate(Uuid::new_v4()).unwrap();
        assert_ne!(h_old, h_new, "in-flight signals from the stopped deployment must not reach the new one");
        assert!(!a.release(&first), "double release is a no-op");
    }

    #[test]
    fn exhaustion_is_reported_and_recovers_after_a_release() {
        let a = HandleAllocator::new();
        let all: Vec<Uuid> = ids(MAX_HANDLES);
        for id in &all {
            a.allocate(*id).unwrap();
        }
        assert_eq!(a.active(), MAX_HANDLES);
        assert_eq!(a.allocate(Uuid::new_v4()), Err(HandleError::Exhausted));
        a.release(&all[42]);
        let (h, fresh) = a.allocate(Uuid::new_v4()).unwrap();
        assert!(fresh && h != 0);
    }

    #[test]
    fn a_lease_that_is_dropped_uncommitted_releases_its_handle() {
        let a = HandleAllocator::new();
        let id = Uuid::new_v4();
        {
            let lease = a.lease(id).unwrap();
            assert_eq!(a.handle_of(&id), Some(lease.handle()));
        } // rejected deployment: early `continue`
        assert_eq!(a.handle_of(&id), None);
        assert_eq!(a.active(), 0);
    }

    #[test]
    fn a_committed_lease_keeps_its_handle() {
        let a = HandleAllocator::new();
        let id = Uuid::new_v4();
        let lease = a.lease(id).unwrap();
        let h = lease.handle();
        lease.commit();
        assert_eq!(a.handle_of(&id), Some(h));
    }

    #[test]
    fn dropping_a_lease_on_an_already_live_instance_never_releases_it() {
        let a = HandleAllocator::new();
        let id = Uuid::new_v4();
        a.lease(id).unwrap().commit(); // the live deployment
        {
            let _redeploy = a.lease(id).unwrap(); // a re-deploy attempt that then fails
        }
        assert!(a.handle_of(&id).is_some(), "the running deployment lost its handle");
    }
}
