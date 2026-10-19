use std::{sync::Arc, time::Duration};

use wasmer_wasix_types::types::Signal;

#[derive(thiserror::Error, Debug)]
#[error("Signal could not be delivered")]
pub struct SignalDeliveryError;

/// Signal handles...well...they process signals
pub trait SignalHandlerAbi
where
    Self: std::fmt::Debug,
{
    /// Processes a signal
    fn signal(&self, signal: u8) -> Result<(), SignalDeliveryError>;
}

pub type DynSignalHandlerAbi = dyn SignalHandlerAbi + Send + Sync + 'static;

#[derive(Debug)]
pub struct WasiSignalInterval {
    /// Signal that will be raised
    pub signal: Signal,
    /// Time between the signals
    pub interval: Duration,
    /// Flag that indicates if the signal should repeat
    pub repeat: bool,
    /// Last time that a signal was triggered
    pub last_signal: u128,
}

pub fn default_signal_handler() -> Arc<DynSignalHandlerAbi> {
    #[derive(Debug)]
    struct DefaultHandler {}
    impl SignalHandlerAbi for DefaultHandler {
        fn signal(&self, signal: u8) -> Result<(), SignalDeliveryError> {
            if let Ok(signal) = TryInto::<Signal>::try_into(signal) {
                match signal {
                    Signal::Sigkill
                    | Signal::Sigterm
                    | Signal::Sigabrt
                    | Signal::Sigquit
                    | Signal::Sigint
                    | Signal::Sigstop => {
                        tracing::debug!("handling terminate signal");
                        std::process::exit(1);
                    }
                    signal => tracing::info!("unhandled signal - {:?}", signal),
                }
            } else {
                tracing::info!("unknown signal - {}", signal)
            }
            Ok(())
        }
    }
    Arc::new(DefaultHandler {})
}

/// Opt-in guest ABI: standard process signals remain shared until one eligible
/// guest thread claims them. The process mutex serializes all queue operations.
#[derive(Debug, Default)]
pub(crate) struct ProcessSignalQueue {
    enabled: bool,
    pending: Vec<Signal>,
}

impl ProcessSignalQueue {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn enqueue(&mut self, signal: Signal) -> bool {
        if !self.enabled
            || matches!(
                signal,
                Signal::Signone | Signal::Sigkill | Signal::Sigstop | Signal::Sigwakeup
            )
        {
            return false;
        }
        if !self.pending.contains(&signal) {
            self.pending.push(signal);
        }
        true
    }

    pub fn claim(&mut self, blocked: u64) -> Option<Signal> {
        self.enabled = true;
        let index = self
            .pending
            .iter()
            .position(|s| blocked & (1u64 << (*s as u32 - 1)) == 0)?;
        Some(self.pending.remove(index))
    }

    pub fn pending_mask(&self) -> u64 {
        self.pending
            .iter()
            .fold(0, |mask, s| mask | (1u64 << (*s as u32 - 1)))
    }

    pub fn reset_for_exec(&mut self) -> Vec<Signal> {
        self.enabled = false;
        std::mem::take(&mut self.pending)
    }
}

#[cfg(test)]
mod process_signal_tests {
    use super::*;

    #[test]
    fn shared_pending_coalesces_and_respects_masks() {
        let mut queue = ProcessSignalQueue::default();
        assert!(!queue.enqueue(Signal::Sigint));
        assert_eq!(queue.claim(0), None);
        assert!(queue.enqueue(Signal::Sigint));
        assert!(queue.enqueue(Signal::Sigint));
        let mask = 1 << (Signal::Sigint as u32 - 1);
        assert_eq!(queue.claim(mask), None);
        assert_eq!(queue.pending_mask(), mask);
        assert_eq!(queue.claim(0), Some(Signal::Sigint));
        assert_eq!(queue.claim(0), None);
    }

    #[test]
    fn pending_survives_exit_and_transfers_to_legacy_exec() {
        let mut queue = ProcessSignalQueue::default();
        queue.claim(0);
        queue.enqueue(Signal::Sigint);
        // No thread owns this signal, so retiring a thread changes nothing.
        assert_eq!(queue.claim(u64::MAX), None);
        assert_eq!(queue.reset_for_exec(), vec![Signal::Sigint]);
        assert!(!queue.enqueue(Signal::Sigint));
        assert_eq!(queue.pending_mask(), 0);
    }

    #[test]
    fn forced_signals_never_wait_for_guest_claims() {
        let mut queue = ProcessSignalQueue::default();
        queue.claim(0);
        for sig in [
            Signal::Signone,
            Signal::Sigkill,
            Signal::Sigstop,
            Signal::Sigwakeup,
        ] {
            assert!(!queue.enqueue(sig));
        }
    }

    #[test]
    fn concurrent_eligible_threads_claim_exactly_once() {
        use std::sync::{Barrier, Mutex};
        let queue = Arc::new(Mutex::new(ProcessSignalQueue::default()));
        queue.lock().unwrap().claim(0);
        queue.lock().unwrap().enqueue(Signal::Sigint);
        let barrier = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let queue = queue.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    queue.lock().unwrap().claim(0).is_some() as usize
                })
            })
            .collect();
        assert_eq!(
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .sum::<usize>(),
            1
        );
    }
}
