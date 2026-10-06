//! Platform termination signal integration.

use std::{
    fmt::Debug,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

/// Target notified by the process termination signal adapter.
pub trait Shutdown {
    /// Requests the target's idempotent graceful-shutdown path.
    fn shutdown(self: &Arc<Self>);
}

/// Process-lifetime termination signal registration for a weak shutdown target.
pub struct Signals<T: Shutdown + Send + Sync + 'static> {
    target: Weak<T>,
    iterations: AtomicU64,
}

impl<T: Shutdown + Send + Sync> Signals<T> {
    /// Creates an uninstalled signal adapter without extending the target's lifetime.
    pub fn new(target: &Arc<T>) -> Self {
        Self { target: Arc::downgrade(target), iterations: AtomicU64::new(0) }
    }

    /// Installs the platform termination handler.
    ///
    /// # Panics
    ///
    /// Panics with `Error setting signal handler` when the process handler cannot
    /// be installed.
    pub fn init(self: &Arc<Self>) {
        let signals = Arc::clone(self);
        Self::expect_handler_installed(ctrlc::set_handler(move || signals.handle_signal()));
    }

    fn handle_signal(&self) {
        let iteration = self.iterations.fetch_add(1, Ordering::SeqCst);
        if iteration > 1 {
            println!("^SIGTERM - halting");
            std::process::exit(1);
        }

        println!("^SIGTERM - shutting down...");
        if let Some(target) = self.target.upgrade() {
            target.shutdown();
        }
    }

    fn expect_handler_installed<E: Debug>(result: Result<(), E>) {
        result.expect("Error setting signal handler");
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        process::Command,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use super::{Shutdown, Signals};

    const THIRD_SIGNAL_CHILD: &str = "KGI_THIRD_SIGNAL_TEST_CHILD";

    #[derive(Default)]
    struct ShutdownTarget {
        calls: AtomicU64,
    }

    impl Shutdown for ShutdownTarget {
        fn shutdown(self: &Arc<Self>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn first_two_signals_request_shutdown() {
        let target = Arc::new(ShutdownTarget::default());
        let signals = Signals::new(&target);

        signals.handle_signal();
        signals.handle_signal();

        assert_eq!(target.calls.load(Ordering::SeqCst), 2);
        assert_eq!(signals.iterations.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn adapter_holds_only_a_weak_target() {
        let target = Arc::new(ShutdownTarget::default());
        let weak_target = Arc::downgrade(&target);
        let signals = Signals::new(&target);
        drop(target);

        assert!(weak_target.upgrade().is_none());
        signals.handle_signal();
        assert_eq!(signals.iterations.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn handler_installation_failure_has_the_settled_panic() {
        let panic = std::panic::catch_unwind(|| Signals::<ShutdownTarget>::expect_handler_installed(Err("injected failure")))
            .expect_err("installation failure must panic");
        let message = panic.downcast_ref::<String>().map(String::as_str).or_else(|| panic.downcast_ref::<&str>().copied());

        assert!(message.is_some_and(|message| message.contains("Error setting signal handler")));
    }

    #[test]
    fn third_signal_forces_process_exit() {
        if env::var_os(THIRD_SIGNAL_CHILD).is_some() {
            let target = Arc::new(ShutdownTarget::default());
            let signals = Signals::new(&target);

            signals.handle_signal();
            signals.handle_signal();
            assert_eq!(target.calls.load(Ordering::SeqCst), 2);
            signals.handle_signal();
            unreachable!("the third signal must terminate the process");
        }

        let output = Command::new(env::current_exe().expect("current test executable must be available"))
            .args(["--exact", "signals::tests::third_signal_forces_process_exit", "--nocapture"])
            .env(THIRD_SIGNAL_CHILD, "1")
            .output()
            .expect("third-signal subprocess must start");

        assert_eq!(output.status.code(), Some(1));
        let stdout = String::from_utf8(output.stdout).expect("signal diagnostics must be UTF-8");
        assert_eq!(stdout.matches("^SIGTERM - shutting down...").count(), 2);
        assert_eq!(stdout.matches("^SIGTERM - halting").count(), 1);
    }
}
