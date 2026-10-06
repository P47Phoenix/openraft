//! tick emitter emits a `RaftMsg::Tick` event at a certain interval.

use std::sync::Mutex;
use std::time::Duration;

use futures::future::Either;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::Level;
use tracing::Span;

use crate::core::notify::Notify;
use crate::type_config::alias::JoinHandleOf;
use crate::type_config::TypeConfigExt;
use crate::RaftTypeConfig;

/// Emit RaftMsg::Tick event at regular `interval`.
pub(crate) struct Tick<C>
where C: RaftTypeConfig
{
    interval: Duration,

    tx: mpsc::UnboundedSender<Notify<C>>,

    /// Emit event or not.
    ///
    /// A watch channel, so that a disabled tick loop waits for a change instead of polling.
    enabled: watch::Receiver<bool>,
}

pub(crate) struct TickHandle<C>
where C: RaftTypeConfig
{
    enabled: watch::Sender<bool>,
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    join_handle: Mutex<Option<JoinHandleOf<C, ()>>>,
}

impl<C> Drop for TickHandle<C>
where C: RaftTypeConfig
{
    /// Signal the tick loop to stop, without waiting for it to stop.
    fn drop(&mut self) {
        if self.shutdown.lock().unwrap().is_none() {
            return;
        }
        let _ = self.shutdown();
    }
}

impl<C> Tick<C>
where C: RaftTypeConfig
{
    pub(crate) fn spawn(interval: Duration, tx: mpsc::UnboundedSender<Notify<C>>, enabled: bool) -> TickHandle<C> {
        let (enabled, enabled_rx) = watch::channel(enabled);
        let this = Self {
            interval,
            enabled: enabled_rx,
            tx,
        };

        let (shutdown, shutdown_rx) = oneshot::channel();

        let shutdown = Mutex::new(Some(shutdown));

        let join_handle = C::spawn(this.tick_loop(shutdown_rx).instrument(tracing::span!(
            parent: &Span::current(),
            Level::DEBUG,
            "tick"
        )));

        TickHandle {
            enabled,
            shutdown,
            join_handle: Mutex::new(Some(join_handle)),
        }
    }

    pub(crate) async fn tick_loop(mut self, cancel_rx: oneshot::Receiver<()>) {
        let mut i = 0;

        let mut cancel = std::pin::pin!(cancel_rx);

        loop {
            let at = C::now() + self.interval;
            let sleep_fut = C::sleep_until(at);
            let sleep_fut = std::pin::pin!(sleep_fut);
            let cancel_fut = cancel.as_mut();

            match futures::future::select(cancel_fut, sleep_fut).await {
                Either::Left((_canceled, _)) => {
                    tracing::info!("TickLoop received cancel signal, quit");
                    return;
                }
                Either::Right((_, _)) => {
                    // sleep done
                }
            }

            if !*self.enabled.borrow() {
                // Wait for the tick to be enabled instead of waking every interval to re-check it.
                // Once enabled, `continue` re-arms `now + interval` as every pass does, so the first
                // tick comes one interval after the enable, with no catch-up for the disabled span.
                let enabled_fut = std::pin::pin!(Self::wait_enabled(&mut self.enabled));

                match futures::future::select(cancel.as_mut(), enabled_fut).await {
                    Either::Left((_canceled, _)) => {
                        tracing::info!("TickLoop received cancel signal while disabled, quit");
                        return;
                    }
                    Either::Right((false, _)) => {
                        tracing::info!("TickLoop: TickHandle dropped while disabled, quit");
                        return;
                    }
                    Either::Right((true, _)) => {
                        tracing::debug!("Tick re-enabled");
                    }
                }
                continue;
            }

            i += 1;

            let send_res = self.tx.send(Notify::Tick { i });
            if let Err(_e) = send_res {
                tracing::info!("Stopping tick_loop(), main loop terminated");
                break;
            } else {
                tracing::debug!("Tick sent: {}", i)
            }
        }
    }

    /// Wait until the tick is enabled.
    ///
    /// Returns `false` if the [`TickHandle`] is dropped first. `borrow_and_update()` marks the
    /// value as seen before `changed()` waits, so an enable that lands in between is not lost:
    /// `changed()` returns at once for any version newer than the one last seen.
    async fn wait_enabled(enabled: &mut watch::Receiver<bool>) -> bool {
        loop {
            if *enabled.borrow_and_update() {
                return true;
            }
            if enabled.changed().await.is_err() {
                return false;
            }
        }
    }
}

impl<C> TickHandle<C>
where C: RaftTypeConfig
{
    pub(crate) fn enable(&self, enabled: bool) {
        // Unlike `send()`, this stores the value even after the tick loop has exited, and it
        // notifies the loop only when the value actually changes.
        self.enabled.send_if_modified(|current| {
            if *current == enabled {
                false
            } else {
                *current = enabled;
                true
            }
        });
    }

    /// Signal the tick loop to stop. And return a JoinHandle to wait for the loop to stop.
    ///
    /// If it is called twice, the second call will return None.
    pub(crate) fn shutdown(&self) -> Option<JoinHandleOf<C, ()>> {
        {
            let shutdown = {
                let mut x = self.shutdown.lock().unwrap();
                x.take()
            };

            if let Some(shutdown) = shutdown {
                let send_res = shutdown.send(());
                tracing::info!("Timer shutdown signal sent: {send_res:?}");
            } else {
                tracing::warn!("Double call to Raft::shutdown()");
            }
        }

        let jh = {
            let mut x = self.join_handle.lock().unwrap();
            x.take()
        };
        jh
    }
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "singlethreaded"))]
    use std::future::Future;
    use std::io::Cursor;
    #[cfg(not(feature = "singlethreaded"))]
    use std::pin::Pin;
    #[cfg(not(feature = "singlethreaded"))]
    use std::sync::atomic::AtomicUsize;
    #[cfg(not(feature = "singlethreaded"))]
    use std::sync::atomic::Ordering;
    #[cfg(not(feature = "singlethreaded"))]
    use std::sync::Arc;
    use std::sync::Mutex;
    #[cfg(not(feature = "singlethreaded"))]
    use std::task::Context;
    #[cfg(not(feature = "singlethreaded"))]
    use std::task::Poll;
    use std::time::Instant;

    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio::sync::watch;
    use tokio::time::Duration;

    use crate::core::notify::Notify;
    use crate::core::Tick;
    use crate::core::TickHandle;
    use crate::type_config::TypeConfigExt;
    use crate::MessageSummary;
    use crate::RaftTypeConfig;
    use crate::TokioRuntime;

    #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd)]
    #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
    pub(crate) struct TickUTConfig {}
    impl RaftTypeConfig for TickUTConfig {
        type D = ();
        type R = ();
        type NodeId = u64;
        type Node = ();
        type Entry = crate::Entry<TickUTConfig>;
        type SnapshotData = Cursor<Vec<u8>>;
        type AsyncRuntime = TokioRuntime;
        type Responder = crate::impls::OneshotResponder<Self>;
    }

    // AsyncRuntime::spawn is `spawn_local` with singlethreaded enabled.
    // It will result in a panic:
    // `spawn_local` called from outside of a `task::LocalSet`.
    #[cfg(not(feature = "singlethreaded"))]
    #[tokio::test]
    async fn test_shutdown() -> anyhow::Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let th = Tick::<TickUTConfig>::spawn(Duration::from_millis(100), tx, true);

        TickUTConfig::sleep(Duration::from_millis(500)).await;
        let _ = th.shutdown().unwrap().await;
        TickUTConfig::sleep(Duration::from_millis(500)).await;

        let mut received = vec![];
        while let Some(x) = rx.recv().await {
            received.push(x);
        }

        assert!(
            received.len() < 10,
            "no more tick will be received after shutdown: {}",
            received.len()
        );

        Ok(())
    }

    /// Receive the next notification, which must be a tick, and return its index.
    async fn recv_tick(rx: &mut mpsc::UnboundedReceiver<Notify<TickUTConfig>>) -> u64 {
        match rx.recv().await {
            Some(Notify::Tick { i }) => i,
            Some(other) => panic!("expected a tick, got: {}", other.summary()),
            None => panic!("tick channel closed"),
        }
    }

    /// Build a tick loop that starts disabled, without spawning it, and the handle that controls
    /// it, as `Tick::spawn()` would. The caller runs `tick_loop()` with the returned cancel
    /// receiver.
    #[allow(clippy::type_complexity)]
    fn disabled_tick(
        interval: Duration,
    ) -> (
        Tick<TickUTConfig>,
        oneshot::Receiver<()>,
        TickHandle<TickUTConfig>,
        mpsc::UnboundedReceiver<Notify<TickUTConfig>>,
    ) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (enabled, enabled_rx) = watch::channel(false);
        let (shutdown, cancel_rx) = oneshot::channel();
        let tick = Tick::<TickUTConfig> {
            interval,
            tx,
            enabled: enabled_rx,
        };
        let handle = TickHandle {
            enabled,
            shutdown: Mutex::new(Some(shutdown)),
            join_handle: Mutex::new(None),
        };
        (tick, cancel_rx, handle, rx)
    }

    /// Counts how often the wrapped future is polled: one poll per wakeup of the tick task.
    #[cfg(not(feature = "singlethreaded"))]
    struct CountPolls<F> {
        inner: Pin<Box<F>>,
        polls: Arc<AtomicUsize>,
    }

    #[cfg(not(feature = "singlethreaded"))]
    impl<F> Future for CountPolls<F>
    where F: Future
    {
        type Output = F::Output;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
            self.polls.fetch_add(1, Ordering::Relaxed);
            self.inner.as_mut().poll(cx)
        }
    }

    /// A disabled tick must park instead of waking once per interval to re-check the flag.
    ///
    /// A loop that re-checks the flag every interval would be polled about 50 times here. The
    /// parked loop is polled for the first wait, once more to park, and not again until it is
    /// re-enabled.
    #[cfg(not(feature = "singlethreaded"))]
    #[tokio::test]
    async fn test_a_disabled_tick_parks_without_waking() {
        const INTERVAL: Duration = Duration::from_millis(10);

        let (tick, cancel_rx, th, mut rx) = disabled_tick(INTERVAL);
        let polls = Arc::new(AtomicUsize::new(0));
        let join_handle = tokio::spawn(CountPolls {
            inner: Box::pin(tick.tick_loop(cancel_rx)),
            polls: polls.clone(),
        });
        *th.join_handle.lock().unwrap() = Some(join_handle);

        tokio::time::sleep(INTERVAL * 50).await;
        let parked_polls = polls.load(Ordering::Relaxed);
        assert!(
            parked_polls <= 3,
            "a disabled tick must not wake every interval: polled {parked_polls} times in 50 intervals"
        );
        assert!(rx.try_recv().is_err(), "a disabled tick must not emit");

        // Re-enabling resumes ticking. The bound is loose on purpose, for slow timers and loaded
        // machines; `test_resume_ticks_one_interval_after_enabling` checks when the first tick lands.
        th.enable(true);
        let i = tokio::time::timeout(Duration::from_secs(1), recv_tick(&mut rx))
            .await
            .expect("re-enabling must resume ticking");
        assert_eq!(1, i);

        // Disable again and cancel while parked: the loop must exit promptly.
        th.enable(false);
        tokio::time::sleep(INTERVAL * 3).await;
        tokio::time::timeout(Duration::from_millis(500), th.shutdown().unwrap())
            .await
            .expect("cancel must stop a parked tick loop")
            .expect("tick loop must not panic");
    }

    /// Re-enabling keeps the loop's schedule: every tick, including the first one after a disabled
    /// span, comes one full interval after the loop re-arms its timer. There is no tick at the
    /// moment of enabling and no burst of catch-up ticks for the disabled span.
    ///
    /// The loop is driven in this task by `join`, so the test needs no spawn and also runs with
    /// `singlethreaded`.
    #[tokio::test]
    async fn test_resume_ticks_one_interval_after_enabling() {
        const INTERVAL: Duration = Duration::from_millis(200);

        let (tick, cancel_rx, th, mut rx) = disabled_tick(INTERVAL);

        let loop_fut = tick.tick_loop(cancel_rx);
        let check = async move {
            tokio::time::sleep(INTERVAL * 2 + INTERVAL / 2).await;
            let enabled_at = Instant::now();
            th.enable(true);
            let i = tokio::time::timeout(Duration::from_secs(5), recv_tick(&mut rx))
                .await
                .expect("re-enabling must resume ticking");
            let got = Instant::now();
            assert_eq!(1, i);

            // The timer never fires early, so the first tick is at least one interval after the
            // enable. The margin only absorbs clock rounding; an immediate tick or one on the old
            // polling schedule (anywhere within the interval) fails it.
            assert!(
                got - enabled_at >= INTERVAL * 9 / 10,
                "resumed early: first tick {:?} after enabling",
                got - enabled_at
            );

            // No burst: the next tick is again one interval later.
            let i2 = recv_tick(&mut rx).await;
            assert_eq!(2, i2);
            assert!(
                got.elapsed() >= INTERVAL * 9 / 10,
                "burst of catch-up ticks: second tick {:?} after the first",
                got.elapsed()
            );

            // The loop runs in this task, so there is no join handle to return.
            assert!(th.shutdown().is_none());
        };
        futures::future::join(loop_fut, check).await;
    }

    /// An enable that follows a disable always resumes ticking, wherever it lands in the loop.
    ///
    /// On the multi-threaded runtime the test body and the tick loop run on different threads, so
    /// `enable()` runs concurrently with the loop. Each round disables the tick, drops the ticks
    /// already queued, and busy-waits before re-enabling it. The busy-wait sweeps a little more
    /// than one observed tick interval, so across the rounds the enable lands before, during and
    /// after the loop parks. The interval is measured rather than assumed, because the timer may
    /// be much coarser than `INTERVAL` (about 15 ms on Windows).
    ///
    /// This catches an enable that never wakes a parked loop. It cannot reliably hit the
    /// nanosecond windows of the check-then-park race itself; that relies on the watch channel's
    /// version counter, which `changed()` checks against the version `borrow_and_update()` saw.
    #[cfg(not(feature = "singlethreaded"))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_enable_after_disable_always_resumes_ticking() {
        const INTERVAL: Duration = Duration::from_millis(1);
        const ROUNDS: u32 = 50;

        fn spin_for(d: Duration) {
            let end = Instant::now() + d;
            while Instant::now() < end {
                std::hint::spin_loop();
            }
        }

        let (tx, mut rx) = mpsc::unbounded_channel();
        let th = Tick::<TickUTConfig>::spawn(INTERVAL, tx, true);

        recv_tick(&mut rx).await;
        let start = Instant::now();
        for _ in 0..4 {
            recv_tick(&mut rx).await;
        }
        let interval = start.elapsed() / 4;

        for round in 0..ROUNDS {
            th.enable(false);
            // At most one tick can still arrive after this: one the loop had already decided to
            // send when it was disabled. Receiving three ticks below proves the loop resumed.
            while rx.try_recv().is_ok() {}
            // Coprime with ROUNDS, so the rounds visit every step of the sweep once.
            spin_for(interval * 5 * (round * 37 % ROUNDS) / (4 * ROUNDS));
            th.enable(true);

            for _ in 0..3 {
                tokio::time::timeout(Duration::from_millis(500), recv_tick(&mut rx))
                    .await
                    .unwrap_or_else(|_| panic!("round {round}: an enabled tick stayed parked"));
            }
        }

        th.shutdown().unwrap().await.unwrap();
    }
}
