//! Single-threaded, cooperatively-scheduled executor with an integrated timer
//! set. No background threads: `block_on` drives spawned tasks, timers, and the
//! reactor together.

use std::cell::RefCell;
use std::collections::{BinaryHeap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use web_time::Instant;

use crate::platform;

thread_local! {
    static QUEUE: RefCell<VecDeque<async_task::Runnable>> = RefCell::new(VecDeque::new());
    static TIMERS: RefCell<BinaryHeap<Timer>> = RefCell::new(BinaryHeap::new());
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

// --- timers ----------------------------------------------------------------

struct Timer {
    deadline: Instant,
    id: u64,
    waker: Waker,
}
impl PartialEq for Timer {
    fn eq(&self, o: &Self) -> bool {
        self.deadline == o.deadline && self.id == o.id
    }
}
impl Eq for Timer {}
impl PartialOrd for Timer {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Timer {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        // Reversed so the BinaryHeap behaves as a min-heap on deadline.
        o.deadline.cmp(&self.deadline).then(o.id.cmp(&self.id))
    }
}

/// A future that completes after a duration, backed by the executor's timer set.
pub struct Sleep {
    deadline: Instant,
    registered: bool,
    id: u64,
}
impl Future for Sleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        if !self.registered {
            self.registered = true;
            let t = Timer {
                deadline: self.deadline,
                id: self.id,
                waker: cx.waker().clone(),
            };
            TIMERS.with(|h| h.borrow_mut().push(t));
        }
        Poll::Pending
    }
}

/// Return a future that completes after `dur`.
pub fn sleep(dur: Duration) -> Sleep {
    Sleep {
        deadline: Instant::now() + dur,
        registered: false,
        id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
    }
}

// --- executor --------------------------------------------------------------

/// Spawn a detached task.
pub fn spawn<F>(fut: F)
where
    F: Future<Output = ()> + 'static,
{
    let schedule = |runnable| QUEUE.with(|q| q.borrow_mut().push_back(runnable));
    // Safety: single-threaded runtime; tasks never migrate across threads.
    let (runnable, task) = unsafe { async_task::spawn_unchecked(fut, schedule) };
    runnable.schedule();
    task.detach();
}

/// Drop pending tasks without running their destructors. Arti's long-lived
/// background tasks never complete; dropping them normally re-enters the
/// being-destroyed thread-local queue and panics, so at shutdown we leak them.
pub fn forget_pending() {
    QUEUE.with(|q| {
        let mut q = q.borrow_mut();
        while let Some(r) = q.pop_front() {
            std::mem::forget(r);
        }
    });
}

fn run_ready() {
    loop {
        let next = QUEUE.with(|q| q.borrow_mut().pop_front());
        match next {
            Some(r) => {
                r.run();
            }
            None => break,
        }
    }
}

/// Time until the earliest pending timer, or `None` if no timers are armed.
fn next_timer_timeout() -> Option<Duration> {
    TIMERS.with(|h| {
        h.borrow()
            .peek()
            .map(|t| t.deadline.saturating_duration_since(Instant::now()))
    })
}

/// Wake every timer whose deadline has passed.
fn fire_expired_timers() {
    let now = Instant::now();
    loop {
        let fired = TIMERS.with(|h| {
            let mut h = h.borrow_mut();
            match h.peek() {
                Some(t) if t.deadline <= now => h.pop(),
                _ => None,
            }
        });
        match fired {
            Some(t) => t.waker.wake(),
            None => break,
        }
    }
}

/// Drive spawned tasks, timers, and the reactor for up to `budget`, then return.
/// Unlike [`block_on`] there is no top-level future: this lets background tasks
/// (onion accept loops) progress between capi calls while serving.
pub fn pump(budget: Duration) {
    let start = Instant::now();
    loop {
        run_ready();

        let elapsed = start.elapsed();
        if elapsed >= budget {
            return;
        }

        // More queued work: keep running it without blocking the reactor.
        if QUEUE.with(|q| !q.borrow().is_empty()) {
            continue;
        }

        let budget_left = budget - elapsed;
        let timeout = match next_timer_timeout() {
            Some(t) => t.min(budget_left),
            None => budget_left,
        };
        platform::poll(Some(timeout));
        fire_expired_timers();
    }
}

struct MainWaker(AtomicBool);
impl Wake for MainWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Run `fut` to completion, driving spawned tasks, timers and the reactor.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    futures::pin_mut!(fut);
    let mw = Arc::new(MainWaker(AtomicBool::new(true)));
    let waker: Waker = mw.clone().into();

    loop {
        run_ready();

        if mw.0.swap(false, Ordering::Relaxed) {
            let mut cx = Context::from_waker(&waker);
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
            run_ready();
        }

        let has_work =
            QUEUE.with(|q| !q.borrow().is_empty()) || mw.0.load(Ordering::Relaxed);
        if has_work {
            continue;
        }

        platform::poll(next_timer_timeout());
        fire_expired_timers();
    }
}
