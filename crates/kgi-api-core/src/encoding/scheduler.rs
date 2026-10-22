use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use kgi_core::timing::Timing;
use serde::Serialize;
use tokio::sync::{oneshot, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::{EncodedGraphBody, GraphEncodingError, encode_graph};

/// Maximum graph serialization and compression calls active at once.
pub(crate) const MAX_GRAPH_ENCODING_JOBS: usize = 4;
/// Maximum ready or pre-reserved jobs waiting outside active execution.
pub(crate) const MAX_QUEUED_GRAPH_ENCODING_JOBS: usize = 32;
/// Maximum historical graph encodes included in the total active bound.
pub(crate) const MAX_HISTORICAL_ENCODING_JOBS: usize = 2;
/// Maximum time a submitted job may wait to begin blocking execution.
pub(crate) const GRAPH_ENCODING_QUEUE_TIMEOUT: Duration = Duration::from_secs(30);

type BlockingJob = Box<dyn FnOnce() -> Result<EncodedGraphBody, GraphEncodingError> + Send>;
type BlockingResult = Result<Result<EncodedGraphBody, GraphEncodingError>, BlockingExecutorError>;
type BlockingFuture = Pin<Box<dyn Future<Output = BlockingResult> + Send>>;

/// Failure of one admitted graph encoding operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum EncodingSchedulerError {
    /// The bounded ready-or-reserved queue has no remaining capacity.
    #[error("graph encoding admission is saturated")]
    AdmissionSaturated,
    /// The admitted job did not begin before its queue deadline.
    #[error("graph encoding queue deadline elapsed")]
    QueueTimeout,
    /// The operation's owner cancelled its interest.
    #[error("graph encoding was cancelled")]
    Cancelled,
    /// The scheduler is shutting down and accepts no further work.
    #[error("graph encoding scheduler is shutting down")]
    ShuttingDown,
    /// JSON serialization or gzip construction failed.
    #[error(transparent)]
    Encoding(#[from] GraphEncodingError),
    /// The blocking executor failed before returning the encoding result.
    #[error("graph blocking executor failed")]
    BlockingExecutorFailed,
    /// An encoding-scheduler orchestration task panicked or was otherwise lost.
    #[error("graph encoding scheduler task could not be joined")]
    WorkerFailed,
}

/// Bounded owner of every graph JSON serialization and gzip operation.
#[derive(Clone)]
pub(crate) struct EncodingScheduler {
    inner: Arc<EncodingSchedulerInner>,
}

impl EncodingScheduler {
    /// Creates a production scheduler backed exclusively by Tokio's blocking executor.
    pub(crate) fn production() -> Self {
        Self::with_dependencies(Timing::production(), Arc::new(TokioBlockingExecutor))
    }

    fn with_dependencies(timing: Timing, executor: Arc<dyn BlockingExecutor>) -> Self {
        let (completion, _) = watch::channel(None);
        Self {
            inner: Arc::new(EncodingSchedulerInner {
                state: Mutex::new(SchedulerState::default()),
                tasks: TaskTracker::new(),
                completion,
                timing,
                executor,
            }),
        }
    }

    /// Attempts to reserve one queue position before historical database work begins.
    pub(crate) fn try_reserve_historical(&self) -> Result<HistoricalEncodingReservation, EncodingSchedulerError> {
        let reservation_id = self.inner.reserve_historical()?;
        Ok(HistoricalEncodingReservation { inner: self.inner.clone(), reservation_id: Some(reservation_id) })
    }

    /// Encodes one publication or Head response through the priority lane.
    pub(crate) async fn encode_priority<T>(
        &self,
        value: T,
        cancellation: CancellationToken,
    ) -> Result<EncodedGraphBody, EncodingSchedulerError>
    where
        T: Serialize + Send + 'static,
    {
        let admitted = self.inner.submit(JobClass::Priority, Box::new(move || encode_graph(&value)), None)?;
        admitted.wait(cancellation).await
    }

    /// Rejects new work, cancels queued work, drains active blocking jobs, and joins their owners.
    pub(crate) async fn shutdown(&self) -> Result<(), EncodingSchedulerError> {
        let completion = self.inner.completion.subscribe();
        self.inner.begin_shutdown();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        self.inner.complete(Ok(()));
        EncodingSchedulerInner::wait_completion(completion).await
    }

    /// Returns an owned future that observes scheduler completion without requesting shutdown.
    pub(crate) fn completion(&self) -> impl Future<Output = Result<(), EncodingSchedulerError>> + Send + 'static {
        let completion = self.inner.completion.subscribe();
        async move { EncodingSchedulerInner::wait_completion(completion).await }
    }
}

/// One historical queue position retained across its database projection phase.
pub(crate) struct HistoricalEncodingReservation {
    inner: Arc<EncodingSchedulerInner>,
    reservation_id: Option<u64>,
}

impl HistoricalEncodingReservation {
    /// Submits a detached historical projection through this reserved position.
    pub(crate) async fn encode<T>(
        mut self,
        value: T,
        cancellation: CancellationToken,
    ) -> Result<EncodedGraphBody, EncodingSchedulerError>
    where
        T: Serialize + Send + 'static,
    {
        let reservation_id = self.reservation_id.take().expect("a historical reservation is consumed once");
        let admitted = self.inner.submit(JobClass::Historical, Box::new(move || encode_graph(&value)), Some(reservation_id))?;
        admitted.wait(cancellation).await
    }
}

impl Drop for HistoricalEncodingReservation {
    fn drop(&mut self) {
        if let Some(reservation_id) = self.reservation_id.take() {
            self.inner.release_reservation(reservation_id);
        }
    }
}

struct EncodingSchedulerInner {
    state: Mutex<SchedulerState>,
    tasks: TaskTracker,
    completion: watch::Sender<Option<Result<(), EncodingSchedulerError>>>,
    timing: Timing,
    executor: Arc<dyn BlockingExecutor>,
}

impl EncodingSchedulerInner {
    fn reserve_historical(&self) -> Result<u64, EncodingSchedulerError> {
        let mut state = self.lock_state();
        if !state.accepting {
            return Err(EncodingSchedulerError::ShuttingDown);
        }
        if state.queue_occupancy() >= MAX_QUEUED_GRAPH_ENCODING_JOBS {
            return Err(EncodingSchedulerError::AdmissionSaturated);
        }
        let id = state.allocate_id();
        assert!(state.reservations.insert(id), "a fresh encoding reservation ID is unique");
        Ok(id)
    }

    fn release_reservation(&self, reservation_id: u64) {
        self.lock_state().reservations.remove(&reservation_id);
    }

    fn submit(
        self: &Arc<Self>,
        class: JobClass,
        work: BlockingJob,
        reservation_id: Option<u64>,
    ) -> Result<AdmittedJob, EncodingSchedulerError> {
        let (result_tx, result_rx) = oneshot::channel();
        let completion = Arc::new(JobCompletion::new(result_tx));
        let mut state = self.lock_state();

        if let Some(reservation_id) = reservation_id
            && !state.reservations.remove(&reservation_id)
        {
            return Err(EncodingSchedulerError::ShuttingDown);
        }
        if !state.accepting {
            return Err(EncodingSchedulerError::ShuttingDown);
        }

        let id = reservation_id.unwrap_or_else(|| state.allocate_id());
        let job = QueuedJob { id, class, work, completion: completion.clone() };
        let starts_immediately = state.can_start(class) && (class == JobClass::Priority || state.priority_queue.is_empty());

        if starts_immediately {
            state.start(job, self);
        } else {
            if reservation_id.is_none() && state.queue_occupancy() >= MAX_QUEUED_GRAPH_ENCODING_JOBS {
                return Err(EncodingSchedulerError::AdmissionSaturated);
            }
            state.queue(job);
        }

        Ok(AdmittedJob { inner: self.clone(), id, result: result_rx, queue_timeout_applies: !starts_immediately, armed: true })
    }

    fn cancel(&self, id: u64, reason: EncodingSchedulerError) {
        let completion = {
            let mut state = self.lock_state();
            state.remove_queued(id).map(|job| job.completion).or_else(|| state.active.get(&id).map(|active| active.completion.clone()))
        };
        if let Some(completion) = completion {
            completion.complete(Err(reason));
        }
    }

    fn timeout_queued(&self, id: u64) -> bool {
        let completion = self.lock_state().remove_queued(id).map(|job| job.completion);
        if let Some(completion) = completion {
            completion.complete(Err(EncodingSchedulerError::QueueTimeout));
            true
        } else {
            false
        }
    }

    fn finish(self: &Arc<Self>, id: u64, result: BlockingResult) {
        let (completion, terminal) = {
            let mut state = self.lock_state();
            let active = state.active.remove(&id).expect("a completed encoding job is active");
            state.active_total = state.active_total.checked_sub(1).expect("active encoding count is positive");
            if active.class == JobClass::Historical {
                state.active_historical = state.active_historical.checked_sub(1).expect("active historical count is positive");
            }

            let terminal = if state.accepting {
                match result {
                    Ok(Ok(body)) => Ok(body),
                    Ok(Err(error)) => Err(error.into()),
                    Err(BlockingExecutorError) => Err(EncodingSchedulerError::BlockingExecutorFailed),
                }
            } else {
                Err(EncodingSchedulerError::ShuttingDown)
            };
            state.dispatch(self);
            (active.completion, terminal)
        };
        completion.complete(terminal);
    }

    fn begin_shutdown(&self) {
        let completions = {
            let mut state = self.lock_state();
            if !state.accepting {
                return;
            }
            state.accepting = false;
            state.reservations.clear();
            let mut completions: Vec<_> = state.priority_queue.drain(..).map(|job| job.completion).collect();
            completions.extend(state.historical_queue.drain(..).map(|job| job.completion));
            completions.extend(state.active.values().map(|active| active.completion.clone()));
            completions
        };
        for completion in completions {
            completion.complete(Err(EncodingSchedulerError::ShuttingDown));
        }
    }

    fn spawn(self: &Arc<Self>, job: QueuedJob) {
        let inner = self.clone();
        let worker = self.tasks.spawn(async move {
            let result = inner.executor.execute(job.work).await;
            inner.finish(job.id, result);
        });
        let inner = self.clone();
        self.tasks.spawn(async move {
            if worker.await.is_err() {
                inner.record_failure();
            }
        });
    }

    fn record_failure(&self) {
        self.begin_shutdown();
        self.tasks.close();
        self.complete(Err(EncodingSchedulerError::WorkerFailed));
    }

    fn complete(&self, result: Result<(), EncodingSchedulerError>) {
        self.completion.send_if_modified(|completion| {
            if completion.is_some() {
                return false;
            }
            *completion = Some(result);
            true
        });
    }

    async fn wait_completion(
        mut completion: watch::Receiver<Option<Result<(), EncodingSchedulerError>>>,
    ) -> Result<(), EncodingSchedulerError> {
        loop {
            if let Some(result) = *completion.borrow_and_update() {
                return result;
            }
            if completion.changed().await.is_err() {
                return Err(EncodingSchedulerError::WorkerFailed);
            }
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, SchedulerState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

struct AdmittedJob {
    inner: Arc<EncodingSchedulerInner>,
    id: u64,
    result: oneshot::Receiver<Result<EncodedGraphBody, EncodingSchedulerError>>,
    queue_timeout_applies: bool,
    armed: bool,
}

impl AdmittedJob {
    async fn wait(mut self, cancellation: CancellationToken) -> Result<EncodedGraphBody, EncodingSchedulerError> {
        if !self.queue_timeout_applies {
            return self.wait_without_timeout(cancellation).await;
        }

        let timing = self.inner.timing.clone();
        let timeout = timing.sleep(GRAPH_ENCODING_QUEUE_TIMEOUT);
        tokio::pin!(timeout);
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                self.inner.cancel(self.id, EncodingSchedulerError::Cancelled);
                self.armed = false;
                Err(EncodingSchedulerError::Cancelled)
            }
            result = &mut self.result => self.finish_result(result),
            () = &mut timeout => {
                if self.inner.timeout_queued(self.id) {
                    self.armed = false;
                    Err(EncodingSchedulerError::QueueTimeout)
                } else {
                    self.wait_without_timeout(cancellation).await
                }
            }
        }
    }

    async fn wait_without_timeout(&mut self, cancellation: CancellationToken) -> Result<EncodedGraphBody, EncodingSchedulerError> {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                self.inner.cancel(self.id, EncodingSchedulerError::Cancelled);
                self.armed = false;
                Err(EncodingSchedulerError::Cancelled)
            }
            result = &mut self.result => self.finish_result(result),
        }
    }

    fn finish_result(
        &mut self,
        result: Result<Result<EncodedGraphBody, EncodingSchedulerError>, oneshot::error::RecvError>,
    ) -> Result<EncodedGraphBody, EncodingSchedulerError> {
        self.armed = false;
        result.unwrap_or(Err(EncodingSchedulerError::BlockingExecutorFailed))
    }
}

impl Drop for AdmittedJob {
    fn drop(&mut self) {
        if self.armed {
            self.inner.cancel(self.id, EncodingSchedulerError::Cancelled);
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JobClass {
    Priority,
    Historical,
}

struct QueuedJob {
    id: u64,
    class: JobClass,
    work: BlockingJob,
    completion: Arc<JobCompletion>,
}

struct ActiveJob {
    class: JobClass,
    completion: Arc<JobCompletion>,
}

struct JobCompletion {
    result: Mutex<Option<oneshot::Sender<Result<EncodedGraphBody, EncodingSchedulerError>>>>,
}

impl JobCompletion {
    fn new(result: oneshot::Sender<Result<EncodedGraphBody, EncodingSchedulerError>>) -> Self {
        Self { result: Mutex::new(Some(result)) }
    }

    fn complete(&self, result: Result<EncodedGraphBody, EncodingSchedulerError>) {
        if let Some(sender) = self.result.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take() {
            let _ = sender.send(result);
        }
    }
}

struct SchedulerState {
    accepting: bool,
    next_id: u64,
    reservations: HashSet<u64>,
    priority_queue: VecDeque<QueuedJob>,
    historical_queue: VecDeque<QueuedJob>,
    active: HashMap<u64, ActiveJob>,
    active_total: usize,
    active_historical: usize,
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            accepting: true,
            next_id: 0,
            reservations: HashSet::new(),
            priority_queue: VecDeque::new(),
            historical_queue: VecDeque::new(),
            active: HashMap::new(),
            active_total: 0,
            active_historical: 0,
        }
    }
}

impl SchedulerState {
    fn allocate_id(&mut self) -> u64 {
        self.next_id = self.next_id.checked_add(1).expect("encoding job IDs do not exhaust u64");
        self.next_id
    }

    fn queue_occupancy(&self) -> usize {
        self.reservations.len() + self.priority_queue.len() + self.historical_queue.len()
    }

    fn can_start(&self, class: JobClass) -> bool {
        self.active_total < MAX_GRAPH_ENCODING_JOBS
            && (class == JobClass::Priority || self.active_historical < MAX_HISTORICAL_ENCODING_JOBS)
    }

    fn queue(&mut self, job: QueuedJob) {
        match job.class {
            JobClass::Priority => self.priority_queue.push_back(job),
            JobClass::Historical => self.historical_queue.push_back(job),
        }
    }

    fn start(&mut self, job: QueuedJob, inner: &Arc<EncodingSchedulerInner>) {
        self.active_total += 1;
        if job.class == JobClass::Historical {
            self.active_historical += 1;
        }
        let previous = self.active.insert(job.id, ActiveJob { class: job.class, completion: job.completion.clone() });
        assert!(previous.is_none(), "an encoding job starts once");
        inner.spawn(job);
    }

    fn dispatch(&mut self, inner: &Arc<EncodingSchedulerInner>) {
        while self.active_total < MAX_GRAPH_ENCODING_JOBS {
            let next = self.priority_queue.pop_front().or_else(|| {
                (self.active_historical < MAX_HISTORICAL_ENCODING_JOBS).then(|| self.historical_queue.pop_front()).flatten()
            });
            let Some(job) = next else {
                break;
            };
            self.start(job, inner);
        }
    }

    fn remove_queued(&mut self, id: u64) -> Option<QueuedJob> {
        remove_job(&mut self.priority_queue, id).or_else(|| remove_job(&mut self.historical_queue, id))
    }
}

fn remove_job(queue: &mut VecDeque<QueuedJob>, id: u64) -> Option<QueuedJob> {
    queue.iter().position(|job| job.id == id).and_then(|position| queue.remove(position))
}

#[derive(Clone, Copy, Debug)]
struct BlockingExecutorError;

trait BlockingExecutor: Send + Sync {
    fn execute(&self, job: BlockingJob) -> BlockingFuture;
}

struct TokioBlockingExecutor;

impl BlockingExecutor for TokioBlockingExecutor {
    fn execute(&self, job: BlockingJob) -> BlockingFuture {
        Box::pin(async move { tokio::task::spawn_blocking(job).await.map_err(|_| BlockingExecutorError) })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex},
        thread::ThreadId,
        time::Duration,
    };

    use async_trait::async_trait;
    use kgi_core::timing::{Clock, Jitter, Timing};
    use serde::{Serialize, Serializer};
    use tokio::{sync::mpsc, task::JoinHandle};
    use tokio_util::sync::CancellationToken;

    use super::{
        BlockingExecutor, BlockingExecutorError, BlockingFuture, BlockingJob, EncodingScheduler, EncodingSchedulerError,
        GRAPH_ENCODING_QUEUE_TIMEOUT, MAX_GRAPH_ENCODING_JOBS, MAX_HISTORICAL_ENCODING_JOBS, MAX_QUEUED_GRAPH_ENCODING_JOBS,
    };

    struct ControlledExecutor {
        submitted: mpsc::UnboundedSender<PendingExecution>,
    }

    struct PanickingExecutor;

    impl BlockingExecutor for PanickingExecutor {
        fn execute(&self, _job: BlockingJob) -> BlockingFuture {
            Box::pin(async { panic!("simulated encoding-scheduler task panic") })
        }
    }

    impl BlockingExecutor for ControlledExecutor {
        fn execute(&self, job: BlockingJob) -> BlockingFuture {
            let (result_tx, result_rx) = tokio::sync::oneshot::channel();
            self.submitted.send(PendingExecution { job, result: result_tx }).expect("test executor remains installed");
            Box::pin(async move { result_rx.await.unwrap_or(Err(BlockingExecutorError)) })
        }
    }

    struct PendingExecution {
        job: BlockingJob,
        result: tokio::sync::oneshot::Sender<super::BlockingResult>,
    }

    impl PendingExecution {
        fn complete(self) {
            let result = (self.job)();
            let _ = self.result.send(Ok(result));
        }

        fn fail(self) {
            let _ = self.result.send(Err(BlockingExecutorError));
        }
    }

    fn controlled_scheduler() -> (EncodingScheduler, mpsc::UnboundedReceiver<PendingExecution>) {
        controlled_scheduler_with_timing(Timing::production())
    }

    fn controlled_scheduler_with_timing(timing: Timing) -> (EncodingScheduler, mpsc::UnboundedReceiver<PendingExecution>) {
        let (submitted_tx, submitted_rx) = mpsc::unbounded_channel();
        let scheduler = EncodingScheduler::with_dependencies(timing, Arc::new(ControlledExecutor { submitted: submitted_tx }));
        (scheduler, submitted_rx)
    }

    fn spawn_priority<T>(
        scheduler: &EncodingScheduler,
        value: T,
    ) -> JoinHandle<Result<super::EncodedGraphBody, EncodingSchedulerError>>
    where
        T: Serialize + Send + 'static,
    {
        let scheduler = scheduler.clone();
        tokio::spawn(async move { scheduler.encode_priority(value, CancellationToken::new()).await })
    }

    fn spawn_historical<T>(
        scheduler: &EncodingScheduler,
        value: T,
    ) -> JoinHandle<Result<super::EncodedGraphBody, EncodingSchedulerError>>
    where
        T: Serialize + Send + 'static,
    {
        let reservation = scheduler.try_reserve_historical().expect("historical queue position is available");
        tokio::spawn(async move { reservation.encode(value, CancellationToken::new()).await })
    }

    async fn next_execution(submitted: &mut mpsc::UnboundedReceiver<PendingExecution>) -> PendingExecution {
        submitted.recv().await.expect("an admitted job reaches the controlled executor")
    }

    #[derive(Clone)]
    struct TaggedValue {
        tag: &'static str,
        observed: mpsc::UnboundedSender<&'static str>,
    }

    impl Serialize for TaggedValue {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            self.observed.send(self.tag).expect("tag observer remains installed");
            serializer.serialize_str(self.tag)
        }
    }

    #[tokio::test]
    async fn priority_jobs_precede_queued_historical_work_and_historical_activity_is_capped() {
        let (scheduler, mut submitted) = controlled_scheduler();
        let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();

        let historical_one = spawn_historical(&scheduler, TaggedValue { tag: "historical-active-1", observed: observed_tx.clone() });
        let historical_two = spawn_historical(&scheduler, TaggedValue { tag: "historical-active-2", observed: observed_tx.clone() });
        let mut active = vec![next_execution(&mut submitted).await, next_execution(&mut submitted).await];

        let historical_queued = spawn_historical(&scheduler, TaggedValue { tag: "historical-queued", observed: observed_tx.clone() });
        tokio::task::yield_now().await;
        assert!(submitted.try_recv().is_err(), "the historical sublimit keeps the third job queued");

        let priority_one = spawn_priority(&scheduler, TaggedValue { tag: "priority-active-1", observed: observed_tx.clone() });
        let priority_two = spawn_priority(&scheduler, TaggedValue { tag: "priority-active-2", observed: observed_tx.clone() });
        active.push(next_execution(&mut submitted).await);
        active.push(next_execution(&mut submitted).await);
        assert_eq!(active.len(), MAX_GRAPH_ENCODING_JOBS);
        assert_eq!(MAX_HISTORICAL_ENCODING_JOBS, 2);

        let priority_queued = spawn_priority(&scheduler, TaggedValue { tag: "priority-queued", observed: observed_tx.clone() });
        active.pop().expect("one active job").complete();

        let next = next_execution(&mut submitted).await;
        next.complete();
        assert!(
            observed_rx.recv().await.expect("completed active tag").starts_with("priority-active-"),
            "the released active job is one of the priority jobs"
        );
        assert_eq!(observed_rx.recv().await, Some("priority-queued"));

        for execution in active {
            execution.complete();
        }
        next_execution(&mut submitted).await.complete();

        for task in [historical_one, historical_two, historical_queued, priority_one, priority_two, priority_queued] {
            task.await.expect("encoding task joins").expect("encoding succeeds");
        }
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    #[tokio::test]
    async fn queue_capacity_counts_reservations_and_releases_dropped_reservations() {
        let (scheduler, mut submitted) = controlled_scheduler();
        let mut reservations = Vec::new();
        for _ in 0..MAX_QUEUED_GRAPH_ENCODING_JOBS {
            reservations.push(scheduler.try_reserve_historical().expect("bounded reservation is admitted"));
        }
        assert!(matches!(scheduler.try_reserve_historical(), Err(EncodingSchedulerError::AdmissionSaturated)));

        let priority = spawn_priority(&scheduler, "active-capacity-is-separate");
        next_execution(&mut submitted).await.complete();
        priority.await.expect("priority task joins").expect("free active capacity bypasses full queued capacity");

        reservations.pop();
        reservations.push(scheduler.try_reserve_historical().expect("dropping a reservation restores capacity"));
        drop(reservations);
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    struct ManualClock {
        sleeps: mpsc::UnboundedSender<(Duration, tokio::sync::oneshot::Sender<()>)>,
    }

    #[async_trait]
    impl Clock for ManualClock {
        async fn sleep(&self, duration: Duration) {
            let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
            self.sleeps.send((duration, resume_tx)).expect("manual clock observer remains installed");
            let _ = resume_rx.await;
        }
    }

    struct IdentityJitter;

    impl Jitter for IdentityJitter {
        fn apply(&self, nominal: Duration) -> Duration {
            nominal
        }
    }

    #[tokio::test]
    async fn queued_job_times_out_without_entering_the_blocking_executor() {
        let (sleep_tx, mut sleep_rx) = mpsc::unbounded_channel();
        let timing = Timing::new(Arc::new(ManualClock { sleeps: sleep_tx }), Arc::new(IdentityJitter));
        let (scheduler, mut submitted) = controlled_scheduler_with_timing(timing);

        let mut active_tasks = Vec::new();
        let mut active_executions = Vec::new();
        for index in 0..MAX_GRAPH_ENCODING_JOBS {
            active_tasks.push(spawn_priority(&scheduler, format!("active-{index}")));
            active_executions.push(next_execution(&mut submitted).await);
        }
        let queued = spawn_priority(&scheduler, "queued");
        let (duration, resume) = sleep_rx.recv().await.expect("queued job installs its deadline");
        assert_eq!(duration, GRAPH_ENCODING_QUEUE_TIMEOUT);
        resume.send(()).expect("queued deadline is released");
        assert_eq!(queued.await.expect("queued task joins"), Err(EncodingSchedulerError::QueueTimeout));
        assert!(submitted.try_recv().is_err(), "timed-out work never reaches the executor");

        for execution in active_executions {
            execution.complete();
        }
        for task in active_tasks {
            task.await.expect("active task joins").expect("active encoding succeeds");
        }
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    #[tokio::test]
    async fn cancellation_removes_queued_work_and_abandons_active_output() {
        let (scheduler, mut submitted) = controlled_scheduler();
        let mut active_tasks = Vec::new();
        let mut active_executions = Vec::new();
        for index in 0..MAX_GRAPH_ENCODING_JOBS {
            active_tasks.push(spawn_priority(&scheduler, format!("active-{index}")));
            active_executions.push(next_execution(&mut submitted).await);
        }

        let queued_token = CancellationToken::new();
        let queued_scheduler = scheduler.clone();
        let queued_token_for_task = queued_token.clone();
        let queued = tokio::spawn(async move { queued_scheduler.encode_priority("queued", queued_token_for_task).await });
        tokio::task::yield_now().await;
        queued_token.cancel();
        assert_eq!(queued.await.expect("queued request task joins"), Err(EncodingSchedulerError::Cancelled));

        let active_token = CancellationToken::new();
        let cancelled_active_scheduler = scheduler.clone();
        let active_token_for_task = active_token.clone();
        let cancelled_active =
            tokio::spawn(async move { cancelled_active_scheduler.encode_priority("cancelled-active", active_token_for_task).await });
        active_executions.pop().expect("one active execution").complete();
        let cancelled_execution = next_execution(&mut submitted).await;

        active_token.cancel();
        assert_eq!(cancelled_active.await.expect("active request task joins"), Err(EncodingSchedulerError::Cancelled));

        let shutdown_scheduler = scheduler.clone();
        let shutdown = tokio::spawn(async move { shutdown_scheduler.shutdown().await });
        tokio::task::yield_now().await;
        assert!(!shutdown.is_finished(), "active blocking work remains scheduler-owned after request cancellation");
        for execution in active_executions {
            execution.complete();
        }
        cancelled_execution.complete();
        for task in active_tasks {
            let _ = task.await.expect("active task joins");
        }
        shutdown.await.expect("shutdown task joins").expect("scheduler drains cancelled active work");
        assert!(submitted.try_recv().is_err(), "cancelled queued work never reaches the blocking executor");
    }

    #[tokio::test]
    async fn shutdown_cancels_queued_and_active_waiters_but_joins_running_blocking_work() {
        let (scheduler, mut submitted) = controlled_scheduler();
        let mut active = Vec::new();
        let mut executions = Vec::new();
        for index in 0..MAX_GRAPH_ENCODING_JOBS {
            active.push(spawn_priority(&scheduler, format!("active-{index}")));
            executions.push(next_execution(&mut submitted).await);
        }
        let queued = spawn_priority(&scheduler, "queued");
        tokio::task::yield_now().await;

        let shutdown_scheduler = scheduler.clone();
        let shutdown = tokio::spawn(async move { shutdown_scheduler.shutdown().await });
        for task in active {
            assert_eq!(task.await.expect("active waiter joins"), Err(EncodingSchedulerError::ShuttingDown));
        }
        assert_eq!(queued.await.expect("queued waiter joins"), Err(EncodingSchedulerError::ShuttingDown));
        assert!(matches!(scheduler.try_reserve_historical(), Err(EncodingSchedulerError::ShuttingDown)));
        assert_eq!(scheduler.encode_priority("rejected", CancellationToken::new()).await, Err(EncodingSchedulerError::ShuttingDown));
        assert!(!shutdown.is_finished(), "scheduler shutdown waits for active blocking calls");

        for execution in executions {
            execution.complete();
        }
        shutdown.await.expect("shutdown task joins").expect("scheduler shutdown succeeds");
        scheduler.shutdown().await.expect("scheduler shutdown is idempotent");
        assert!(submitted.try_recv().is_err(), "queued and rejected work never reaches the executor");
    }

    #[tokio::test]
    async fn blocking_executor_failure_is_typed_and_releases_capacity() {
        let (scheduler, mut submitted) = controlled_scheduler();
        let failed = spawn_priority(&scheduler, "failed");
        next_execution(&mut submitted).await.fail();
        assert_eq!(failed.await.expect("failed task joins"), Err(EncodingSchedulerError::BlockingExecutorFailed));

        let replacement = spawn_priority(&scheduler, "replacement");
        next_execution(&mut submitted).await.complete();
        replacement.await.expect("replacement joins").expect("released capacity is reusable");
        scheduler.shutdown().await.expect("scheduler shuts down");
    }

    #[tokio::test]
    async fn orchestration_task_panic_is_retained_for_observation_and_shutdown() {
        let scheduler = EncodingScheduler::with_dependencies(Timing::production(), Arc::new(PanickingExecutor));
        let completion = scheduler.completion();
        let request = spawn_priority(&scheduler, "panic");

        assert_eq!(completion.await, Err(EncodingSchedulerError::WorkerFailed));
        assert_eq!(request.await.expect("request owner joins"), Err(EncodingSchedulerError::ShuttingDown));
        assert_eq!(scheduler.shutdown().await, Err(EncodingSchedulerError::WorkerFailed));
        assert_eq!(scheduler.shutdown().await, Err(EncodingSchedulerError::WorkerFailed));
    }

    struct ThreadRecordingValue {
        serialization_thread: Arc<Mutex<Option<ThreadId>>>,
    }

    impl Serialize for ThreadRecordingValue {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            *self.serialization_thread.lock().expect("thread record lock") = Some(std::thread::current().id());
            serializer.serialize_str("production-adapter")
        }
    }

    #[tokio::test]
    async fn production_adapter_serializes_on_tokios_blocking_executor() {
        let scheduler = EncodingScheduler::production();
        let caller_thread = std::thread::current().id();
        let serialization_thread = Arc::new(Mutex::new(None));

        scheduler
            .encode_priority(ThreadRecordingValue { serialization_thread: serialization_thread.clone() }, CancellationToken::new())
            .await
            .expect("production encoding succeeds");

        assert_ne!(*serialization_thread.lock().expect("thread record lock"), Some(caller_thread));
        scheduler.shutdown().await.expect("scheduler shuts down");
    }
}
