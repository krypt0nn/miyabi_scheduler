use std::collections::HashMap;
use std::time::{Instant, Duration};

use flume::{Receiver, SendError};
use intmap::IntMap;

use super::scope::SchedulerScopeMessage;

use super::prelude::*;

// Do not implement Clone for the scheduler to prevent
// all the possible problems with workers task sync
// between made copies of the struct.

#[derive(Debug)]
/// Scheduler keeps track of all the workers and their queues,
/// listens for incoming tasks and updates from the progress scopes,
/// allows you to process "exclusive" tasks - ones which steal
/// all the available workers until their execution is finished.
/// This is the main reason I've made this library. The use case is
/// when you have both large and small tasks, and large task can
/// produce smaller ones. You want to process the large task and all
/// the smaller ones which were generated by it, and only then continue
/// working on the normal tasks.
///
/// ## Example
///
/// ```
/// use miyabi_scheduler::prelude::*;
///
/// // Scheduler with 2 workers and 4 tasks queue size in each.
/// let scheduler = Scheduler::new(2, 4);
///
/// // Obtain the scheduler's context to spawn new tasks.
/// let context = scheduler.context();
///
/// // Run scheduler updates in background.
/// scheduler.demonize(|_| {});
///
/// // Create an exclusive task and lock scheduler's workers
/// // from executing other tasks until this exclusive one
/// // finishes its work.
/// context.schedule_exclusive(Box::new(|context| {
///     println!("Task 1");
///
///     // Schedule locked workers to execute these tasks.
///     context.schedule(Box::new(|_| {
///         println!("Task 2");
///     })).unwrap();
///
///     context.schedule(Box::new(|_| {
///         println!("Task 3");
///     })).unwrap();
/// })).unwrap();
///
/// // Schedule to run these tasks after the exclusive one is finished.
/// context.schedule(Box::new(|_| {
///     println!("Task 4");
/// })).unwrap();
///
/// context.schedule(Box::new(|_| {
///     println!("Task 5");
/// })).unwrap();
/// ```
pub struct Scheduler {
    workers: Vec<Worker>,
    context: Context,
    task_listener: Receiver<Task>,
    lock_listener: Receiver<Task>,
    scope_listener: Receiver<SchedulerScopeMessage>,
    scope_records: HashMap<Option<String>, NamedScopeRecords>
}

impl Scheduler {
    /// Create new scheduler with given amount of workers for tasks execution
    /// and amount of tasks each of this workers can store in their own queue.
    pub fn new(workers_num: usize, worker_queue_size: usize) -> Self {
        let mut workers = Vec::with_capacity(workers_num);

        for _ in 0..workers_num {
            workers.push(Worker::new(worker_queue_size));
        }

        let (
            context,
            task_listener,
            lock_listener,
            scope_listener
        ) = Context::new();

        Self {
            workers,
            context,
            task_listener,
            lock_listener,
            scope_listener,
            scope_records: HashMap::new()
        }
    }

    #[inline]
    /// Get owned copy of the current scheduler's context.
    /// Contexts can be used to assign new tasks to the scheduler
    /// and report their progress.
    pub fn context(&self) -> Context {
        self.context.clone()
    }

    #[inline]
    /// Wait for a free worker in the scheduler and assign it
    /// the given task.
    ///
    /// This is a blocking method. Closes when spare worker
    /// is found and the task was scheduled.
    pub fn schedule(&self, task: Task) -> Result<(), SendError<Task>> {
        self.context.schedule(task)
    }

    #[inline]
    /// Lock the scheduler and assign all the spare
    /// workers to start executing given task and its children.
    ///
    /// This is a blocking method. Closes when there's no other
    /// locking tasks scheduled.
    ///
    /// Locked tasks spawn new threads for their exclusive execution.
    /// This is done to avoid complete locking of the scheduler
    /// when amount of locks is equal to the amount of available
    /// workers.
    pub fn schedule_exclusive(&self, task: Task) -> Result<(), SendError<Task>> {
        self.context.schedule_exclusive(task)
    }

    #[inline]
    /// Check if there are any alive contexts of the current scheduler,
    /// or if some tasks are buffered for execution, or if there are
    /// scope reports which were not received and processed yet.
    ///
    /// Alive contexts can potentially can be used to spawn new
    /// tasks, thus scheduler should be alive to process them.
    ///
    /// When `false` is returned, unless directly or if `context` method
    /// is called, there's no way to spawn new tasks in the scheduler, thus
    /// it's not alive. *It does not means you should stop updating it*.
    /// It only means that you should write your own checks to decide
    /// what to do now.
    ///
    /// ## Example
    ///
    /// ```
    /// use miyabi_scheduler::prelude::*;
    ///
    /// // Create new scheduler with 1 worker.
    /// let mut scheduler = Scheduler::new(1, 2);
    ///
    /// // Schedule it to execute this task (wait 1 second).
    /// scheduler.schedule(Box::new(|_| {
    ///     std::thread::sleep(std::time::Duration::from_secs(1));
    /// })).unwrap();
    ///
    /// // Update scheduler while it's alive.
    /// while scheduler.is_alive() {
    ///     scheduler.update(|_| {}).unwrap();
    /// }
    ///
    /// println!("All the tasks finished and I did not write any code to make new ones");
    /// ```
    pub fn is_alive(&self) -> bool {
        // If there's an alive context out of the current struct or any number of buffered tasks.
        self.task_listener.sender_count() > 1 || !self.task_listener.is_empty() ||

        // If there's an alive context out of the current struct or any number of buffered lock tasks.
        self.lock_listener.sender_count() > 1 || !self.lock_listener.is_empty() ||

        // If there are some scope reports awaiting for processing.
        !self.scope_listener.is_empty()
    }

    /// Create new background thread, run scheduler updates there
    /// and return context of the current scheduler to spawn new tasks,
    /// and an abort function which, when called, will stop the thread.
    pub fn demonize(self, mut scope_handler: impl FnMut(ScopeReport) + Send + 'static) -> impl FnOnce() {
        let Self {
            workers,
            context,
            task_listener,
            lock_listener,
            scope_listener,
            mut scope_records
        } = self;

        let (send, recv) = flume::bounded(1);

        let abort = move || {
            let _ = send.try_send(());
        };

        std::thread::spawn(move || {
            loop {
                if let Ok(()) = recv.try_recv() {
                    break;
                }

                let result = Self::update_for_given(
                    &context,
                    &task_listener,
                    &lock_listener,
                    &workers,
                    &mut scope_records,
                    &mut scope_handler
                );

                if result.is_err() {
                    break;
                }

                while let Ok(message) = scope_listener.try_recv() {
                    Self::handle_scope_message(&mut scope_records, &mut scope_handler, message);
                }
            }
        });

        abort
    }

    #[inline]
    /// Drop scheduler's context and listeners
    /// and join all its workers' threads.
    ///
    /// Note that if the scheduler is alive (there are
    /// tasks in the queue or if new ones can be spawned)
    /// then this method will most likely block your thread
    /// forever because workers will never close until
    /// all the contexts are dropped.
    ///
    /// ## Example
    ///
    /// ```
    /// use miyabi_scheduler::prelude::*;
    ///
    /// // Create new scheduler with 1 worker.
    /// let mut scheduler = Scheduler::new(1, 1);
    ///
    /// // Schedule task execution (wait for 1 second).
    /// scheduler.schedule(Box::new(|_| {
    ///     std::thread::sleep(std::time::Duration::from_secs(1));
    /// })).unwrap();
    ///
    /// // Assign task to the worker.
    /// scheduler.update(|_| {}).unwrap();
    ///
    /// // Join the scheduler (wait until all the workers are closed).
    /// // We will wait until all the
    /// scheduler.join();
    /// ```
    pub fn join(self) {
        let Self { mut workers, .. } = self;

        for worker in workers.drain(..) {
            let _ = worker.join();
        }
    }

    /// Try to read incoming task from the contexts
    /// and schedule it to the worker or execute
    /// on the current thread. You'd normally want to
    /// put this method's call in a background thread
    /// in an infinite loop with some custom exit check
    /// because there's no mechanism which would be used
    /// by the scheduler to know that you don't want to schedule
    /// any more tasks to it in any future.
    pub fn update(&mut self, mut scope_handler: impl FnMut(ScopeReport)) -> Result<(), SendError<(Context, Task)>> {
        // Listen for the task and schedule its processing.
        Self::update_for_given(
            &self.context,
            &self.task_listener,
            &self.lock_listener,
            &self.workers,
            &mut self.scope_records,
            &mut scope_handler
        )?;

        // Handle all the reports generated by the task.
        while let Ok(message) = self.scope_listener.try_recv() {
            Self::handle_scope_message(&mut self.scope_records, &mut scope_handler, message);
        }

        Ok(())
    }

    /// Private funciton to recursively process scheduled tasks.
    ///
    /// This can be done without recursion but I would need to create
    /// vector to store stack of contexts here which is resource heavy
    /// considering I would need to do this every function call, and
    /// this function should be called in a loop thousands of times.
    fn update_for_given(
        context: &Context,
        task_listener: &Receiver<Task>,
        lock_listener: &Receiver<Task>,
        workers: &[Worker],
        scope_records: &mut HashMap<Option<String>, NamedScopeRecords>,
        scope_handler: &mut impl FnMut(ScopeReport)
    ) -> Result<(), SendError<(Context, Task)>> {
        /// Handle lock task with given workers and scope handler.
        fn handle_lock_task(
            workers: &[Worker],
            scope_records: &mut HashMap<Option<String>, NamedScopeRecords>,
            scope_handler: &mut impl FnMut(ScopeReport),
            lock_task: Task
        ) -> Result<(), SendError<(Context, Task)>> {
            // Create new context for the in-lock tasks.
            let (
                new_context,
                new_task_listener,
                new_lock_listener,
                new_scope_listener
            ) = Context::new();

            // Spawn lock task in a new thread to not to block workers of the scheduler.
            let thread = {
                let new_context = new_context.clone();

                std::thread::spawn(move || lock_task(new_context))
            };

            // Listen for the tasks from within the lock until it's fully executed.
            while !thread.is_finished() || !new_task_listener.is_empty() || !new_lock_listener.is_empty() {
                Scheduler::update_for_given(&new_context, &new_task_listener, &new_lock_listener, workers, scope_records, scope_handler)?;

                // Handle all the reports generated by the tasks.
                while let Ok(message) = new_scope_listener.try_recv() {
                    Scheduler::handle_scope_message(scope_records, scope_handler, message);
                }
            }

            // Drop the context and listen for all the scope reports
            // until the sender is dropped by the task.
            drop(new_context);

            while let Ok(message) = new_scope_listener.recv() {
                Scheduler::handle_scope_message(scope_records, scope_handler, message);
            }

            Ok(())
        }

        /// Handle normal task with the given context and workers.
        fn handle_task(
            context: Context,
            workers: &[Worker],
            mut task: Task
        ) -> Result<(), SendError<(Context, Task)>> {
            // Prepare context for this task.
            let mut context = context.clone();

            // Iterate over all the workers we have in the scheduler.
            for worker in workers.iter().cycle() {
                // Try to schedule the task in one of the workers.
                let Some((ret_context, ret_task)) = worker.try_schedule(context, task)? else {
                    // Stop searching for a free worker.
                    break;
                };

                // Reset returned context and task to try them on the next worker.
                context = ret_context;
                task = ret_task;
            }

            Ok(())
        }

        // Read locking task if some is buffered.
        if let Ok(lock_task) = lock_listener.try_recv() {
            // Store all the tasks and locks into a vector before starting
            // processing the received lock.
            //
            // TODO: drain or try_iter?
            //
            // - With drain I immediately obtain list of tasks, but some other ones
            //   could be pending from other threads.
            //
            // - With try_iter I read all the pending tasks before processing the
            //   exclusive one.
            let mut lock_tasks = Vec::from_iter(lock_listener.drain());
            let mut tasks = Vec::from_iter(task_listener.drain());

            // Process the lock task.
            handle_lock_task(workers, scope_records, scope_handler, lock_task)?;

            // Process following locks and tasks.
            //
            // We're required to do it here because we can't just schedule them
            // to be executed later using the context due to:
            //
            // 1. Requirement to preserve the tasks execution order.
            // 2. The fact that the context's buffer is limited to only 1 task at a time.

            for lock_task in lock_tasks.drain(..) {
                handle_lock_task(workers, scope_records, scope_handler, lock_task)?;
            }

            for task in tasks.drain(..) {
                handle_task(context.clone(), workers, task)?;
            }
        }

        // Read normal task if some is buffered.
        else if let Ok(task) = task_listener.try_recv() {
            handle_task(context.clone(), workers, task)?;
        }

        Ok(())
    }

    fn handle_scope_message(
        scope_records: &mut HashMap<Option<String>, NamedScopeRecords>,
        scope_handler: &mut impl FnMut(ScopeReport),
        message: SchedulerScopeMessage
    ) {
        match message {
            SchedulerScopeMessage::Create { id, name } => {
                scope_records.entry(name)
                    .or_default()
                    .insert(id, ScopeRecord::default());
            }

            SchedulerScopeMessage::Drop { id, name } => {
                if let Some(named_scope) = scope_records.get_mut(&name) {
                    named_scope.remove(id);
                }
            }

            SchedulerScopeMessage::Update(report) => {
                if let ScopeReport::Progress { id, name, current, total } = &report {
                    if let Some(named_scope) = scope_records.get_mut(name) {
                        named_scope.update_progress(*id, *current, *total);
                    }
                }

                scope_handler(report);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
/// Summary information about a scope.
pub struct ScopeInfo {
    /// Last reported current and total scope progress.
    pub progress: Option<(u64, u64)>,

    /// Last reported scope status.
    pub status: Option<String>,

    /// Progress of the scope execution.
    /// Guaranteed to be in range `[0, 1]`.
    /// Defaults to 0.
    pub fraction: f64,

    /// Time elapsed since the first report was sent from the scope.
    /// Defaults to 0.
    pub elapsed_time: Duration,

    /// Estimation of how long it will take to finish the scope.
    /// Defaults to 0.
    pub estimate_time: Duration
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ScopeRecord {
    pub progress: Option<(u64, u64)>,
    pub status: Option<String>,
    pub created_at: Instant
}

impl ScopeRecord {
    pub fn get_summary(&self) -> ScopeInfo {
        let fraction = self.progress.map(|(current, total)| {
            if total == 0 {
                0.0
            } else if current > total {
                1.0
            } else {
                current as f64 / total as f64
            }
        }).unwrap_or(0.0);

        let elapsed_time = self.created_at.elapsed();

        let estimate_time = if fraction > 0.0 {
            let estimate_time = elapsed_time.as_millis() as f64 / fraction;

            Duration::from_millis(estimate_time as u64)
        } else {
            Duration::default()
        };

        ScopeInfo {
            progress: self.progress,
            status: self.status.clone(),
            fraction,
            elapsed_time,
            estimate_time
        }
    }
}

impl Default for ScopeRecord {
    #[inline]
    fn default() -> Self {
        Self {
            progress: None,
            status: None,
            created_at: Instant::now()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NamedScopeRecords {
    progress: Option<(u64, u64)>,
    created_at: Instant,
    records: IntMap<u64, ScopeRecord>
}

impl NamedScopeRecords {
    /// Insert record info to the current named scope.
    pub fn insert(&mut self, id: u64, record: ScopeRecord) {
        // Update named scope's current and total progress.
        if let Some(prev_record) = self.records.remove(id) {
            if let Some((current, total)) = &mut self.progress {
                // Substract previous progress of this record.
                if let Some((prev_current, prev_total)) = prev_record.progress {
                    *current -= prev_current;
                    *total -= prev_total;
                }

                // Add new progress of this record.
                if let Some((new_current, new_total)) = &record.progress {
                    *current += *new_current;
                    *total += *new_total;
                }
            } else {
                self.progress = record.progress;
            }
        }

        // Update named scope's creation time.
        if record.created_at < self.created_at {
            self.created_at = record.created_at;
        }

        // Insert the record.
        self.records.insert(id, record);
    }

    #[allow(clippy::field_reassign_with_default)]
    /// Update progress of the scope with the given id.
    pub fn update_progress(&mut self, id: u64, current: u64, total: u64) {
        if let Some(mut prev_record) = self.records.remove(id) {
            if let Some((prev_current, prev_total)) = &mut prev_record.progress {
                *prev_current = current;
                *prev_total = total;
            } else {
                prev_record.progress = Some((current, total));
            }

            self.insert(id, prev_record);
        } else {
            let mut record = ScopeRecord::default();

            record.progress = Some((current, total));

            self.insert(id, record);
        }
    }

    #[inline]
    /// Remove record from the named scope.
    ///
    /// This method will free the memory but keep
    /// already calculated summary values.
    pub fn remove(&mut self, id: u64) {
        self.records.remove(id);
    }
}

impl Default for NamedScopeRecords {
    #[inline]
    fn default() -> Self {
        Self {
            progress: None,
            created_at: Instant::now(),
            records: IntMap::new()
        }
    }
}

#[test]
fn test_scheduling() {
    let mut scheduler = Scheduler::new(1, 4);

    // Start background thread to listen for incoming tasks.
    let context = scheduler.context();

    let handle = std::thread::spawn(move || {
        let (send, recv) = flume::bounded(5);

        while !recv.is_full() {
            let send = send.clone();

            scheduler.update(move |report| {
                if let ScopeReport::Progress { total, .. } = report {
                    let _ = send.send(total);
                }
            }).unwrap();
        }

        for i in 1..=5 {
            assert_eq!(recv.try_recv(), Ok(i), "Out of order scope report");
        }
    });

    // Create an exclusive task and lock scheduler's workers
    // from executing other tasks until this exclusive one
    // finishes its work.
    context.schedule_exclusive(Box::new(|context| {
        context.scope(|scope| scope.finish(1));

        // Schedule locked workers to execute these tasks.
        context.schedule(Box::new(|context| {
            context.scope(|scope| scope.finish(2));
        })).unwrap();

        context.schedule(Box::new(|context| {
            context.scope(|scope| scope.finish(3));
        })).unwrap();
    })).unwrap();

    // Schedule to run these tasks after the exclusive one is finished.
    context.schedule(Box::new(|context| {
        context.scope(|scope| scope.finish(4));
    })).unwrap();

    context.schedule(Box::new(|context| {
        context.scope(|scope| scope.finish(5));
    })).unwrap();

    handle.join().unwrap();
}