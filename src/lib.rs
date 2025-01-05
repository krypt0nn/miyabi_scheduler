use std::cell::Cell;

use flume::{Sender, Receiver, SendError};

/// General task type which can be executed by the worker.
pub type Task = Box<dyn FnOnce(Context) + Send + 'static>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Report sent from the task's scope.
pub enum ScopeReport {
    Progress {
        name: Option<String>,
        current: u64,
        total: u64
    },

    Status {
        name: Option<String>,
        status: String
    }
}

impl ScopeReport {
    #[inline]
    /// Get name of the report's scope.
    pub const fn name(&self) -> Option<&String> {
        match self {
            Self::Progress { name, .. } |
            Self::Status { name, .. } => name.as_ref()
        }
    }
}

#[derive(Debug, Clone)]
/// Progress scope of some task within the worker.
///
/// This struct is made by the context and can be used
/// to send progress reports to the scheduler which
/// would be processed by the updates handler.
///
/// ## Example
///
/// ```ignore
/// // let scope: Scope = ...;
///
/// // Report status of the current job.
/// scope.status("Processing...").unwrap();
///
/// // Do the job and report its completion progress.
/// for i in 1..=100 {
///     std::thread::sleep(std::time::Duration::from_millis(100));
///
///     scope.progress(i, 100);
/// }
///
/// // This one is not needed, just showing that you can do this.
/// // Scope remembers total value of the last sent progress update
/// // and sends final update report in `drop` method call.
/// scope.finish(100);
/// ```
pub struct Scope<'context> {
    name: Option<String>,
    sender: &'context Sender<ScopeReport>,
    last_total: Cell<Option<u64>>
}

impl Scope<'_> {
    /// Report back progress update within the current scope.
    ///
    /// Reports are handled by the scheduler. Return error if
    /// report couldn't be sent. This generally means that the
    /// scheduler is closed.
    pub fn progress(&self, current: u64, total: u64) -> Result<(), SendError<ScopeReport>> {
        self.last_total.set(Some(total));

        self.sender.send(ScopeReport::Progress {
            name: self.name.clone(),
            current,
            total
        })
    }

    /// Report back status update within the current scope.
    ///
    /// Reports are handled by the scheduler. Return error if
    /// report couldn't be sent. This generally means that the
    /// scheduler is closed.
    pub fn status(&self, status: impl ToString) -> Result<(), SendError<ScopeReport>> {
        self.sender.send(ScopeReport::Status {
            name: self.name.clone(),
            status: status.to_string()
        })
    }

    /// Report finished progress of the scope.
    ///
    /// Equal to running `progress(total, total)`.
    pub fn finish(mut self, finished: u64) {
        self.last_total.set(None);

        let _ = self.sender.send(ScopeReport::Progress {
            name: self.name.take(),
            current: finished,
            total: finished
        });
    }
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        if let Some(last_total) = self.last_total.take() {
            let _ = self.sender.send(ScopeReport::Progress {
                name: self.name.take(),
                current: last_total,
                total: last_total
            });
        }
    }
}

#[derive(Debug, Clone)]
/// Context of the scheduler which can be used to push
/// new tasks to the execution queue or create new
/// progress reporting scopes.
///
/// ## Example
///
/// ```
/// use miyabi_scheduler::*;
///
/// // Create new context and drop all the listeners meant for the scheduler.
/// let (context, _, _, _) = Context::new();
///
/// // Schedule some task.
/// context.schedule(Box::new(|new_context: Context| {
///     // Create new progress scope.
///     let result = new_context.scope(|scope| {
///         // Send this status update to the context's scope listener.
///         // We dropped it, thus silencing the error here.
///         let _ = scope.status("Doing something...");
///
///         // Return value from the progress scope.
///         "Hello, World!"
///     });
///
///     // Print this returned value.
///     dbg!(result);
/// }));
/// ```
pub struct Context {
    task_sender: Sender<Task>,
    lock_sender: Sender<Task>,
    scope_sender: Sender<ScopeReport>
}

impl Context {
    /// Create new context and return task and lock listeners,
    /// and listener of the scope progress reports.
    pub fn new() -> (Self, Receiver<Task>, Receiver<Task>, Receiver<ScopeReport>) {
        let (task_sender, task_listener) = flume::bounded(1);
        let (lock_sender, lock_listener) = flume::bounded(1);
        let (scope_sender, scope_listener) = flume::unbounded();

        let context = Self {
            task_sender,
            lock_sender,
            scope_sender
        };

        (context, task_listener, lock_listener, scope_listener)
    }

    /// Wait for a free worker in the connected scheduler
    /// and assign it the given task.
    ///
    /// This is a blocking method. Closes when spare worker
    /// is found and the task was scheduled.
    pub fn schedule(&self, task: Task) -> Result<(), SendError<Task>> {
        self.task_sender.send(task)
    }

    /// Lock connected scheduler and assign all the spare
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
        self.lock_sender.send(task)
    }

    /// Create progress scope and execute given callback in it.
    ///
    /// Scopes are meant to differentiate progress reports
    /// between different tasks. All the reports are processed
    /// in the scheduler.
    pub fn scope<T>(&self, callback: impl FnOnce(Scope) -> T) -> T {
        callback(Scope {
            name: None,
            sender: &self.scope_sender,
            last_total: Cell::new(None)
        })
    }

    /// Create named progress scope and execute given callback in it.
    ///
    /// Scopes are meant to differentiate progress reports
    /// between different tasks. All the reports are processed
    /// in the scheduler.
    pub fn named_scope<T>(&self, name: impl ToString, callback: impl FnOnce(Scope) -> T) -> T {
        callback(Scope {
            name: Some(name.to_string()),
            sender: &self.scope_sender,
            last_total: Cell::new(None)
        })
    }
}

#[derive(Debug, Clone)]
/// Worker is a background thread executer that can listen to
/// incoming tasks, store them in a local queue and process one
/// by another. Size of the queue is configurable.
///
/// You generally want to use `Scheduler` instead of individual workers.
///
/// ## Example
///
/// ```rust
/// use miyabi_scheduler::*;
///
/// // Create new context and drop all the listeners.
/// let (context, _, _, _) = Context::new();
///
/// // Create new worker which can store up to 10 tasks.
/// let worker = Worker::new(10);
///
/// // Schedule 10 tasks to be run in the worker,
/// // then schedule other tasks one by one when
/// // first 10 are slowly finishing.
/// for i in 0..20 {
///     worker.lock_on(context.clone(), Box::new(|_| {
///         // Do some job
///         std::thread::sleep(std::time::Duration::from_millis(100));
///     }));
/// }
/// ```
pub struct Worker {
    task_sender: Sender<(Context, Task)>
}

impl Worker {
    /// Create new worker that infinitely listens in background
    /// for incoming tasks and processes them.
    ///
    /// `queue_size` specifies amount of tasks that can be scheduled
    /// for the execution. Larger value means larger waste of RAM.
    /// There's no point in it besides minimising potential amount
    /// of time waste on waiting for a new task.
    pub fn new(queue_size: usize) -> Self {
        let (task_sender, task_listener) = flume::bounded::<(Context, Task)>(queue_size);

        std::thread::spawn(move || {
            while let Ok((context, task)) = task_listener.recv() {
                task(context);
            }
        });

        Self {
            task_sender
        }
    }

    /// Lock current thread until given task is scheduled to the worker.
    ///
    /// Note that the end of this method execution doesn't mean that the underlying
    /// task has been executed as well. It means that it was scheduled for execution.
    pub fn lock_on(&self, context: Context, task: Task) -> Result<(), SendError<(Context, Task)>> {
        self.task_sender.send((context, task))
    }

    #[allow(clippy::type_complexity)]
    /// Try to schedule given task to the worker.
    ///
    /// This method will put the task to the worker's queue
    /// or close immediately if the queue is already full,
    /// returning values which were given to it.
    pub fn try_schedule(&self, context: Context, task: Task) -> Result<Option<(Context, Task)>, SendError<(Context, Task)>> {
        if !self.task_sender.is_full() {
            self.task_sender.send((context, task))?;

            return Ok(None);
        }

        Ok(Some((context, task)))
    }
}

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
/// use miyabi_scheduler::*;
///
/// // Scheduler with 2 workers and 4 tasks queue size in each.
/// let scheduler = Scheduler::new(2, 4);
///
/// // Run scheduler updates in background.
/// let (context, _) = scheduler.demonize(|_| {});
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
    scope_listener: Receiver<ScopeReport>
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
            scope_listener
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

    /// Create new background thread, run scheduler updates there
    /// and return context of the current scheduler to spawn new tasks,
    /// and an abort function which, when called, will stop the thread.
    pub fn demonize(self, mut scope_handler: impl FnMut(ScopeReport) + Send + 'static) -> (Context, impl FnOnce()) {
        let Self {
            workers,
            context,
            task_listener,
            lock_listener,
            scope_listener
        } = self;

        let demon_context = context.clone();

        let (send, recv) = flume::bounded(1);

        let abort = move || {
            let _ = send.try_send(());
        };

        std::thread::spawn(move || {
            loop {
                if let Ok(()) = recv.try_recv() {
                    break;
                }

                if Self::update_for_given(&demon_context, &task_listener, &lock_listener, &workers, &mut scope_handler).is_err() {
                    break;
                }

                while let Ok(report) = scope_listener.try_recv() {
                    scope_handler(report);
                }
            }
        });

        (context, abort)
    }

    /// Try to read incoming task from the contexts
    /// and schedule it to the worker or execute
    /// on the current thread. You'd normally want to
    /// put this method's call in a background thread
    /// in an infinite loop with some custom exit check
    /// because there's no mechanism which would be used
    /// by the scheduler to know that you don't want to schedule
    /// any more tasks to it in any future.
    pub fn update(&self, mut scope_handler: impl FnMut(ScopeReport)) -> Result<(), SendError<(Context, Task)>> {
        // Listen for the task and schedule its processing.
        Self::update_for_given(&self.context, &self.task_listener, &self.lock_listener, &self.workers, &mut scope_handler)?;

        // Handle all the reports generated by the task.
        while let Ok(report) = self.scope_listener.try_recv() {
            scope_handler(report);
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
        scope_handler: &mut impl FnMut(ScopeReport)
    ) -> Result<(), SendError<(Context, Task)>> {
        /// Handle lock task with given workers and scope handler.
        fn handle_lock_task(
            workers: &[Worker],
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
                Scheduler::update_for_given(&new_context, &new_task_listener, &new_lock_listener, workers, scope_handler)?;

                // Handle all the reports generated by the tasks.
                while let Ok(report) = new_scope_listener.try_recv() {
                    scope_handler(report);
                }
            }

            // Drop the context and listen for all the scope reports
            // until the sender is dropped by the task.
            drop(new_context);

            while let Ok(report) = new_scope_listener.recv() {
                scope_handler(report);
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
            handle_lock_task(workers, scope_handler, lock_task)?;

            // Process following locks and tasks.
            //
            // We're required to do it here because we can't just schedule them
            // to be executed later using the context due to:
            //
            // 1. Requirement to preserve the tasks execution order.
            // 2. The fact that the context's buffer is limited to only 1 task at a time.

            for lock_task in lock_tasks.drain(..) {
                handle_lock_task(workers, scope_handler, lock_task)?;
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
}

#[test]
fn test_scheduling() {
    let scheduler = Scheduler::new(1, 4);

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
