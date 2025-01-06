use std::thread::JoinHandle;

use flume::{Sender, SendError};

use super::prelude::*;

#[derive(Debug)]
/// Worker is a background thread executer that can listen to
/// incoming tasks, store them in a local queue and process one
/// by another. Size of the queue is configurable.
///
/// You generally want to use `Scheduler` instead of individual workers.
///
/// ## Example
///
/// ```rust
/// use miyabi_scheduler::prelude::*;
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
    task_sender: Sender<(Context, Task)>,
    handle: JoinHandle<()>
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

        let handle = std::thread::spawn(move || {
            while let Ok((context, task)) = task_listener.recv() {
                task(context);
            }
        });

        Self {
            task_sender,
            handle
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

    #[inline]
    /// Check if worker's thread can't receive tasks anymore.
    /// This means that the connected tasks sending channel
    /// was closed.
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    #[inline]
    /// Join worker's thread.
    pub fn join(self) -> std::thread::Result<()> {
        drop(self.task_sender);

        self.handle.join()
    }
}
