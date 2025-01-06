use flume::{Sender, Receiver, SendError};

use super::scope::SchedulerScopeMessage;

use super::prelude::*;

#[derive(Debug, Clone)]
/// Context of the scheduler which can be used to push
/// new tasks to the execution queue or create new
/// progress reporting scopes.
///
/// ## Example
///
/// ```
/// use miyabi_scheduler::prelude::*;
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
    scope_sender: Sender<SchedulerScopeMessage>
}

impl Context {
    /// Create new context and return task and lock listeners,
    /// and listener of the scope progress reports.
    pub fn new() -> (Self, Receiver<Task>, Receiver<Task>, Receiver<SchedulerScopeMessage>) {
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
        callback(Scope::create(None, &self.scope_sender))
    }

    /// Create named progress scope and execute given callback in it.
    ///
    /// Scopes are meant to differentiate progress reports
    /// between different tasks. All the reports are processed
    /// in the scheduler.
    pub fn named_scope<T>(&self, name: impl ToString, callback: impl FnOnce(Scope) -> T) -> T {
        callback(Scope::create(Some(name.to_string()), &self.scope_sender))
    }
}
