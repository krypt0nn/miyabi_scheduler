pub mod worker;
pub mod context;
pub mod scope;
pub mod scheduler;

/// General task type which can be executed by the worker.
pub type Task = Box<dyn FnOnce(context::Context) + Send + 'static>;

pub mod prelude {
    pub use super::Task;

    pub use super::worker::Worker;
    pub use super::context::Context;

    pub use super::scope::{
        Scope,
        ScopeReport
    };

    pub use super::scheduler::Scheduler;
}
