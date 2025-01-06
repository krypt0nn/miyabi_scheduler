use std::cell::Cell;

use flume::{Sender, SendError};

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
    id: u64,
    name: Option<String>,
    sender: &'context Sender<SchedulerScopeMessage>,
    last_total: Cell<Option<u64>>
}

impl<'context> Scope<'context> {
    /// Create new scope with given name and sender.
    pub fn create(name: Option<String>, sender: &'context Sender<SchedulerScopeMessage>) -> Self {
        let id = fastrand::u64(..);

        let _ = sender.send(SchedulerScopeMessage::Create {
            id,
            name: name.clone()
        });

        Self {
            id: fastrand::u64(..),
            name,
            sender,
            last_total: Cell::new(None)
        }
    }

    /// Report back progress update within the current scope.
    ///
    /// Reports are handled by the scheduler. Return error if
    /// report couldn't be sent. This generally means that the
    /// scheduler is closed.
    pub fn progress(&self, current: u64, total: u64) -> Result<(), SendError<SchedulerScopeMessage>> {
        self.last_total.set(Some(total));

        self.sender.send(ScopeReport::Progress {
            id: self.id,
            name: self.name.clone(),
            current,
            total
        }.into())
    }

    /// Report back status update within the current scope.
    ///
    /// Reports are handled by the scheduler. Return error if
    /// report couldn't be sent. This generally means that the
    /// scheduler is closed.
    pub fn status(&self, status: impl ToString) -> Result<(), SendError<SchedulerScopeMessage>> {
        self.sender.send(ScopeReport::Status {
            id: self.id,
            name: self.name.clone(),
            status: status.to_string()
        }.into())
    }

    /// Report finished progress of the scope.
    ///
    /// Equal to running `progress(total, total)`.
    pub fn finish(mut self, finished: u64) {
        self.last_total.set(None);

        let name = self.name.take();

        let _ = self.sender.send(ScopeReport::Progress {
            id: self.id,
            name: name.clone(),
            current: finished,
            total: finished
        }.into());

        let _ = self.sender.send(SchedulerScopeMessage::Drop {
            id: self.id,
            name
        });
    }
}

impl Drop for Scope<'_> {
    fn drop(&mut self) {
        let name = self.name.take();

        if let Some(last_total) = self.last_total.take() {
            let _ = self.sender.send(ScopeReport::Progress {
                id: self.id,
                name: name.clone(),
                current: last_total,
                total: last_total
            }.into());
        }

        let _ = self.sender.send(SchedulerScopeMessage::Drop {
            id: self.id,
            name
        });
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Report sent from the task's scope.
pub enum ScopeReport {
    Progress {
        id: u64,
        name: Option<String>,
        current: u64,
        total: u64
    },

    Status {
        id: u64,
        name: Option<String>,
        status: String
    }
}

impl ScopeReport {
    #[inline]
    /// Get id of the report's scope.
    pub const fn id(&self) -> u64 {
        match self {
            Self::Progress { id, .. } |
            Self::Status { id, .. } => *id
        }
    }

    #[inline]
    /// Get name of the report's scope.
    pub const fn name(&self) -> Option<&String> {
        match self {
            Self::Progress { name, .. } |
            Self::Status { name, .. } => name.as_ref()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SchedulerScopeMessage {
    Create {
        id: u64,
        name: Option<String>
    },

    Drop {
        id: u64,
        name: Option<String>
    },

    Update(ScopeReport)
}

impl From<ScopeReport> for SchedulerScopeMessage {
    #[inline]
    fn from(value: ScopeReport) -> Self {
        Self::Update(value)
    }
}
