# Miyabi Scheduler

`miyabi_scheduler` is a specific threads scheduling library for rust where
you expect to have two main types of tasks: "thin" and "thick"
(internally called "normal" and "exclusive" / "locking"). This scheduler
allows you to gain "exclusive lock" for one specific task and all the tasks
which will be spawned by it.

Normally tasks queue looks like this:

<img src="./images/normal_tasks_queue.png" />

Scheduler uses 2 workers to process them one by another, with all 3 sitting
in the buffer. However, sometimes you want to spawn new tasks while processing
the current ones! And sometimes you want these new tasks to use data owned
by its ancestor task and drop it later to not to waste user's RAM. That's what
I call an "exclusive lock" or "exclusive task": you let scheduler know that
you really want to process this one specific task using all the available
powers, and only then switch to the other tasks. That's how it looks like:

<img src="./images/tasks_queue_with_exclusive_lock.png" />

Scheduler will spawn a new thread for this one exclusive task to process it there,
and use all available workers to process all the new tasks which will be spawned
by the exclusive one. New thread is needed to ensure that exclusive tasks
will not be bounded by the amount of available workers. Otherwise, say
your scheduler has only 1 worker and you have 2 exclusive tasks: you would
have a little problem processing such a queue. Thus new thread is a necessary evil.

It's important to note that the scheduler will not be unlocked until
the exclusive task's *context* (special struct to spawn new tasks)
is dropped. So if you move it in another thread and forget to drop
at some point - you must count on your own! Make a copy of the scheduler's
original context struct if you want to spawn new tasks in it.

Sometimes your exclusive tasks can be nested. This is also handled properly
by this library, by making a recursive lock (so you can't repeat the proces
infinitely, but I really didn't want to implement some loop logic for this).
That's how it would look like:

<img src="./images/tasks_queue_with_nested_exclusive_lock.png" />

Task 1 will spawn 2, 3, and 6. Then, while processing 2, we will lock
scheduler *again* and process 4 and 5. Then, once 3's context is dropped,
unlock second lock and continue processing 6. Then unlock 2's lock and
continue processing 7 and 8.

## Example: substring search in files within a directory

Imagine we want to find a substring in any files within a directory.
Or naturally do any computation task that can easily be paralleled.
You can use any threadpool library for this, implement your own one,
or use async runtime for this goal. And of course you can use
Miyabi Scheduler! It's not what it was made for, but as a general
scheduler of course it can solve this problem.

You can run the following example with this command:

```bash
cargo run --example find_substring -- <path to a folder> 'your substring'
```

Code (from the `examples/find_substring.rs` file):

```rust
use std::io::Read;
use std::path::PathBuf;
use std::fs::File;

use miyabi_scheduler::prelude::*;

// It's important to silence all the errors or process them
// in a different place to not to crash threads where tasks
// are executed.

/// Return scheduler task to scan given folder for a substring.
fn task_scan_dir(path: PathBuf, substring: String) -> Task {
    Box::new(move |context| {
        if let Ok(entries) = path.read_dir() {
            for entry in entries.flatten() {
                let path = entry.path();

                if path.is_dir() {
                    // Schedule exclusive task for directory scanning.
                    // This will force Miyabi Scheduler to execute this function
                    // (directory scanning) in a new thread and use all the workers
                    // to process the tasks it produces (either scan other folders
                    // or scan found files).
                    //
                    // From technical perspective it's absolutely pointless.
                    // But it's an example project, so I use it just to demonstrate
                    // that this thing exists.
                    let _ = context.schedule_exclusive(task_scan_dir(path, substring.clone()));
                }

                else if path.is_file() {
                    // Schedule normal task for file reading. Not different
                    // from any other schedulers.
                    let _ = context.schedule(task_scan_file(path, substring.clone()));
                }
            }
        }
    })
}

/// Return scheduler task to scan given file for a substring.
fn task_scan_file(path: PathBuf, substring: String) -> Task {
    Box::new(move |context| {
        let mut current = 0;

        let total = path.metadata()
            .expect("Failed to read file's metadata")
            .len();

        let mut file = File::open(&path)
            .expect("Failed to open file");

        context.named_scope("scan_file", move |scope| {
            let mut buf = [0; 4096];

            let substring = substring.as_bytes();
            let substring_len = substring.len();

            loop {
                let Ok(n) = file.read(&mut buf) else {
                    break;
                };

                if n == 0 {
                    break;
                }

                // Sure there's a chance we have half substring in this buffer,
                // and another half in the next buffer. But I don't want to
                // overcomplicate the simple example.
                if n > substring.len() {
                    for i in 0..n - substring_len {
                        if &buf[i..i + substring_len] == substring {
                            let _ = scope.status(format!("Found substring in file {path:#?}, offset {}", current + i as u64));
                        }
                    }
                }

                current += n as u64;

                let _ = scope.progress(current, total);
            }
        });
    })
}

fn main() {
    let path = std::env::args().nth(1)
        .map(PathBuf::from)
        .expect("Directory path expected, got nothing");

    let substring = std::env::args().nth(2)
        .expect("Search substring expected, got nothing");

    if !path.is_dir() {
        panic!("Directory path expected: {path:#?}");
    }

    let mut scheduler = Scheduler::new(8, 1024);

    scheduler.schedule_exclusive(task_scan_dir(path, substring))
        .expect("Failed to schedule directory scanning");

    let mut last_fraction = 0.0;

    while scheduler.is_alive() {
        scheduler.update(|report, handler| {
            if let ScopeReport::Status { status, .. } = report {
                println!("{status}");
            }

            if let Some(status) = handler.named("scan_file") {
                // Print progress with 1% step.
                if status.progress.is_some() && status.fraction > last_fraction && status.fraction - last_fraction >= 0.01 {
                    last_fraction = status.fraction;

                    println!(
                        "Scanning files: {}%, elapsed {:.2} seconds, remaining {:.2} seconds",
                        last_fraction * 100.0,
                        status.elapsed_time.as_millis() as f64 / 1000.0,
                        status.estimate_time.as_millis() as f64 / 1000.0
                    );
                }
            }
        }).expect("Failed to update scheduler");
    }
}
```

*Named after [Hoshimi Miyabi](https://zenless-zone-zero.fandom.com/wiki/Hoshimi_Miyabi) from Zenless Zone Zero 🙏*

<img src="./images/miyabi.png">

*Image was taken from [here](https://www.pinterest.com/pin/350858627237946126)*

Author: [Nikita Podvirnyi](https://github.com/krypt0nn)\
Licensed under [MIT](LICENSE)
