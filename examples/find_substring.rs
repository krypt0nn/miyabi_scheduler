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
