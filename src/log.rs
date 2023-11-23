use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        mpsc, Arc, OnceLock, RwLock,
    },
};

use indicatif::{ProgressBar as ProgressBarImpl, ProgressStyle};
use tokio::{fs::File, io::AsyncWriteExt as _};

type ProgressBarLock = RwLock<Option<ProgressBarImpl>>;

static LOGGER: OnceLock<Logger> = OnceLock::new();

pub struct Logger {
    progress_bar: ProgressBarLock,
    tx: mpsc::Sender<String>,
    dropped: Arc<AtomicBool>,
}

impl Logger {
    fn new(logfile_path: PathBuf) -> Self {
        let (tx, rx) = mpsc::channel::<String>();
        let dropped = Arc::new(AtomicBool::new(false));

        {
            let dropped = dropped.clone();
            tokio::spawn(async move {
                let mut file = File::create(logfile_path).await.unwrap();
                while let (Ok(str), false) = (rx.recv(), dropped.load(AtomicOrdering::Relaxed)) {
                    file.write_all(str.as_bytes()).await.unwrap();
                    file.write_all(b"\n").await.unwrap();
                }
            });
        }

        Self {
            progress_bar: RwLock::new(None),
            tx,
            dropped,
        }
    }

    pub fn initialize(logfile_path: impl AsRef<Path>) -> LoggerGuard {
        let logger = LOGGER.get_or_init(|| Self::new(logfile_path.as_ref().to_owned()));

        LoggerGuard {
            dropped: logger.dropped.clone(),
        }
    }

    pub fn get() -> &'static Self {
        LOGGER.get().unwrap()
    }

    pub fn create_progress_spinner(message: impl Into<Cow<'static, str>>) -> ProgressBar {
        Self::create_progress_bar_impl(|| {
            ProgressBarImpl::new_spinner()
                .with_style(ProgressStyle::with_template("{msg} {spinner} {pos}").unwrap())
                .with_message(message)
        })
    }

    pub fn create_progress_bar(len: u64, message: impl Into<Cow<'static, str>>) -> ProgressBar {
        Self::create_progress_bar_impl(|| {
            ProgressBarImpl::new(len)
                .with_style(ProgressStyle::with_template("{msg} {wide_bar} {pos}/{len}").unwrap())
                .with_message(message)
        })
    }

    fn create_progress_bar_impl(f: impl FnOnce() -> ProgressBarImpl) -> ProgressBar {
        let logger = Self::get();
        let mut guard = logger.progress_bar.write().unwrap();

        if guard.is_some() {
            panic!(
                "Progress bar already exists. Drop it using finish() before creating a new one."
            );
        }

        *guard = Some(f());

        ProgressBar {
            progress_bar: &logger.progress_bar,
        }
    }

    pub fn log(&self, str: impl AsRef<str>) {
        self.trace(&str);
        if let Some(progress_bar) = self.progress_bar.read().unwrap().as_ref() {
            progress_bar.println(str);
        } else {
            println!("{}", str.as_ref());
        }
    }

    pub fn trace(&self, str: impl AsRef<str>) {
        self.tx.send(str.as_ref().to_string()).unwrap();
    }
}

pub struct LoggerGuard {
    dropped: Arc<AtomicBool>,
}

impl Drop for LoggerGuard {
    fn drop(&mut self) {
        self.dropped.store(true, AtomicOrdering::Relaxed);
    }
}

pub struct ProgressBar {
    progress_bar: &'static ProgressBarLock,
}

impl ProgressBar {
    pub fn inc(&self) {
        self.progress_bar.read().unwrap().as_ref().unwrap().inc(1);
    }

    pub fn add_len(&self, difference: i64) {
        let new_len: fn(_, i64) -> _ = match difference.cmp(&0) {
            std::cmp::Ordering::Less => |len, diff| len - diff.unsigned_abs(),
            std::cmp::Ordering::Greater => |len, diff| len + diff as u64,
            std::cmp::Ordering::Equal => {
                return;
            }
        };
        self.progress_bar
            .read()
            .unwrap()
            .as_ref()
            .unwrap()
            .update(|state| {
                let Some(previous_len) = state.len() else {
                    return;
                };

                state.set_len(new_len(previous_len, difference));
            });
    }

    pub fn finish(self) {}
}

impl Drop for ProgressBar {
    fn drop(&mut self) {
        match self.progress_bar.write().unwrap().take() {
            Some(progress_bar) => progress_bar.finish(),
            None => panic!("Progress bar does not exist"),
        }
    }
}

#[macro_export]
macro_rules! log {
    (trace, $($arg:tt)+) => {
        $crate::log::Logger::get().trace(format!($($arg)+))
    };
    ($($arg:tt)+) => {
        $crate::log::Logger::get().log(format!($($arg)+))
    };
}
