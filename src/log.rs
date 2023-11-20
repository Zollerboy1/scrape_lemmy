use std::sync::{Arc, RwLock};

use indicatif::ProgressBar;

static CURRENT_PROGRESS_BAR: RwLock<Option<Arc<ProgressBar>>> = RwLock::new(None);

pub fn set_progress_bar(progress_bar: Arc<ProgressBar>) {
    *CURRENT_PROGRESS_BAR.write().unwrap() = Some(progress_bar);
}

pub fn clear_progress_bar() {
    *CURRENT_PROGRESS_BAR.write().unwrap() = None;
}

#[inline]
pub fn log<I>(str: I)
where
    I: AsRef<str>,
{
    if let Some(progress_bar) = CURRENT_PROGRESS_BAR.read().unwrap().as_ref() {
        progress_bar.println(str);
    } else {
        println!("{}", str.as_ref());
    }
}

#[macro_export]
macro_rules! log {
    () => {
        $crate::log::log("")
    };
    ($($arg:tt)*) => {
        $crate::log::log(format!($($arg)*))
    };
}
