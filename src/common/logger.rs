use std::io::{IsTerminal, stdout};
use std::time::{SystemTime, UNIX_EPOCH};

pub const LOG_LEVEL_OFF: i32 = 1000;
pub const LOG_LEVEL_ERROR: i32 = 500;
pub const LOG_LEVEL_WARN: i32 = 400;
pub const LOG_LEVEL_INFO: i32 = 300;
pub const LOG_LEVEL_DEBUG: i32 = 200;
pub const LOG_LEVEL_TRACE: i32 = 100;
pub const LOG_LEVEL_ALL: i32 = 0;

#[cfg(debug_assertions)]
pub const LOG_LEVEL: i32 = LOG_LEVEL_TRACE;
#[cfg(not(debug_assertions))]
pub const LOG_LEVEL: i32 = LOG_LEVEL_INFO;

#[macro_export]
macro_rules! short_file {
    () => {{
        let file = file!();
        file.rsplit_once("/").map(|(_, name)| name).unwrap_or(file)
    }};
}

fn format_timestamp() -> String {
    let total_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let seconds = total_secs % 60;
    let total_minutes = total_secs / 60;
    let minutes = total_minutes % 60;
    let total_hours = total_minutes / 60;
    let hours = total_hours % 24;

    // H:MM:SS in UTC
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

pub fn output_log_header(file: &str, line: u32, func: &str, level: i32) {
    let use_color = stdout().is_terminal();
    let timestamp = format_timestamp();

    let (yellow, blue, reset) = if use_color {
        ("\x1b[33m", "\x1b[34m", "\x1b[0m")
    } else {
        ("", "", "")
    };

    let level_str = match level {
        LOG_LEVEL_ERROR => "ERROR",
        LOG_LEVEL_WARN => "WARN",
        LOG_LEVEL_INFO => "INFO",
        LOG_LEVEL_DEBUG => "DEBUG",
        LOG_LEVEL_TRACE => "TRACE",
        _ => "UNKNOWN",
    };

    let level_color = if use_color {
        match level {
            LOG_LEVEL_ERROR => "\x1b[31m",      // Red
            LOG_LEVEL_WARN => "\x1b[33m",       // Yellow
            LOG_LEVEL_INFO => "\x1b[34m",       // Blue
            LOG_LEVEL_DEBUG => "\x1b[35m",      // Magenta
            LOG_LEVEL_TRACE => "\x1b[38;5;92m", // Deep purple
            _ => "",
        }
    } else {
        ""
    };

    print!(
        "{yellow}{}{reset} [{blue}{}:{}:{}{reset}] {}{}{reset} - ",
        timestamp, file, line, func, level_color, level_str
    );
}

#[macro_export]
macro_rules! log_error {
    ($($arg:tt)*) => {
        if $crate::common::logger::LOG_LEVEL <= $crate::common::logger::LOG_LEVEL_ERROR {
            $crate::common::logger::output_log_header($crate::short_file!(), line!(), module_path!(), $crate::common::logger::LOG_LEVEL_ERROR);
            println!($($arg)*);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    };
}

#[macro_export]
macro_rules! log_warn {
    ($($arg:tt)*) => {
        if $crate::common::logger::LOG_LEVEL <= $crate::common::logger::LOG_LEVEL_WARN {
            $crate::common::logger::output_log_header($crate::short_file!(), line!(), module_path!(), $crate::common::logger::LOG_LEVEL_WARN);
            println!($($arg)*);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    };
}

#[macro_export]
macro_rules! log_info {
    ($($arg:tt)*) => {
        if $crate::common::logger::LOG_LEVEL <= $crate::common::logger::LOG_LEVEL_INFO {
            $crate::common::logger::output_log_header($crate::short_file!(), line!(), module_path!(), $crate::common::logger::LOG_LEVEL_INFO);
            println!($($arg)*);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    };
}

#[macro_export]
macro_rules! log_debug {
    ($($arg:tt)*) => {
        if $crate::common::logger::LOG_LEVEL <= $crate::common::logger::LOG_LEVEL_DEBUG {
            $crate::common::logger::output_log_header($crate::short_file!(), line!(), module_path!(), $crate::common::logger::LOG_LEVEL_DEBUG);
            println!($($arg)*);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    };
}

#[macro_export]
macro_rules! log_trace {
    ($($arg:tt)*) => {
        if $crate::common::logger::LOG_LEVEL <= $crate::common::logger::LOG_LEVEL_TRACE {
            $crate::common::logger::output_log_header($crate::short_file!(), line!(), module_path!(), $crate::common::logger::LOG_LEVEL_TRACE);
            println!($($arg)*);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }
    };
}
