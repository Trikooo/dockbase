#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceptionType {
    Invalid = 0,
    OutOfRange = 1,
    Conversion = 2,
    UnknownType = 3,
    Decimal = 4,
    MismatchType = 5,
    DivideByZero = 6,
    IncompatibleType = 8,
    OutOfMemory = 9,
    NotImplemented = 11,
    Execution = 12,
    IO = 13,
}

#[macro_export]
macro_rules! throw {
    ($variant:ident, $msg:expr) => {
        return Err(Exception::$variant($msg))
    };
}

macro_rules! define_exceptions {
  ($($variant:ident => ($enum_val:path, $string:expr)),* $(,)?) => {
    #[derive(Debug)]
    pub enum Exception {
      $($variant(&'static str),)*
    }

    impl Exception {
      pub fn get_type(&self) -> ExceptionType {
        match self {
          $(Self::$variant(_) => $enum_val,)*
        }
      }

      pub fn type_to_string(exception_type: ExceptionType) -> &'static str {
        match exception_type {
          $($enum_val => $string,)*
        }
      }
    }

    impl std::fmt::Display for Exception {
      fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::io::{stdout, IsTerminal};
        let use_color = stdout().is_terminal();

        let red = if use_color { "\x1b[1;31m" } else { "" };
        let yellow = if use_color { "\x1b[33m" } else { "" };
        let reset = if use_color { "\x1b[0m" } else { "" };

        match self {
          $(
            Self::$variant(msg) => write!(
              f,
              "{}Exception Type: {}{}\n{}Message: {}{}",
              red, $string, reset, yellow, msg, reset
            ),
          )*
        }
      }
    }
  }
}

define_exceptions! {
    Invalid => (ExceptionType::Invalid, "Invalid"),
    OutOfRange => (ExceptionType::OutOfRange, "Out of Range"),
    Conversion => (ExceptionType::Conversion, "Conversion"),
    UnknownType => (ExceptionType::UnknownType, "Unknown Type"),
    Decimal => (ExceptionType::Decimal, "Decimal"),
    MismatchType => (ExceptionType::MismatchType, "Mismatch Type"),
    DivideByZero => (ExceptionType::DivideByZero, "Divide by Zero"),
    IncompatibleType => (ExceptionType::IncompatibleType, "Incompatible type"),
    OutOfMemory => (ExceptionType::OutOfMemory, "Out of Memory"),
    NotImplemented => (ExceptionType::NotImplemented, "Not implemented"),
    Execution => (ExceptionType::Execution, "Execution"),
    IO => (ExceptionType::IO, "IO Error"),
}

impl std::error::Error for Exception {}

impl From<std::io::Error> for Exception {
    fn from(_error: std::io::Error) -> Self {
        Self::IO("Internal I/O subsystem error")
    }
}

impl<T> From<std::sync::PoisonError<T>> for Exception {
    fn from(_: std::sync::PoisonError<T>) -> Self {
        Self::Execution("Lock poisoned")
    }
}
