/// Non-panicking version of `assert!` — returns `Err` instead.
macro_rules! check {
    ($cond:expr) => {
        if !($cond) {
            return Err(zero_mysql::error::Error::LibraryBug(
                zero_mysql::error::eyre!(concat!("check failed: `", stringify!($cond), "`")),
            )
            .into());
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err(zero_mysql::error::Error::LibraryBug(
                zero_mysql::error::eyre!($($arg)+),
            )
            .into());
        }
    };
}
