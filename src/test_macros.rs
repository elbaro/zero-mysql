/// Non-panicking version of `assert!` — returns `Err` instead.
macro_rules! check {
    ($cond:expr) => {
        if !($cond) {
            return Err($crate::error::Error::LibraryBug(
                $crate::error::eyre!(concat!("check failed: `", stringify!($cond), "`")),
            )
            .into());
        }
    };
    ($cond:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err($crate::error::Error::LibraryBug(
                $crate::error::eyre!($($arg)+),
            )
            .into());
        }
    };
}

/// Non-panicking version of `assert_eq!` — returns `Err` instead.
macro_rules! check_eq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val != *right_val {
                    return Err($crate::error::Error::LibraryBug(
                        $crate::error::eyre!("{:?} != {:?}", left_val, right_val),
                    )
                    .into());
                }
            }
        }
    };
    ($left:expr, $right:expr, $($arg:tt)+) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val != *right_val {
                    return Err($crate::error::Error::LibraryBug(
                        $crate::error::eyre!($($arg)+),
                    )
                    .into());
                }
            }
        }
    };
}

/// Non-panicking version of `.unwrap_err()` — returns `Err` if the Result is `Ok`.
macro_rules! check_err {
    ($result:expr) => {
        match $result {
            Err(e) => e,
            Ok(_) => {
                return Err(
                    $crate::error::Error::LibraryBug($crate::error::eyre!(concat!(
                        "expected Err: `",
                        stringify!($result),
                        "`"
                    )))
                    .into(),
                );
            }
        }
    };
}

pub(crate) use check;
pub(crate) use check_eq;
pub(crate) use check_err;
