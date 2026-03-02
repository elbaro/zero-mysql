/// Non-panicking version of `assert_eq!` — returns `Err` instead.
macro_rules! check_eq {
    ($left:expr, $right:expr $(,)?) => {
        match (&$left, &$right) {
            (left_val, right_val) => {
                if *left_val != *right_val {
                    return Err(zero_mysql::error::Error::LibraryBug(
                        zero_mysql::error::eyre!("{:?} != {:?}", left_val, right_val),
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
                    return Err(zero_mysql::error::Error::LibraryBug(
                        zero_mysql::error::eyre!($($arg)+),
                    )
                    .into());
                }
            }
        }
    };
}
