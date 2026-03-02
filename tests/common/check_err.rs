/// Non-panicking version of `.unwrap_err()` — returns `Err` if the Result is `Ok`.
macro_rules! check_err {
    ($result:expr) => {
        match $result {
            Err(e) => e,
            Ok(_) => {
                return Err(zero_mysql::error::Error::LibraryBug(
                    zero_mysql::error::eyre!(concat!(
                        "expected Err: `",
                        stringify!($result),
                        "`"
                    )),
                )
                .into());
            }
        }
    };
}
