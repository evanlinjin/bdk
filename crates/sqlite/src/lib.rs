#![doc = include_str!("../README.md")]
// only enables the `doc_cfg` feature when the `docsrs` configuration attribute is defined
#![cfg_attr(docsrs, feature(doc_cfg))]

mod schema;
mod store;

pub use rusqlite;
#[cfg(feature = "serde_json")]
pub use serde_json;
pub use store::MigrationPlan;
pub use store::Storable;
pub use store::Store;

// /// Error that occurs while reading or writing change sets with the SQLite database.
// #[derive(Debug)]
// pub enum Error {
//     /// Invalid network, cannot change the one already stored in the database.
//     Network { expected: Network, given: Network },
//     /// SQLite error.
//     Sqlite(rusqlite::Error),
// }
//
// impl core::fmt::Display for Error {
//     fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
//         match self {
//             Self::Network { expected, given } => write!(
//                 f,
//                 "network error trying to read or write change set, expected {}, given {}",
//                 expected, given
//             ),
//             Self::Sqlite(e) => write!(f, "sqlite error reading or writing changeset: {}", e),
//         }
//     }
// }
//
// impl std::error::Error for Error {}

pub mod util {
    /// Execute `scripts` in order.
    ///
    /// This is useful for migrating data to newer schemas versions.
    pub fn execute_scripts<'a>(
        db_tx: &rusqlite::Transaction,
        scripts: impl IntoIterator<Item = &'a str>,
    ) -> rusqlite::Result<()> {
        let statements = scripts
            .into_iter()
            .flat_map(|script| {
                // remove lines with comments
                let without_comments = script
                    .split('\n')
                    .map(|l| l.trim())
                    .filter(|l| !l.starts_with("--") && !l.is_empty())
                    .fold(String::new(), |mut acc, l| {
                        if acc.is_empty() {
                            l.to_string()
                        } else {
                            acc.push(' ');
                            acc.push_str(l);
                            acc
                        }
                    });
                // split into statements
                let statements = without_comments
                    .split(';')
                    // remove extra spaces
                    .map(|s| {
                        s.trim()
                            .split(' ')
                            .filter(|s| !s.is_empty())
                            .collect::<Vec<_>>()
                            .join(" ")
                    });
                statements.collect::<Vec<_>>()
            })
            // remove empty statements
            .filter(|s| !s.is_empty());

        for statement in statements {
            db_tx.execute(&statement, [])?;
        }

        Ok(())
    }

    pub fn is_no_such_table_error(err: &rusqlite::Error) -> bool {
        matches!(err, rusqlite::Error::SqliteFailure(_, Some(msg)) if msg.contains("no such table"))
    }
}
