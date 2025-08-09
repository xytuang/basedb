use std::{collections::HashMap, path::Path};

use sqlparser::ast::TransactionIsolationLevel;

use crate::{concurrency::transaction::Transaction, storage::disk::disk_manager::DiskManager};

pub struct BaseDBInstance {
    disk_manager: DiskManager,
    session_variables: HashMap<String, String>,
    current_txn: Transaction,
    managed_txn_mode: bool,
}

impl BaseDBInstance {
    pub fn new(db_file_name: &str) -> BaseDBInstance {
        let disk_manager = DiskManager::new(Path::new(db_file_name)).unwrap();

        BaseDBInstance {
            disk_manager: disk_manager,
            session_variables: HashMap::new(),
            current_txn: Transaction::new(0, TransactionIsolationLevel::ReadUncommitted),
            managed_txn_mode: false,
        }
    }

    pub fn execute_sql(&self, query: String) {}
}
