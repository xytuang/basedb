use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicI64, AtomicU32},
        Arc, Mutex,
    },
};

use sqlparser::ast::TransactionIsolationLevel;

use crate::{
    common::{
        config::{TableOid, TxnId},
        rid::RID,
    },
    execution::expressions::abstract_expression::AbstractExpression,
    storage::table::tuple::Tuple,
};

#[repr(u32)]
enum TransactionState {
    Running = 0,
    Tainted = 1,
    Committed = 2,
    Aborted = 3,
}

pub struct Transaction {
    state: AtomicU32,
    read_ts: AtomicI64,
    commit_ts: AtomicI64,
    latch: Mutex<()>,
    undo_logs: Vec<UndoLog>,
    write_set: HashMap<TableOid, HashSet<RID>>,
    scan_predicates: HashMap<TableOid, Vec<Arc<AbstractExpression>>>,

    isolation_level: u32,
    thread_id: std::thread::ThreadId,
    txn_id: TxnId,
}

struct UndoLog {
    is_deleted: bool,
    modified_fields: Vec<bool>,
    tuple: Tuple,
    ts: u64,
    prev_version: UndoLink,
}

struct UndoLink {
    prev_txn: TxnId,
    prev_log_idx: u32,
}

impl Transaction {
    pub fn new(txn_id: TxnId, isolation_level: TransactionIsolationLevel) -> Transaction {
        Transaction {
            state: AtomicU32::new(TransactionState::Running as u32),
            read_ts: AtomicI64::new(0),
            commit_ts: AtomicI64::new(-1),
            latch: Mutex::new(()),
            undo_logs: Vec::new(),
            write_set: HashMap::new(),
            scan_predicates: HashMap::new(),
            isolation_level: isolation_level as u32,
            thread_id: std::thread::current().id(),
            txn_id: txn_id,
        }
    }
}
