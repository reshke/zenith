//! This module acts as a switchboard to access different repositories managed by this
//! page server. Currently, a Page Server can only manage one repository, so there
//! isn't much here. If we implement multi-tenancy, this will probably be changed into
//! a hash map, keyed by the tenant ID.

use crate::rocksdb_storage::RocksStorage;
use crate::repository::Repository;
use crate::walredo::PostgresRedoManager;
use crate::PageServerConf;
use lazy_static::lazy_static;
use std::sync::{Arc, Mutex};

lazy_static! {
    pub static ref REPOSITORY: Mutex<Option<Arc<Repository>>> = Mutex::new(None);
}

pub fn init(conf: &'static PageServerConf) {
    let mut m = REPOSITORY.lock().unwrap();

    let storage = RocksStorage::open(conf).unwrap();

    // Set up a WAL redo manager, for applying WAL records.
    let walredo_mgr = PostgresRedoManager::new(conf);

    // we have already changed current dir to the repository.
    let repo = Repository::new(conf, Arc::new(storage), Arc::new(walredo_mgr));

    *m = Some(Arc::new(repo));
}

pub fn get_repository() -> Arc<Repository> {
    let o = &REPOSITORY.lock().unwrap();
    Arc::clone(o.as_ref().unwrap())
}
