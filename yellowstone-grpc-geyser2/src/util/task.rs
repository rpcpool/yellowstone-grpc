use std::sync::{Arc, Mutex};

use tokio::task::JoinSet;





pub struct SharedJoinSet<O> {
    inner: Arc<Mutex<JoinSet<O>>>
}