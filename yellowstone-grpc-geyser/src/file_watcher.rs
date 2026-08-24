use {
    log::error,
    notify::{Config, Event, EventHandler, RecommendedWatcher, RecursiveMode, Watcher},
    std::{
        fmt,
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    },
};

type WatchCallback = Arc<dyn Fn(&Event) + Send + Sync + 'static>;

#[derive(Clone, Debug)]
enum WatchScope {
    File(PathBuf),
    Folder { path: PathBuf, recursive: bool },
}

impl WatchScope {
    fn matches_event_path(&self, event_path: &Path) -> bool {
        match self {
            Self::File(path) => event_path == path,
            Self::Folder { path, recursive } => {
                if *recursive {
                    event_path.starts_with(path)
                } else {
                    event_path == path
                        || event_path
                            .parent()
                            .is_some_and(|parent_path| parent_path == path)
                }
            }
        }
    }
}

#[derive(Clone)]
struct CallbackRegistration {
    id: u64,
    scope: WatchScope,
    callback: WatchCallback,
}

impl CallbackRegistration {
    fn new(id: u64, scope: WatchScope, callback: WatchCallback) -> Self {
        Self {
            id,
            scope,
            callback,
        }
    }

    fn matches_event(&self, event: &Event) -> bool {
        event
            .paths
            .iter()
            .any(|event_path| self.scope.matches_event_path(event_path))
    }
}

pub struct FileWatchHandle {
    registration_id: u64,
    inner: Arc<Mutex<Inner>>,
    deregister_on_drop: bool,
}

impl FileWatchHandle {
    const fn new(registration_id: u64, inner: Arc<Mutex<Inner>>) -> Self {
        Self {
            registration_id,
            inner,
            deregister_on_drop: true,
        }
    }

    pub fn forget(mut self) {
        self.deregister_on_drop = false;
    }
}

impl Drop for FileWatchHandle {
    fn drop(&mut self) {
        if !self.deregister_on_drop {
            return;
        }

        let mut guard = self.inner.lock().expect("file watcher mutex poisoned");
        guard
            .registrations
            .retain(|registration| registration.id != self.registration_id);
    }
}

struct Inner {
    watcher: Option<RecommendedWatcher>,
    registrations: Vec<CallbackRegistration>,
    next_registration_id: u64,
}

#[derive(Clone)]
pub struct FileWatcher {
    inner: Arc<Mutex<Inner>>,
}

impl fmt::Debug for FileWatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileWatcher").finish()
    }
}

struct FileChangeEvHandler {
    inner: Arc<Mutex<Inner>>,
}

impl FileChangeEvHandler {
    const fn is_event_relevant(event: &Event) -> bool {
        matches!(
            event.kind,
            notify::EventKind::Create(_)
                | notify::EventKind::Modify(_)
                | notify::EventKind::Remove(_)
        )
    }
}

impl EventHandler for FileChangeEvHandler {
    fn handle_event(&mut self, event_result: notify::Result<Event>) {
        match event_result {
            Ok(event) => {
                if !FileChangeEvHandler::is_event_relevant(&event) {
                    return;
                }

                let registrations_snapshot = {
                    let guard = self.inner.lock().expect("file watcher mutex poisoned");
                    guard.registrations.clone()
                };

                for registration in registrations_snapshot {
                    if registration.matches_event(&event) {
                        registration.callback.as_ref()(&event);
                    }
                }
            }
            Err(error) => {
                error!("file watcher notify error: {error}");
            }
        }
    }
}

impl FileWatcher {
    pub fn new() -> notify::Result<Self> {
        let inner = Arc::new(Mutex::new(Inner {
            watcher: None,
            registrations: Vec::new(),
            next_registration_id: 0,
        }));
        let handler = FileChangeEvHandler {
            inner: Arc::clone(&inner),
        };
        // Unless we see a useful case in prod to follow symlinks, we will disable it to avoid potential security issues with symlink attacks.
        let watcher = notify::RecommendedWatcher::new(
            handler,
            Config::default().with_follow_symlinks(false),
        )?;
        inner.lock().expect("file watcher mutex poisoned").watcher = Some(watcher);
        Ok(Self { inner })
    }

    fn allocate_registration_id(&self) -> u64 {
        let mut guard = self.inner.lock().expect("file watcher mutex poisoned");
        let id = guard.next_registration_id;
        guard.next_registration_id += 1;
        id
    }

    pub fn watch_file<F>(
        &self,
        path: impl Into<PathBuf>,
        callback: F,
    ) -> notify::Result<FileWatchHandle>
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        let path = path.into();
        let registration_id = self.allocate_registration_id();
        let mut guard = self.inner.lock().expect("file watcher mutex poisoned");
        guard
            .watcher
            .as_mut()
            .expect("file watcher not initialized")
            .watch(&path, RecursiveMode::NonRecursive)?;
        guard.registrations.push(CallbackRegistration::new(
            registration_id,
            WatchScope::File(path),
            Arc::new(callback),
        ));
        Ok(FileWatchHandle::new(
            registration_id,
            Arc::clone(&self.inner),
        ))
    }

    pub fn watch_folder<F>(
        &self,
        path: impl Into<PathBuf>,
        recursive: bool,
        callback: F,
    ) -> notify::Result<FileWatchHandle>
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        let path = path.into();
        let registration_id = self.allocate_registration_id();
        let mode = if recursive {
            RecursiveMode::Recursive
        } else {
            RecursiveMode::NonRecursive
        };
        let mut guard = self.inner.lock().expect("file watcher mutex poisoned");
        guard
            .watcher
            .as_mut()
            .expect("file watcher not initialized")
            .watch(&path, mode)?;
        guard.registrations.push(CallbackRegistration::new(
            registration_id,
            WatchScope::Folder { path, recursive },
            Arc::new(callback),
        ));
        Ok(FileWatchHandle::new(
            registration_id,
            Arc::clone(&self.inner),
        ))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{CallbackRegistration, FileChangeEvHandler, FileWatchHandle, Inner, WatchScope},
        notify::{
            event::{AccessKind, AccessMode, CreateKind, DataChange, ModifyKind},
            Event, EventKind,
        },
        std::{
            path::PathBuf,
            sync::{Arc, Mutex},
        },
    };

    #[test]
    fn file_scope_matches_only_exact_path() {
        let watched = PathBuf::from("/tmp/config.json");
        let scope = WatchScope::File(watched.clone());

        assert!(scope.matches_event_path(&watched));
        assert!(!scope.matches_event_path(&PathBuf::from("/tmp/other.json")));
        assert!(!scope.matches_event_path(&PathBuf::from("/tmp/sub/config.json")));
    }

    #[test]
    fn non_recursive_folder_matches_direct_children_only() {
        let folder = PathBuf::from("/tmp/watched-dir");
        let scope = WatchScope::Folder {
            path: folder.clone(),
            recursive: false,
        };

        assert!(scope.matches_event_path(&folder));
        assert!(scope.matches_event_path(&PathBuf::from("/tmp/watched-dir/item.txt")));
        assert!(!scope.matches_event_path(&PathBuf::from("/tmp/watched-dir/nested/item.txt")));
        assert!(!scope.matches_event_path(&PathBuf::from("/tmp/other/item.txt")));
    }

    #[test]
    fn recursive_folder_matches_nested_paths() {
        let folder = PathBuf::from("/tmp/watched-dir");
        let scope = WatchScope::Folder {
            path: folder,
            recursive: true,
        };

        assert!(scope.matches_event_path(&PathBuf::from("/tmp/watched-dir/item.txt")));
        assert!(scope.matches_event_path(&PathBuf::from("/tmp/watched-dir/nested/item.txt")));
        assert!(!scope.matches_event_path(&PathBuf::from("/tmp/other/item.txt")));
    }

    #[test]
    fn callback_registration_matches_any_event_path() {
        let callback = Arc::new(|_: &Event| {});
        let registration = CallbackRegistration::new(
            1,
            WatchScope::Folder {
                path: PathBuf::from("/tmp/watched-dir"),
                recursive: false,
            },
            callback,
        );

        let event = Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            paths: vec![
                PathBuf::from("/tmp/other/no-match.txt"),
                PathBuf::from("/tmp/watched-dir/item.txt"),
            ],
            attrs: Default::default(),
        };

        assert!(registration.matches_event(&event));
    }

    #[test]
    fn event_relevance_accepts_create_modify_remove_and_rejects_access() {
        let create_event = Event {
            kind: EventKind::Create(CreateKind::File),
            paths: vec![PathBuf::from("/tmp/watched-dir/item.txt")],
            attrs: Default::default(),
        };
        let modify_event = Event {
            kind: EventKind::Modify(ModifyKind::Data(DataChange::Content)),
            paths: vec![PathBuf::from("/tmp/watched-dir/item.txt")],
            attrs: Default::default(),
        };
        let remove_event = Event {
            kind: EventKind::Remove(notify::event::RemoveKind::File),
            paths: vec![PathBuf::from("/tmp/watched-dir/item.txt")],
            attrs: Default::default(),
        };
        let access_event = Event {
            kind: EventKind::Access(AccessKind::Close(AccessMode::Read)),
            paths: vec![PathBuf::from("/tmp/watched-dir/item.txt")],
            attrs: Default::default(),
        };

        assert!(FileChangeEvHandler::is_event_relevant(&create_event));
        assert!(FileChangeEvHandler::is_event_relevant(&modify_event));
        assert!(FileChangeEvHandler::is_event_relevant(&remove_event));
        assert!(!FileChangeEvHandler::is_event_relevant(&access_event));
    }

    #[test]
    fn watch_receipt_drop_deregisters_matching_registration() {
        let inner = Arc::new(Mutex::new(Inner {
            watcher: None,
            registrations: vec![
                CallbackRegistration::new(
                    10,
                    WatchScope::File(PathBuf::from("/tmp/one.txt")),
                    Arc::new(|_: &Event| {}),
                ),
                CallbackRegistration::new(
                    20,
                    WatchScope::File(PathBuf::from("/tmp/two.txt")),
                    Arc::new(|_: &Event| {}),
                ),
            ],
            next_registration_id: 0,
        }));

        let receipt = FileWatchHandle::new(20, Arc::clone(&inner));
        drop(receipt);

        let guard = inner.lock().expect("file watcher mutex poisoned");
        assert_eq!(guard.registrations.len(), 1);
        assert_eq!(guard.registrations[0].id, 10);
    }

    #[test]
    fn watch_receipt_forget_keeps_registration_on_drop() {
        let inner = Arc::new(Mutex::new(Inner {
            watcher: None,
            registrations: vec![CallbackRegistration::new(
                42,
                WatchScope::File(PathBuf::from("/tmp/three.txt")),
                Arc::new(|_: &Event| {}),
            )],
            next_registration_id: 0,
        }));

        let receipt = FileWatchHandle::new(42, Arc::clone(&inner));
        receipt.forget();

        let guard = inner.lock().expect("file watcher mutex poisoned");
        assert_eq!(guard.registrations.len(), 1);
        assert_eq!(guard.registrations[0].id, 42);
    }
}
