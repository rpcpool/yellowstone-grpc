///
/// A utility that runs a closure when dropped.
/// This is useful for ensuring cleanup code runs when a scope is exited,
/// even if it is exited via a panic.
///
pub struct OnDrop<F: FnOnce()> {
    f: Option<F>,
}

impl<F: FnOnce()> OnDrop<F> {
    pub fn new(f: F) -> Self {
        Self { f: Some(f) }
    }
}

impl<F: FnOnce()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

impl<F: FnOnce()> std::fmt::Debug for OnDrop<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnDrop").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::OnDrop,
        std::sync::atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn test_on_drop() {
        let flag = AtomicBool::new(false);
        {
            let _on_drop = OnDrop::new(|| {
                flag.store(true, Ordering::SeqCst);
            });
            assert!(!flag.load(Ordering::SeqCst));
        }
        assert!(flag.load(Ordering::SeqCst));
    }
}
