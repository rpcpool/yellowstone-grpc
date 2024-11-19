use std::{
    borrow::Borrow,
    collections::HashSet,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, thiserror::Error)]
pub enum FilterNameError {
    #[error("oversized filter name (max allowed size {limit}), found {size}")]
    Oversized { limit: usize, size: usize },
}

pub type FilterNameResult<T> = Result<T, FilterNameError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FilterName(Arc<String>);

impl AsRef<str> for FilterName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Deref for FilterName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for FilterName {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0[..]
    }
}

impl FilterName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Arc::new(name.into()))
    }

    pub fn is_uniq(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }
}

#[derive(Debug)]
pub struct FilterNames {
    name_size_limit: usize,
    names: HashSet<FilterName>,
    names_size_limit: usize,
    cleanup_ts: Instant,
    cleanup_interval: Duration,
}

impl FilterNames {
    pub fn new(
        name_size_limit: usize,
        names_size_limit: usize,
        cleanup_interval: Duration,
    ) -> Self {
        Self {
            name_size_limit,
            names: HashSet::with_capacity(names_size_limit),
            names_size_limit,
            cleanup_ts: Instant::now(),
            cleanup_interval,
        }
    }

    pub fn try_clean(&mut self) {
        if self.names.len() > self.names_size_limit
            && self.cleanup_ts.elapsed() > self.cleanup_interval
        {
            self.names.retain(|name| !name.is_uniq());
            self.cleanup_ts = Instant::now();
        }
    }

    pub fn get(&mut self, name: &str) -> FilterNameResult<FilterName> {
        match self.names.get(name) {
            Some(name) => Ok(name.clone()),
            None => {
                if name.len() > self.name_size_limit {
                    Err(FilterNameError::Oversized {
                        limit: self.name_size_limit,
                        size: name.len(),
                    })
                } else {
                    let name = FilterName::new(name);
                    self.names.insert(name.clone());
                    Ok(name)
                }
            }
        }
    }
}
