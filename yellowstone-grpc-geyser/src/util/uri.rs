use {
    http::uri::PathAndQuery,
    std::hash::{Hash, Hasher},
};

///
/// Wrapper type around the shared-type [`http::uri::PathAndQuery`].
/// It's useful to only compare the path portion of the URI, and ignore the query portion.
///
/// [`http::uri::PathAndQuery`] is cheap to clone as its internal data types uses `Bytes`,
/// instead of using `String` we can avoid unnecessary allocations when comparing the path portion of the URI.
///
#[derive(Debug, Clone)]
pub struct PathName(PathAndQuery);

impl From<&PathAndQuery> for PathName {
    fn from(path_and_query: &PathAndQuery) -> Self {
        // We can safely clone the `PathAndQuery` as it uses `Bytes` internally, which is cheap to clone.
        PathName(path_and_query.clone())
    }
}

impl PartialEq for PathName {
    fn eq(&self, other: &Self) -> bool {
        self.0.path() == other.0.path()
    }
}

impl Eq for PathName {}

impl AsRef<str> for PathName {
    fn as_ref(&self) -> &str {
        self.0.path()
    }
}

impl Hash for PathName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.path().hash(state);
    }
}

pub mod path_name_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(path_name: &super::PathName, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(path_name.as_ref())
    }

    /// We need dead_code, because this function must existing in order to use with `serde` macros
    #[allow(dead_code)]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<super::PathName, D::Error>
    where
        D: Deserializer<'de>,
    {
        // `serialize` above already writes `path_name.as_ref()`, which always carries its own
        // leading slash, so don't prepend another one here or round-tripping doubles it.
        let s = String::deserialize(deserializer)?;
        let path_and_query =
            http::uri::PathAndQuery::from_maybe_shared(s).map_err(serde::de::Error::custom)?;
        Ok(super::PathName(path_and_query))
    }
}
