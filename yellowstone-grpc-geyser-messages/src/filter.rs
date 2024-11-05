use {
    bytes::buf::BufMut,
    prost::encoding::{encode_key, encode_varint, message, WireType},
    std::{
        borrow::Borrow,
        collections::HashSet,
        sync::Arc,
        time::{Duration, Instant},
    },
    tonic::{codec::EncodeBuf, Status},
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

#[derive(Debug)]
pub struct Message {
    pub filters: Vec<FilterName>, // 1
    pub message: MessageWeak,     // 2, 3, 4, 10, 5, 6, 9, 7, 8
}

impl Message {
    pub fn encode(self, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        for name in self.filters.iter().map(|filter| filter.as_ref()) {
            encode_key(1, WireType::LengthDelimited, buf);
            encode_varint(name.len() as u64, buf);
            buf.put_slice(name.as_bytes());
        }
        self.message.encode(buf)
    }
}

#[derive(Debug)]
pub enum MessageWeak {
    Account,               // 2
    Slot,                  // 3
    Transaction,           // 4
    TransactionStatus,     // 10
    Block,                 // 5
    Ping,                  // 6
    Pong(MessageWeakPong), // 9
    BlockMeta,             // 7
    Entry,                 // 8
}

impl MessageWeak {
    pub fn encode(self, buf: &mut EncodeBuf<'_>) -> Result<(), Status> {
        match self {
            MessageWeak::Account => todo!(),
            MessageWeak::Slot => todo!(),
            MessageWeak::Transaction => todo!(),
            MessageWeak::TransactionStatus => todo!(),
            MessageWeak::Block => todo!(),
            MessageWeak::Ping => encode_key(6, WireType::LengthDelimited, buf),
            MessageWeak::Pong(msg) => message::encode(9, &msg, buf),
            MessageWeak::BlockMeta => todo!(),
            MessageWeak::Entry => todo!(),
        }

        Ok(())
    }
}

#[derive(prost::Message)]
pub struct MessageWeakPong {
    #[prost(int32, tag = "1")]
    pub id: i32,
}

// pub struct MessageWeakEntry
