use serde::{Deserialize, Serialize};

const SUB_BUCKETS: u64 = 16;
const MAX_EXP: usize = 40;

/// Log-linear histogram of non-negative integer values (nanoseconds here), relative error under 7%.
#[derive(Clone)]
pub struct LogHist {
    buckets: Vec<u64>,
    count: u64,
    sum: u128,
    max: u64,
}

impl Default for LogHist {
    fn default() -> Self {
        Self {
            buckets: vec![0; MAX_EXP * SUB_BUCKETS as usize],
            count: 0,
            sum: 0,
            max: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub count: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
}

impl LogHist {
    pub fn merge(&mut self, other: &Self) {
        for (bucket, count) in self.buckets.iter_mut().zip(&other.buckets) {
            *bucket += count;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.max = self.max.max(other.max);
    }

    pub fn record(&mut self, value: u64) {
        let index = Self::index(value).min(self.buckets.len() - 1);
        self.buckets[index] += 1;
        self.count += 1;
        self.sum += u128::from(value);
        self.max = self.max.max(value);
    }

    pub fn quantile(&self, q: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((self.count as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (index, bucket) in self.buckets.iter().enumerate() {
            seen += bucket;
            if seen >= target {
                return Self::upper_bound(index).min(self.max);
            }
        }
        self.max
    }

    pub fn summary(&self) -> Summary {
        Summary {
            count: self.count,
            mean: if self.count == 0 {
                0.0
            } else {
                self.sum as f64 / self.count as f64
            },
            p50: self.quantile(0.5),
            p90: self.quantile(0.9),
            p99: self.quantile(0.99),
            p999: self.quantile(0.999),
            max: self.max,
        }
    }

    const fn index(value: u64) -> usize {
        if value < SUB_BUCKETS {
            return value as usize;
        }
        let exp = 63 - value.leading_zeros() as u64;
        let shift = exp - SUB_BUCKETS.trailing_zeros() as u64;
        let sub = (value >> shift) - SUB_BUCKETS;
        ((exp - 3) * SUB_BUCKETS + sub) as usize
    }

    const fn upper_bound(index: usize) -> u64 {
        let index = index as u64;
        if index < SUB_BUCKETS {
            return index;
        }
        let exp = index / SUB_BUCKETS + 3;
        let sub = index % SUB_BUCKETS;
        let shift = exp - SUB_BUCKETS.trailing_zeros() as u64;
        ((SUB_BUCKETS + sub + 1) << shift) - 1
    }
}

#[cfg(test)]
mod tests {
    use super::LogHist;

    #[test]
    fn quantiles_are_within_bucket_error() {
        let mut hist = LogHist::default();
        for value in 1..=100_000u64 {
            hist.record(value * 1000);
        }
        for (q, expected) in [(0.5, 50_000_000f64), (0.99, 99_000_000f64)] {
            let got = hist.quantile(q) as f64;
            assert!(
                (got - expected).abs() / expected < 0.07,
                "q{q}: {got} vs {expected}"
            );
        }
        assert_eq!(hist.quantile(1.0), 100_000_000);
    }

    #[test]
    fn small_values_are_exact() {
        let mut hist = LogHist::default();
        for value in 0..16 {
            hist.record(value);
        }
        assert_eq!(hist.quantile(0.5), 7);
    }
}
