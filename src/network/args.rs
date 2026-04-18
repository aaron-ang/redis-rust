use anyhow::{bail, Result};
use resp::Value;

use crate::types::RedisError;

pub(crate) fn unpack_bulk_string(value: &Value) -> Result<&str> {
    if let Value::Bulk(ref s) = value {
        Ok(s)
    } else {
        bail!("Expected bulk string")
    }
}

pub(crate) trait Args {
    fn min_len(&self, n: usize) -> Result<()>;
    fn exact_len(&self, n: usize) -> Result<()>;
    fn bulk(&self, index: usize) -> Result<&str>;
    fn bulks_from(&self, from: usize) -> Result<Vec<&str>>;
}

impl Args for [Value] {
    fn min_len(&self, n: usize) -> Result<()> {
        if self.len() < n {
            bail!(RedisError::InvalidArgument)
        }
        Ok(())
    }

    fn exact_len(&self, n: usize) -> Result<()> {
        if self.len() != n {
            bail!(RedisError::InvalidArgument)
        }
        Ok(())
    }

    fn bulk(&self, i: usize) -> Result<&str> {
        let v = self.get(i).ok_or(RedisError::InvalidArgument)?;
        unpack_bulk_string(v)
    }

    fn bulks_from(&self, from: usize) -> Result<Vec<&str>> {
        self.get(from..)
            .ok_or(RedisError::InvalidArgument)?
            .iter()
            .map(unpack_bulk_string)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::Bulk(s.to_string())
    }

    #[test]
    fn min_len_ok_when_equal_or_greater() {
        let args = vec![bulk("a"), bulk("b")];
        assert!(args.min_len(2).is_ok());
        assert!(args.min_len(1).is_ok());
        assert!(args.min_len(0).is_ok());
    }

    #[test]
    fn min_len_errors_when_short() {
        let args = vec![bulk("a")];
        let err = args.min_len(2).unwrap_err();
        assert!(err.to_string().contains("wrong number of arguments"));
    }

    #[test]
    fn exact_len_rejects_both_sides() {
        let args = vec![bulk("a"), bulk("b")];
        assert!(args.exact_len(2).is_ok());
        assert!(args.exact_len(1).is_err());
        assert!(args.exact_len(3).is_err());
    }

    #[test]
    fn bulk_returns_at_index() {
        let args = vec![bulk("a"), bulk("b")];
        assert_eq!(args.bulk(0).unwrap(), "a");
        assert_eq!(args.bulk(1).unwrap(), "b");
    }

    #[test]
    fn bulk_errors_on_out_of_bounds() {
        let args = vec![bulk("a")];
        assert!(args.bulk(1).is_err());
    }

    #[test]
    fn bulk_errors_on_non_bulk_value() {
        let args = vec![Value::Integer(42)];
        let err = args.bulk(0).unwrap_err();
        assert!(err.to_string().contains("Expected bulk string"));
    }

    #[test]
    fn bulks_from_collects_remainder() {
        let args = vec![bulk("a"), bulk("b"), bulk("c")];
        assert_eq!(args.bulks_from(1).unwrap(), vec!["b", "c"]);
    }

    #[test]
    fn bulks_from_empty_slice_at_end() {
        let args = vec![bulk("a")];
        assert_eq!(args.bulks_from(1).unwrap(), Vec::<&str>::new());
    }

    #[test]
    fn bulks_from_errors_past_end() {
        let args = vec![bulk("a")];
        assert!(args.bulks_from(2).is_err());
    }

    #[test]
    fn bulks_from_propagates_non_bulk_error() {
        let args = vec![bulk("a"), Value::Integer(1)];
        assert!(args.bulks_from(0).is_err());
    }
}
