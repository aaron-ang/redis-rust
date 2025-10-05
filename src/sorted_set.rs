use std::collections::HashMap;

use ordered_float::OrderedFloat;
use skiplist::OrderedSkipList;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Entry {
    score: OrderedFloat<f64>,
    member: String,
}

#[derive(Default)]
pub struct SortedSetRecord {
    by_score: OrderedSkipList<Entry>,
    by_member: HashMap<String, OrderedFloat<f64>>,
}

impl SortedSetRecord {
    pub fn new() -> Self {
        Self {
            by_score: OrderedSkipList::new(),
            by_member: HashMap::new(),
        }
    }

    pub fn len(&self) -> i64 {
        self.by_score.len() as i64
    }

    pub fn add(&mut self, member: &str, score: f64) -> bool {
        let member_str = member.to_string();
        let score_of = OrderedFloat(score);

        match self.by_member.get(&member_str) {
            Some(&old_score) if old_score == score_of => {
                // Member exists with the same score, nothing to do
                false
            }
            Some(&old_score) => {
                // Member exists with a different score, update
                let old_entry = Entry {
                    score: old_score,
                    member: member_str.clone(),
                };
                self.by_score.remove(&old_entry);
                self.by_score.insert(Entry {
                    score: score_of,
                    member: member_str.clone(),
                });
                self.by_member.insert(member_str, score_of);
                false
            }
            None => {
                // New member, insert
                self.by_score.insert(Entry {
                    score: score_of,
                    member: member_str.clone(),
                });
                self.by_member.insert(member_str, score_of);
                true
            }
        }
    }

    pub fn remove(&mut self, members: &[&str]) -> i64 {
        members
            .iter()
            .filter_map(|&member| {
                self.by_member.remove(member).map(|score| {
                    self.by_score.remove(&Entry {
                        score,
                        member: member.to_string(),
                    });
                })
            })
            .count() as i64
    }

    pub fn range(&self, start: i64, end: i64) -> Vec<String> {
        let len = self.by_score.len();

        if len == 0 {
            return Vec::new();
        }

        let start = Self::normalize(start, len);
        let mut end = Self::normalize(end, len);

        if start >= len {
            return Vec::new();
        }

        end = end.min(len - 1);

        if start > end {
            return Vec::new();
        }

        self.by_score
            .index_range(start..end + 1)
            .map(|e| e.member.clone())
            .collect()
    }

    fn normalize(idx: i64, len: usize) -> usize {
        if idx < 0 {
            (idx + len as i64).max(0) as usize
        } else {
            idx as usize
        }
    }

    pub fn rank(&self, member: &str) -> Option<i64> {
        let score = *self.by_member.get(member)?;
        let key = Entry {
            score,
            member: member.to_string(),
        };
        self.by_score.index_of(&key).map(|i| i as i64)
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.by_member.get(member).map(|s| s.into_inner())
    }

    pub fn members(&self) -> impl Iterator<Item = &str> {
        self.by_member.keys().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let mut sorted_set = SortedSetRecord::new();
        sorted_set.add("a", 1.0);
        sorted_set.add("b", 2.0);
        sorted_set.add("c", 3.0);

        assert_eq!(sorted_set.range(0, 0), vec!["a"]);
        assert_eq!(sorted_set.range(0, 1), vec!["a", "b"]);
        assert_eq!(sorted_set.range(0, 2), vec!["a", "b", "c"]);
        assert_eq!(sorted_set.range(0, 3), vec!["a", "b", "c"]);
        assert_eq!(sorted_set.range(1, 1), vec!["b"]);
        assert_eq!(sorted_set.range(1, 2), vec!["b", "c"]);
        assert_eq!(sorted_set.range(1, 3), vec!["b", "c"]);
        assert_eq!(sorted_set.range(2, 2), vec!["c"]);
        assert_eq!(sorted_set.range(2, 3), vec!["c"]);

        assert_eq!(sorted_set.range(2, 1), Vec::<String>::new());
        assert_eq!(sorted_set.range(3, 2), Vec::<String>::new());
    }

    #[test]
    fn test_negative_indices() {
        let mut sorted_set = SortedSetRecord::new();
        sorted_set.add("a", 1.0);
        sorted_set.add("b", 2.0);
        sorted_set.add("c", 3.0);

        assert_eq!(sorted_set.range(-2, -1), vec!["b", "c"]);
        assert_eq!(sorted_set.range(0, -2), vec!["a", "b"]);
    }
}
