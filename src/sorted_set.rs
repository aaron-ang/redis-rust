use std::collections::HashMap;

use ordered_float::OrderedFloat;
use skiplist::OrderedSkipList;

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Entry {
    score: OrderedFloat<f64>,
    member: String,
}

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
                if let Some(idx) = self.by_score.index_of(&old_entry) {
                    self.by_score.remove_index(idx);
                }
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

    pub fn range(&self, start: i64, mut end: i64) -> Vec<String> {
        if start >= self.by_score.len() as i64 {
            return Vec::new();
        }
        if end >= self.by_score.len() as i64 {
            end = (self.by_score.len() as i64) - 1;
        }
        if start > end {
            return Vec::new();
        }
        self.by_score
            .index_range(start.max(0) as usize..(end + 1).max(0) as usize)
            .map(|e| e.member.clone())
            .collect()
    }

    pub fn rank(&self, member: &str) -> Option<i64> {
        let score = *self.by_member.get(member)?;
        let key = Entry {
            score,
            member: member.to_string(),
        };
        self.by_score.index_of(&key).map(|i| i as i64)
    }
}
