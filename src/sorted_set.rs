use std::collections::BTreeMap;

#[derive(Clone)]
pub struct SortedSetValue(BTreeMap<String, f64>);

#[derive(Clone)]
pub struct SortedSetRecord {
    set_id: String,
    value: SortedSetValue,
}

impl SortedSetRecord {
    pub fn new(id: &str) -> Self {
        Self {
            set_id: id.to_string(),
            value: SortedSetValue(BTreeMap::new()),
        }
    }

    pub fn add(&mut self, member: &str, score: f64) -> bool {
        let old_value = self.value.0.insert(member.to_string(), score);
        let is_new_member = old_value.is_none();
        is_new_member
    }
}
