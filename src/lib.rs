use std::collections::HashMap;

struct DB {
    kv_map: HashMap<String, String>,
}

impl DB {
    #[allow(dead_code)]
    fn get(&self, key: &String) -> Option<&String> {
        self.kv_map.get(key)
    }

    #[allow(dead_code)]
    fn set(&mut self, key: String, value: String) {
        self.kv_map.insert(key, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut db = DB {
            kv_map: HashMap::new(),
        };

        db.set("key".to_string(), "value".to_string());
        let res = db.get(&"key".to_string());
        
        match res {
            Some(p) => assert_eq!("value", p),
            None => panic!("Failure, key is missing"),
        }
    }
}
