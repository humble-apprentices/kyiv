use std::io;
use serde_json;
use std::fs::{File};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::fmt::{Debug, Formatter};
use std::io::{Seek, SeekFrom, Write};

pub struct Database<S: Storage> {
    storage: S,
}

pub trait Storage: Sized {
    fn from(source: &str) -> Result<Self, io::Error> where Self: Storage;

    fn flush(&mut self) -> Result<(), io::Error>;

    fn get(&self, key: &str) -> Option<&str>;
    fn del(&mut self, key: &str) -> Result<(), io::Error>;
    fn set(&mut self, key: &str, value: &str) -> Result<(), io::Error>;
}

impl<S: Storage> Drop for Database<S> {
    fn drop(&mut self) {
        self.storage.flush().expect("failed to flush");
    }
}

impl<S: Storage> Deref for Database<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

impl<S: Storage> DerefMut for Database<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.storage
    }
}

pub struct JSONStorage {
    file: File,
    data: BTreeMap<String, String>,
}

impl Debug for JSONStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JSONStorage")
            .field("file", &self.file)
            .field("data", &self.data)
            .finish()
    }
}

impl Storage for JSONStorage {
    fn from(source: &str) -> Result<JSONStorage, io::Error> {
        let file = File::options().create(true).read(true).write(true).open(source)?;
        let data = if let Ok(map) = serde_json::from_reader(file.try_clone()?) {
            map
        } else {
            BTreeMap::new()
        };

        Ok(JSONStorage {
            data,
            file: file.try_clone()?,
        })
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let content = serde_json::to_vec(&self.data).expect("should be able to serialize");
        self.file.write_all(&content)?;
        self.file.flush()
    }

    fn get(&self, key: &str) -> Option<&str> {
        Some(self.data.get(key)?.as_str())
    }

    fn del(&mut self, key: &str) -> Result<(), io::Error> {
        self.data.remove(key);
        Ok(())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<(), io::Error> {
        self.data.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

pub fn open<S: Storage>(path: &str) -> Result<Database<S>, io::Error> {
    Ok(Database { storage: S::from(path)? })
}

#[cfg(test)]
mod tests {
    use crate::{JSONStorage, Storage};

    const DB_PATH: &str = "./shit.db";

    #[test]
    fn it_opens_a_database() {
        let result = crate::open::<JSONStorage>(DB_PATH);
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn it_sets_deletes_and_gets_values() {
        let mut db = crate::open::<JSONStorage>(DB_PATH).unwrap();

        let set_result = db.set("xixi", "haha");

        assert_eq!(set_result.is_ok(), true);

        let get_result = db.get("xixi");

        assert_eq!(get_result, Some("haha"));

        let _ = db.del("xixi");

        assert_eq!(db.get("xixi"), None);
    }

    #[test]
    fn it_persists_data() {
        let mut db = crate::open::<JSONStorage>(DB_PATH).unwrap();

        let _ = db.set("xixi", "haha");
        let _ = db.set("somethingBig", "BBBBBBiiiiiigggggggggg");
        let _ = db.set("hehe", "heihei");

        drop(db);

        let db = crate::open::<JSONStorage>(DB_PATH).unwrap();

        let xixi_result = db.get("xixi");
        let big_result = db.get("somethingBig");
        let hehe_result = db.get("hehe");

        assert_eq!(xixi_result, Some("haha"));
        assert_eq!(big_result, Some("BBBBBBiiiiiigggggggggg"));
        assert_eq!(hehe_result, Some("heihei"));
    }
}
