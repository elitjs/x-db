//! XDB — API เดียวจบ (Rust) รวมอ่าน/เขียน/อัพเดต/ลบ/ไล่ ไว้ใน struct เดียวบนไฟล์ .xdb ไฟล์เดียว
//!
//! ```no_run
//! use x_db::XDB;
//!
//! let db = XDB::open("app.xdb")?;
//! db.set("user:1", "สมชาย")?;               // &str หรือ &[u8] ก็ได้
//! db.set_many(&[("a", "1"), ("b", "2")])?;
//! let name = db.get_utf8("user:1")?;          // Some("สมชาย")
//! db.set("user:1", "สมชาย (อัพเดต)")?;        // update บนไฟล์เดียวกัน
//! db.del(&["a", "b"])?;
//! for entry in db.prefix(b"user:") {
//!     let (_key, _value) = entry?;
//! }
//! db.save()?;                                  // บีบเป็นไฟล์เดียวแบบ atomic
//! # Ok::<(), std::io::Error>(())
//! ```

use crate::singlefile::XdbSingleFile;
use crate::store::StoreOptions;
use crate::XDBReader;
use std::io;
use std::path::Path;

/// ระดับความปลอดภัยของข้อมูล
pub enum XDBDurability {
    /// fsync ทุก operation — ไฟดับไม่เสียข้อมูลเลย (set ~1ms)
    Safe,
    /// set เร็ว (~5µs) + ซิงก์ดิสก์ทุก 200ms — ไฟดับเสียได้สูงสุด 200ms (แนะนำ)
    Balanced,
    /// เร็วสุด ไม่รอดิสก์ — เหมาะกับ cache/ข้อมูลสร้างใหม่ได้
    Fast,
}

pub struct XDBOptions {
    pub durability: XDBDurability,
    /// entries ใน memtable ก่อน flush เป็น layer (default 4096, 0 = ตาม default)
    pub flush_entries: usize,
    /// จำนวน layers ที่ trigger compact อัตโนมัติ (default 8)
    pub compact_threshold: usize,
}

impl Default for XDBOptions {
    fn default() -> Self {
        Self {
            durability: XDBDurability::Safe,
            flush_entries: 0,
            compact_threshold: 0,
        }
    }
}

pub struct XDB {
    sf: XdbSingleFile,
}

impl XDB {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with(path, XDBOptions::default())
    }

    pub fn open_with<P: AsRef<Path>>(path: P, options: XDBOptions) -> io::Result<Self> {
        let mut store = StoreOptions::default();
        match options.durability {
            XDBDurability::Safe => {}
            XDBDurability::Balanced => {
                store.sync = false;
                store.sync_interval_ms = 200;
            }
            XDBDurability::Fast => store.sync = false,
        }
        if options.flush_entries > 0 {
            store.flush_entries = options.flush_entries;
        }
        if options.compact_threshold > 0 {
            store.compact_threshold = options.compact_threshold;
        }
        Ok(Self { sf: XdbSingleFile::open_with(path, store)? })
    }

    /// ตั้งค่า — key/value รับ &str หรือ &[u8] ก็ได้ (AsRef<[u8]>)
    pub fn set<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> io::Result<()> {
        self.sf.put(&[(key.as_ref(), value.as_ref())])
    }

    /// ตั้งหลายค่าในคำสั่งเดียว (batch — ยิ่งเยอะยิ่งเร็ว ~2-5µs/key)
    pub fn set_many<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, entries: &[(K, V)]) -> io::Result<()> {
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_ref(), v.as_ref()))
            .collect();
        self.sf.put(&refs)
    }

    /// อ่านค่าเป็น bytes
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<Vec<u8>>> {
        self.sf.get(key.as_ref())
    }

    /// อ่านค่าเป็น String (None ถ้าไม่เจอหรือไม่ใช่ UTF-8 ที่สมบูรณ์)
    pub fn get_utf8<K: AsRef<[u8]>>(&self, key: K) -> io::Result<Option<String>> {
        Ok(self
            .sf
            .get(key.as_ref())?
            .and_then(|v| String::from_utf8(v).ok()))
    }

    pub fn has<K: AsRef<[u8]>>(&self, key: K) -> io::Result<bool> {
        self.sf.has(key.as_ref())
    }

    /// บวกค่าตัวเลขเข้า key (แบบ INCRBY ของ Redis) — ถ้า key ไม่มีเริ่มที่ 0
    /// คืนค่าใหม่หลังบวก (delta ติดลบ = ลด) / Err ถ้าค่าเดิมไม่ใช่ตัวเลข
    pub fn add<K: AsRef<[u8]>>(&self, key: K, delta: f64) -> io::Result<f64> {
        let current: f64 = match self.sf.get(key.as_ref())? {
            Some(v) => String::from_utf8(v)
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "x-db add: existing value is not a number")
                })?,
            None => 0.0,
        };
        let next = current + delta;
        let s = format!("{next}");
        self.sf.put(&[(key.as_ref(), s.as_bytes())])?;
        Ok(next)
    }

    /// ลบหลาย key ในคำสั่งเดียว
    pub fn del<K: AsRef<[u8]>>(&self, keys: &[K]) -> io::Result<()> {
        let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_ref()).collect();
        self.sf.delete(&refs)
    }

    /// ไล่ทั้งหมดเรียงตาม key
    pub fn iter(&self) -> crate::store::StoreIter {
        self.sf.iter()
    }

    /// ไล่เฉพาะ key ที่ขึ้นต้นด้วย prefix
    pub fn prefix(&self, prefix: &[u8]) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + '_ {
        self.sf.prefix(prefix)
    }

    /// ไล่ช่วง [start, end) — end exclusive
    pub fn range(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + '_ {
        self.sf.range(start, end)
    }

    /// เริ่มไล่จาก key >= start
    pub fn seek(&self, start: &[u8]) -> crate::store::StoreIter {
        self.sf.seek(start)
    }

    /// เปิดอ่านแบบ snapshot (เร็วสุด ~0.5µs/get) — เห็นข้อมูล ณ ตอน save() ล่าสุด
    pub fn snapshot(&self) -> io::Result<XDBReader> {
        self.sf.open_snapshot()
    }

    /// บีบทุกอย่างเข้าไฟล์ .xdb ไฟล์เดียวแบบ atomic (reader เก่าไม่พัง)
    pub fn save(&self) -> io::Result<()> {
        self.sf.save()
    }

    /// ปิด + save + เหลือไฟล์เดียวพกไปไหนก็ได้ (เปิดครั้งหน้าข้อมูลครบ)
    pub fn close(self) -> io::Result<()> {
        self.sf.export_and_close()
    }
}
