//! XDBStore — layered store สำหรับแอปที่ update แบบ realtime (LSM-lite)
//!
//! หลักการ: ข้อมูลอยู่เป็น "layers" (ไฟล์ .xdb เรียงตามลำดับ 000001.xdb, 000002.xdb, ...)
//! - `put` = เขียน layer เล็กใหม่ (เร็ว — ไม่แตะตารางหลัก)
//! - `get` = ค้นจาก layer ใหม่ → เก่า (bloom filter ทำให้ miss ถูกมาก)
//! - `delete` = เขียน tombstone กด key ใน layer ที่เก่ากว่า
//! - `compact` = รวมทุก layers เป็นไฟล์เดียว (เกิดอัตโนมัติเมื่อ layers สะสมถึง threshold)

use crate::{parse_entry, merge_tables, TableBuilder, XDBReader};
use fs4::fs_std::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{self};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// จำนวน layers ที่ทำให้ compact อัตโนมัติ (0 = ปิด)
pub const DEFAULT_COMPACT_THRESHOLD: usize = 8;

struct Layer {
    path: PathBuf,
    reader: Arc<XDBReader>,
}

pub struct XDBStore {
    dir: PathBuf,
    layers: RwLock<Vec<Layer>>,
    /// กันเขียน layer พร้อมกัน
    write_lock: Mutex<()>,
    next_seq: AtomicU64,
    compact_threshold: usize,
    /// ถือ exclusive lock ไว้ตลอดอายุ store — กันอีก process (หรือ instance) เปิด dir เดียวกัน
    _lock_file: File,
}

impl XDBStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Self::open_with(dir, DEFAULT_COMPACT_THRESHOLD)
    }

    /// `compact_threshold` = จำนวน layers ที่จะ trigger compact อัตโนมัติ (0 = compact เองเท่านั้น)
    pub fn open_with<P: AsRef<Path>>(dir: P, compact_threshold: usize) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;

        // exclusive lock บนไฟล์ LOCK — instance/process ที่สองจะเปิดไม่ได้จนกว่าตัวแรกจะปิด
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(dir.as_ref().join("LOCK"))?;
        let acquired = lock_file.try_lock_exclusive()?;
        if !acquired {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "x-db store at {} is already locked by another process/instance",
                    dir.as_ref().display()
                ),
            ));
        }

        let mut layers = Vec::new();
        let mut max_seq = 0u64;

        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let Some(seq) = layer_seq(&path) else { continue };
            max_seq = max_seq.max(seq);
            let reader = XDBReader::open(&path)
                .map_err(|e| io::Error::new(e.kind(), format!("layer {}: {e}", path.display())))?;
            layers.push(Layer { path, reader: Arc::new(reader) });
        }
        layers.sort_by_key(|l| layer_seq(&l.path).unwrap_or(0));

        Ok(Self {
            dir: dir.as_ref().to_path_buf(),
            layers: RwLock::new(layers),
            write_lock: Mutex::new(()),
            next_seq: AtomicU64::new(max_seq + 1),
            compact_threshold,
            _lock_file: lock_file,
        })
    }

    /// จำนวน layers ปัจจุบัน
    pub fn layer_count(&self) -> usize {
        self.layers.read().unwrap().len()
    }

    /// ค้นหา key ข้ามทุก layers (ตัวใหม่ชนะ, tombstone = ถูกลบ)
    /// คืนค่าเป็น owned Vec เพราะ layers อยู่หลัง RwLock
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let layers = self.layers.read().unwrap();
        for layer in layers.iter().rev() {
            match layer.reader.get_entry(key)? {
                Some(Some(v)) => return Ok(Some(v.to_vec())),
                Some(None) => return Ok(None), // tombstone — ถูกลบ หยุดค้น
                None => continue,             // ไม่มีใน layer นี้ ไปต่อ
            }
        }
        Ok(None)
    }

    pub fn has(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// เพิ่ม/แก้ค่า (upsert) — เขียน layer ใหม่ (key ซ้ำใน batch ตัวหลังชนะ, ไม่เรียงก็ได้)
    pub fn put(&self, entries: &[(&[u8], &[u8])]) -> io::Result<()> {
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = entries
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        dedup_last_wins(&mut pairs);

        let compact_needed = {
            let _g = self.write_lock.lock().unwrap();
            self.write_layer(|b| {
                for (k, v) in &pairs {
                    b.add(k, v)?;
                }
                Ok(())
            })?;
            self.layers.read().unwrap().len() >= self.compact_threshold && self.compact_threshold > 0
        };
        if compact_needed {
            self.compact()?;
        }
        Ok(())
    }

    /// ลบ keys — เขียน layer ที่มี tombstone กดค่าเก่า
    pub fn delete(&self, keys: &[&[u8]]) -> io::Result<()> {
        let mut sorted: Vec<Vec<u8>> = keys.iter().map(|k| k.to_vec()).collect();
        sorted.sort();
        sorted.dedup();

        let compact_needed = {
            let _g = self.write_lock.lock().unwrap();
            self.write_layer(|b| {
                for k in &sorted {
                    b.add_tombstone(k)?;
                }
                Ok(())
            })?;
            self.layers.read().unwrap().len() >= self.compact_threshold && self.compact_threshold > 0
        };
        if compact_needed {
            self.compact()?;
        }
        Ok(())
    }

    /// รวมทุก layers เป็นไฟล์เดียว — คืนค่าจำนวน layers หลัง compact
    /// tombstone ถูกคงไว้ (กันกรณีลบไฟล์เก่าไม่สำเร็จบน Windows แล้วข้อมูลโผล่อีก)
    pub fn compact(&self) -> io::Result<usize> {
        let _g = self.write_lock.lock().unwrap();

        let mut current: Vec<(PathBuf, Arc<XDBReader>)> = {
            let layers = self.layers.read().unwrap();
            layers.iter().map(|l| (l.path.clone(), l.reader.clone())).collect()
        };
        if current.len() <= 1 {
            return Ok(current.len());
        }

        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let merged_path = self.dir.join(format!("{seq:06}.xdb"));
        let inputs: Vec<&Path> = current.iter().map(|(p, _)| p.as_path()).collect();
        merge_tables(&inputs, &merged_path)?;

        let merged_reader = Arc::new(XDBReader::open(&merged_path)?);
        let old: Vec<(PathBuf, Arc<XDBReader>)> = {
            let mut layers = self.layers.write().unwrap();
            *layers = vec![Layer { path: merged_path, reader: merged_reader }];
            std::mem::take(&mut current)
        };
        let old_paths: Vec<PathBuf> = old.iter().map(|(p, _)| p.clone()).collect();
        drop(old); // ปล่อย mmap ของ layers เก่าก่อนลบไฟล์ (ถ้าไม่มี in-flight get ถืออยู่)

        // พยายามลบไฟล์เก่า — ถ้าลบไม่ได้ (Windows ยังถืออยู่) ปล่อยไว้เป็น orphan:
        // ตอนเปิดใหม่มันถูกโหลดเป็น layer เก่า (seq ต่ำกว่า) ซึ่งถูก merged layer กดอยู่เสมอ
        for path in &old_paths {
            let _ = std::fs::remove_file(path);
        }
        Ok(1)
    }

    /// เขียน layer ใหม่แบบ atomic (เขียน .tmp แล้ว rename) แล้วเพิ่มเข้ารายการ
    fn write_layer<F>(&self, fill: F) -> io::Result<()>
    where
        F: FnOnce(&mut TableBuilder) -> io::Result<()>,
    {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let final_path = self.dir.join(format!("{seq:06}.xdb"));
        let tmp = final_path.with_extension("xdb.tmp");

        let mut builder = TableBuilder::create(&tmp, 16)?;
        fill(&mut builder)?;
        builder.finish()?;

        if final_path.exists() {
            std::fs::remove_file(&final_path)?;
        }
        std::fs::rename(&tmp, &final_path)?;

        let reader = Arc::new(XDBReader::open(&final_path)?);
        self.layers.write().unwrap().push(Layer { path: final_path, reader });
        Ok(())
    }

    /// ไล่ "มุมมองปัจจุบัน" ของทั้ง store — เรียงตาม key, ตัด key ที่ถูกลบแล้ว,
    /// key ซ้ำให้ค่าจาก layer ใหม่สุด
    pub fn iter(&self) -> StoreIter {
        self.iter_from(&[])
    }

    /// iterator เริ่มที่ entry แรกที่ key >= start (seek)
    pub fn iter_from(&self, start: &[u8]) -> StoreIter {
        let layers = self.layers.read().unwrap();
        let readers: Vec<Arc<XDBReader>> = layers.iter().map(|l| l.reader.clone()).collect();
        StoreIter::new(readers, start.to_vec())
    }

    /// ไล่ keys ในช่วง [start, end) — end exclusive
    pub fn range<'a>(
        &'a self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + 'a {
        let end = end.to_vec();
        self.iter_from(start)
            .take_while(move |r| matches!(r, Ok((k, _)) if k < &end))
    }

    /// ไล่ทุก entries ที่ key ขึ้นต้นด้วย prefix
    pub fn prefix<'a>(
        &'a self,
        prefix: &[u8],
    ) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + 'a {
        let p = prefix.to_vec();
        self.iter_from(prefix)
            .take_while(move |r| matches!(r, Ok((k, _)) if k.starts_with(&p)))
    }
}

fn dedup_last_wins(pairs: &mut Vec<(Vec<u8>, Vec<u8>)>) {
    let mut unique: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pairs.len());
    for (k, v) in pairs.drain(..) {
        if let Some(last) = unique.last_mut() {
            if last.0 == k {
                last.1 = v;
                continue;
            }
        }
        unique.push((k, v));
    }
    *pairs = unique;
}

/// ชื่อไฟล์ layer = `{seq:06}.xdb` → คืน seq
fn layer_seq(path: &Path) -> Option<u64> {
    if path.extension()?.to_str()? != "xdb" {
        return None;
    }
    path.file_stem()?.to_str()?.parse().ok()
}

/// Iterator มุมมองรวมของหลาย layers (ใหม่ชนะ, ตัด tombstone) — เก็บ Arc ไว้เองจึงไม่ยืม lifetime จาก store
pub struct StoreIter {
    readers: Vec<Arc<XDBReader>>,
    /// block/offset ถัดไปของแต่ละ layer
    block: Vec<usize>,
    offset: Vec<usize>,
    heads: Vec<Option<(Vec<u8>, Option<Vec<u8>>)>>,
    /// (seek) ข้าม keys ที่น้อยกว่าจุดเริ่ม
    skip_below: Vec<u8>,
}

impl StoreIter {
    fn new(readers: Vec<Arc<XDBReader>>, start: Vec<u8>) -> Self {
        let n = readers.len();
        let mut it = Self {
            readers,
            block: vec![0; n],
            offset: vec![0; n],
            heads: vec![None; n],
            skip_below: start,
        };
        for i in 0..n {
            it.advance(i);
        }
        it
    }

    /// เลื่อน head ของ layer i ไป entry ถัดไป (ข้ามข้าม block อัตโนมัติ)
    fn advance(&mut self, i: usize) {
        loop {
            let reader = &self.readers[i];
            if self.block[i] >= reader.block_count() {
                self.heads[i] = None;
                return;
            }
            let data = match reader.block_data_at(self.block[i]) {
                Ok(d) => d,
                Err(_) => {
                    self.heads[i] = None; // ไฟล์เสีย — หยุด layer นี้ไปเลย (k-way ยังได้ส่วนที่ดี)
                    return;
                }
            };
            if self.offset[i] >= data.len() {
                self.block[i] += 1;
                self.offset[i] = 0;
                continue;
            }
            match parse_entry(&data[self.offset[i]..]) {
                Ok(Some((k, v, consumed))) => {
                    self.offset[i] += consumed;
                    self.heads[i] = Some((k.to_vec(), v.map(|v| v.to_vec())));
                    return;
                }
                _ => {
                    self.block[i] += 1;
                    self.offset[i] = 0;
                }
            }
        }
    }
}

impl Iterator for StoreIter {
    type Item = io::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(min_key) = self.heads.iter().flatten().map(|(k, _)| k.as_slice()).min() else {
                return None;
            };
            let min_key = min_key.to_vec();

            // (seek) ข้าม keys ที่น้อยกว่าจุดเริ่ม — เลื่อนทุก head ที่ถือ key นี้
            if min_key < self.skip_below {
                for i in 0..self.heads.len() {
                    if let Some((k, _)) = &self.heads[i] {
                        if k.as_slice() == min_key {
                            self.advance(i);
                        }
                    }
                }
                continue;
            }

            // ค่าจาก layer ใหม่สุด (index สูงสุด) ชนะ
            let mut winner: Option<Option<Vec<u8>>> = None;
            for i in 0..self.heads.len() {
                let Some((k, v)) = &self.heads[i] else { continue };
                if k.as_slice() == min_key {
                    winner = Some(v.clone());
                    let i = i; // เลื่อนทุก head ที่ถือ key นี้
                    self.advance(i);
                }
            }

            match winner {
                Some(Some(v)) => return Some(Ok((min_key, v))),
                Some(None) => continue, // tombstone — key นี้ถูกลบ ข้ามไป key ถัดไป
                None => continue,
            }
        }
    }
}
