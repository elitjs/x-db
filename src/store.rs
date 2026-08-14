//! XDBStore — layered store สำหรับแอปที่ update แบบ realtime (LSM-lite)
//!
//! หลักการ: ข้อมูลอยู่เป็น "layers" (ไฟล์ .xdb เรียงตามลำดับ 000001.xdb, 000002.xdb, ...)
//! - `put` = เขียน WAL + memtable (เร็วมาก — fsync รวมทั้ง batch เดียว)
//! - memtable เต็ม → flush เป็น layer ใหม่อัตโนมัติ
//! - `get` = memtable → layer ใหม่ → เก่า (bloom filter ทำให้ miss ถูกมาก)
//! - `delete` = tombstone กด key ใน layer ที่เก่ากว่า
//! - `compact` = รวมทุก layers เป็นไฟล์เดียว (บีบอัด LZ4, เกิดอัตโนมัติเมื่อถึง threshold)

use crate::{parse_entry, merge_tables_with, TableBuilder, XDBReader};
use crate::wal::Wal;
use fs4::fs_std::FileExt;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// จำนวน layers ที่ทำให้ compact อัตโนมัติ (0 = ปิด)
pub const DEFAULT_COMPACT_THRESHOLD: usize = 8;
/// จำนวน entries ใน memtable ที่ trigger flush เป็น layer (0 = ไม่ auto-flush)
pub const DEFAULT_FLUSH_ENTRIES: usize = 4096;

/// ตัวเลือกการเปิด XDBStore
pub struct StoreOptions {
    /// จำนวน layers ที่ trigger compact อัตโนมัติ (default 8, 0 = ปิด)
    pub compact_threshold: usize,
    /// จำนวน entries ใน memtable ที่ trigger flush เป็น layer (default 4096, 0 = flush เองเท่านั้น)
    pub flush_entries: usize,
    /// fsync WAL ทุก put (default true) — false = เร็วขึ้นแต่ถ้าไฟดัน/พัง อาจเสีย put ล่าสุด
    pub sync: bool,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            compact_threshold: DEFAULT_COMPACT_THRESHOLD,
            flush_entries: DEFAULT_FLUSH_ENTRIES,
            sync: true,
        }
    }
}

struct Layer {
    path: PathBuf,
    reader: Arc<XDBReader>,
}

struct StoreInner {
    dir: PathBuf,
    layers: RwLock<Vec<Layer>>,
    /// memtable — entries ใหม่สุด (Option = tombstone) อยู่ที่นี่ก่อน flush เป็น layer
    memtable: RwLock<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    /// WAL — คู่หูความ durable ของ memtable
    wal: Mutex<Wal>,
    /// กันเขียนพร้อมกัน (put/delete/flush/compact)
    write_lock: Mutex<()>,
    next_seq: AtomicU64,
    options: StoreOptions,
    /// กัน compact ซ้อนกัน (true = มี background compaction กำลังรว)
    compacting: AtomicBool,
    /// ถือ exclusive lock ไว้ตลอดอายุ store — กันอีก process (หรือ instance) เปิด dir เดียวกัน
    _lock_file: File,
}

pub struct XDBStore {
    inner: Arc<StoreInner>,
}

impl XDBStore {
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Self::open_opts(dir, StoreOptions::default())
    }

    /// `compact_threshold` = จำนวน layers ที่จะ trigger compact อัตโนมัติ (0 = compact เองเท่านั้น)
    pub fn open_with<P: AsRef<Path>>(dir: P, compact_threshold: usize) -> io::Result<Self> {
        let mut opts = StoreOptions::default();
        opts.compact_threshold = compact_threshold;
        Self::open_opts(dir, opts)
    }

    pub fn open_opts<P: AsRef<Path>>(dir: P, options: StoreOptions) -> io::Result<Self> {
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

        // Replay WAL ลง memtable (torn tail จาก crash ถูกตัดทิ้งใน Wal::open)
        let (wal, wal_entries) = Wal::open(&dir.as_ref().join("wal.log"), options.sync)?;
        let memtable: BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            wal_entries.into_iter().collect();

        Ok(Self {
            inner: Arc::new(StoreInner {
                dir: dir.as_ref().to_path_buf(),
                layers: RwLock::new(layers),
                memtable: RwLock::new(memtable),
                wal: Mutex::new(wal),
                write_lock: Mutex::new(()),
                next_seq: AtomicU64::new(max_seq + 1),
                options,
                compacting: AtomicBool::new(false),
                _lock_file: lock_file,
            }),
        })
    }

    /// จำนวน layers ปัจจุบัน
    pub fn layer_count(&self) -> usize {
        self.inner.layers.read().unwrap().len()
    }

    /// จำนวน entries ใน memtable ที่ยังไม่ได้ flush
    pub fn memtable_len(&self) -> usize {
        self.inner.memtable.read().unwrap().len()
    }

    /// มี background compaction กำลังรวอยู่หรือไม่ (ไว้เช็คตอนปิดแอป/testing)
    pub fn is_compacting(&self) -> bool {
        self.inner.compacting.load(Ordering::SeqCst)
    }

    /// ค้นหา key: memtable (ใหม่สุด) → layers ใหม่ → เก่า (tombstone = ถูกลบ)
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(v) = self.inner.memtable.read().unwrap().get(key) {
            return Ok(v.clone());
        }
        let layers = self.inner.layers.read().unwrap();
        for layer in layers.iter().rev() {
            match layer.reader.get_entry(key)? {
                Some(Some(v)) => return Ok(Some(v)),
                Some(None) => return Ok(None), // tombstone — ถูกลบ หยุดค้น
                None => continue,
            }
        }
        Ok(None)
    }

    pub fn has(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// เพิ่ม/แก้ค่า (upsert) — เขียน WAL ก่อน แล้วลง memtable (key ซ้ำตัวหลังชนะ, ไม่เรียงก็ได้)
    pub fn put(&self, entries: &[(&[u8], &[u8])]) -> io::Result<()> {
        let mut batch: Vec<(Vec<u8>, Option<Vec<u8>>)> = entries
            .iter()
            .map(|(k, v)| (k.to_vec(), Some(v.to_vec())))
            .collect();
        batch.sort_by(|a, b| a.0.cmp(&b.0));
        batch.dedup_by(|a, b| a.0 == b.0);
        self.write_batch(batch)
    }

    /// เหมือน `put` แต่รับ owned entries — ประหยัดการ copy หนึ่งรอบ (ใช้จาก binding)
    pub fn put_owned(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) -> io::Result<()> {
        let mut batch: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            entries.into_iter().map(|(k, v)| (k, Some(v))).collect();
        batch.sort_by(|a, b| a.0.cmp(&b.0));
        batch.dedup_by(|a, b| a.0 == b.0);
        self.write_batch(batch)
    }

    /// ลบ keys — เขียน tombstone ลง WAL + memtable กดค่าเก่า
    pub fn delete(&self, keys: &[&[u8]]) -> io::Result<()> {
        let mut batch: Vec<(Vec<u8>, Option<Vec<u8>>)> =
            keys.iter().map(|k| (k.to_vec(), None)).collect();
        batch.sort_by(|a, b| a.0.cmp(&b.0));
        batch.dedup_by(|a, b| a.0 == b.0);
        self.write_batch(batch)
    }

    /// แกนร่วมของ put/delete: WAL → memtable → flush ถ้าเต็ม → compact ถ้าถึง threshold
    fn write_batch(&self, batch: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> io::Result<()> {
        let compact_needed;
        {
            let _g = self.inner.write_lock.lock().unwrap();
            self.inner.wal.lock().unwrap().append(&batch)?;
            {
                // move เข้า memtable เลย — batch เป็นเจ้าของ owned อยู่แล้ว ไม่ต้อง clone
                let mut mem = self.inner.memtable.write().unwrap();
                for (k, v) in batch {
                    mem.insert(k, v);
                }
            }
            if self.inner.options.flush_entries > 0
                && self.inner.memtable.read().unwrap().len() >= self.inner.options.flush_entries
            {
                self.flush_locked()?;
            }
            compact_needed = self.inner.options.compact_threshold > 0
                && self.inner.layers.read().unwrap().len() >= self.inner.options.compact_threshold;
        }
        if compact_needed {
            // background: ไม่ให้ caller ต้องรอ merge ตารางใหญ่
            self.compact_async();
        }
        Ok(())
    }

    /// รวม layers ใน thread แยก — caller ไม่ต้องรอ (ถ้ามี compaction กำลังรวอยู่จะข้ามรอบนี้)
    pub fn compact_async(&self) {
        // มี compaction รวอยู่แล้ว → รอรอบหน้า (write_batch ครั้งถัดไปจะ trigger ใหม่)
        if self.inner.compacting.swap(true, Ordering::SeqCst) {
            return;
        }
        let inner = self.inner.clone();
        std::thread::spawn(move || {
            // วนรวจไปเรื่อย ๆ จนกว่า layers จะเบากว่า threshold
            // (เพราะ trigger หลักผูกกับ put — รอบสุดท้ายไม่มี put มาปลุกก็ต้องเก็บให้จบเอง)
            while inner.options.compact_threshold > 0
                && inner.layers.read().unwrap().len() >= inner.options.compact_threshold
            {
                if let Err(e) = compact_layers_sync(&inner) {
                    eprintln!("x-db: background compaction failed: {e}");
                    break;
                }
            }
            inner.compacting.store(false, Ordering::SeqCst);
        });
    }

    /// Flush memtable เป็น layer ใหม่ + ล้าง WAL — เรียกเองได้ (ปกติ auto ตาม flush_entries)
    pub fn flush(&self) -> io::Result<()> {
        let _g = self.inner.write_lock.lock().unwrap();
        self.flush_locked()
    }

    /// flush โดยถือ write_lock อยู่แล้ว
    fn flush_locked(&self) -> io::Result<()> {
        // ยึด memtable ทั้งก้อนไปเลย (take) — ไม่ clone หลายพัน entries
        let taken = { std::mem::take(&mut *self.inner.memtable.write().unwrap()) };
        if taken.is_empty() {
            return Ok(());
        }

        match self.write_memtable_layer(&taken) {
            Ok((final_path, reader)) => {
                // push layer (write_lock ครอบอยู่แล้ว ไม่มี writer แทรก จึงสลับได้อย่างปลอดภัย)
                {
                    let mut layers = self.inner.layers.write().unwrap();
                    layers.push(Layer { path: final_path, reader });
                }
                // layer ถาวรแล้ว → ล้าง WAL ได้
                self.inner.wal.lock().unwrap().reset()?;
                Ok(())
            }
            Err(e) => {
                // เขียน layer พัง — คืน entries กลับเข้า memtable ก่อนแจ้ง error (ไม่เสียข้อมูล)
                let mut mem = self.inner.memtable.write().unwrap();
                for (k, v) in taken {
                    mem.insert(k, v);
                }
                Err(e)
            }
        }
    }

    /// เขียน memtable เป็น layer ใหม่แบบ atomic (tmp + rename + fsync)
    fn write_memtable_layer(
        &self,
        mem: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ) -> io::Result<(PathBuf, Arc<XDBReader>)> {
        let seq = self.inner.next_seq.fetch_add(1, Ordering::SeqCst);
        let final_path = self.inner.dir.join(format!("{seq:06}.xdb"));
        let tmp = final_path.with_extension("xdb.tmp");
        {
            let mut b = TableBuilder::create(&tmp, mem.len())?;
            for (k, v) in mem {
                match v {
                    Some(v) => b.add(k, v)?,
                    None => b.add_tombstone(k)?,
                }
            }
            b.finish()?;
        }
        if final_path.exists() {
            std::fs::remove_file(&final_path)?;
        }
        std::fs::rename(&tmp, &final_path)?;
        let reader = Arc::new(XDBReader::open(&final_path)?);
        Ok((final_path, reader))
    }

    /// รวมทุก layers เป็นไฟล์เดียว — คืนค่าจำนวน layers หลัง compact
    /// tombstone ถูกคงไว้ (กันกรณีลบไฟล์เก่าไม่สำเร็จบน Windows แล้วข้อมูลโผล่อีก)
    pub fn compact(&self) -> io::Result<usize> {
        let _g = self.inner.write_lock.lock().unwrap();
        // ดัน memtable ลง layer ก่อน จะได้รวมข้อมูลทั้งหมด
        self.flush_locked()?;
        if let Some((seq, merged_path, merged_reader, input_paths)) = {
            let n = self.inner.layers.read().unwrap().len();
            merge_layers_range(&self.inner, 0, n)?
        } {
            swap_layers_locked(&self.inner, seq, merged_path, merged_reader, &input_paths);
            for path in &input_paths {
                let _ = std::fs::remove_file(path);
            }
        }
        Ok(self.inner.layers.read().unwrap().len())
    }

    /// ไล่ "มุมมองปัจจุบัน" ของทั้ง store — รวม memtable — เรียงตาม key,
    /// ตัด key ที่ถูกลบแล้ว, key ซ้ำให้ค่าจากตัวใหม่สุด
    pub fn iter(&self) -> StoreIter {
        self.iter_from(&[])
    }

    /// iterator เริ่มที่ entry แรกที่ key >= start (seek)
    pub fn iter_from(&self, start: &[u8]) -> StoreIter {
        let layers = self.inner.layers.read().unwrap();
        let readers: Vec<Arc<XDBReader>> = layers.iter().map(|l| l.reader.clone()).collect();
        let mem: Vec<(Vec<u8>, Option<Vec<u8>>)> = {
            let m = self.inner.memtable.read().unwrap();
            m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        StoreIter::new(readers, mem, start.to_vec())
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

/// เลือกช่วง layers ที่ควรรว (tiered): กลุ่มใหม่สุดย้อนไปหาเก่า หยุดก่อนกลืน layer
/// ที่ใหญ่กว่ากลุ่มสะสม 4 เท่า — layer base ตัวใหญ่จึงไม่โดน rewrite บ่อย ๆ
/// คืน (start, end) ของช่วง หรือ None ถ้าไม่มีกลุ่มที่คุ้ม (ต้องมีอย่างน้อย 2 ตัว)
/// ถ้า layers ล้น `hard_cap` จะรวทั้งหมดเลย (กันจำนวน layer บวมเกิน)
fn pick_tier(layers: &[Layer], hard_cap: usize) -> Option<(usize, usize)> {
    if layers.len() < 2 {
        return None;
    }
    if layers.len() >= hard_cap {
        return Some((0, layers.len()));
    }
    let mut group_count: u64 = 0;
    let mut start = layers.len();
    for i in (0..layers.len()).rev() {
        let n = layers[i].reader.len();
        // ตัวที่กำลังจะกลืนใหญ่กว่ากลุ่มที่สะสมไว้ 4 เท่า → ไม่กลืน (แพงเกินไป)
        if start < layers.len() && n > group_count.saturating_mul(4) {
            break;
        }
        group_count += n;
        start = i;
    }
    if layers.len() - start >= 2 {
        Some((start, layers.len()))
    } else {
        None
    }
}

/// รวม layers ช่วง [start, end) เป็นไฟล์เดียว (LZ4) — ขั้นตอน merge ไม่จับ lock เลย
/// (โหมด background รวนานเท่าไหร่ก็ได้ writer ไปต่อได้)
/// คืน (seq ของ merged, path, reader, paths ของ inputs) หรือ None ถ้าช่วงเล็กเกินไป
fn merge_layers_range(
    inner: &StoreInner,
    start: usize,
    end: usize,
) -> io::Result<Option<(u64, PathBuf, Arc<XDBReader>, Vec<PathBuf>)>> {
    let current: Vec<(PathBuf, Arc<XDBReader>)> = {
        let layers = inner.layers.read().unwrap();
        if end - start <= 1 || end > layers.len() {
            return Ok(None);
        }
        layers[start..end].iter().map(|l| (l.path.clone(), l.reader.clone())).collect()
    };
    if current.len() <= 1 {
        return Ok(None);
    }

    // จอง seq ล่วงหน้า → layer ใหม่ที่เกิดระหว่าง merge มี seq สูงกว่าเสมอ ลำดับจึงถูกต้องเสมอ
    let seq = inner.next_seq.fetch_add(1, Ordering::SeqCst);
    let merged_path = inner.dir.join(format!("{seq:06}.xdb"));
    let inputs: Vec<&Path> = current.iter().map(|(p, _)| p.as_path()).collect();
    // บีบอัด merged layer ด้วย LZ4 — ข้อมูลเย็น (cold) ไฟล์เล็กลง ส่วน layer ร้อนที่เขียนใหม่ยังเร็วเหมือนเดิม
    merge_tables_with(&inputs, &merged_path, true)?;
    let merged_reader = Arc::new(XDBReader::open(&merged_path)?);
    let input_paths: Vec<PathBuf> = current.iter().map(|(p, _)| p.clone()).collect();
    Ok(Some((seq, merged_path, merged_reader, input_paths)))
}

/// สลับ merged layer เข้ารายการ — ผู้เรียกต้องถือ write_lock อยู่แล้ว
/// (ป้องกัน deadlock: std Mutex ไม่ reentrant)
fn swap_layers_locked(
    inner: &StoreInner,
    seq: u64,
    merged_path: PathBuf,
    merged_reader: Arc<XDBReader>,
    input_paths: &[PathBuf],
) {
    let mut layers = inner.layers.write().unwrap();
    // ตัว input ออก (บางตัวอาจถูกรอบก่อนหน้าเอาออกไปแล้ว — ไม่เป็นไร)
    let input_set: std::collections::HashSet<&Path> = input_paths.iter().map(|p| p.as_path()).collect();
    layers.retain(|l| !input_set.contains(l.path.as_path()));
    // แทรก merged ตามลำดับ seq (หลัง layer เก่าที่เหลือ, ก่อน layer ใหม่ที่เกิดระหว่าง merge)
    let insert_at = layers
        .iter()
        .position(|l| layer_seq(&l.path).unwrap_or(0) > seq)
        .unwrap_or(layers.len());
    layers.insert(insert_at, Layer { path: merged_path, reader: merged_reader });
}

/// รวน layers ตามนโยบาย tiered — สำหรับ background thread
fn compact_layers_sync(inner: &StoreInner) -> io::Result<()> {
    let (start, end) = {
        let layers = inner.layers.read().unwrap();
        // tiered ก่อน; ถ้าเลือกกลุ่มไม่ได้ (เช่นมีแค่ base ใหญ่ + layer เดียว) ให้รวทั้งหมด
        pick_tier(&layers, inner.options.compact_threshold.max(2) * 2)
            .unwrap_or((0, layers.len()))
    };
    let Some((seq, merged_path, merged_reader, input_paths)) = merge_layers_range(inner, start, end)?
    else {
        return Ok(());
    };
    {
        let _g = inner.write_lock.lock().unwrap();
        swap_layers_locked(inner, seq, merged_path, merged_reader, &input_paths);
    }
    // พยายามลบไฟล์เก่า — ถ้าลบไม่ได้ (Windows ยังถืออยู่) ปล่อยไว้เป็น orphan:
    // ตอนเปิดใหม่มันถูกโหลดเป็น layer เก่า (seq ต่ำกว่า) ซึ่งถูก merged layer กดอยู่เสมอ
    for path in &input_paths {
        let _ = std::fs::remove_file(path);
    }
    Ok(())
}

/// ชื่อไฟล์ layer = `{seq:06}.xdb` → คืน seq
fn layer_seq(path: &Path) -> Option<u64> {
    if path.extension()?.to_str()? != "xdb" {
        return None;
    }
    path.file_stem()?.to_str()?.parse().ok()
}

/// Iterator มุมมองรวมของหลาย layers + memtable (ใหม่ชนะ, ตัด tombstone)
/// — เก็บ Arc/owned ไว้เองจึงไม่ยืม lifetime จาก store
pub struct StoreIter {
    readers: Vec<Arc<XDBReader>>,
    /// block/offset ถัดไปของแต่ละ layer
    block: Vec<usize>,
    offset: Vec<usize>,
    /// head ของแต่ละ "แหล่งข้อมูล" — index สุดท้าย = memtable (ใหม่สุด)
    heads: Vec<Option<(Vec<u8>, Option<Vec<u8>>)>>,
    /// cursor บน memtable snapshot (index ใน mem_snap)
    mem_snap: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    mem_idx: usize,
    /// (seek) ข้าม keys ที่น้อยกว่าจุดเริ่ม
    skip_below: Vec<u8>,
}

impl StoreIter {
    fn new(readers: Vec<Arc<XDBReader>>, mem: Vec<(Vec<u8>, Option<Vec<u8>>)>, start: Vec<u8>) -> Self {
        let n = readers.len();
        let mut it = Self {
            readers,
            block: vec![0; n],
            offset: vec![0; n],
            heads: vec![None; n],
            mem_snap: mem,
            mem_idx: 0,
            skip_below: start,
        };
        // memtable เป็น head ตัวสุดท้าย (index n) — ใหม่สุดจึงชนะใน k-way
        it.heads.push(it.mem_head());
        for i in 0..n {
            it.advance(i);
        }
        it
    }

    /// head ปัจจุบันของ memtable snapshot
    fn mem_head(&self) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
        self.mem_snap.get(self.mem_idx).cloned()
    }

    /// เลื่อน memtable cursor
    fn advance_mem(&mut self) {
        self.mem_idx += 1;
        let n = self.heads.len();
        self.heads[n - 1] = self.mem_head();
    }

    /// เลื่อน head ตัวที่ i — ตัวสุดท้ายคือ memtable, ที่เหลือคือ layers
    fn advance_any(&mut self, i: usize) {
        if i == self.readers.len() {
            self.advance_mem();
        } else {
            self.advance(i);
        }
    }

    /// เลื่อน head ของแหล่งข้อมูลที่ i (layers ธรรมดา)
    fn advance(&mut self, i: usize) {
        loop {
            let reader = &self.readers[i];
            if self.block[i] >= reader.block_count() {
                self.heads[i] = None;
                return;
            }
            let idx = self.block[i];
            let data = match reader.block_payload(idx) {
                Ok(d) => d,
                Err(_) => {
                    self.heads[i] = None; // ไฟล์เสีย — หยุด layer นี้ไปเลย (k-way ยังได้ส่วนที่ดี)
                    return;
                }
            };
            let entries_len = match reader.entries_len_of(idx) {
                Ok(n) => n,
                Err(_) => {
                    self.heads[i] = None;
                    return;
                }
            };
            if self.offset[i] >= entries_len {
                self.block[i] += 1;
                self.offset[i] = 0;
                continue;
            }
            match parse_entry(&data.as_slice()[self.offset[i]..]) {
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
                            self.advance_any(i);
                        }
                    }
                }
                continue;
            }

            // ค่าจากแหล่งข้อมูลใหม่สุด (index สูงสุด — memtable ชนะทุก layer) ชนะ
            let mut winner: Option<Option<Vec<u8>>> = None;
            for i in 0..self.heads.len() {
                let Some((k, v)) = &self.heads[i] else { continue };
                if k.as_slice() == min_key {
                    winner = Some(v.clone());
                    self.advance_any(i);
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
