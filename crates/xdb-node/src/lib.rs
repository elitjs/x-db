//! napi-rs bindings: เรียกใช้ x-db จาก Node.js/TypeScript โดยตรง (ไม่ผ่าน HTTP)
use napi::bindgen_prelude::*;
use napi_derive::napi;
use napi::Result;
use std::sync::Arc;
use x_db::{merge_tables as inner_merge, parse_entry, XDBReader as InnerReader, XDBWriter};

/// key/value รับได้ทั้ง string (UTF-8) และ Buffer (binary)
#[napi(object)]
pub struct Entry {
  pub key: Either<String, Buffer>,
  pub value: Either<String, Buffer>,
}

/// entry เดียวที่ได้จาก iterator — value = null หมายถึง tombstone (คีย์ถูกลบ)
#[napi(object)]
pub struct IterEntry {
  pub key: Buffer,
  pub value: Option<Buffer>,
}

fn into_bytes(v: Either<String, Buffer>) -> Vec<u8> {
  match v {
    Either::A(s) => s.into_bytes(),
    Either::B(b) => b.as_ref().to_vec(),
  }
}

fn io_err(e: std::io::Error) -> Error {
  Error::new(Status::GenericFailure, format!("{e}"))
}

/// สร้างไฟล์ .xdb จาก entries — เรียง key และกรองตัวซ้ำให้เอง (ตัวหลังสุดชนะ)
#[napi]
pub fn write_table(path: String, entries: Vec<Entry>) -> Result<()> {
  let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = entries
    .into_iter()
    .map(|e| (into_bytes(e.key), into_bytes(e.value)))
    .collect();

  pairs.sort_by(|a, b| a.0.cmp(&b.0));
  let mut unique: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pairs.len());
  for (k, v) in pairs {
    if let Some(last) = unique.last_mut() {
      if last.0 == k {
        last.1 = v;
        continue;
      }
    }
    unique.push((k, v));
  }

  let refs: Vec<(&[u8], &[u8])> = unique
    .iter()
    .map(|(k, v)| (k.as_slice(), v.as_slice()))
    .collect();
  XDBWriter::write_table(&path, &refs).map_err(io_err)
}

/// รวมหลายไฟล์ .xdb เป็นไฟล์เดียว (streaming — ตารางใหญ่ก็ไม่กิน RAM)
/// key ซ้ำกัน: ไฟล์ที่อยู่หลังสุดใน `inputs` ชนะ
/// คืนจำนวน entries ในตารางผลลัพธ์
#[napi]
pub fn merge_tables(inputs: Vec<String>, output: String) -> Result<u32> {
  inner_merge(&inputs, output)
    .map_err(io_err)
    .map(|n| n.min(u32::MAX as u64) as u32)
}

/// Reader แบบ mmap — เปิดครั้งเดียวใช้ได้ตลอดอายุ process
/// ไฟล์เสียหาย (checksum ไม่ผ่าน ฯลฯ) จะ throw แทนที่จะ crash
#[napi]
pub struct XdbReader {
  inner: Arc<InnerReader>,
}

#[napi]
impl XdbReader {
  #[napi(constructor)]
  pub fn new(path: String) -> Result<Self> {
    InnerReader::open(&path)
      .map_err(io_err)
      .map(|inner| Self { inner: Arc::new(inner) })
  }

  /// จำนวน entries ทั้งหมดในตาราง
  #[napi(getter)]
  pub fn len(&self) -> u32 {
    self.inner.len().min(u32::MAX as u64) as u32
  }

  /// จำนวน blocks (64KB ต่อ block โดยประมาณ)
  #[napi(getter)]
  pub fn block_count(&self) -> u32 {
    self.inner.block_count().min(u32::MAX as usize) as u32
  }

  /// ค้นหา key → Buffer หรือ null ถ้าไม่พบ
  #[napi]
  pub fn get(&self, key: Either<String, Buffer>) -> Result<Option<Buffer>> {
    let key = into_bytes(key);
    Ok(self
      .inner
      .get(&key)
      .map_err(io_err)?
      .map(|v| Buffer::from(v.to_vec())))
  }

  /// ค้นหา key → UTF-8 string หรือ null (ถ้าไม่พบ หรือค่าไม่ใช่ valid UTF-8)
  #[napi]
  pub fn get_utf8(&self, key: Either<String, Buffer>) -> Result<Option<String>> {
    let key = into_bytes(key);
    Ok(self
      .inner
      .get(&key)
      .map_err(io_err)?
      .and_then(|v| String::from_utf8(v.to_vec()).ok()))
  }

  #[napi]
  pub fn has(&self, key: Either<String, Buffer>) -> Result<bool> {
    let key = into_bytes(key);
    self.inner.get(&key).map_err(io_err).map(|v| v.is_some())
  }

  /// สร้าง iterator ไล่ทุก entries เรียงตาม key
  /// ส่ง `start` เพื่อ seek: เริ่มที่ entry แรกที่ key >= start (ข้ามไป block ตรง ๆ เลย)
  #[napi]
  pub fn iter(&self, start: Option<Either<String, Buffer>>) -> XdbIterator {
    let start = start.map(into_bytes).unwrap_or_default();
    let block = self.inner.find_block_index(&start).unwrap_or(0);
    XdbIterator {
      reader: self.inner.clone(),
      block,
      offset: 0,
      data: Vec::new(),
      skip_below: if start.is_empty() { None } else { Some(start) },
    }
  }
}

/// iterator แบบ native — ถือ Arc ของ reader ไว้เอง จึงปลอดภัยแม้ JS จะ drop reader ก่อน
/// ตรวจ CRC ครั้งเดียวต่อ block (cache เป็น owned Vec)
#[napi]
pub struct XdbIterator {
  reader: Arc<InnerReader>,
  /// index ของ block *ถัดไป* ที่จะโหลด
  block: usize,
  offset: usize,
  /// data ของ block ปัจจุบัน (ผ่าน CRC แล้ว)
  data: Vec<u8>,
  /// (seek) ข้าม entries ที่ key น้อยกว่าค่านี้
  skip_below: Option<Vec<u8>>,
}

#[napi]
impl XdbIterator {
  /// คืน entry ถัดไปเป็น {key, value} หรือ null เมื่อหมด — ไฟล์เสียหายจะ throw
  #[napi]
  pub fn next(&mut self) -> Result<Option<IterEntry>> {
    loop {
      if self.offset >= self.data.len() {
        // โหลด block ถัดไป (ตรวจ CRC ครั้งเดียวต่อ block)
        if self.block >= self.reader.block_count() {
          return Ok(None);
        }
        let idx = self.block;
        let payload = self.reader.block_payload(idx).map_err(io_err)?;
        let entries_len = self.reader.entries_len_of(idx).map_err(io_err)?;
        self.data = payload.as_slice()[..entries_len].to_vec();
        self.block += 1;
        self.offset = 0;
        continue;
      }
      return match parse_entry(&self.data[self.offset..]).map_err(io_err)? {
        Some((k, v, consumed)) => {
          self.offset += consumed;
          // (seek) ข้าม entries ที่น้อยกว่าจุดเริ่ม
          if let Some(m) = &self.skip_below {
            if k < m.as_slice() {
              continue;
            }
            self.skip_below = None;
          }
          Ok(Some(IterEntry {
            key: Buffer::from(k.to_vec()),
            value: v.map(|v| Buffer::from(v.to_vec())),
          }))
        }
        None => {
          self.data.clear();
          continue;
        }
      };
    }
  }
}

// ---------------- XDBStore: realtime updates (LSM-lite) ----------------

use std::sync::Arc as StdArc;
use x_db::XDBStore as InnerStore;

/// Store แบบ layered สำหรับแอปที่ update แบบ realtime
/// put/delete = เขียน WAL + memtable (เร็วมาก) / get = memtable → layers ตัวใหม่ชนะ
/// compact อัตโนมัติเมื่อ layers ถึง threshold (default 8)
#[napi]
pub struct XdbStore {
  /// None = ถูกปิดด้วย close() แล้ว (ปลดล็อก directory ให้คนอื่นเปิดได้)
  inner: Option<StdArc<InnerStore>>,
}

/// ตัวเลือกการเปิด XdbStore
#[napi(object)]
pub struct StoreOptions {
  /// จำนวน layers ที่ trigger compact อัตโนมัติ (default 8, 0 = ปิด)
  pub compact_threshold: Option<u32>,
  /// จำนวน entries ใน memtable ที่ trigger flush เป็น layer (default 4096, 0 = flush เองเท่านั้น)
  pub flush_entries: Option<u32>,
  /// fsync WAL ทุก put (default true) — false = เร็วขึ้นแต่พังกลางทางอาจเสีย put ล่าสุด
  pub sync: Option<bool>,
}

fn closed_err() -> Error {
  Error::new(Status::GenericFailure, "store is closed")
}

#[napi]
impl XdbStore {
  /// เปิด store ที่ directory นั้น (สร้างให้ถ้ายังไม่มี)
  #[napi(constructor)]
  pub fn new(path: String, options: Option<StoreOptions>) -> Result<Self> {
    let opts = options.map(|o| {
      let mut s = x_db::store::StoreOptions::default();
      if let Some(t) = o.compact_threshold { s.compact_threshold = t as usize; }
      if let Some(f) = o.flush_entries { s.flush_entries = f as usize; }
      if let Some(sync) = o.sync { s.sync = sync; }
      s
    }).unwrap_or_default();
    InnerStore::open_opts(&path, opts)
      .map_err(io_err)
      .map(|inner| Self { inner: Some(StdArc::new(inner)) })
  }

  /// ปิด store + ปลด lock ของ directory — เรียกเมื่อใช้งานเสร็จ
  /// (ไม่เรียกก็ได้ จะปลดเองตอน GC แต่จะถือ lock ไว้นานกว่า)
  #[napi]
  pub fn close(&mut self) {
    self.inner = None;
  }

  fn inner(&self) -> Result<&InnerStore> {
    self.inner.as_deref().ok_or_else(closed_err)
  }

  /// จำนวน layers ปัจจุบัน
  #[napi(getter)]
  pub fn layer_count(&self) -> Result<u32> {
    Ok(self.inner()?.layer_count() as u32)
  }

  /// จำนวน entries ใน memtable ที่ยังไม่ได้ flush เป็น layer
  #[napi(getter)]
  pub fn memtable_len(&self) -> Result<u32> {
    Ok(self.inner()?.memtable_len().min(u32::MAX as usize) as u32)
  }

  /// มี background compaction กำลังรวอยู่หรือไม่
  #[napi(getter)]
  pub fn is_compacting(&self) -> Result<bool> {
    Ok(self.inner()?.is_compacting())
  }

  /// ดัน memtable ลง layer ถาวร + ล้าง WAL (ปกติ auto ตาม flushEntries อยู่แล้ว)
  #[napi]
  pub fn flush(&self) -> Result<()> {
    self.inner()?.flush().map_err(io_err)
  }

  /// เพิ่ม/แก้ค่า (upsert) — รับได้ทั้ง string และ Buffer, ไม่เรียงก็ได้, key ซ้ำตัวหลังชนะ
  #[napi]
  pub fn put(&self, entries: Vec<Entry>) -> Result<()> {
    let pairs = normalize_sorted(entries);
    let refs: Vec<(&[u8], &[u8])> = pairs.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
    self.inner()?.put(&refs).map_err(io_err)
  }

  /// ลบ keys — เขียน tombstone กดค่าใน layer เก่า
  #[napi]
  pub fn delete(&self, keys: Vec<Either<String, Buffer>>) -> Result<()> {
    let mut sorted: Vec<Vec<u8>> = keys.into_iter().map(into_bytes).collect();
    sorted.sort();
    sorted.dedup();
    let refs: Vec<&[u8]> = sorted.iter().map(|k| k.as_slice()).collect();
    self.inner()?.delete(&refs).map_err(io_err)
  }

  /// ค้นหา key → Buffer หรือ null (ไม่มี หรือ ถูกลบ)
  #[napi]
  pub fn get(&self, key: Either<String, Buffer>) -> Result<Option<Buffer>> {
    let key = into_bytes(key);
    Ok(self
      .inner()?
      .get(&key)
      .map_err(io_err)?
      .map(Buffer::from))
  }

  /// ค้นหา key → UTF-8 string หรือ null
  #[napi]
  pub fn get_utf8(&self, key: Either<String, Buffer>) -> Result<Option<String>> {
    let key = into_bytes(key);
    Ok(self
      .inner()?
      .get(&key)
      .map_err(io_err)?
      .and_then(|v| String::from_utf8(v).ok()))
  }

  #[napi]
  pub fn has(&self, key: Either<String, Buffer>) -> Result<bool> {
    let key = into_bytes(key);
    self.inner()?.has(&key).map_err(io_err)
  }

  /// รวมทุก layers เป็นไฟล์เดียว — คืนค่าจำนวน layers หลังทำ
  #[napi]
  pub fn compact(&self) -> Result<u32> {
    self.inner()?.compact().map_err(io_err).map(|n| n as u32)
  }

  /// iterator มุมมองรวมของ store — ส่ง `start` เพื่อ seek ไปที่ key >= start
  #[napi]
  pub fn iter(&self, start: Option<Either<String, Buffer>>) -> Result<XdbStoreIterator> {
    let start = start.map(into_bytes).unwrap_or_default();
    Ok(XdbStoreIterator {
      inner: self.inner()?.iter_from(&start),
    })
  }
}

/// sort + dedup (ตัวหลังชนะ) ให้พร้อมส่งให้ store.put
fn normalize_sorted(entries: Vec<Entry>) -> Vec<(Vec<u8>, Vec<u8>)> {
  let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = entries
    .into_iter()
    .map(|e| (into_bytes(e.key), into_bytes(e.value)))
    .collect();
  pairs.sort_by(|a, b| a.0.cmp(&b.0));
  let mut unique: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(pairs.len());
  for (k, v) in pairs {
    if let Some(last) = unique.last_mut() {
      if last.0 == k {
        last.1 = v;
        continue;
      }
    }
    unique.push((k, v));
  }
  unique
}

// ---------------- Store iterator (มุมมองรวมข้าม layers) ----------------

use x_db::store::StoreIter as InnerStoreIter;

/// entry จาก store iterator (ตัด tombstone ออกแล้ว)
#[napi(object)]
pub struct StoreEntry {
  pub key: Buffer,
  pub value: Buffer,
}

/// iterator มุมมองรวมของ store — เรียงตาม key, ใหม่ชนะ, ตัดที่ถูกลบ
#[napi]
pub struct XdbStoreIterator {
  inner: InnerStoreIter,
}

#[napi]
impl XdbStoreIterator {
  /// คืน entry ถัดไปเป็น {key, value} หรือ null เมื่อหมด
  #[napi]
  pub fn next(&mut self) -> Result<Option<StoreEntry>> {
    match self.inner.next().transpose().map_err(io_err)? {
      Some((k, v)) => Ok(Some(StoreEntry {
        key: Buffer::from(k),
        value: Buffer::from(v),
      })),
      None => Ok(None),
    }
  }
}
