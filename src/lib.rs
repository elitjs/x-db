use crc32fast::hash as crc32;
use memmap2::{Mmap, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering};

pub mod store;
pub use store::XDBStore;

pub const MAGIC_HEADER: u32 = 0x58444231; // "XDB1"
pub const MAGIC_FOOTER: u32 = 0x454E4458; // "ENDX"
pub const FORMAT_VERSION: u16 = 5;
pub const BLOCK_SIZE: usize = 16 * 1024; // 16 KB Block Size
/// ทุก ๆ 16 entries จะบันทึก "restart point" ไว้ท้าย block เพื่อให้ binary search ใน block ได้
pub const RESTART_INTERVAL: usize = 16;
/// บิตบนสุดของ v_len = entry นี้เป็น tombstone (คีย์ถูกลบ) — ใช้ใน XDBStore
pub const TOMBSTONE_FLAG: u32 = 1 << 31;

const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 40;
const CRC_SIZE: usize = 4;
/// ไฟล์เล็กสุดที่ถูกต้อง = header เปล่า + bloom 1KB + footer (ยังไม่มี block)
const MIN_FILE_SIZE: usize = HEADER_SIZE + 1024 + FOOTER_SIZE;

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

// -------------------------------------------------------------
// 1. Bit-Shift Fast Bloom Filter
// -------------------------------------------------------------
pub struct BloomFilter {
    bits: Vec<u8>,
    size: usize,
}

impl BloomFilter {
    /// สร้าง filter ให้พอดีกับจำนวน entries (false positive ~1% ที่ 2 hash functions)
    pub fn with_capacity(expected_items: usize) -> Self {
        // m ≈ -n·ln(p)/(ln2)² โดย p=0.01, k=2 → ~9.6 bits ต่อ item
        let bits = (expected_items * 10).max(8 * 1024).next_power_of_two();
        let bytes = bits / 8;
        Self { size: bits, bits: vec![0u8; bytes] }
    }

    #[inline(always)]
    fn get_hashes(key: &[u8]) -> (usize, usize) {
        let mut h1: u32 = 0x811c9dc5;
        let mut h2: u32 = 0x5bd1e995;
        for &byte in key {
            h1 = (h1 ^ byte as u32).wrapping_mul(0x01000193);
            h2 = (h2 ^ byte as u32).wrapping_mul(0x5bd1e995);
        }
        (h1 as usize, h2 as usize)
    }

    pub fn add(&mut self, key: &[u8]) {
        let (h1, h2) = Self::get_hashes(key);
        let bit1 = h1 % self.size;
        let bit2 = h2 % self.size;
        self.bits[bit1 >> 3] |= 1 << (bit1 & 7);
        self.bits[bit2 >> 3] |= 1 << (bit2 & 7);
    }

    #[inline(always)]
    pub fn might_contain(&self, key: &[u8]) -> bool {
        let (h1, h2) = Self::get_hashes(key);
        let bit1 = h1 % self.size;
        let bit2 = h2 % self.size;
        let b1 = self.bits[bit1 >> 3] & (1 << (bit1 & 7));
        let b2 = self.bits[bit2 >> 3] & (1 << (bit2 & 7));
        b1 != 0 && b2 != 0
    }

    pub fn len_bytes(&self) -> usize {
        self.bits.len()
    }
}

// -------------------------------------------------------------
// 2. Writer: Sequential Append + Block Packing
// -------------------------------------------------------------
struct BlockIndexEntry {
    first_key: Vec<u8>,
    offset: u64,
    length: u32,
    num_restarts: u16,
}

/// เขียนตารางแบบ streaming: `create` → `add` ทีละ entry (key เรียงน้อยไปมาก) → `finish`
/// เหมาะกับข้อมูลใหญ่เกิน RAM เช่นตอน merge ตาราง
pub struct TableBuilder {
    writer: BufWriter<File>,
    bloom: BloomFilter,
    index_list: Vec<BlockIndexEntry>,
    block_buffer: Vec<u8>,
    block_first_key: Vec<u8>,
    prev_key: Vec<u8>,
    /// offset ของ entry ที่เป็นจุดเริ่ม binary search ใน block (ทุก RESTART_INTERVAL entries)
    restarts: Vec<u32>,
    entries_in_block: usize,
    started: bool,
    current_offset: u64,
    count: u64,
}

impl TableBuilder {
    /// `expected_entries` ใช้กำหนดขนาด bloom filter (ประมาณการได้ก็พอ — เกินจริงหน่อยไม่พัง)
    pub fn create<P: AsRef<Path>>(path: P, expected_entries: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        let mut writer = BufWriter::with_capacity(128 * 1024, file);

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&MAGIC_HEADER.to_be_bytes());
        header[4..6].copy_from_slice(&FORMAT_VERSION.to_be_bytes());
        header[6..10].copy_from_slice(&(BLOCK_SIZE as u32).to_be_bytes());
        writer.write_all(&header)?;

        Ok(Self {
            writer,
            bloom: BloomFilter::with_capacity(expected_entries),
            index_list: Vec::new(),
            block_buffer: Vec::with_capacity(BLOCK_SIZE),
            block_first_key: Vec::new(),
            prev_key: Vec::new(),
            restarts: Vec::new(),
            entries_in_block: 0,
            started: false,
            current_offset: HEADER_SIZE as u64,
            count: 0,
        })
    }

    /// เพิ่ม entry — key ต้องเรียงจากน้อยไปมากตามลำดับการเรียก
    pub fn add(&mut self, key: &[u8], val: &[u8]) -> io::Result<()> {
        if val.len() >= TOMBSTONE_FLAG as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("value too long: {} bytes (max 2^31-1)", val.len()),
            ));
        }
        self.add_raw(key, Some(val))
    }

    /// เพิ่ม tombstone (ตัวบอกว่า key นี้ถูกลบ) — ใช้ใน XDBStore
    pub fn add_tombstone(&mut self, key: &[u8]) -> io::Result<()> {
        self.add_raw(key, None)
    }

    fn add_raw(&mut self, key: &[u8], val: Option<&[u8]>) -> io::Result<()> {
        if key.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("key too long: {} bytes (max {})", key.len(), u16::MAX),
            ));
        }
        if self.started && key < self.prev_key.as_slice() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "entries must be added in ascending key order",
            ));
        }
        self.started = true;
        self.prev_key.clear();
        self.prev_key.extend_from_slice(key);
        self.count += 1;

        self.bloom.add(key);
        // tombstone (val = None): เข้ารหัส v_len = TOMBSTONE_FLAG อย่างเดียว ไม่มี value bytes
        let (val_bytes, v_len) = match val {
            Some(v) => (v, v.len() as u32),
            None => (&[][..], TOMBSTONE_FLAG),
        };
        let entry_len = 6 + key.len() + val_bytes.len();

        // Flush Block เมื่อเต็มขีดจำกัด
        if self.block_buffer.len() + entry_len > BLOCK_SIZE && !self.block_buffer.is_empty() {
            self.flush_block()?;
        }

        if self.block_buffer.is_empty() {
            self.block_first_key.clear();
            self.block_first_key.extend_from_slice(key);
        }

        // บันทึก restart point ทุก ๆ RESTART_INTERVAL entries (entry แรกของ block คือ offset 0 เสมอ)
        if self.entries_in_block % RESTART_INTERVAL == 0 {
            self.restarts.push(self.block_buffer.len() as u32);
        }
        self.entries_in_block += 1;

        // Encode Entry: [KeyLen: 2B][ValLen: 4B (MSB = tombstone)][Key Bytes][Val Bytes]
        self.block_buffer.extend_from_slice(&(key.len() as u16).to_be_bytes());
        self.block_buffer.extend_from_slice(&v_len.to_be_bytes());
        self.block_buffer.extend_from_slice(key);
        self.block_buffer.extend_from_slice(val_bytes);
        Ok(())
    }

    /// ปิดไฟล์ (flush block สุดท้าย + bloom + index + footer + fsync)
    pub fn finish(mut self) -> io::Result<()> {
        if !self.block_buffer.is_empty() {
            self.flush_block()?;
        }

        let bloom_offset = self.current_offset;
        let bloom_len = self.bloom.len_bytes() as u32;
        self.writer.write_all(&self.bloom.bits)?;
        self.current_offset += bloom_len as u64;

        let index_offset = self.current_offset;
        let mut index_raw = Vec::new();
        for idx in &self.index_list {
            index_raw.extend_from_slice(&(idx.first_key.len() as u16).to_be_bytes());
            index_raw.extend_from_slice(&idx.first_key);
            index_raw.extend_from_slice(&idx.offset.to_be_bytes());
            index_raw.extend_from_slice(&idx.length.to_be_bytes());
            // เก็บ num_restarts ใน index ด้วย → ตอน open ไม่ต้องแตะ tail ของทุก block เลย
            index_raw.extend_from_slice(&(idx.num_restarts as u16).to_be_bytes());
        }
        self.writer.write_all(&index_raw)?;
        let index_len = index_raw.len() as u32;

        let mut footer = [0u8; FOOTER_SIZE];
        footer[0..8].copy_from_slice(&bloom_offset.to_be_bytes());
        footer[8..12].copy_from_slice(&bloom_len.to_be_bytes());
        footer[12..20].copy_from_slice(&index_offset.to_be_bytes());
        footer[20..24].copy_from_slice(&index_len.to_be_bytes());
        footer[24..32].copy_from_slice(&self.count.to_be_bytes());
        let footer_crc = crc32(&footer[0..32]);
        footer[32..36].copy_from_slice(&footer_crc.to_be_bytes());
        footer[36..40].copy_from_slice(&MAGIC_FOOTER.to_be_bytes());
        self.writer.write_all(&footer)?;

        // fsync ให้แน่ใจว่าข้อมูลลงดิสก์จริง (กันไฟล์ครึ่ง ๆ ครึ่ง เวลาไฟดับ)
        self.writer.into_inner()?.sync_all()?;
        Ok(())
    }

    fn flush_block(&mut self) -> io::Result<()> {
        // ท้าย block: [restart array: u32 × R][R: u16] แล้วเข้ารหัส CRC ทั้งหมด
        for r in &self.restarts {
            self.block_buffer.extend_from_slice(&r.to_be_bytes());
        }
        self.block_buffer.extend_from_slice(&(self.restarts.len() as u16).to_be_bytes());

        self.writer.write_all(&self.block_buffer)?;
        self.writer.write_all(&crc32(&self.block_buffer).to_be_bytes())?;
        self.index_list.push(BlockIndexEntry {
            first_key: self.block_first_key.clone(),
            offset: self.current_offset,
            length: (self.block_buffer.len() + CRC_SIZE) as u32,
            num_restarts: self.restarts.len() as u16,
        });
        self.current_offset += (self.block_buffer.len() + CRC_SIZE) as u64;
        self.block_buffer.clear();
        self.restarts.clear();
        self.entries_in_block = 0;
        Ok(())
    }
}

pub struct XDBWriter;

impl XDBWriter {
    pub fn write_table<P: AsRef<Path>>(
        path: P,
        sorted_entries: &[(&[u8], &[u8])],
    ) -> io::Result<()> {
        let mut builder = TableBuilder::create(path, sorted_entries.len())?;
        for (key, val) in sorted_entries {
            builder.add(key, val)?;
        }
        builder.finish()
    }
}

/// รวมหลายตารางเป็นไฟล์เดียว (k-way merge แบบ streaming — ไม่ต้องโหลดทั้งหมดขึ้น RAM)
/// key ซ้ำกัน: ตารางที่อยู่หลังสุดใน `inputs` ชนะ
/// เขียนลง `{output}.tmp` แล้วค่อย rename — input ทั้งหมดปลอดภัยแม้ merge จะพังกลางทาง
pub fn merge_tables<P: AsRef<Path>>(inputs: &[P], output: P) -> io::Result<u64> {
    let output = output.as_ref();
    for input in inputs {
        if input.as_ref() == output {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output path must differ from all input paths",
            ));
        }
    }

    let readers: Vec<XDBReader> = inputs
        .iter()
        .map(XDBReader::open)
        .collect::<io::Result<_>>()?;
    let expected: usize = readers.iter().map(|r| r.len() as usize).sum();

    let tmp = output.with_extension("xdb.tmp");
    let mut builder = TableBuilder::create(&tmp, expected)?;

    let mut iters: Vec<XDBIter> = readers.iter().map(|r| r.iter()).collect();
    let mut heads: Vec<Option<(Vec<u8>, Option<Vec<u8>>)>> = iters
        .iter_mut()
        .map(|it| {
            it.next()
                .transpose()
                .map(|e| e.map(|(k, v)| (k.to_vec(), v.map(|v| v.to_vec()))))
        })
        .collect::<io::Result<_>>()?;

    let mut written: u64 = 0;
    loop {
        // key เล็กสุดในบรรดา head ทั้งหมด
        let Some(min_key) = heads.iter().flatten().map(|(k, _)| k.as_slice()).min() else {
            break;
        };
        let min_key = min_key.to_vec();

        // ค่าจากตารางหลังสุดที่ถือ key นี้ชนะ (tombstone ก็ชนะ — คงไว้เพื่อกดของเก่า)
        let mut value: Option<Vec<u8>> = None;
        for i in 0..heads.len() {
            let Some((k, v)) = &heads[i] else { continue };
            if k.as_slice() == min_key {
                value = v.clone();
                heads[i] = iters[i]
                    .next()
                    .transpose()
                    .map(|e| e.map(|(k, v)| (k.to_vec(), v.map(|v| v.to_vec()))))?;
            }
        }

        match &value {
            Some(v) => builder.add(&min_key, v)?,
            None => builder.add_tombstone(&min_key)?,
        }
        written += 1;
    }
    builder.finish()?;

    // Windows: rename ทับไฟล์เดิมไม่ได้ ต้องลบก่อน
    if output.exists() {
        std::fs::remove_file(output)?;
    }
    std::fs::rename(&tmp, output)?;
    Ok(written)
}

// -------------------------------------------------------------
// 3. Reader: Zero-Copy mmap + Sub-microsecond Search
// -------------------------------------------------------------

/// อ่าน entry ถัดไปจาก block data → (key, value, ไบต์ที่กิน)
/// value = None หมายถึง tombstone (คีย์ถูกลบ)
/// Ok(None) = หมด block, Err = ข้อมูลเสียหาย
pub fn parse_entry(block: &[u8]) -> io::Result<Option<(&[u8], Option<&[u8]>, usize)>> {
    if block.is_empty() {
        return Ok(None);
    }
    if block.len() < 6 {
        return Err(invalid_data("truncated entry header"));
    }
    let k_len = u16::from_be_bytes(block[0..2].try_into().unwrap()) as usize;
    if 6 + k_len > block.len() {
        return Err(invalid_data("truncated entry key"));
    }
    let v_raw = u32::from_be_bytes(block[2..6].try_into().unwrap());
    let tombstone = v_raw & TOMBSTONE_FLAG != 0;
    let v_len = (v_raw & !TOMBSTONE_FLAG) as usize;
    let end = 6 + k_len + v_len;
    if end > block.len() {
        return Err(invalid_data("truncated entry value"));
    }
    let key = &block[6..6 + k_len];
    let val = if tombstone {
        None
    } else {
        Some(&block[6 + k_len..end])
    };
    Ok(Some((key, val, end)))
}

struct BlockEntry {
    first_key: Vec<u8>,
    offset: usize,
    length: usize,
    num_restarts: usize,
}

impl BlockEntry {
    /// ความยาวส่วน entries (ไม่รวม restart array + CRC) — คำนวณจาก metadata ใน index
    #[inline]
    fn entries_len(&self) -> usize {
        self.length - CRC_SIZE - 2 - self.num_restarts * 4
    }
}

pub struct XDBReader {
    mmap: Mmap,
    bloom_offset: usize,
    bloom_len: usize,
    index: Vec<BlockEntry>,
    entry_count: u64,
    /// สถานะ CRC ของแต่ละ block (0 = ยังไม่ตรวจ, 1 = ผ่านแล้ว) — atomic ไม่ต้อง lock
    verified: Vec<AtomicU8>,
}

impl XDBReader {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        let len = mmap.len();

        if len < MIN_FILE_SIZE {
            return Err(invalid_data("file too small"));
        }

        // ตรวจ Header
        if u32::from_be_bytes(mmap[0..4].try_into().unwrap()) != MAGIC_HEADER {
            return Err(invalid_data("invalid magic header"));
        }
        let version = u16::from_be_bytes(mmap[4..6].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(invalid_data(&format!(
                "unsupported format version {version} (expected {FORMAT_VERSION})"
            )));
        }

        // ตรวจ Footer (40 ไบต์สุดท้าย): magic + CRC ของตัว footer เอง
        let footer = &mmap[len - FOOTER_SIZE..];
        if u32::from_be_bytes(footer[36..40].try_into().unwrap()) != MAGIC_FOOTER {
            return Err(invalid_data("invalid magic footer"));
        }
        let footer_crc = u32::from_be_bytes(footer[32..36].try_into().unwrap());
        if crc32(&footer[0..32]) != footer_crc {
            return Err(invalid_data("footer checksum mismatch"));
        }

        let bloom_offset = u64::from_be_bytes(footer[0..8].try_into().unwrap()) as usize;
        let bloom_len = u32::from_be_bytes(footer[8..12].try_into().unwrap()) as usize;
        let index_offset = u64::from_be_bytes(footer[12..20].try_into().unwrap()) as usize;
        let index_len = u32::from_be_bytes(footer[20..24].try_into().unwrap()) as usize;
        let entry_count = u64::from_be_bytes(footer[24..32].try_into().unwrap());

        // ตรวจขอบเขตทุก region ก่อนแตะ
        if bloom_offset > len || bloom_len > len - bloom_offset {
            return Err(invalid_data("bloom region out of bounds"));
        }
        if index_offset > len || index_len > len - index_offset {
            return Err(invalid_data("index region out of bounds"));
        }

        // Parse sparse index ครั้งเดียวตอน open → binary search ตอน lookup
        let index_data = &mmap[index_offset..index_offset + index_len];
        let mut index = Vec::new();
        let mut cursor = 0;
        while cursor < index_data.len() {
            if cursor + 2 > index_data.len() {
                return Err(invalid_data("truncated index entry"));
            }
            let k_len = u16::from_be_bytes(index_data[cursor..cursor + 2].try_into().unwrap()) as usize;
            if cursor + 16 + k_len > index_data.len() {
                return Err(invalid_data("truncated index entry"));
            }
            let first_key = index_data[cursor + 2..cursor + 2 + k_len].to_vec();
            let offset = u64::from_be_bytes(index_data[cursor + 2 + k_len..cursor + 10 + k_len].try_into().unwrap()) as usize;
            let length = u32::from_be_bytes(index_data[cursor + 10 + k_len..cursor + 14 + k_len].try_into().unwrap()) as usize;
            let num_restarts = u16::from_be_bytes(index_data[cursor + 14 + k_len..cursor + 16 + k_len].try_into().unwrap()) as usize;
            if offset > len || length > len - offset {
                return Err(invalid_data("block region out of bounds"));
            }
            if num_restarts == 0 || CRC_SIZE + 2 + num_restarts * 4 > length {
                return Err(invalid_data("invalid restart array"));
            }

            index.push(BlockEntry { first_key, offset, length, num_restarts });
            cursor += 16 + k_len;
        }

        let verified = std::iter::repeat_with(|| AtomicU8::new(0))
            .take(index.len())
            .collect();
        Ok(Self { mmap, bloom_offset, bloom_len, index, entry_count, verified })
    }

    /// จำนวน entries ทั้งหมดในตาราง
    pub fn len(&self) -> u64 {
        self.entry_count
    }

    pub fn block_count(&self) -> usize {
        self.index.len()
    }

    /// (ใช้ภายใน/สำหรับ binding) block สุดท้ายที่ first_key <= key — ใช้ positioning iterator
    pub fn find_block_index(&self, key: &[u8]) -> Option<usize> {
        self.find_block(key)
    }

    pub fn bloom_len(&self) -> usize {
        self.bloom_len
    }

    /// ค้นหาแบบ Zero-Copy: คืนค่าเป็น byte slice ชี้ตรงไปยัง memory ทันที
    /// Err = ไฟล์เสียหาย, Ok(None) = ไม่มี key นี้ (หรือเจอ tombstone)
    #[inline]
    pub fn get(&self, target_key: &[u8]) -> io::Result<Option<&[u8]>> {
        match self.get_entry(target_key)? {
            Some(Some(v)) => Ok(Some(v)),
            _ => Ok(None),
        }
    }

    /// ค้นหาแบบละเอียด: Some(Some(v)) = เจอค่า, Some(None) = เจอ tombstone, None = ไม่มี key
    /// (XDBStore ใช้ตัวนี้เพื่อแยก "ถูกลบ" ออกจาก "ไม่มี" แล้วหยุดค้น layer ที่เก่ากว่า)
    #[inline]
    pub fn get_entry(&self, target_key: &[u8]) -> io::Result<Option<Option<&[u8]>>> {
        // Stage 1: Bloom Filter Check
        if self.bloom_len == 0 || !self.bloom_check(target_key) {
            return Ok(None); // Key ไม่มีแน่นอน 100% ข้ามการ Scan ทันที
        }

        // Stage 2: Binary Search on Sparse Index
        let Some(block_idx) = self.find_block(target_key) else {
            return Ok(None);
        };

        // Stage 3: ตรวจ CRC ครั้งแรกที่แตะ block แล้ว binary search บน restart points
        self.ensure_verified(block_idx)?;
        self.scan_block(block_idx, target_key)
    }

    /// ไล่ทุก entries เรียงตาม key (ใช้ทำ range/prefix scan ด้วย skip_while/take_while ได้)
    pub fn iter(&self) -> XDBIter<'_> {
        XDBIter { reader: self, block: 0, offset: 0, data: &[], done: false, skip_below: None }
    }

    /// iterator เริ่มที่ entry แรกที่ key >= start (seek) — เร็วกว่าไล่+filter เพราะข้ามไป block ตรง ๆ
    pub fn iter_from(&self, start: &[u8]) -> XDBIter<'_> {
        // block ที่ start น่าจะอยู่ (block สุดท้ายที่ first_key <= start) — เริ่มจากตรงนั้น
        let block = self.find_block(start).unwrap_or(0);
        XDBIter {
            reader: self,
            block,
            offset: 0,
            data: &[],
            done: false,
            skip_below: Some(start.to_vec()),
        }
    }

    /// ไล่ keys ในช่วง [start, end) — end เป็น exclusive
    pub fn range<'a>(
        &'a self,
        start: &[u8],
        end: &[u8],
    ) -> impl Iterator<Item = io::Result<(&'a [u8], Option<&'a [u8]>)>> + 'a {
        let end = end.to_vec();
        self.iter_from(start)
            .take_while(move |r| matches!(r, Ok((k, _)) if *k < end.as_slice()))
    }

    /// ไล่ทุก entries ที่ key ขึ้นต้นด้วย prefix ที่กำหนด
    pub fn prefix<'a>(
        &'a self,
        prefix: &[u8],
    ) -> impl Iterator<Item = io::Result<(&'a [u8], Option<&'a [u8]>)>> + 'a {
        let p = prefix.to_vec();
        self.iter_from(prefix)
            .take_while(move |r| matches!(r, Ok((k, _)) if k.starts_with(p.as_slice())))
    }

    /// ข้อมูลของ block ที่ i — เฉพาะส่วน entries (สำหรับ iteration)
    /// ตรวจ checksum เฉพาะครั้งแรกที่แตะ block — ครั้งถัดไปเป็น zero-copy ทันที
    pub fn block_data_at(&self, i: usize) -> io::Result<&[u8]> {
        let be = self.index.get(i).ok_or_else(|| invalid_data("block index out of range"))?;
        self.ensure_verified(i)?;
        Ok(&self.mmap[be.offset..be.offset + be.entries_len()])
    }

    /// ตรวจ CRC ของ block (cache ด้วย atomic — จ่ายครั้งเดียวต่อ block)
    fn ensure_verified(&self, i: usize) -> io::Result<()> {
        if self.verified[i].load(Ordering::Acquire) == 1 {
            return Ok(());
        }
        let be = &self.index[i];
        let block = &self.mmap[be.offset..be.offset + be.length];
        let data_len = be.length - CRC_SIZE;
        let stored = u32::from_be_bytes(block[data_len..].try_into().unwrap());
        if crc32(&block[..data_len]) != stored {
            return Err(invalid_data("block checksum mismatch"));
        }
        self.verified[i].store(1, Ordering::Release);
        Ok(())
    }

    #[inline(always)]
    fn bloom_check(&self, key: &[u8]) -> bool {
        let bits = &self.mmap[self.bloom_offset..self.bloom_offset + self.bloom_len];
        let (h1, h2) = BloomFilter::get_hashes(key);
        // bloom_len เป็น power-of-two เสมอ → ใช้ mask แทนการหาร
        let mask = self.bloom_len * 8 - 1;
        let b1 = h1 & mask;
        let b2 = h2 & mask;
        (bits[b1 >> 3] & (1 << (b1 & 7)) != 0) && (bits[b2 >> 3] & (1 << (b2 & 7)) != 0)
    }

    /// Binary search: block สุดท้ายที่ first_key <= target
    fn find_block(&self, target_key: &[u8]) -> Option<usize> {
        match self.index.binary_search_by(|e| e.first_key.as_slice().cmp(target_key)) {
            Ok(i) => Some(i),
            Err(0) => None,
            Err(i) => Some(i - 1),
        }
    }

    /// Binary search บน restart points แล้วไล่ไม่เกิน RESTART_INTERVAL entries
    /// คืนค่า: Some(Some(v)) = เจอ, Some(None) = เจอ tombstone, None = ไม่มี
    fn scan_block(&self, block_idx: usize, target_key: &[u8]) -> io::Result<Option<Option<&[u8]>>> {
        let be = &self.index[block_idx];
        let entries_len = be.entries_len();
        let block = &self.mmap[be.offset..be.offset + entries_len];
        let restarts = &self.mmap[be.offset + entries_len..be.offset + be.length - CRC_SIZE - 2];

        // key ของ restart point ที่ i
        let restart_key = |i: usize| -> io::Result<&[u8]> {
            let off = u32::from_be_bytes(restarts[i * 4..i * 4 + 4].try_into().unwrap()) as usize;
            match parse_entry(&block[off..])? {
                Some((k, _, _)) if off < block.len() => Ok(k),
                _ => Err(invalid_data("restart offset out of range")),
            }
        };

        // หา restart สุดท้ายที่ key <= target (partition_point)
        let mut lo = 0usize;
        let mut hi = be.num_restarts;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if restart_key(mid)? <= target_key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            return Ok(None); // target เล็กกว่า key แรกของ block
        }
        let start = u32::from_be_bytes(restarts[(lo - 1) * 4..(lo - 1) * 4 + 4].try_into().unwrap()) as usize;

        // ไล่จาก restart จุดนั้น — ไม่เกิน RESTART_INTERVAL entries
        let mut rest = &block[start..];
        while let Some((key, val, consumed)) = parse_entry(rest)? {
            if key == target_key {
                return Ok(Some(val));
            }
            if key > target_key {
                break;
            }
            rest = &rest[consumed..];
        }
        Ok(None)
    }
}

/// Iterator ทั้งตาราง — Item = Result กันข้อมูลเสียหายระหว่างไล่
/// ตรวจ CRC ครั้งเดียวต่อ block (cache block ที่ตรวจแล้วไว้)
pub struct XDBIter<'a> {
    reader: &'a XDBReader,
    /// index ของ block *ถัดไป* ที่จะโหลด
    block: usize,
    offset: usize,
    /// data ของ block ปัจจุบัน (ผ่าน CRC แล้ว) — ว่าง = ยังไม่โหลดอะไรเลย
    data: &'a [u8],
    done: bool,
    /// (seek) ข้าม entries ที่ key น้อยกว่าค่านี้ — keys เรียงอยู่แล้วเลยข้ามได้เรื่อย ๆ
    skip_below: Option<Vec<u8>>,
}

impl XDBIter<'_> {
    /// โหลด block ถัดไป (พร้อมตรวจ CRC ครั้งเดียว) — false = หมดแล้ว
    fn advance_block(&mut self) -> io::Result<bool> {
        while self.block < self.reader.index.len() {
            match self.reader.block_data_at(self.block) {
                Ok(d) => {
                    self.block += 1;
                    self.data = d;
                    self.offset = 0;
                    return Ok(true);
                }
                Err(e) => {
                    self.done = true;
                    return Err(e);
                }
            }
        }
        self.done = true;
        Ok(false)
    }
}

impl<'a> Iterator for XDBIter<'a> {
    /// value = None หมายถึง tombstone (คีย์ถูกลบ)
    type Item = io::Result<(&'a [u8], Option<&'a [u8]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if self.offset >= self.data.len() {
                match self.advance_block() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            return match parse_entry(&self.data[self.offset..]) {
                Err(e) => {
                    self.done = true;
                    Some(Err(e))
                }
                Ok(Some((k, v, consumed))) => {
                    self.offset += consumed;
                    // (seek) ข้าม entries ที่น้อยกว่าจุดเริ่ม — keys เรียงอยู่แล้ว
                    if let Some(m) = &self.skip_below {
                        if k < m.as_slice() {
                            continue;
                        }
                        self.skip_below = None; // ผ่านจุดเริ่มแล้ว ไม่ต้องเช็คอีก
                    }
                    Some(Ok((k, v)))
                }
                Ok(None) => {
                    // ไม่ถึง case นี้ในทางปฏิบัติ (เราเช็ค offset < data.len แล้ว) — ป้องกันไว้
                    self.data = &[];
                    continue;
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn build_entries(n: usize, val_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                let key = format!("key:{:010}", i).into_bytes();
                let val = vec![(i % 251) as u8; val_size];
                (key, val)
            })
            .collect()
    }

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xdb_v2_{}", name));
        let _ = std::fs::create_dir(&dir);
        dir.join("table.xdb")
    }

    #[test]
    fn round_trip_multi_block() {
        let path = temp_path("round");
        let entries = build_entries(2000, 200); // ~400 KB -> multiple 64 KB blocks
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let reader = XDBReader::open(&path).unwrap();
        assert_eq!(reader.len(), 2000);
        assert!(reader.block_count() > 1);
        for (k, v) in &entries {
            assert_eq!(reader.get(k).unwrap(), Some(v.as_slice()));
        }
    }

    #[test]
    fn missing_keys_return_none() {
        let path = temp_path("missing");
        let entries = build_entries(100, 50);
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let reader = XDBReader::open(&path).unwrap();
        assert_eq!(reader.get(b"aaa").unwrap(), None);
        assert_eq!(reader.get(b"key:0000000499").unwrap(), None);
        assert_eq!(reader.get(b"zzz").unwrap(), None);
        assert_eq!(reader.get(b"key:0000001").unwrap(), None); // prefix ต้องไม่ match
    }

    #[test]
    fn empty_table() {
        let path = temp_path("empty");
        XDBWriter::write_table(&path, &[]).unwrap();
        let reader = XDBReader::open(&path).unwrap();
        assert_eq!(reader.len(), 0);
        assert_eq!(reader.get(b"anything").unwrap(), None);
        assert_eq!(reader.iter().count(), 0);
    }

    #[test]
    fn unsorted_entries_rejected() {
        let path = temp_path("unsorted");
        let refs: Vec<(&[u8], &[u8])> = vec![(b"b" as &[u8], b"1" as &[u8]), (b"a" as &[u8], b"2" as &[u8])];
        let err = XDBWriter::write_table(&path, &refs).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn oversized_key_rejected() {
        let path = temp_path("bigkey");
        let key = vec![0u8; u16::MAX as usize + 1];
        let refs: Vec<(&[u8], &[u8])> = vec![(key.as_slice(), b"v" as &[u8])];
        let err = XDBWriter::write_table(&path, &refs).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn single_entry_larger_than_block() {
        let path = temp_path("bigval");
        let big_val = vec![7u8; 200_000]; // > BLOCK_SIZE, must still round-trip
        let refs: Vec<(&[u8], &[u8])> = vec![(b"big" as &[u8], big_val.as_slice())];
        XDBWriter::write_table(&path, &refs).unwrap();

        let reader = XDBReader::open(&path).unwrap();
        assert_eq!(reader.get(b"big").unwrap(), Some(big_val.as_slice()));
    }

    #[test]
    fn arbitrary_byte_keys_sorted_by_btreemap() {
        let path = temp_path("bytes");
        let map: BTreeMap<Vec<u8>, Vec<u8>> = (0..500u32)
            .map(|i| (i.to_be_bytes().to_vec(), format!("value-{}", i).into_bytes()))
            .collect();
        let refs: Vec<(&[u8], &[u8])> = map.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let reader = XDBReader::open(&path).unwrap();
        for (k, v) in &map {
            assert_eq!(reader.get(k).unwrap(), Some(v.as_slice()));
        }
    }

    // ---- format v2: corruption detection ----

    #[test]
    fn corrupted_block_data_is_detected() {
        let path = temp_path("corrupt_block");
        let entries = build_entries(500, 100);
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let mut raw = std::fs::read(&path).unwrap();
        raw[HEADER_SIZE + 10] ^= 0xFF; // flip ไบต์ใน block แรก
        std::fs::write(&path, &raw).unwrap();

        let reader = XDBReader::open(&path).unwrap(); // footer/index ยังดีอยู่
        let err = reader.get(&entries[10].0).unwrap_err(); // ต้องเจอ CRC mismatch
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // iterator ก็ต้องเจอเหมือนกัน
        assert!(reader.iter().any(|e| e.is_err()));
    }

    #[test]
    fn corrupted_footer_is_detected() {
        let path = temp_path("corrupt_footer");
        let entries = build_entries(10, 10);
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let mut raw = std::fs::read(&path).unwrap();
        let len = raw.len();
        raw[len - FOOTER_SIZE + 5] ^= 0xFF; // flip ใน footer ก่อน CRC
        std::fs::write(&path, &raw).unwrap();

        let err = match XDBReader::open(&path) {
            Err(e) => e,
            Ok(_) => panic!("corrupted footer must be rejected"),
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn truncated_file_is_detected() {
        let path = temp_path("truncated");
        let entries = build_entries(100, 100);
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() / 2]).unwrap();

        assert!(XDBReader::open(&path).is_err());
    }

    #[test]
    fn garbage_file_is_rejected() {
        let path = temp_path("garbage");
        std::fs::write(&path, vec![0xABu8; 4096]).unwrap();
        assert!(XDBReader::open(&path).is_err());
    }

    // ---- iteration ----

    #[test]
    fn iter_visits_all_entries_in_order() {
        let path = temp_path("iter");
        let entries = build_entries(1500, 80); // หลาย blocks
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let reader = XDBReader::open(&path).unwrap();
        let collected: Vec<(Vec<u8>, Vec<u8>)> = reader
            .iter()
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.unwrap().to_vec())).unwrap())
            .collect();

        assert_eq!(collected.len(), entries.len());
        assert_eq!(collected, entries); // BTreeMap-sorted == insertion order ของ entries
    }

    #[test]
    fn iter_range_scan_via_take_while() {
        let path = temp_path("range");
        let entries = build_entries(100, 20);
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();

        let reader = XDBReader::open(&path).unwrap();
        let start = b"key:0000000040";
        let end = b"key:0000000050";
        let range: Vec<Vec<u8>> = reader
            .iter()
            .map(|r| r.unwrap())
            .skip_while(|(k, _)| *k < start.as_slice())
            .take_while(|(k, _)| *k <= end.as_slice())
            .map(|(k, _)| k.to_vec())
            .collect();
        assert_eq!(range.len(), 11); // keys 40..=50
        assert_eq!(range.first().unwrap().as_slice(), &start[..]);
        assert_eq!(range.last().unwrap().as_slice(), &end[..]);
    }

    // ---- bloom sizing ----

    #[test]
    fn bloom_scales_with_entry_count() {
        let small = temp_path("bloom_small");
        let big = temp_path("bloom_big");

        let e1 = build_entries(10, 10);
        let r1: Vec<(&[u8], &[u8])> = e1.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&small, &r1).unwrap();

        let e2 = build_entries(100_000, 10);
        let r2: Vec<(&[u8], &[u8])> = e2.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&big, &r2).unwrap();

        let s = XDBReader::open(&small).unwrap();
        let b = XDBReader::open(&big).unwrap();
        assert_eq!(s.bloom_len(), 1024); // min 1KB
        assert!(b.bloom_len() >= 100_000 * 10 / 8 / 2); // ~1.2 bits/byte of the 10 bits/item
        assert!(b.bloom_len() < s.bloom_len() * 400);
    }

    // ---- streaming builder + merge ----

    #[test]
    fn table_builder_streaming_matches_write_table() {
        let path = temp_path("builder");
        let entries = build_entries(1000, 60);
        let mut builder = TableBuilder::create(&path, entries.len()).unwrap();
        for (k, v) in &entries {
            builder.add(k, v).unwrap();
        }
        builder.finish().unwrap();

        let reader = XDBReader::open(&path).unwrap();
        assert_eq!(reader.len(), 1000);
        for (k, v) in &entries {
            assert_eq!(reader.get(k).unwrap(), Some(v.as_slice()));
        }
    }

    #[test]
    fn table_builder_rejects_unsorted_stream() {
        let path = temp_path("builder_unsorted");
        let mut builder = TableBuilder::create(&path, 2).unwrap();
        builder.add(b"b", b"1").unwrap();
        assert!(builder.add(b"a", b"2").is_err());
    }

    #[test]
    fn merge_last_table_wins_on_duplicate_keys() {
        let t1 = temp_path("merge1");
        let t2 = temp_path("merge2");
        let t3 = temp_path("merge3");
        let out = temp_path("merge_out");

        // t1: a, b, c | t2: b(ใหม่), d | t3: a(ใหม่สุด), e
        let refs1: Vec<(&[u8], &[u8])> = vec![(b"a", b"old-a"), (b"b", b"old-b"), (b"c", b"c1")];
        let refs2: Vec<(&[u8], &[u8])> = vec![(b"b", b"new-b"), (b"d", b"d2")];
        let refs3: Vec<(&[u8], &[u8])> = vec![(b"a", b"newest-a"), (b"e", b"e3")];
        XDBWriter::write_table(&t1, &refs1).unwrap();
        XDBWriter::write_table(&t2, &refs2).unwrap();
        XDBWriter::write_table(&t3, &refs3).unwrap();

        let written = merge_tables(&[&t1, &t2, &t3], &out).unwrap();
        assert_eq!(written, 5); // a, b, c, d, e (key ซ้ำถูกยุบเหลือตัวเดียว)

        let reader = XDBReader::open(&out).unwrap();
        assert_eq!(reader.len(), 5);
        assert_eq!(reader.get(b"a").unwrap(), Some(&b"newest-a"[..])); // จาก t3
        assert_eq!(reader.get(b"b").unwrap(), Some(&b"new-b"[..])); // จาก t2
        assert_eq!(reader.get(b"c").unwrap(), Some(&b"c1"[..]));
        assert_eq!(reader.get(b"d").unwrap(), Some(&b"d2"[..]));
        assert_eq!(reader.get(b"e").unwrap(), Some(&b"e3"[..]));

        // เรียงถูกต้อง
        let keys: Vec<Vec<u8>> = reader.iter().map(|r| r.unwrap().0.to_vec()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    }

    #[test]
    fn merge_large_tables_round_trip() {
        let t1 = temp_path("merge_big1");
        let t2 = temp_path("merge_big2");
        let out = temp_path("merge_big_out");

        // t1: keys คู่, t2: keys คี่ — รวมกันได้ 0..1000 ครบ
        let e1: Vec<(Vec<u8>, Vec<u8>)> = (0..1000).step_by(2).map(|i| (format!("key:{:08}", i).into_bytes(), format!("even-{i}").into_bytes())).collect();
        let e2: Vec<(Vec<u8>, Vec<u8>)> = (1..1000).step_by(2).map(|i| (format!("key:{:08}", i).into_bytes(), format!("odd-{i}").into_bytes())).collect();
        let r1: Vec<(&[u8], &[u8])> = e1.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        let r2: Vec<(&[u8], &[u8])> = e2.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&t1, &r1).unwrap();
        XDBWriter::write_table(&t2, &r2).unwrap();

        let written = merge_tables(&[&t1, &t2], &out).unwrap();
        assert_eq!(written, 1000);

        let reader = XDBReader::open(&out).unwrap();
        assert_eq!(reader.len(), 1000);
        assert_eq!(reader.get(b"key:00000000").unwrap(), Some(&b"even-0"[..]));
        assert_eq!(reader.get(b"key:00000999").unwrap(), Some(&b"odd-999"[..]));
    }

    #[test]
    fn merge_with_empty_table() {
        let t1 = temp_path("merge_empty1");
        let empty = temp_path("merge_empty2");
        let out = temp_path("merge_empty_out");

        let refs1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2")];
        XDBWriter::write_table(&t1, &refs1).unwrap();
        XDBWriter::write_table(&empty, &[]).unwrap();

        let written = merge_tables(&[&t1, &empty], &out).unwrap();
        assert_eq!(written, 2);
        assert_eq!(XDBReader::open(&out).unwrap().len(), 2);
    }

    #[test]
    fn merge_output_equal_to_input_is_rejected() {
        let t1 = temp_path("merge_same");
        let refs1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1")];
        XDBWriter::write_table(&t1, &refs1).unwrap();
        let err = merge_tables(&[&t1], &t1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    // ---- tombstone (format v4) ----

    #[test]
    fn tombstone_round_trip() {
        let path = temp_path("tomb");
        let mut b = TableBuilder::create(&path, 3).unwrap();
        b.add(b"a", b"1").unwrap();
        b.add_tombstone(b"b").unwrap();
        b.add(b"c", b"3").unwrap();
        b.finish().unwrap();

        let reader = XDBReader::open(&path).unwrap();
        assert_eq!(reader.get(b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(reader.get(b"b").unwrap(), None); // tombstone = ไม่เจอ
        assert_eq!(reader.get_entry(b"b").unwrap(), Some(None)); // แต่ get_entry บอกว่าเป็น tombstone
        assert_eq!(reader.get(b"c").unwrap(), Some(&b"3"[..]));
        assert_eq!(reader.len(), 3); // tombstone ก็นับเป็น entry

        // iterator เห็น value = None
        let entries: Vec<(Vec<u8>, Option<Vec<u8>>)> = reader
            .iter()
            .map(|r| r.map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()))).unwrap())
            .collect();
        assert_eq!(entries[1], (b"b".to_vec(), None));
    }

    #[test]
    fn merge_preserves_tombstone() {
        let t1 = temp_path("tomb_merge1");
        let t2 = temp_path("tomb_merge2");
        let out = temp_path("tomb_merge_out");

        let refs1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"2")];
        XDBWriter::write_table(&t1, &refs1).unwrap();
        // t2 ลบ b ไป
        let mut b = TableBuilder::create(&t2, 1).unwrap();
        b.add_tombstone(b"b").unwrap();
        b.finish().unwrap();

        merge_tables(&[&t1, &t2], &out).unwrap();
        let reader = XDBReader::open(&out).unwrap();
        assert_eq!(reader.get(b"a").unwrap(), Some(&b"1"[..]));
        assert_eq!(reader.get(b"b").unwrap(), None); // ยังถูกลบอยู่
        assert_eq!(reader.get_entry(b"b").unwrap(), Some(None));
    }

    // ---- XDBStore: realtime updates ----

    fn temp_store(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("xdb_store_{}", name));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn store_put_get_update() {
        let dir = temp_store("basic");
        let store = XDBStore::open(&dir).unwrap();

        store.put(&[(b"alice", b"1"), (b"bob", b"2")]).unwrap();
        assert_eq!(store.get(b"alice").unwrap(), Some(b"1".to_vec()));

        // update = put ซ้ำ (layer ใหม่ชนะ)
        store.put(&[(b"alice", b"999")]).unwrap();
        assert_eq!(store.get(b"alice").unwrap(), Some(b"999".to_vec()));
        assert_eq!(store.get(b"bob").unwrap(), Some(b"2".to_vec()));
        assert_eq!(store.get(b"nope").unwrap(), None);
    }

    #[test]
    fn store_delete() {
        let dir = temp_store("delete");
        let store = XDBStore::open(&dir).unwrap();

        store.put(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")]).unwrap();
        store.delete(&[b"b"]).unwrap();

        assert_eq!(store.get(b"b").unwrap(), None); // ถูกลบ
        assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get(b"c").unwrap(), Some(b"3".to_vec()));

        // ใส่คืนได้หลังลบ
        store.put(&[(b"b", b"new")]).unwrap();
        assert_eq!(store.get(b"b").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn store_iter_merged_view() {
        let dir = temp_store("iter");
        let store = XDBStore::open(&dir).unwrap();

        store.put(&[(b"a", b"1"), (b"c", b"3")]).unwrap();
        store.put(&[(b"b", b"2"), (b"a", b"1-new")]).unwrap(); // layer ใหม่ → a ชนะ
        store.delete(&[b"c"]).unwrap();

        let view: Vec<(Vec<u8>, Vec<u8>)> = store.iter().map(|r| r.unwrap()).collect();
        assert_eq!(
            view,
            vec![(b"a".to_vec(), b"1-new".to_vec()), (b"b".to_vec(), b"2".to_vec())]
        ); // c ถูกลบ, a เอาค่าจาก layer ใหม่สุด
    }

    #[test]
    fn store_compact_and_reopen() {
        let dir = temp_store("compact");
        {
            let store = XDBStore::open_with(&dir, 0).unwrap(); // ปิด auto-compact
            store.put(&[(b"a", b"1")]).unwrap();
            store.put(&[(b"a", b"2")]).unwrap();
            store.delete(&[b"zz"]).unwrap();
            store.put(&[(b"b", b"3")]).unwrap();
            assert_eq!(store.layer_count(), 4);

            assert_eq!(store.compact().unwrap(), 1);
            assert_eq!(store.layer_count(), 1);
            assert_eq!(store.get(b"a").unwrap(), Some(b"2".to_vec()));
            assert_eq!(store.get(b"zz").unwrap(), None); // tombstone ต้องยังกดอยู่
        }
        // เปิดใหม่ — ข้อมูลต้องอยู่ครบ
        let store = XDBStore::open(&dir).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"2".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"3".to_vec()));
        assert_eq!(store.get(b"zz").unwrap(), None);
    }

    #[test]
    fn store_reopen_sees_all_layers() {
        let dir = temp_store("reopen");
        {
            let store = XDBStore::open_with(&dir, 0).unwrap();
            store.put(&[(b"k1", b"v1")]).unwrap();
            store.put(&[(b"k2", b"v2")]).unwrap();
        }
        let store = XDBStore::open(&dir).unwrap();
        assert_eq!(store.layer_count(), 2);
        assert_eq!(store.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get(b"k2").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn store_auto_compact_at_threshold() {
        let dir = temp_store("auto");
        let store = XDBStore::open_with(&dir, 4).unwrap();
        for i in 0..3 {
            store.put(&[(format!("k{i}").as_bytes(), b"v")]).unwrap();
        }
        assert_eq!(store.layer_count(), 3); // ยังไม่ถึง threshold
        store.put(&[(b"k3", b"v")]).unwrap(); // ตัวที่ 4 → compact อัตโนมัติ
        assert_eq!(store.layer_count(), 1);
        assert_eq!(store.get(b"k0").unwrap(), Some(b"v".to_vec()));
        assert_eq!(store.get(b"k3").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn store_realtime_write_read_loop() {
        let dir = temp_store("loop");
        let store = XDBStore::open_with(&dir, 8).unwrap();
        // จำลองแอป realtime: update เร็ว ๆ 100 รอบ อ่านกลับทันทีทุกรอบ
        for i in 0..100u32 {
            let key = format!("counter:{}", i % 10);
            store.put(&[(key.as_bytes(), format!("v{i}").as_bytes())]).unwrap();
            assert_eq!(
                store.get(key.as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes()),
                "ต้องเห็นค่าล่าสุดทันทีหลัง put รอบ {i}"
            );
        }
        // ค่าสุดท้ายของ counter:i = รอบสุดท้ายที่เขียน i นั้น = 90 + i
        for i in 0..10u32 {
            assert_eq!(
                store.get(format!("counter:{i}").as_bytes()).unwrap(),
                Some(format!("v{}", 90 + i).into_bytes())
            );
        }
    }

    // ---- seek / range / prefix ----

    #[test]
    fn reader_iter_from_seeks_correctly() {
        let path = temp_path("seek");
        let entries = build_entries(2000, 50); // หลาย blocks
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();
        let reader = XDBReader::open(&path).unwrap();

        // เริ่มที่กลางตาราง
        let start = format!("key:{:010}", 700);
        let keys: Vec<&[u8]> = reader.iter_from(start.as_bytes()).map(|r| r.unwrap().0).collect();
        assert_eq!(keys.first().unwrap(), &start.as_bytes());
        assert_eq!(keys.len(), 2000 - 700);

        // seek ก่อน key แรก → ได้ทั้งหมด
        assert_eq!(reader.iter_from(b"aaa").count(), 2000);
        // seek เกิน key สุดท้าย → ว่าง
        assert_eq!(reader.iter_from(b"zzz").count(), 0);
    }

    #[test]
    fn reader_range_and_prefix() {
        let path = temp_path("range2");
        // สร้างแบบเรียงแล้ว (BTreeMap) เพราะ write_table ต้องได้ entries เรียง
        let entries: std::collections::BTreeMap<Vec<u8>, Vec<u8>> = (0..1000u32)
            .map(|i| {
                let key = format!("user:{}:{:04}", i % 3, i);
                (key.into_bytes(), format!("v{i}").into_bytes())
            })
            .collect();
        let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        XDBWriter::write_table(&path, &refs).unwrap();
        let reader = XDBReader::open(&path).unwrap();

        // range [user:1:0500, user:1:0600) — keys ที่ i%3==1 ในช่วงนี้มี 33 ตัว
        let count = reader.range(b"user:1:0500", b"user:1:0600").count();
        assert_eq!(count, 33);

        // prefix user:2: — ได้ทุก key ที่ขึ้นต้นด้วย (i % 3 == 2 → 333 keys)
        let count = reader.prefix(b"user:2:").count();
        assert_eq!(count, 333);

        // range ว่าง
        assert_eq!(reader.range(b"user:1:000510", b"user:1:000510").count(), 0);
    }

    #[test]
    fn store_range_prefix_seek() {
        let dir = temp_store("range");
        let store = XDBStore::open_with(&dir, 0).unwrap();
        store.put(&[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")]).unwrap();
        // layer ใหม่แก้ c — range ต้องเห็นค่าใหม่
        store.put(&[(b"c", b"3-new")]).unwrap();
        // ลบ d — range ต้องข้าม
        store.delete(&[b"d"]).unwrap();

        let view: Vec<(Vec<u8>, Vec<u8>)> = store.range(b"b", b"zzz").map(|r| r.unwrap()).collect();
        assert_eq!(
            view,
            vec![(b"b".to_vec(), b"2".to_vec()), (b"c".to_vec(), b"3-new".to_vec())]
        );

        let all: Vec<Vec<u8>> = store.iter_from(b"c").map(|r| r.unwrap().0).collect();
        assert_eq!(all, vec![b"c".to_vec()]); // d ถูกลบ เหลือแค่ c
    }

    // ---- file locking ----

    #[test]
    fn store_second_open_is_rejected_while_locked() {
        let dir = temp_store("locked");
        let store = XDBStore::open(&dir).unwrap();

        // เปิด instance ที่สองที่ dir เดียวกันขณะตัวแรกยังมีชีวิต → ต้อง fail
        let err = match XDBStore::open(&dir) {
            Err(e) => e,
            Ok(_) => panic!("second open must be rejected while locked"),
        };
        assert!(
            err.to_string().contains("locked"),
            "expected lock error, got: {err}"
        );

        drop(store); // ปลดล็อก
        let reopened = XDBStore::open(&dir); // ต้องเปิดได้
        assert!(reopened.is_ok());
    }
}
