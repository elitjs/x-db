//! WAL (write-ahead log) ของ XDBStore
//!
//! put/delete เขียนลง WAL ก่อนตอบกลับ (fsync ตามตัวเลือก sync) แล้วค่อยทำงานบน memtable —
//! ถ้า process พังกลางทาง เปิดใหม่แล้ว replay WAL ก็ได้ข้อมูลครบ
//!
//! Record: `[crc32 u32][klen u16][vraw u32 (MSB = tombstone)][key][value]`
//! ถ้าเจอ record เสียหรือครึ่ง ๆ (torn tail จาก crash ระหว่างเขียน) จะหยุด replay ตรงนั้น

use crate::TOMBSTONE_FLAG;
use crc32fast::hash as crc32;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const RECORD_HEADER: usize = 4 + 2 + 4;

/// entry หนึ่งตัวใน WAL — value = None คือ tombstone
pub type WalEntry = (Vec<u8>, Option<Vec<u8>>);

pub struct Wal {
    file: File,
    sync: bool,
    /// buffer reuse ระหว่าง append รอบ (ไม่ allocate ใหม่ทุก batch)
    buf: Vec<u8>,
}

impl Wal {
    /// เปิด (หรือสร้าง) WAL แล้ว replay ทุก records ที่ตั้งใจเขียนไว้
    /// คืน entries ที่อ่านได้ (ตัวท้ายที่เสียจะถูกตัดทิ้ง — torn tail)
    pub fn open(path: &Path, sync: bool) -> io::Result<(Self, Vec<WalEntry>)> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        let mut entries = Vec::new();
        let mut cursor = 0usize;
        let mut valid_end = 0usize;
        while cursor + RECORD_HEADER <= buf.len() {
            let k_len = u16::from_be_bytes(buf[cursor + 4..cursor + 6].try_into().unwrap()) as usize;
            let v_raw = u32::from_be_bytes(buf[cursor + 6..cursor + 10].try_into().unwrap());
            let v_len = (v_raw & !TOMBSTONE_FLAG) as usize;
            let end = cursor + RECORD_HEADER + k_len + v_len;
            if end > buf.len() {
                break; // record ขาด (crash ระหว่างเขียน)
            }
            // CRC ครอบ klen+vraw+key+value
            let stored_crc = u32::from_be_bytes(buf[cursor..cursor + 4].try_into().unwrap());
            if crc32(&buf[cursor + 4..end]) != stored_crc {
                break; // record เสีย — ถือว่าท้ายไฟล์เสียหาย หยุดตรงนี้
            }
            let key = buf[cursor + RECORD_HEADER..cursor + RECORD_HEADER + k_len].to_vec();
            let value = if v_raw & TOMBSTONE_FLAG != 0 {
                None
            } else {
                Some(buf[cursor + RECORD_HEADER + k_len..end].to_vec())
            };
            entries.push((key, value));
            cursor = end;
            valid_end = end;
        }

        // ตัด torn tail ทิ้ง เพื่อให้ append ต่อได้ถูกต้อง
        if valid_end < buf.len() {
            file.set_len(valid_end as u64)?;
            file.sync_all()?;
        }
        file.seek(SeekFrom::End(0))?;

        Ok((Self { file, sync, buf: Vec::new() }, entries))
    }

    /// เขียน batch ลง WAL (fsync ตาม `sync`) — เขียนทั้ง batch ใน write เดียว
    pub fn append(&mut self, batch: &[WalEntry]) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        // เขียนทุก records ลง buffer เดียว (reserve ครั้งเดียว ไม่มี per-record allocation)
        let buf = &mut self.buf;
        buf.clear();
        let mut size = 0usize;
        for (key, value) in batch {
            size += 4 + RECORD_HEADER + key.len() + value.as_ref().map_or(0, |v| v.len());
        }
        buf.reserve(size);
        for (key, value) in batch {
            let (val_bytes, v_raw): (&[u8], u32) = match value {
                Some(v) => (v.as_slice(), v.len() as u32),
                None => (&[], TOMBSTONE_FLAG),
            };
            let crc_slot = buf.len();
            buf.extend_from_slice(&[0u8; 4]); // ที่ว่าสำหรับ crc — เติมทีหลัง
            let rec_start = buf.len();
            buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
            buf.extend_from_slice(&v_raw.to_be_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(val_bytes);
            let crc = crc32(&buf[rec_start..]);
            buf[crc_slot..crc_slot + 4].copy_from_slice(&crc.to_be_bytes());
        }
        self.file.write_all(buf)?;
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// fsync เฉพาะเมื่ออยู่โหมด nosync (โหมด sync ทำแล้วทุก append)
    /// เรียกจาก background periodic-sync thread
    pub fn sync_periodic(&mut self) -> io::Result<()> {
        if !self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// ล้าง WAL (หลัง memtable ถูก flush เป็น layer ถาวรแล้ว)
    /// fsync เฉพาะโหมด sync — nosync ไม่จำเป็น (layer เอง fsync แล้ว)
    pub fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }
}
