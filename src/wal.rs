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
use std::path::{Path, PathBuf};

const RECORD_HEADER: usize = 4 + 2 + 4;

/// entry หนึ่งตัวใน WAL — value = None คือ tombstone
pub type WalEntry = (Vec<u8>, Option<Vec<u8>>);

pub struct Wal {
    file: File,
    path: PathBuf,
    sync: bool,
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

        Ok((
            Self { file, path: path.to_path_buf(), sync },
            entries,
        ))
    }

    /// เขียน batch ลง WAL (fsync ตาม `sync`) — เขียนทั้ง batch ใน write เดียว
    pub fn append(&mut self, batch: &[WalEntry]) -> io::Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::with_capacity(batch.len() * 32);
        for (key, value) in batch {
            let (val_bytes, v_raw): (&[u8], u32) = match value {
                Some(v) => (v.as_slice(), v.len() as u32),
                None => (&[], TOMBSTONE_FLAG),
            };
            let mut rec = Vec::with_capacity(RECORD_HEADER + key.len() + val_bytes.len());
            rec.extend_from_slice(&(key.len() as u16).to_be_bytes());
            rec.extend_from_slice(&v_raw.to_be_bytes());
            rec.extend_from_slice(key);
            rec.extend_from_slice(val_bytes);
            buf.extend_from_slice(&crc32(&rec).to_be_bytes());
            buf.extend_from_slice(&rec);
        }
        self.file.write_all(&buf)?;
        if self.sync {
            self.file.sync_data()?;
        }
        Ok(())
    }

    /// ล้าง WAL (หลัง memtable ถูก flush เป็น layer ถาวรแล้ว)
    pub fn reset(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_data()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }
}
