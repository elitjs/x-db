//! XdbSingleFile — ใช้ไฟล์ .xdb **ไฟล์เดียว** ทำทุกอย่าง: สร้าง / อัพเดต / ลบ / อ่าน — ไม่พัง
//!
//! หลักการเดียวกับ `XdbSingleFile` ฝั่ง TypeScript: ภายนอกเห็นแค่ไฟล์เดียว (`data.xdb`)
//! งานเขียน realtime ทำในห้องเครื่องข้าง ๆ (`data.xdb.store/`) แล้ว `save()` บีบรวมทุกอย่าง
//! เขียนทับไฟล์เดียวแบบ atomic (tmp + rename) — XDBReader ที่เปิดค้างอยู่ก็ไม่พัง
//! เพราะยังอ่าน snapshot เดิมของตัวเองต่อไปได้ (บน Windows ใช้ POSIX-delete ผ่าน
//! SetFileInformationByHandle เมื่อ rename ทับ mmap ค้างไม่ได้)

use crate::store::{StoreOptions, XDBStore};
use crate::{XDBReader, XDBWriter};
use std::io;
use std::path::{Path, PathBuf};

pub struct XdbSingleFile {
    file: PathBuf,
    dir: PathBuf,
    store: XDBStore,
}

impl XdbSingleFile {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::open_with(path, StoreOptions::default())
    }

    pub fn open_with<P: AsRef<Path>>(path: P, options: StoreOptions) -> io::Result<Self> {
        let file = path.as_ref().to_path_buf();
        let dir = PathBuf::from(format!("{}.store", file.display()));
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
            // มีไฟล์อยู่แล้ว (เคย save ไว้ / เอามาจากเครื่องอื่น) → ใช้เป็นฐาน layer แรก
            if file.exists() {
                std::fs::copy(&file, dir.join("000001.xdb"))?;
            }
        }
        let store = XDBStore::open_opts(&dir, options)?;
        Ok(Self { file, dir, store })
    }

    /// เพิ่ม/แก้ค่า (upsert)
    pub fn put(&self, entries: &[(&[u8], &[u8])]) -> io::Result<()> {
        self.store.put(entries)
    }

    /// ลบ keys (tombstone)
    pub fn delete(&self, keys: &[&[u8]]) -> io::Result<()> {
        self.store.delete(keys)
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.store.get(key)
    }

    pub fn has(&self, key: &[u8]) -> io::Result<bool> {
        self.store.has(key)
    }

    /// มุมมองรวมทั้งหมด (รวมของที่ยังไม่ save) เรียงตาม key
    pub fn iter(&self) -> crate::store::StoreIter {
        self.store.iter()
    }

    pub fn range(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + '_ {
        self.store.range(start, end)
    }

    pub fn prefix(&self, prefix: &[u8]) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> + '_ {
        self.store.prefix(prefix)
    }

    /// เปิด XDBReader บนไฟล์เดียวนั้น (snapshot ณ ตอน save() ล่าสุด)
    pub fn open_snapshot(&self) -> io::Result<XDBReader> {
        if !self.file.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "ยังไม่มีไฟล์ — เรียก save() ก่อน",
            ));
        }
        XDBReader::open(&self.file)
    }

    /// บีบทุกอย่าง (ฐาน + memtable + layers) แล้ว**แทนที่ไฟล์เดิมแบบ atomic** (tmp + rename)
    /// ระหว่างนี้ XDBReader ตัวเก่าที่เปิดค้างยังอ่าน snapshot เดิมได้ต่อ
    pub fn save(&self) -> io::Result<()> {
        // compact รอบแรกเสมอ — flush memtable ลง layer ก่อน (กรณียังไม่มี layer เลย)
        self.store.compact()?;
        loop {
            while self.store.is_compacting() {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            let layers = self.layer_files()?;
            if layers.len() <= 1 {
                break;
            }
            self.store.compact()?;
        }

        let layers = self.layer_files()?;
        let tmp = self.tmp_path();
        if layers.is_empty() {
            // ยังไม่มีข้อมูลเลย → เขียนตารางเปล่าให้ไฟล์มีรูปแบบถูกต้องเสมอ
            XDBWriter::write_table(&tmp, &[])?;
        } else {
            std::fs::copy(self.dir.join(&layers[0]), &tmp)?;
        }
        replace_file(&tmp, &self.file)
    }

    /// save + ปิด store + **ลบห้องเครื่อง** → เหลือไฟล์เดียวจริง ๆ
    /// พกไปเครื่องอื่นได้ (เปิดครั้งหน้า seed จากไฟล์นี้อัตโนมัติ)
    pub fn export_and_close(self) -> io::Result<()> {
        self.save()?;
        drop(self.store);
        // ลองหลายรอบ — bg thread อาจยังถือ file handle อยู่แป๊บหนึ่ง
        let mut last_err = None;
        for _ in 0..5 {
            match std::fs::remove_dir_all(&self.dir) {
                Ok(_) => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
            }
        }
        Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "cannot remove store dir")))
    }

    fn layer_files(&self) -> io::Result<Vec<String>> {
        let mut names: Vec<String> = std::fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("xdb"))
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        names.sort();
        Ok(names)
    }

    fn tmp_path(&self) -> PathBuf {
        self.file.with_extension("xdb.tmp")
    }
}

/// แทนที่ `dest` ด้วย `src` แบบ atomic — บน Windows ถ้ามี reader ถือ mmap ของ dest อยู่
/// rename ทับจะ EPERM → ใช้ POSIX-delete (ชื่อว่างทันที แต่ mmap เก่ายังอ่านได้ต่อ) แล้ว rename ตาม
fn replace_file(src: &Path, dest: &Path) -> io::Result<()> {
    match std::fs::rename(src, dest) {
        Ok(_) => Ok(()),
        Err(e) => {
            #[cfg(windows)]
            {
                if let Ok(()) = posix_delete(dest) {
                    return std::fs::rename(src, dest);
                }
            }
            Err(e)
        }
    }
}

/// ลบไฟล์แบบ POSIX semantics (Windows): ชื่อหายทันทีแม้มี handle/mmap เปิดค้างอยู่
/// — ตัวที่ถืออยู่ยังอ่านข้อมูลเดิมต่อได้จนปิด (ต้องเปิดด้วย FILE_SHARE_DELETE ซึ่ง std ทำให้)
#[cfg(windows)]
fn posix_delete(path: &Path) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::SetFileInformationByHandle;

    const FILE_INFO_BY_HANDLE_CLASS_FILE_DISPOSITION_INFO_EX: i32 = 21;
    const FILE_DISPOSITION_FLAG_DELETE: u32 = 0x1;
    const FILE_DISPOSITION_FLAG_POSIX_SEMANTICS: u32 = 0x2;

    #[repr(C)]
    struct FileDispositionInfoEx {
        flags: u32,
    }

    let file = std::fs::File::open(path)?;
    let info = FileDispositionInfoEx {
        flags: FILE_DISPOSITION_FLAG_DELETE | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
    };
    let ok = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as _,
            FILE_INFO_BY_HANDLE_CLASS_FILE_DISPOSITION_INFO_EX,
            &info as *const _ as *const _,
            std::mem::size_of::<FileDispositionInfoEx>() as u32,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
