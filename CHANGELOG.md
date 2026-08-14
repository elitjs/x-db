# Changelog

## Unreleased (v0.3.0)

### Tiered compaction — base ใหญ่ไม่โดน rewrite บ่อย ๆ
- นโยบาย size-tiered: รวเฉพาะกลุ่ม layer เล็กล่าสุด หยุดก่อนกลืน layer ที่ใหญ่กว่ากลุ่ม 4 เท่า
- ลด write amplification มหาศาลเมื่อ base ใหญ่ (ก่อนหน้า: rewrite ทั้งก้อนทุก 8 layers)
- ถ้า layers ล้น 2x threshold → รวทั้งหมด (กันจำนวน layer บวม)
- manual compact() ยังรวทั้งหมดเหมือนเดิม (เจตนาชัด = อยากได้ไฟล์เดียว)

### Background compaction — writer ไม่ต้องรอ merge อีกต่อไป
- compaction รวเป็น layer เดียวใน **thread แยก** — put/delete ไม่โดน stall ตอน merge ตารางใหญ่
- atomic flag กัน compaction ซ้อนกัน + วนรวจนเบากว่า threshold เอง
- seq จองล่วงหน้า → layer ที่เกิดระหว่าง merge มีลำดับถูกต้องเสมอ
- `is_compacting()` / TS `store.isCompacting` สำหรับเช็คตอนปิดแอป
- manual `compact()` ยังเป็น blocking เหมือนเดิม (เหมาะตอน idle)

### WAL + memtable — เขียนเร็วขึ้น 20-1000 เท่า
- put/delete เขียนลง **WAL + memtable** แทนการสร้างไฟล์ layer ทุกครั้ง
  - single key: ~0.9ms (รวม fsync) จาก ~5ms
  - batch 1000 keys: **~2.2µs/key** (~450k puts/sec)
- memtable เต็ม (default 4096 entries) → flush เป็น layer อัตโนมัติ
- **Crash-safe**: เปิดใหม่แล้ว replay WAL ครบ (torn tail จาก crash ถูกตัดอัตโนมัติ)
- `StoreOptions { compact_threshold, flush_entries, sync }` + `flush()` + `memtable_len()`
- TS: `new XdbStore(path, { compactThreshold, flushEntries, sync })`, `store.flush()`, `store.memtableLen`

## v0.2.0 (2026-08-14)

เวอร์ชันที่เพิ่มของจริงเยอะหลังจาก v0.1.0 — สรุปเป็นกลุ่ม:

### LZ4 compression ต่อ block (format v6)
- `TableBuilder::create_with(path, expected, compression)` — บีบอัด block ด้วย LZ4 เมื่อคุ้ม
  (ข้อมูลบีบไม่อยู่จะ fallback เก็บ raw ให้เอง)
- Block รูปแบบใหม่: `[payload (LZ4 ถ้าคุ้ม)][raw_len][CRC32]` — CRC ตรวจก่อน decompress
- Reader มี block cache 64MB — decompress ครั้งเดียวต่อ block, get ยังเร็ว ~1µs
- `XDBStore.compact()` บีบอัด merged layer อัตโนมัติ (layer ร้อนยังเขียนเร็วเหมือนเดิม)
- `merge_tables_with(inputs, output, compression)`
- **เปลี่ยนแปลง API**: `get`/`get_entry`/`iter` คืนค่า owned (`Vec<u8>`) แทน slice ยืม
  — get แทบไม่เปลี่ยน (~1µs), iterate 1→7ms ต่อ 100k entries

### XDBStore — realtime updates (LSM-lite)
- `XdbStore`: put/delete แบบ layered — put ~5ms (fsync ทุกครั้ง), get ข้าม layers ตัวใหม่ชนะ
- Tombstone (format v4) สำหรับ delete จริง — เปิดใหม่ข้อมูลถูกลบยังถูกลบอยู่
- **File locking** — เปิด store เดียวกัน 2 process ถูกปฏิเสธทันที (กัน corruption เงียบ ๆ) + `close()`
- Auto-compact ที่ 8 layers (ปรับได้ `compactThreshold`, 0 = ปิด)

### seek / range / prefix
- `XdbReader` + `XdbStore`: `seek(key)`, `range(start, end)`, `prefix(p)` — iterator กระโดดไป
  block ตรง ๆ ด้วย binary search (ไม่ไล่ตั้งแต่ต้น)

### CLI (`xdb`)
- `xdb check` — ตรวจ CRC ทุก block + key ordering + entry count ตรง footer
- `xdb stats` — entries / blocks / bloom / อัตราบีบอัด
- `xdb dump` (--prefix / --start / --limit / --keys-only), `xdb get`, `xdb merge` (--compress)

### Performance (จากงาน optimize ตลอดรอบ)
- get เจอ key: 1.3µs → **~0.7-1.0µs** (restart points ทำ binary search ใน block + atomic CRC cache
  + bloom ใช้ mask)
- open: 11.9ms → **0.2ms** (metadata ย้ายเข้า sparse index ทั้งหมด)
- เทียบ SQLite: เร็วกว่า ~50x บน point lookup / เทียบ redb: เร็วกว่าทุกด้าน
  (ดูตารางเต็มใน README — `cargo run --release -p xdb-bench`)

### โครงสร้าง
- CI ผ่านครบ Linux / Windows / macOS (Rust tests + Node addon + TS tests)
- Release pipeline สำหรับ npm (napi-rs, 6 platform targets) — พร้อมใช้เมื่อตั้ง NPM_TOKEN
- LICENSE (MIT), metadata สำหรับ crates.io / npm

### Breaking
- Format เป็น **v6** — ไฟล์จาก v1-v5 ต้อง rebuild (โปรเจกต์ pre-1.0, ยังไม่มี migration)
- Rust API: `get`/`get_entry`/`XDBIter::Item` เป็น owned แล้ว

## v0.1.0 (2026-08-14)

- รอบแรก: format SSTable (bloom + sparse index + CRC + mmap), Rust lib,
  Node addon (napi-rs) + TypeScript SDK, merge/compaction, HTTP server (xdb-server),
  iterator, corruption detection, fsync, key-length validation, benchmark เทียบ SQLite/redb
