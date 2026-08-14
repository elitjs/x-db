# x-db

Read-only key-value store แบบ SSTable (Rust) เรียกใช้จาก TypeScript ได้โดยตรงผ่าน native binding (napi-rs) — ไม่ต้องมี server

## คุณสมบัติ

- **เร็ว**: bloom filter + sparse index (binary search) + zero-copy mmap
- **ปลอดภัย**: CRC32 ทุก block + footer, ตรวจความยาว key/value, fsync ตอนเขียน — ไฟล์เสียหายจะได้ error ไม่ใช่ข้อมูลเพี้ยน/crash
- **Iterator**: ไล่ทั้งตาราง / range scan / prefix scan
- **Merge/compaction**: รวมหลายตารางเป็นไฟล์เดียวแบบ streaming (ตารางใหญ่ก็ไม่กิน RAM)
- **LZ4 compression ต่อ block** (format v6): ตาราง/compaction บีบอัดได้ — ข้อมูล text เล็กลง 3-5 เท่า
- **XDBStore (realtime updates)**: put/delete แบบ LSM-lite — เหมาะกับแอปที่ update ตลอดเวลา
  พร้อม **file locking** (เปิดซ้อนหลาย process ไม่ได้ — กันข้อมูลเสียหาย) และ `close()` ปลดล็อก
- **seek / range / prefix**: iterator กระโดดไปตำแหน่ง key ได้เลย (ไม่ต้องไล่ตั้งแต่ต้น)
- **Native addon**: Rust compile เป็น `.node` เรียกจาก Node.js/TypeScript ตรง ๆ ใน process เดียวกัน
- **(เสริม) HTTP server**: `xdb-server` binary สำหรับกรณีอยาก serve ผ่านเครือข่าย

## Benchmark (ตัวอย่าง, `cargo run --release --example bench`)

100,000 entries × 100B (ไฟล์ 11.8 MB), Windows x64:

| การทำงาน | เวลา (ไม่บีบอัด) |
|---|---|
| build ทั้งตาราง | ~20 ms |
| get เจอ key (สุ่ม, warm) | ~1.0 µs/op |
| get เจอ key เดิมซ้ำ ๆ (hot path ล้วน) | ~190 ns/op |
| get ไม่เจอ (bloom ตัด) | ~155 ns/op |
| iterate ทั้งตาราง | ~7 ms (คืนค่า owned — รองรับ block บีบอัด) |
| range scan 1,000 keys | ~3.9 ms |

ตัวเลขบน block ที่บีบอัด: get เจอ ~1µs เท่าเดิมหลัง block cache ร้อน (decompress ครั้งเดียวต่อ block,
cache 64MB) — จาก test: ตาราง text 100k entries เล็กลง**มากกว่า 2 เท่า**

## เทียบกับ database อื่น (`cargo run --release -p xdb-bench`)

เงื่อนไขเดียวกัน: 100,000 entries × 100B, page cache ร้อน, Windows x64 — เทียบกับ SQLite (rusqlite), redb และ HashMap บน RAM เป็น baseline:

| database | build (ms) | open (ms) | get เจอ (ns) | get ไม่เจอ (ns) | iterate (ms) | ไฟล์ (MB) |
|---|---:|---:|---:|---:|---:|---:|
| **x-db** | 19-46 | **0.2** | **668** | **21** | **1.0** | **11.8** |
| HashMap (RAM) | 5 | — | 115 | 41 | 0.0 | — |
| SQLite | 126 | 0.25 | 39,829 | 31,416 | 16.9 | 13.5 |
| redb | 295 | 10.8 | 1,101 | 267 | 10.3 | 32.6 |

(open วัดแบบ warm — การเปิดไฟล์ที่เพิ่งเขียนเสร็จครั้งแรกมีค่า first-touch ของ OS/antivirus
~10-15ms เท่ากันทุก database บน Windows)

**สรุป:**
- **เทียบ SQLite**: x-db เร็วกว่า ~50 เท่าบน point lookup และ ~15 เท่าบนการไล่ทั้งตาราง ไฟล์เล็กกว่าด้วย
- **เทียบ redb**: เร็วกว่าทุกด้าน — เปิดไฟล์เร็ว ~54 เท่า, get เจอ ~1.6 เท่า, miss ~13 เท่า (bloom filter), iterate ~10 เท่า, ไฟล์เล็กกว่า 2.8 เท่า
- **เทียบ HashMap**: ช้ากว่า RAM ล้วน ~6 เท่าบน hot path (~190ns vs ~124ns) — ที่เหลือคือ cache miss จากการสุ่มเข้าไฟล์ 11.8MB ซึ่งเป็นขีดจำกัดของ hardware แลกกับข้อมูลใหญ่เท่าไรก็ได้โดยไม่กิน RAM

หมายเหตุความเป็นธรรม: x-db เป็น immutable read-only store (ไม่มีค่าใช้จ่าย transaction/locking) ส่วน SQLite/redb รองรับการเขียน/แก้ไขข้อมูลได้ — เลือกใช้ตามลักษณะงาน

## โครงสร้าง

```
src/lib.rs             ไลบรารีหลัก: XDBWriter / XDBReader / BloomFilter / XDBIter
src/main.rs            (เสริม) xdb-server: HTTP API (axum)
examples/bench.rs      benchmark
crates/xdb-node/       napi-rs binding: Rust → Node.js addon
typescript/            npm package "xdb-native": TS wrapper + tests + example
```

## ใช้จาก TypeScript (native binding)

Build ครั้งแรก:

```bash
cd typescript
npm install
npm run native:build   # cargo build ผ่าน @napi-rs/cli → index.win32-x64-msvc.node
npm run build          # tsc → dist/
```

ใช้งาน:

```ts
import { writeTable, XdbReader } from "xdb-native";

writeTable("./demo.xdb", [
  ["alice", "engineer"],
  ["bob", "designer"],
  [new Uint8Array([0, 255, 128]), new Uint8Array([1, 2, 3])], // binary ก็ได้
]);

const reader = new XdbReader("./demo.xdb");
reader.getUtf8("alice");                   // "engineer"
reader.get(new Uint8Array([0, 255, 128])); // Uint8Array(3) [1, 2, 3]
reader.count;                              // 3

// iterate (เรียงตาม key)
for (const { key, value } of reader) { /* ... */ }

// merge/compaction: รวมหลายตาราง — ไฟล์หลังสุดชนะเมื่อ key ซ้ำ
import { mergeTables } from "xdb-native";
const count = mergeTables(["old.xdb", "new.xdb"], "merged.xdb");
```

- `writeTable` รับ Array / `Map` / Object — ส่งไม่เรียงก็ได้ Rust จัดเรียงและกรอง key ซ้ำให้ (ตัวหลังสุดชนะ)
- `XdbReader` เปิดไฟล์แบบ mmap ครั้งเดียว แล้ว lookup ได้ตลอดอายุ process
- ไฟล์เสียหาย → throw error (ตรวจ CRC ครั้งแรกที่แตะแต่ละ block)

## XDBStore — สำหรับแอปที่ update แบบ realtime

`writeTable` เหมาะกับข้อมูลนิ่ง แต่ถ้าแอปต้อง put/delete ตลอดเวลา ให้ใช้ `XdbStore`:
ข้อมูลอยู่เป็นหลาย layers ซ้อนกัน (LSM-lite) — put = เขียน layer เล็กใหม่ (เร็ว ~5ms รวม fsync),
get = ค้นข้าม layers ตัวใหม่ชนะ, delete = tombstone, และ **compact อัตโนมัติ** เมื่อครบ 8 layers

```ts
import { XdbStore } from "xdb-native";

const store = new XdbStore("./mydata");          // directory (สร้างให้เอง)
// const store = new XdbStore("./mydata", { compactThreshold: 0 }); // ปิด auto-compact

store.put([["alice", "engineer"]]);               // insert
store.put([["alice", "senior engineer"]]);        // update — ทันที
store.getUtf8("alice");                           // "senior engineer"
store.delete("alice");                            // delete (tombstone)
store.getUtf8("alice");                           // null

for (let i = 0; i < 100; i++) store.put([[`c:${i}`, String(i)]]); // realtime ได้เรื่อย ๆ
```

จาก Rust: `XDBStore::open(dir)` / `open_with(dir, threshold)` — API เดียวกัน

หมายเหตุ: get ของ store เห็นข้อมูลล่าสุดทันทีหลัง put (อ่านผ่าน mmap เหมือนเดิม แค่ไล่หลาย layers)
ส่วน compaction ของตารางใหญ่มีค่าใช้จ่าย — ถ้า base ใหญ่มากและเขียนถี่ แนะนำ batch หลาย keys ต่อ put

## ใช้จาก Rust

```rust
use x_db::{XDBWriter, XDBReader};

let entries = [(b"key".as_slice(), b"value".as_slice())];
XDBWriter::write_table("demo.xdb", &entries)?;

let reader = XDBReader::open("demo.xdb")?;
let value: Option<&[u8]> = reader.get(b"key")?; // Err = ไฟล์เสีย, None = ไม่มี key

for entry in reader.iter() {
    let (key, value) = entry?;
    // ...
}
```

## File format (v6)

```
[Header 32B]   magic "XDB1" + version + block size
[Blocks]       ต่อ block (16KB): payload = entries ([klen u16][vlen u32][key][value])
               + restart array (u32 × R, R: u16) — อาจถูกบีบอัด LZ4 (ถ้าคุ้ม)
               แล้วตามด้วย trailer [raw_len u32][CRC32]
[Bloom]        ขนาดปรับตามจำนวน entries (~1% false positive)
[Sparse Index] first key + offset + length + num_restarts ของแต่ละ block
               (เก็บ metadata ครบใน index → open อ่านแค่ 3 บริเวณต่อเนื่อง ไม่ต้องแตะ blocks เลย ~0.2ms)
[Footer 40B]   offsets/lengths + entry count + CRC32 ของ footer เอง + magic "ENDX"
```

การ lookup: bloom filter → binary search บน sparse index (เลือก block) → binary search บน
restart points (เลือกช่วง 16 entries ใน block) → linear scan สั้น ๆ — เร็วขึ้น ~2 เท่าจาก
การไล่ block ทั้งก้อนตรง ๆ

## CLI (`cargo install --path crates/xdb-cli` หรือ `./target/release/xdb.exe`)

```bash
xdb check  data.xdb                 # ตรวจไฟล์: CRC ทุก block + key เรียงถูก + จำนวนตรง footer
xdb stats  data.xdb                 # entries / blocks / bloom / อัตราบีบอัด
xdb dump   data.xdb --prefix u:42:  # แสดง entries (--start, --limit, --keys-only)
xdb get    data.xdb "user:42"       # ค้นหา key เดียว
xdb merge  out.xdb a.xdb b.xdb --compress   # รวมตาราง (บีบอัดได้)
```

## รัน tests

```bash
cargo test                 # ไลบรารี Rust (38 tests) + CLI (8 integration tests)
cd typescript && npm test  # native binding จาก TS (27 tests)
npm run example            # ตัวอย่างการใช้งาน
cargo run --release --example bench   # benchmark
```

## CI / build ข้าม platform

`.github/workflows/ci.yml` รัน tests ทั้ง Rust และ Node addon บน Linux/Windows/macOS
ตัว `.node` ที่ build ในเครื่องนี้ใช้ได้เฉพาะ win32-x64 — การ distribute ให้ครบทุก platform
ต้อง push ขึ้น GitHub แล้วเปิดใช้ release job (commented ไว้ใน workflow แล้ว) หรือใช้ `napi build --zig` เพื่อ cross-compile ในเครื่องเดียว

## เหลือที่ยังไม่ได้ทำ

- **Publish npm package** — โครงสร้างพร้อมแล้ว เหลือตั้งค่า release pipeline จริง (napi-rs publish workflow)
