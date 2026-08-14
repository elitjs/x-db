/**
 * ตัวอย่างการใช้งาน x-db จาก TypeScript (native binding — ไม่มี server)
 * ก่อนรัน: npm run native:build && npm run build
 * แล้วสั่ง: npm run example
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { mergeTables, writeTable, XdbReader } from "./index.js";

const dir = mkdtempSync(join(tmpdir(), "xdb-example-"));
const tablePath = join(dir, "demo.xdb");

// 1. สร้าง table — ส่งไม่เรียงก็ได้ Rust จะจัดการให้
writeTable(tablePath, [
  ["hello", "สวัสดี"],
  ["bye", "ลาก่อน"],
  [new Uint8Array([0, 255, 128]), new Uint8Array([1, 2, 3])], // binary ก็ได้
]);

// 2. เปิด reader (mmap) แล้วอ่านได้เลย
const reader = new XdbReader(tablePath);
console.log(reader.getUtf8("hello")); // สวัสดี
console.log(reader.getUtf8("nope")); // null
console.log(reader.get(new Uint8Array([0, 255, 128]))); // Uint8Array(3) [1, 2, 3]
console.log(reader.has("bye")); // true
console.log(reader.count); // 3

// 3. iterate ทั้งตาราง (เรียงตาม key)
for (const { key } of reader) {
  console.log(Buffer.from(key).toString("utf8"));
}

// 4. merge/compaction: รวมหลายตารางเป็นไฟล์เดียว (ไฟล์หลังสุดชนะเมื่อ key ซ้ำ)
const otherPath = join(dir, "other.xdb");
writeTable(otherPath, [["hello", "hello world (ใหม่)"]]);
const mergedPath = join(dir, "merged.xdb");
const count = mergeTables([tablePath, otherPath], mergedPath);
console.log(count); // 3 (hello ซ้ำถูกแทนที่, bye + binary key เดิม)
console.log(new XdbReader(mergedPath).getUtf8("hello")); // hello world (ใหม่)
