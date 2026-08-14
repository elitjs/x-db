/**
 * x-db Cookbook — ตัวอย่างการใช้งานครบทุกเคส รันได้จริงทั้งหมด
 *
 * รัน: npm run cookbook
 *
 * สารบัญ:
 *   1.  ตารางเดี่ยว read-only (writeTable + get/iter)
 *   2.  Binary keys/values
 *   3.  เก็บ JSON object
 *   4.  Range / Prefix / Seek
 *   5.  Realtime store: CRUD ครบ + เปิดใหม่ข้อมูลไม่หาย
 *   6.  Batch import ข้อมูลใหญ่ (เร็วมาก)
 *   7.  เลือกระดับ durability (sync / syncIntervalMs / nosync)
 *   8.  เก็บไฟล์ (blob) + หั่นไฟล์ใหญ่เป็น chunk
 *   9.  ทำ cache ที่มี TTL เอง
 *   10. Counter (read-modify-write)
 *   11. Session store + ล้าง session หมดอายุ
 *   12. เก็บค่าตัวเลขที่เรียงถูกต้อง (key design)
 *   13. Merge ตารางหลายไฟล์ (overlay)
 *   14. Compact มือ + ดูสถานะ layers/memtable
 *   15. Error handling: ไฟล์เสีย / เปิดซ้อน
 *   16. ใช้กับ xdb-server ผ่าน HTTP (แนะนำสั้น ๆ)
 */
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  mergeTables,
  writeTable,
  XdbReader,
  XdbStore,
  type KeyValue,
} from "./index.js";

let section = 0;
function head(title: string): void {
  section++;
  console.log(`\n━━━ [${section}] ${title} ━━━`);
}

const workdir = mkdtempSync(join(tmpdir(), "xdb-cookbook-"));
const file = (name: string): string => join(workdir, name);
const bytes = (v: Uint8Array): string => Buffer.from(v).toString("utf8");

async function main(): Promise<void> {
  console.log("workdir:", workdir);

  // ────────────────────────────────────────────────────────────
  head("ตารางเดี่ยว read-only — เขียนครั้งเดียว อ่านเร็วมาก");
  // เหมาะกับ: dictionary, lookup table, ข้อมูลนิ่ง — สร้างเสร็จเป็นไฟล์เดียว .xdb
  {
    writeTable(file("users.xdb"), [
      ["alice", "engineer"],
      ["bob", "designer"],
      ["carol", "manager"],
      ["dave", "devops"],
    ]);

    const reader = new XdbReader(file("users.xdb"));
    console.log("get alice  :", reader.getUtf8("alice"));   // "engineer"
    console.log("has bob    :", reader.has("bob"));          // true
    console.log("has nobody :", reader.has("nobody"));       // false
    console.log("count      :", reader.count);               // 4
    console.log("ทั้งหมด     :", [...reader].map(({ key }) => bytes(key)));
    // ["alice","bob","carol","dave"] — เรียงตาม key เสมอ แม้ใส่ไม่เรียง
  }

  // ────────────────────────────────────────────────────────────
  head("Binary keys/values — bytes ดิบอะไรก็ได้");
  {
    const binKey = new Uint8Array([0x00, 0xff, 0x80, 0x7f]);
    const binVal = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);

    writeTable(file("binary.xdb"), [[binKey, binVal]]);

    const reader = new XdbReader(file("binary.xdb"));
    console.log("binary get :", Buffer.from(reader.get(binKey)!).toString("hex"));
    // deadbeef
  }

  // ────────────────────────────────────────────────────────────
  head("เก็บ JSON object — serialize เอง (ค่าเป็น bytes ดิบ)");
  interface UserProfile {
    name: string;
    age: number;
    tags: string[];
  }
  {
    const users: UserProfile[] = [
      { name: "สมชาย", age: 30, tags: ["admin", "vip"] },
      { name: "สมหญิง", age: 25, tags: ["user"] },
    ];
    // key design: prefix + id ให้เรียงสวย
    writeTable(file("profiles.xdb"), users.map((u, i) => [`user:${String(i).padStart(6, "0")}`, JSON.stringify(u)]));

    const reader = new XdbReader(file("profiles.xdb"));
    const first = JSON.parse(reader.getUtf8("user:000000")!) as UserProfile;
    console.log("json round-trip:", first.name, first.age, first.tags);
  }

  // ────────────────────────────────────────────────────────────
  head("Range / Prefix / Seek — ค้นหาแบบมีช่วง (เร็ว กระโดดไป block ตรง ๆ)");
  {
    const rows: Array<[string, string]> = [];
    for (let i = 0; i < 1000; i++) {
      rows.push([`order:${String(i).padStart(6, "0")}`, `amount-${i}`]);
    }
    writeTable(file("orders.xdb"), rows);
    const reader = new XdbReader(file("orders.xdb"));

    // prefix: ทุก key ที่ขึ้นต้นด้วย
    const prefixCount = [...reader.prefix("order:00099")].length;
    console.log("prefix order:00099 →", prefixCount, "รายการ"); // 11 (000990..000999)

    // range [start, end) — end exclusive
    const range = [...reader.range("order:000500", "order:000503")].map(({ key }) => bytes(key));
    console.log("range 500..503 →", range); // 3 ตัว (500,501,502)

    // seek: เริ่มไล่จาก key >= ที่กำหนด
    const seeked = [...reader.seek("order:000998")].map(({ key }) => bytes(key));
    console.log("seek 998 →", seeked); // 998, 999
  }

  // ────────────────────────────────────────────────────────────
  head("Realtime store — CRUD ครบ + เปิดใหม่ข้อมูลไม่หาย");
  {
    const dir = file("live-store");
    {
      const store = new XdbStore(dir);
      store.put([["counter", "0"]]);              // INSERT
      store.put([["counter", "1"]]);              // UPDATE (ทับ key เดิม)
      store.put([["temp", "will-delete"]]);
      store.delete("temp");                       // DELETE
      console.log("counter =", store.getUtf8("counter")); // "1"
      console.log("temp   =", store.getUtf8("temp"));     // null (ถูกลบ)
      store.close(); // ปลด file lock — จะเปิดใหม่/ย้ายเครื่องอื่นต่อได้เลย
    }
    // เปิดใหม่ — ข้อมูลอยู่ครบ (WAL replay + layers)
    const store = new XdbStore(dir);
    console.log("หลังเปิดใหม่: counter =", store.getUtf8("counter"));
    store.close();
  }

  // ────────────────────────────────────────────────────────────
  head("Batch import ข้อมูลใหญ่ — 10,000 rows ในคำสั่งเดียว (fsync ทั้ง batch ครั้งเดียว)");
  {
    const store = new XdbStore(file("import"), { sync: false });
    const rows: Array<[string, string]> = [];
    for (let i = 0; i < 10_000; i++) {
      rows.push([`row:${String(i).padStart(6, "0")}`, JSON.stringify({ i, data: "x".repeat(50) })]);
    }
    const t0 = performance.now();
    store.put(rows); // ← ทั้ง 10,000 ใน put เดียว = 1 fsync
    const ms = performance.now() - t0;
    console.log(`import 10,000 rows ใน ${ms.toFixed(1)} ms (~${(ms * 1000 / 10_000).toFixed(1)} µs/row)`);
    console.log("ตรวจ:", store.getUtf8("row:009999") !== null);
    store.flush(); // ดันลง layer ถาวร
    store.close();
  }

  // ────────────────────────────────────────────────────────────
  head("เลือกระดับ durability — 3 โหมดตามชนิดข้อมูล");
  {
    // 1) sync: true (default) — ไฟดับไม่เสียเลย, put ~1ms → ข้อมูลการเงิน/ออเดอร์
    const orders = new XdbStore(file("orders-store"));

    // 2) sync:false + syncIntervalMs — put เร็ว ~5µs, ไฟดับเสียได้ ≤ 200ms → ข้อมูลทั่วไป
    const content = new XdbStore(file("content-store"), { sync: false, syncIntervalMs: 200 });

    // 3) sync:false ล้วน — เร็วสุด เสียได้ไม่จำกัดจนกว่า OS flush → cache/ข้อมูลสร้างใหม่ได้
    const cache = new XdbStore(file("cache-store"), { sync: false });

    orders.put([["o:1", "paid"]]);
    content.put([["page:home", "<h1>hi</h1>"]]);
    cache.put([["query:x", "result..."]]);

    console.log("สาม store ใช้งานพร้อมกันได้ (คนละ directory)");
    console.log("orders.get:", orders.getUtf8("o:1"), "/ content.get len:", content.getUtf8("page:home")?.length);
    orders.close(); content.close(); cache.close();
  }

  // ────────────────────────────────────────────────────────────
  head("เก็บไฟล์ (blob) + หั่นไฟล์ใหญ่เป็น chunk");
  {
    // ไฟล์เล็ก: ใส่ทั้งก้อนเลย
    const pngBytes = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 1, 2, 3, 4]);
    writeTable(file("small-assets.xdb"), [["logo.png", pngBytes]]);
    const reader = new XdbReader(file("small-assets.xdb"));
    console.log("logo.png คืนมา:", Buffer.from(reader.get("logo.png")!).toString("hex").slice(0, 8), "...");

    // ไฟล์ใหญ่: หั่นเป็น chunk 256KB — ดึงเฉพาะช่วงที่ต้องการได้โดยไม่โหลดทั้งไฟล์
    const store = new XdbStore(file("big-assets"), { sync: false });
    const fakeBigFile = new Uint8Array(700_000); // สมมติเป็นไฟล์ 700KB
    fakeBigFile.set([1, 2, 3], 0);
    const CHUNK = 256 * 1024;
    const chunks: Array<[string, Uint8Array]> = [];
    for (let off = 0, i = 0; off < fakeBigFile.length; off += CHUNK, i++) {
      chunks.push([`video.mp4:${String(i).padStart(5, "0")}`, fakeBigFile.subarray(off, off + CHUNK)]);
    }
    store.put(chunks);
    console.log("เก็บ 700KB เป็น", chunks.length, "chunks");

    // อ่านกลับ: ต่อ chunk หรือดึงเฉพาะ chunk ที่สนใจ
    const parts: Uint8Array[] = [];
    for (const { key } of store.prefix("video.mp4:")) {
      const v = store.get(key as KeyValue);
      if (v) parts.push(v);
    }
    const total = parts.reduce((n, p) => n + p.length, 0);
    console.log("อ่านกลับได้:", total, "bytes ✓");
    store.close();
  }

  // ────────────────────────────────────────────────────────────
  head("Cache ที่มี TTL เอง — เก็บ expiry ใน value แล้วเช็คตอนอ่าน");
  {
    const store = new XdbStore(file("ttl-cache"), { sync: false });

    const cacheSet = (key: string, value: string, ttlMs: number): void => {
      store.put([[key, JSON.stringify({ exp: Date.now() + ttlMs, v: value })]]);
    };
    const cacheGet = (key: string): string | null => {
      const raw = store.getUtf8(key);
      if (raw === null) return null;
      const { exp, v } = JSON.parse(raw) as { exp: number; v: string };
      if (Date.now() > exp) {
        store.delete(key); // lazy expire — ลบเมื่อเจอ
        return null;
      }
      return v;
    };

    cacheSet("api:/users", "[]", 50);   // TTL 50ms
    console.log("ทันที      :", cacheGet("api:/users")); // "[]"
    await new Promise((r) => setTimeout(r, 80));
    console.log("หลัง 80ms  :", cacheGet("api:/users")); // null (หมดอายุ)
    store.close();
  }

  // ────────────────────────────────────────────────────────────
  head("Counter — read-modify-write (อย่าลืม: writer เดียวต่อ directory)");
  {
    const store = new XdbStore(file("counter"));
    const incr = (key: string, by = 1): number => {
      const current = Number(store.getUtf8(key) ?? "0");
      const next = current + by;
      store.put([[key, String(next)]]);
      return next;
    };
    incr("views:page1"); incr("views:page1"); incr("views:page1", 10);
    console.log("views:page1 =", store.getUtf8("views:page1")); // "12"
    store.close();
    // หมายเหตุ: ถ้ามีหลาย thread/process ต้องเขียนพร้อมกัน → ใช้ xdb-server (เดียวเขียน)
    // หรือ queue การเขียนให้เป็นทางเดียว
  }

  // ────────────────────────────────────────────────────────────
  head("Session store — สร้าง/ใช้/ล้าง session หมดอายุทั้งก้อน");
  {
    const store = new XdbStore(file("sessions"), { sync: true });
    const now = Date.now();

    // สร้าง session หลายตัว (บางตัวหมดอายุแล้ว)
    store.put([
      ["sess:a1", JSON.stringify({ user: "alice", exp: now + 3600_000 })],
      ["sess:b2", JSON.stringify({ user: "bob", exp: now - 1000 })],   // หมดอายุ
      ["sess:c3", JSON.stringify({ user: "carol", exp: now - 2000 })], // หมดอายุ
    ]);

    // ล้าง session หมดอายุทั้งหมด: ไล่ prefix → เก็บ key ที่หมด → delete เป็น batch
    const expired: string[] = [];
    for (const { key, value } of store.prefix("sess:")) {
      const { exp } = JSON.parse(Buffer.from(value).toString("utf8")) as { exp: number };
      if (exp < now) expired.push(Buffer.from(key).toString("utf8"));
    }
    if (expired.length > 0) store.delete(expired); // ลบทีเดียวเป็น batch
    console.log("ลบ session หมดอายุ:", expired.length, "ตัว");

    // session ที่เหลือ
    const alive = [...store.prefix("sess:")].map(({ key }) => Buffer.from(key).toString());
    console.log("ที่ยังใช้ได้:", alive); // ["sess:a1"]
    store.close();
  }

  // ────────────────────────────────────────────────────────────
  head("Key design — ตัวเลขต้อง pad ให้กว้างคงที่ ไม่งั้นเรียงผิด");
  {
    // ✗ ผิด: "item:9" > "item:10" ในทาง string!
    // ✓ ถูก: pad ให้คงที่ → "item:0009" < "item:0010"
    writeTable(file("keydesign.xdb"), [
      ["item:0009", "a"],
      ["item:0010", "b"],
      ["item:0100", "c"],
    ]);
    const reader = new XdbReader(file("keydesign.xdb"));
    console.log("เรียงถูก:", [...reader].map(({ key }) => bytes(key)));

    // ตัวเลข binary: encode เป็น u64 big-endian จะเรียงตรงตาม string แบบ pad
    const u64be = (n: number): Uint8Array => {
      const b = new Uint8Array(8);
      new DataView(b.buffer).setBigUint64(0, BigInt(n), false);
      return b;
    };
    writeTable(file("u64keys.xdb"), [[u64be(300), "three-hundred"], [u64be(50), "fifty"]]);
    const r2 = new XdbReader(file("u64keys.xdb"));
    console.log("u64 keys เรียงตามค่า:", [...r2].map(({ value }) => bytes(value!)));
    // ["fifty", "three-hundred"] — 50 มาก่อน 300 ถูกต้อง
  }

  // ────────────────────────────────────────────────────────────
  head("Merge ตารางหลายไฟล์ — overlay ข้อมูลใหม่ทับเก่า (ตารางหลังสุดชนะ)");
  {
    // เคสจริง: มีตารางฐาน 1 ล้าน rows (สร้างนานครั้ง) + delta รายวันเล็ก ๆ
    writeTable(file("base.xdb"), [["price:a", "100"], ["price:b", "200"], ["price:c", "300"]]);
    writeTable(file("delta.xdb"), [["price:b", "250"]]); // b อัพเดตราคา

    // merge = ฐาน + delta → ไฟล์เดียว (key ซ้ำ delta ชนะ)
    const count = mergeTables([file("base.xdb"), file("delta.xdb")], file("merged.xdb"));
    const reader = new XdbReader(file("merged.xdb"));
    console.log("merged entries:", count);
    console.log("price:a =", reader.getUtf8("price:a")); // 100 (จาก base)
    console.log("price:b =", reader.getUtf8("price:b")); // 250 (delta ชนะ)
    console.log("price:c =", reader.getUtf8("price:c")); // 300
  }

  // ────────────────────────────────────────────────────────────
  head("ดูสถานะ store + compact มือ (ปกติ auto อยู่แล้ว)");
  {
    const store = new XdbStore(file("maintenance"), { flushEntries: 100, compactThreshold: 4 });
    for (let round = 0; round < 5; round++) {
      store.put(Array.from({ length: 60 }, (_, i) => [`r${round}:${i}`, String(i)] as [string, string]));
    }
    console.log("layers   :", store.layerCount);   // จำนวนไฟล์ layer
    console.log("memtable :", store.memtableLen);  // entries ที่ยังไม่ลงดิสก์เป็น layer
    store.flush();
    console.log("หลัง flush → layers:", store.layerCount, "memtable:", store.memtableLen);
    store.compact(); // รวทุก layer เป็นไฟล์เดียว (แบบ blocking — เหมาะตอน idle)
    console.log("หลัง compact → layers:", store.layerCount);
    // รอ background compaction ถ้ามี (ควรเช็คตอนจะปิดแอป)
    while (store.isCompacting) await new Promise((r) => setTimeout(r, 20));
    console.log("isCompacting:", store.isCompacting);
    store.close();
  }

  // ────────────────────────────────────────────────────────────
  head("Error handling — ไฟล์เสีย / เปิดซ้อน");
  {
    // ไฟล์เสียหาย: อ่านไม่ได้จะ throw (ไม่ crash) — ตรวจก่อนด้วย `xdb check` ก็ได้
    writeFileSync(file("broken.xdb"), "garbage-not-an-xdb-file");
    try {
      new XdbReader(file("broken.xdb"));
    } catch (e) {
      console.log("ไฟล์เสีย → throw:", (e as Error).message.slice(0, 40), "...");
    }

    // เปิด store เดียวกันซ้อน: ตัวที่สอง throw ทันที (file lock กันข้อมูลเสียหาย)
    const s1 = new XdbStore(file("locking"));
    try {
      new XdbStore(file("locking"));
    } catch (e) {
      console.log("เปิดซ้อน → throw:", (e as Error).message.includes("locked") ? "locked ✓" : e);
    }
    s1.close(); // ปลด lock แล้วเปิดใหม่ได้ทันที
    const s2 = new XdbStore(file("locking"));
    console.log("หลัง close → เปิดใหม่ได้ ✓");
    s2.close();
  }

  // ────────────────────────────────────────────────────────────
  head("ผูกกับ xdb-server (HTTP) — เมื่อหลาย process ต้องใช้ข้อมูลร่วมกัน");
  {
    console.log(`
เริ่ม server:  XDB_DATA_DIR=./data ./target/release/xdb-server
API:
  POST /api/build   { table, entries: [{key, valueB64}] }   สร้าง/ทับตาราง
  GET  /api/get?key=alice[&table=users]                      ค้นหา
  GET  /api/tables                                           รายชื่อตาราง
  POST /api/reload                                           โหลดไฟล์ใหม่

แพทเทิร์น: process เดียวเป็นเจ้าข้อมูล (server) — ที่เหลือต่อผ่าน HTTP
เหมาะเมื่อ: หลาย service / หลายเครื่องต้องอ่านข้อมูลชุดเดียวกัน`);
  }

  // ────────────────────────────────────────────────────────────
  head("Performance cheat sheet");
  {
    console.log(`
• เขียนเยอะ → รวมเป็น put() เดียวให้ใหญ่ที่สุด (batch 1000 keys = ~2-5µs/key)
• อ่านเร็วสุด → XdbReader บนตารางเดี่ยว (get ~0.7µs) แทนการไล่ layers ของ store
• ตารางนิ่งใหญ่ → writeTable ครั้งเดียว + ใช้ mergeTables ทำ "ฐาน + delta"
• ข้อมูลร้อนใน store → ปรับ flushEntries สูงขึ้น (ค่าเริ่ม 4096) ให้ memtable กินแรง
• ปิดแอปเนียน ๆ → store.flush() + รอ isCompacting เป็น false แล้วค่อย close()
• key เลข → pad คงที่ หรือ u64 big-endian เพื่อให้เรียงตามค่าตัวเลข
• ไฟล์ .xdb ย้ายได้ — copy = backup (store = copy ทั้ง directory หลัง close)`);
  }

  rmSync(workdir, { recursive: true, force: true });
  console.log("\n✓ cookbook จบ — ลบไฟล์ชั่วคราวแล้ว");
}

main().catch((e) => {
  console.error("cookbook failed:", e);
  process.exit(1);
});
