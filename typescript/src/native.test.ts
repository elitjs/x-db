/**
 * Tests สำหรับ native binding (เรียก Rust ตรง ๆ ผ่าน napi — ไม่มี server)
 */
import { test } from "node:test";
import assert from "node:assert/strict";
import { closeSync, existsSync, openSync, statSync, writeFileSync, writeSync } from "node:fs";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { mergeTables, writeTable, XdbReader, XdbStore } from "./index.js";

function tempTable(name: string): string {
  return join(mkdtempSync(join(tmpdir(), "xdb-native-")), `${name}.xdb`);
}

test("write creates file on disk", () => {
  const path = tempTable("create");
  writeTable(path, [["a", "1"]]);
  assert.ok(existsSync(path));
  assert.ok(statSync(path).size > 64); // header + bloom + index + footer
});

test("string round-trip (input need not be sorted)", () => {
  const path = tempTable("strings");
  writeTable(path, [
    ["carol", "manager"],
    ["alice", "engineer"],
    ["bob", "designer"],
  ]);
  const reader = new XdbReader(path);
  assert.equal(reader.getUtf8("alice"), "engineer");
  assert.equal(reader.getUtf8("bob"), "designer");
  assert.equal(reader.getUtf8("carol"), "manager");
});

test("duplicate keys: last write wins", () => {
  const path = tempTable("dupes");
  writeTable(path, [
    ["k", "old"],
    ["a", "1"],
    ["k", "new"],
  ]);
  assert.equal(new XdbReader(path).getUtf8("k"), "new");
});

test("binary keys and values", () => {
  const path = tempTable("binary");
  const key = new Uint8Array([0x00, 0xff, 0x80, 0x01]);
  const val = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);

  writeTable(path, [[key, val]]);
  const reader = new XdbReader(path);

  assert.deepEqual(reader.get(key), val);
  assert.equal(reader.getUtf8(key), null); // ค่าไม่ใช่ valid UTF-8
});

test("missing key → null / has=false", () => {
  const path = tempTable("missing");
  writeTable(path, [["alice", "1"]]);
  const reader = new XdbReader(path);

  assert.equal(reader.get("nope"), null);
  assert.equal(reader.getUtf8("nope"), null);
  assert.equal(reader.has("nope"), false);
  assert.equal(reader.has("alice"), true);
  assert.equal(reader.get("alic"), null); // prefix ต้องไม่ match
});

test("unicode keys and values", () => {
  const path = tempTable("unicode");
  writeTable(path, [["สวัสดี", "ครับ"], ["日本語", "こんにちは"]]);
  const reader = new XdbReader(path);

  assert.equal(reader.getUtf8("สวัสดี"), "ครับ");
  assert.equal(reader.getUtf8("日本語"), "こんにちは");
});

test("large table round-trip (multi-block, 2000 entries)", () => {
  const path = tempTable("big");
  const entries: Array<[string, string]> = [];
  for (let i = 0; i < 2000; i++) {
    entries.push([`key:${String(i).padStart(8, "0")}`, `value-${i}-`.repeat(20)]);
  }
  writeTable(path, entries);

  const reader = new XdbReader(path);
  assert.equal(reader.getUtf8("key:00000000"), "value-0-".repeat(20));
  assert.equal(reader.getUtf8("key:00001999"), "value-1999-".repeat(20));
  assert.equal(reader.getUtf8("key:00002000"), null);
});

test("Map / Object / mixed entry forms", () => {
  const path = tempTable("forms");

  writeTable(path, new Map([["from-map", "1"]]));
  writeTable(path, { "from-object": "2" });
  writeTable(path, [{ key: "from-entry", value: "3" }]);

  const reader = new XdbReader(path);
  assert.equal(reader.getUtf8("from-entry"), "3");
  assert.equal(reader.getUtf8("from-map"), null); // เขียนทับไปแล้ว
  assert.equal(reader.getUtf8("from-object"), null);
});

test("opening invalid file throws", () => {
  const notXdb = join(mkdtempSync(join(tmpdir(), "xdb-native-")), "bad.xdb");
  writeFileSync(notXdb, "garbage data — definitely not an xdb file");
  assert.throws(() => new XdbReader(notXdb));
});

test("count property reflects entry count", () => {
  const path = tempTable("count");
  writeTable(path, [["a", "1"], ["b", "2"], ["c", "3"]]);
  assert.equal(new XdbReader(path).count, 3);

  const empty = tempTable("count_empty");
  writeTable(empty, []);
  assert.equal(new XdbReader(empty).count, 0);
});

test("iter visits all entries in key order (for...of)", () => {
  const path = tempTable("iter");
  writeTable(path, [
    ["m", "2"],
    ["z", "3"],
    ["a", "1"],
    ["b", "1.5"],
  ]);

  const reader = new XdbReader(path);
  const keys: string[] = [];
  for (const { key, value } of reader) {
    keys.push(Buffer.from(key).toString("utf8"));
    assert.ok(value !== null && value.length > 0);
  }
  assert.deepEqual(keys, ["a", "b", "m", "z"]);
  assert.equal(reader.count, 4);
});

test("iter works across multiple blocks", () => {
  const path = tempTable("iter_big");
  const entries: Array<[string, string]> = [];
  for (let i = 0; i < 3000; i++) {
    entries.push([`key:${String(i).padStart(6, "0")}`, `v${i}-`.repeat(20)]);
  }
  writeTable(path, entries);

  const reader = new XdbReader(path);
  assert.ok(reader.blockCount > 1);
  let count = 0;
  let lastKey = "";
  for (const { key } of reader.iter()) {
    const k = Buffer.from(key).toString("utf8");
    assert.ok(k > lastKey, "iteration must be in ascending key order");
    lastKey = k;
    count++;
  }
  assert.equal(count, 3000);
});

test("prefix scan via iterator filter", () => {
  const path = tempTable("prefix");
  writeTable(path, [
    ["user:1", "a"],
    ["user:2", "b"],
    ["user:10", "c"],
    ["admin:1", "d"],
  ]);

  const reader = new XdbReader(path);
  const users: string[] = [];
  for (const { key } of reader) {
    const k = Buffer.from(key).toString("utf8");
    if (k > "user:") {
      if (!k.startsWith("user:")) break;
      users.push(k);
    }
  }
  assert.deepEqual(users, ["user:1", "user:10", "user:2"]);
});

test("corrupted block data makes get() throw (not crash)", () => {
  const path = tempTable("corrupt");
  const entries: Array<[string, string]> = [];
  for (let i = 0; i < 500; i++) entries.push([`key:${String(i).padStart(6, "0")}`, "x".repeat(100)]);
  writeTable(path, entries);

  // flip ไบต์ใน block แรก (header 32B + offset 10)
  const fh = openSync(path, "r+");
  writeSync(fh, Buffer.from([0xFF]), 0, 1, 42);
  closeSync(fh);

  const reader = new XdbReader(path); // footer/index ยังดี → เปิดได้
  assert.throws(() => reader.get("key:000010")); // CRC mismatch → throw
});

test("mergeTables: last table wins, output is valid", () => {
  const dir = mkdtempSync(join(tmpdir(), "xdb-native-"));
  const t1 = join(dir, "t1.xdb");
  const t2 = join(dir, "t2.xdb");
  const out = join(dir, "merged.xdb");

  writeTable(t1, [["a", "old-a"], ["b", "old-b"], ["c", "c1"]]);
  writeTable(t2, [["a", "new-a"], ["d", "d2"]]);

  const count = mergeTables([t1, t2], out);
  assert.equal(count, 4);

  const reader = new XdbReader(out);
  assert.equal(reader.count, 4);
  assert.equal(reader.getUtf8("a"), "new-a"); // จาก t2
  assert.equal(reader.getUtf8("b"), "old-b");
  assert.equal(reader.getUtf8("c"), "c1");
  assert.equal(reader.getUtf8("d"), "d2");

  // เรียงถูกต้อง
  const keys = [...reader].map(({ key }) => Buffer.from(key).toString("utf8"));
  assert.deepEqual(keys, ["a", "b", "c", "d"]);
});

test("mergeTables: multi-block tables merge correctly", () => {
  const dir = mkdtempSync(join(tmpdir(), "xdb-native-"));
  const t1 = join(dir, "even.xdb");
  const t2 = join(dir, "odd.xdb");
  const out = join(dir, "merged.xdb");

  const evens: Array<[string, string]> = [];
  const odds: Array<[string, string]> = [];
  for (let i = 0; i < 1000; i++) {
    const entry: [string, string] = [`key:${String(i).padStart(6, "0")}`, `v${i}-`.repeat(30)];
    (i % 2 === 0 ? evens : odds).push(entry);
  }
  writeTable(t1, evens);
  writeTable(t2, odds);

  const count = mergeTables([t1, t2], out);
  assert.equal(count, 1000);

  const reader = new XdbReader(out);
  assert.ok(reader.blockCount > 1);
  assert.equal(reader.getUtf8("key:000000"), "v0-".repeat(30));
  assert.equal(reader.getUtf8("key:000999"), "v999-".repeat(30));
  assert.equal(reader.getUtf8("key:001000"), null);
});

test("mergeTables: output == input is rejected", () => {
  const dir = mkdtempSync(join(tmpdir(), "xdb-native-"));
  const t = join(dir, "t.xdb");
  writeTable(t, [["a", "1"]]);
  assert.throws(() => mergeTables([t], t));
});

// ---------------- XdbStore: realtime updates ----------------

import { rmSync } from "node:fs";

function tempStore(name: string): string {
  const dir = join(tmpdir(), `xdb-store-${name}-${Date.now()}`);
  rmSync(dir, { recursive: true, force: true });
  return dir;
}

test("store: put → get → update → get", () => {
  const store = new XdbStore(tempStore("basic"));
  store.put([["alice", "1"], ["bob", "2"]]);
  assert.equal(store.getUtf8("alice"), "1");

  store.put([["alice", "999"]]); // update ทันที
  assert.equal(store.getUtf8("alice"), "999");
  assert.equal(store.getUtf8("bob"), "2");
  assert.equal(store.getUtf8("nope"), null);
});

test("store: delete แล้วใส่คืนได้", () => {
  const store = new XdbStore(tempStore("delete"));
  store.put([["a", "1"], ["b", "2"], ["c", "3"]]);
  store.delete("b");

  assert.equal(store.getUtf8("b"), null);
  assert.equal(store.has("b"), false);
  assert.equal(store.getUtf8("a"), "1");

  store.put([["b", "revived"]]);
  assert.equal(store.getUtf8("b"), "revived");
});

test("store: realtime loop 100 puts อ่านกลับทันที", () => {
  const store = new XdbStore(tempStore("realtime"));
  for (let i = 0; i < 100; i++) {
    store.put([[`counter:${i % 10}`, `v${i}`]]);
    assert.equal(store.getUtf8(`counter:${i % 10}`), `v${i}`); // เห็นค่าล่าสุดทันที
  }
  for (let i = 0; i < 10; i++) {
    assert.equal(store.getUtf8(`counter:${i}`), `v${90 + i}`);
  }
});

test("store: auto-compact ที่ 8 layers", async () => {
  // flushEntries: 1 → ทุก put กลายเป็น layer ทันที (จะได้เห็น threshold ทำงาน)
  const store = new XdbStore(tempStore("autocompact"), { compactThreshold: 8, flushEntries: 1 });
  for (let i = 0; i < 7; i++) {
    store.put([[`k${i}`, `v${i}`]]);
  }
  assert.equal(store.layerCount, 7);
  store.put([["k7", "v7"]]); // ตัวที่ 8 → compact (background)

  // รอ background compaction จบ
  const deadline = Date.now() + 10_000;
  while ((store.layerCount as number) !== 1) {
    assert.ok(Date.now() < deadline, "background compaction ไม่จบ");
    await new Promise((r) => setTimeout(r, 10));
  }
  for (let i = 0; i < 8; i++) {
    assert.equal(store.getUtf8(`k${i}`), `v${i}`); // ข้อมูลครบหลัง compact
  }
});

test("store: persistence ข้ามการเปิดใหม่", () => {
  const dir = tempStore("persist");
  {
    const store = new XdbStore(dir);
    store.put([["k1", "v1"]]);
    store.put([["k2", "v2"]]);
    store.delete("k1");
    store.close(); // ปลด lock ให้เปิดใหม่ได้ทันที
  }
  const store = new XdbStore(dir); // เปิดใหม่
  assert.equal(store.getUtf8("k1"), null); // ยังถูกลบอยู่
  assert.equal(store.getUtf8("k2"), "v2");
});

test("store: binary keys/values", () => {
  const store = new XdbStore(tempStore("binary"));
  const key = new Uint8Array([0, 255, 128]);
  const val = new Uint8Array([9, 8, 7]);

  store.put([[key, val]]);
  assert.deepEqual(store.get(key), val);

  store.delete(key);
  assert.equal(store.get(key), null);
});

// ---------------- seek / range / prefix ----------------


test("reader: seek ไปที่กลางตารางได้ถูกต้อง", () => {
  const path = tempTable("seek");
  const entries: Array<[string, string]> = [];
  for (let i = 0; i < 2000; i++) entries.push([`key:${String(i).padStart(6, "0")}`, `v${i}`]);
  writeTable(path, entries);

  const reader = new XdbReader(path);
  const start = "key:000700";
  const keys = [...reader.seek(start)].map(({ key }) => Buffer.from(key).toString("utf8"));
  assert.equal(keys[0], start);
  assert.equal(keys.length, 1300); // 2000 - 700

  assert.equal([...reader.seek("aaa")].length, 2000); // ก่อนตาราง = ทั้งหมด
  assert.equal([...reader.seek("zzz")].length, 0); // หลังตาราง = ว่าง
});

test("reader: range และ prefix", () => {
  const path = tempTable("rp");
  const entries: Array<[string, string]> = [];
  for (let i = 0; i < 300; i++) {
    entries.push([`user:${i % 3}:${String(i).padStart(4, "0")}`, `v${i}`]);
  }
  writeTable(path, entries); // Rust จะเรียงให้

  const reader = new XdbReader(path);

  // range [user:1:0100, user:1:0200) — i%3==1 ใน [100,200) = 33 ตัว
  assert.equal([...reader.range("user:1:0100", "user:1:0200")].length, 34);

  // prefix user:2: — i%3==2 ใน 0..300 = 100 ตัว
  assert.equal([...reader.prefix("user:2:")].length, 100);

  // range ว่าง
  assert.equal([...reader.range("user:1:0100", "user:1:0100")].length, 0);
});

test("store: range / prefix เห็นข้อมูลใหม่สุดและตัดที่ถูกลบ", () => {
  const store = new XdbStore(tempStore("rp"), { compactThreshold: 0 });
  store.put([["a", "1"], ["b", "2"], ["c", "3"], ["d", "4"]]);
  store.put([["c", "3-new"]]);
  store.delete("d");

  const view = [...store.range("b", "zzz")].map(
    ({ key, value }) => [Buffer.from(key).toString(), Buffer.from(value).toString()] as const
  );
  assert.deepEqual(view, [["b", "2"], ["c", "3-new"]]);

  const afterC = [...store.seek("c")].map(({ key }) => Buffer.from(key).toString());
  assert.deepEqual(afterC, ["c"]); // d ถูกลบ
});

test("store: เปิดซ้อน directory เดียวกันต้อง throw (file lock)", () => {
  const dir = tempStore("lock");
  const store = new XdbStore(dir);
  store.put([["a", "1"]]);

  assert.throws(() => new XdbStore(dir), /locked/);

  store.close(); // ปลดล็อก
  const reopened = new XdbStore(dir); // ต้องเปิดได้ทันที
  assert.equal(reopened.getUtf8("a"), "1");
  reopened.close();
});

// ---------------- WAL + memtable ----------------

test("store: put เข้า memtable → get เห็นทันที → flush → เป็น layer", () => {
  const store = new XdbStore(tempStore("wal_basic"));
  store.put([["a", "1"], ["b", "2"]]);

  assert.equal(store.memtableLen, 2);
  assert.equal(store.layerCount, 0);
  assert.equal(store.getUtf8("a"), "1");

  store.flush();
  assert.equal(store.memtableLen, 0);
  assert.equal(store.layerCount, 1);
  assert.equal(store.getUtf8("a"), "1"); // ข้อมูลยังอยู่จาก layer
  store.close();
});

test("store: WAL replay — เปิดใหม่โดยไม่ flush ข้อมูลไม่หาย", () => {
  const dir = tempStore("wal_replay");
  {
    const store = new XdbStore(dir);
    store.put([["a", "1"], ["b", "2"]]);
    store.delete("b");
    store.close(); // ไม่ได้ flush — ปล่อยให้ WAL ทำหน้าที่
  }
  const store = new XdbStore(dir);
  assert.equal(store.memtableLen, 2); // replay รวมเป็น {a, b:tombstone}
  assert.equal(store.getUtf8("a"), "1");
  assert.equal(store.getUtf8("b"), null); // ยังถูกลบอยู่
  store.close();
});

test("store: batch put 1000 keys ในคำสั่งเดียว", () => {
  const store = new XdbStore(tempStore("wal_batch"));
  const entries: Array<[string, string]> = [];
  for (let i = 0; i < 1000; i++) entries.push([`k:${String(i).padStart(5, "0")}`, `v${i}`]);

  const t0 = performance.now();
  store.put(entries);
  const ms = performance.now() - t0;
  console.log(`       batch put 1000 keys: ${ms.toFixed(2)} ms (${(ms * 1000 / 1000).toFixed(1)} µs/key)`);

  assert.equal(store.memtableLen, 1000);
  assert.equal(store.getUtf8("k:00999"), "v999");
  store.close();
});

test("store: sync=false ใช้ได้ปกติ", () => {
  const dir = tempStore("nosync");
  {
    const store = new XdbStore(dir, { sync: false });
    store.put([["fast", "1"]]);
    assert.equal(store.getUtf8("fast"), "1");
    store.close();
  }
  const store = new XdbStore(dir);
  assert.equal(store.getUtf8("fast"), "1");
  store.close();
});
