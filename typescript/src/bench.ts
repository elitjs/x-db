/**
 * Benchmark ของ XdbSingleFile (ไฟล์ .xdb เดียวจบ) เทียบ XdbStore ตรง ๆ
 *
 * รัน: npm run bench
 *
 * วัดอะไร:
 *   1. CRUD ราย operation (put/update/delete/get) — เทียบ sync/nosync และ XdbStore
 *   2. save() — ต้นทุนเฉพาะของ single-file (compact + แทนที่ไฟล์แบบ atomic)
 *   3. snapshot read — XdbReader เปิดไฟล์เดียวนั้น (ทางอ่านที่เร็วสุดของระบบนี้)
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { XdbReader, XdbSingleFile, XdbStore } from "./index.js";

const OP_N = 10_000;
const BATCH = 1_000;

const key = (i: number): string => `k:${String(i).padStart(7, "0")}`;
const val = (i: number): string => `value-of-${i}-xxxxxxxxxxxxxxxxxxxx`;

function bench(label: string, ops: number, fn: () => void): number {
  const t0 = performance.now();
  fn();
  const nsPerOp = ((performance.now() - t0) * 1e6) / ops;
  console.log(`  ${label.padEnd(34)} ${nsPerOp.toFixed(0).padStart(9)} ns/op`);
  return nsPerOp;
}

function crud(label: string, mk: (dir: string) => { db: XdbSingleFile | XdbStore; save?: () => void }): void {
  const dir = mkdtempSync(join(tmpdir(), "xdb-bench-single-"));
  const { db } = mk(dir);

  // preload base
  const base: Array<[string, string]> = [];
  for (let i = 0; i < OP_N; i++) base.push([key(i), val(i)]);
  db.put(base);

  bench("put เดี่ยว (insert)", OP_N, () => {
    for (let i = 0; i < OP_N; i++) db.put([[`w:${i}`, val(i)]]);
  });
  bench("put batch 1000 (ns/key)", OP_N, () => {
    for (let b = 0; b < OP_N / BATCH; b++) {
      const rows: Array<[string, string]> = [];
      for (let j = 0; j < BATCH; j++) rows.push([`b:${b}:${j}`, val(j)]);
      db.put(rows);
    }
  });
  bench("update (overwrite)", OP_N, () => {
    for (let i = 0; i < OP_N; i++) db.put([[key(i), `updated-${i}`]]);
  });
  let found = 0;
  bench("get เจอ", OP_N, () => {
    for (let i = 0; i < OP_N; i++) if (db.getUtf8(key((i * 7919) % OP_N)) !== null) found++;
  });
  let rejected = 0;
  bench("get ไม่เจอ", OP_N, () => {
    for (let i = 0; i < OP_N; i++) if (db.getUtf8(`zzz:${i}`) === null) rejected++;
  });
  bench("delete", OP_N, () => {
    for (let i = 0; i < OP_N; i++) db.delete(key(i));
  });

  // correctness ก่อนจบ
  if (found !== OP_N || rejected !== OP_N) throw new Error("correctness failed");
  console.log(`  ✓ correctness ผ่าน (get ${found}/${OP_N}, miss ${rejected}/${OP_N})`);

  db.close();
  rmSync(dir, { recursive: true, force: true });
}

function main(): void {
  console.log(`=== XdbSingleFile benchmark — ${OP_N} ops/phase, Node ${process.version} ===\n`);

  console.log("── [1] XdbSingleFile (sync: true) ──");
  crud("single(sync)", (dir) => ({ db: new XdbSingleFile(join(dir, "app.xdb")) }));

  console.log("\n── [2] XdbSingleFile (sync: false) ──");
  crud("single(nosync)", (dir) => ({ db: new XdbSingleFile(join(dir, "app.xdb"), { sync: false }) }));

  console.log("\n── [3] XdbStore (sync: false) — เทียบ overhead ตรง ๆ (ไม่มี save) ──");
  crud("store(nosync)", (dir) => ({ db: new XdbStore(dir, { sync: false }) }));

  // ────────────────────────────────────────────────
  console.log("\n── [4] save() — ต้นทุนเฉพาะของ single-file (compact + atomic replace) ──");
  for (const n of [1_000, 10_000, 50_000]) {
    const dir = mkdtempSync(join(tmpdir(), "xdb-bench-save-"));
    const file = join(dir, "app.xdb");
    const db = new XdbSingleFile(file, { sync: false });
    const rows: Array<[string, string]> = [];
    for (let i = 0; i < n; i++) rows.push([key(i), val(i)]);
    db.put(rows);

    const t0 = performance.now();
    db.save();
    const ms = performance.now() - t0;
    console.log(`  save() กับข้อมูล ${String(n).padStart(6)} entries   ${ms.toFixed(2).padStart(9)} ms`);

    // ไฟล์ที่ออกมาต้องถูกต้อง
    const r = new XdbReader(file);
    if (r.getUtf8(key(0)) !== val(0) || r.getUtf8(key(n - 1)) !== val(n - 1)) throw new Error("save output corrupt");
    db.close();
    rmSync(dir, { recursive: true, force: true });
  }

  // ────────────────────────────────────────────────
  console.log("\n── [5] snapshot read — XdbReader บนไฟล์เดียวที่ save() ออกมา (50k entries) ──");
  {
    const dir = mkdtempSync(join(tmpdir(), "xdb-bench-snap-"));
    const file = join(dir, "app.xdb");
    const db = new XdbSingleFile(file, { sync: false });
    const rows: Array<[string, string]> = [];
    for (let i = 0; i < 50_000; i++) rows.push([key(i), val(i)]);
    db.put(rows);
    db.save();
    db.close();

    // warm + วัด
    const reader = new XdbReader(file);
    let found = 0;
    bench("get เจอ (ไฟล์เดียว, mmap)", 50_000, () => {
      for (let i = 0; i < 50_000; i++) if (reader.getUtf8(key((i * 7919) % 50_000)) !== null) found++;
    });
    let rejected = 0;
    bench("get ไม่เจอ (bloom ตัด)", 50_000, () => {
      for (let i = 0; i < 50_000; i++) if (reader.getUtf8(`zzz:${i}`) === null) rejected++;
    });
    bench("prefix scan 1,000 keys", 1_000, () => {
      let n = 0;
      for (const _e of reader.range(key(10_000), key(11_000))) n++;
      if (n !== 1_000) throw new Error("range wrong");
    });
    if (found !== 50_000 || rejected !== 50_000) throw new Error("correctness failed");
    console.log("  ✓ correctness ผ่าน");
    rmSync(dir, { recursive: true, force: true });
  }

  console.log("\nจบ ✓ (ทุก phase ตรวจ correctness ก่อนผ่าน)");
}

main();
