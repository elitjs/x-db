export { Mongo, Collection, FindCursor } from "./mongo.js";
export type { MongoDoc, Filter, UpdateSpec, SortSpec, FieldOps } from "./mongo.js";

/**
 * x-db native binding สำหรับ TypeScript/Node.js
 *
 * โหลด Rust addon (.node) ที่ build ด้วย `npm run native:build` —
 * เรียกใช้โดยตรงใน process เดียวกัน ไม่ผ่าน HTTP
 */
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// dist/index.js -> package root/xdb-native.cjs
const native = require("../xdb-native.cjs") as NativeApi;

export type KeyValue = string | Uint8Array;

export interface Entry {
  key: KeyValue;
  value: KeyValue;
}

export type BuildInput =
  | Map<KeyValue, KeyValue>
  | Array<[KeyValue, KeyValue]>
  | Array<Entry>
  | Record<string, KeyValue>;

// ---- types ของ native addon (ดู xdb-native.d.ts ที่ generate ไว้ด้วยก็ได้) ----

interface NativeIterEntry {
  key: Buffer;
  value: Buffer | null; // null = tombstone (คีย์ถูกลบ)
}

interface NativeIterator {
  next(): NativeIterEntry | null;
}

interface NativeReader {
  readonly len: number;
  readonly blockCount: number;
  get(key: string | Uint8Array): Buffer | null;
  getUtf8(key: string | Uint8Array): string | null;
  has(key: string | Uint8Array): boolean;
  iter(start?: string | Uint8Array): NativeIterator;
}

interface NativeStoreEntry {
  key: Buffer;
  value: Buffer;
}

interface NativeStoreIterator {
  next(): NativeStoreEntry | null;
}

interface NativeStore {
  close(): void;
  readonly layerCount: number;
  readonly memtableLen: number;
  readonly isCompacting: boolean;
  flush(): void;
  put(entries: Entry[]): void;
  delete(keys: Array<string | Uint8Array>): void;
  get(key: string | Uint8Array): Buffer | null;
  getUtf8(key: string | Uint8Array): string | null;
  has(key: string | Uint8Array): boolean;
  compact(): number;
  iter(start?: string | Uint8Array): NativeStoreIterator;
}

export interface XdbStoreOptions {
  /** จำนวน layers ที่ trigger compact อัตโนมัติ (default 8, 0 = ปิด — เรียก compact() เอง) */
  compactThreshold?: number;
  /** จำนวน entries ใน memtable ที่ trigger flush เป็น layer (default 4096, 0 = flush เองเท่านั้น) */
  flushEntries?: number;
  /** fsync WAL ทุก put (default true) — false = เร็วขึ้นแต่ process พังกลางทางอาจเสีย put ล่าสุด */
  sync?: boolean;
  /** (เมื่อ sync=false) fsync WAL เป็นระยะทุก N ms → เสียข้อมูลตอนไฟดับได้สูงสุดแค่ N ms (0 = ปิด) */
  syncIntervalMs?: number;
}

interface NativeApi {
  writeTable(path: string, entries: Entry[]): void;
  /** รวมหลายไฟล์ .xdb เป็นไฟล์เดียว — ไฟล์หลังสุดชนะเมื่อ key ซ้ำ คืนจำนวน entries ผลลัพธ์ */
  mergeTables(inputs: string[], output: string): number;
  XdbReader: new (path: string) => NativeReader;
  XdbStore: new (
    path: string,
    options?: { compactThreshold?: number; flushEntries?: number; sync?: boolean; syncIntervalMs?: number },
  ) => NativeStore;
}

// ---- helpers ----

function toBuffer(v: KeyValue): Buffer {
  return typeof v === "string" ? Buffer.from(v, "utf8") : Buffer.from(v);
}

function normalize(input: BuildInput): Entry[] {
  const list: Entry[] = [];
  const push = (k: KeyValue, v: KeyValue) => list.push({ key: k, value: v });

  if (input instanceof Map) {
    for (const [k, v] of input) push(k, v);
  } else if (Array.isArray(input)) {
    for (const e of input) {
      if (Array.isArray(e)) push(e[0], e[1]);
      else push(e.key, e.value);
    }
  } else {
    for (const [k, v] of Object.entries(input)) push(k, v);
  }
  return list;
}

// ---- public API ----

/**
 * สร้างไฟล์ .xdb จาก entries (รับได้ทั้ง Array, Map, Object)
 * Rust จะจัดเรียง key และกรองตัวซ้ำให้เอง — ตัวหลังสุดชนะ
 */
export function writeTable(path: string, entries: BuildInput): void {
  native.writeTable(path, normalize(entries));
}

/**
 * รวมหลายไฟล์ .xdb เป็นไฟล์เดียว (compaction) — streaming ไม่กิน RAM แม้ตารางใหญ่
 * key ซ้ำกัน: ไฟล์ที่อยู่หลังสุดใน `inputs` ชนะ
 * คืนจำนวน entries ในตารางผลลัพธ์
 */
export function mergeTables(inputs: string[], output: string): number {
  return native.mergeTables(inputs, output);
}

export interface IterEntry {
  key: Uint8Array;
  /** null = tombstone (คีย์ถูกลบ) — เกิดเฉพาะเมื่อไล่ตารางที่มี tombstone ตรง ๆ */
  value: Uint8Array | null;
}

/** ไล่ entries เรียงตาม key — ใช้ for...of ได้เลย */
export class XdbIterator implements IterableIterator<IterEntry> {
  readonly #inner: NativeIterator;
  #bound: IterBound | null;

  constructor(inner: NativeIterator, bound: IterBound | null = null) {
    this.#inner = inner;
    this.#bound = bound;
  }

  next(): IteratorResult<IterEntry> {
    const entry = this.#inner.next();
    if (entry === null) return { value: undefined, done: true };
    const key = new Uint8Array(entry.key);
    // ตรวจเงื่อนไขหยุดของ range/prefix — keys เรียงอยู่แล้ว เกินขอบ = จบ
    if (this.#bound) {
      const { kind, bytes } = this.#bound;
      if (kind === "end" ? compareBytes(key, bytes) >= 0 : !startsWith(key, bytes)) {
        return { value: undefined, done: true };
      }
    }
    return {
      value: { key, value: entry.value === null ? null : new Uint8Array(entry.value) },
      done: false,
    };
  }

  [Symbol.iterator](): IterableIterator<IterEntry> {
    return this;
  }
}

/** Reader แบบ mmap — เปิดครั้งเดียว แล้ว lookup ได้เร็วมาก (bloom filter + binary search) */
export class XdbReader {
  readonly #inner: NativeReader;

  constructor(path: string) {
    this.#inner = new native.XdbReader(path);
  }

  /** จำนวน entries ทั้งหมดในตาราง */
  get count(): number {
    return this.#inner.len;
  }

  /** จำนวน blocks (64KB ต่อ block โดยประมาณ) */
  get blockCount(): number {
    return this.#inner.blockCount;
  }

  /** คืนค่าเป็น Uint8Array หรือ null ถ้าไม่พบ (ไฟล์เสียหายจะ throw) */
  get(key: KeyValue): Uint8Array | null {
    const buf = this.#inner.get(toBuffer(key));
    return buf === null ? null : new Uint8Array(buf);
  }

  /** คืนค่าเป็น UTF-8 string หรือ null */
  getUtf8(key: KeyValue): string | null {
    return this.#inner.getUtf8(toBuffer(key));
  }

  has(key: KeyValue): boolean {
    return this.#inner.has(toBuffer(key));
  }

  /** iterator เรียงตาม key ทั้งตาราง */
  iter(): XdbIterator {
    return new XdbIterator(this.#inner.iter());
  }

  /** iterator เริ่มที่ entry แรกที่ key >= start — เร็วกว่าไล่+filter เพราะข้ามไป block ตรง ๆ */
  seek(start: KeyValue): XdbIterator {
    return new XdbIterator(this.#inner.iter(toBuffer(start)));
  }

  /** ไล่ keys ในช่วง [start, end) — end exclusive */
  range(start: KeyValue, end: KeyValue): XdbIterator {
    return new XdbIterator(this.#inner.iter(toBuffer(start)), {
      kind: "end",
      bytes: bytesOf(end),
    });
  }

  /** ไล่ทุก entries ที่ key ขึ้นต้นด้วย prefix */
  prefix(p: KeyValue): XdbIterator {
    return new XdbIterator(this.#inner.iter(toBuffer(p)), {
      kind: "prefix",
      bytes: bytesOf(p),
    });
  }

  /** ทำให้ instance ใช้กับ for...of ได้ตรง ๆ */
  *[Symbol.iterator](): IterableIterator<IterEntry> {
    yield* this.iter();
  }
}

// ---------------- XdbStore: realtime updates ----------------

/**
 * Store แบบ layered (LSM-lite) สำหรับแอปที่ update แบบ realtime
 *
 * - `put`/`delete` = เขียน layer เล็กใหม่ (เร็ว ระดับ ms — เหมาะกับ UI ที่ต้องการ feedback ทันที)
 * - `get` = ค้นจาก layer ใหม่ → เก่า (bloom filter ทำให้ miss ถูกมาก)
 * - auto-compact: รวมทุก layers เป็นไฟล์เดียวเมื่อสะสมถึง 8 layers
 */
export class XdbStore {
  readonly #inner: NativeStore;

  constructor(path: string, options: XdbStoreOptions = {}) {
    this.#inner = new native.XdbStore(path, options);
  }

  /** ปิด store + ปลด lock ของ directory — เรียกเมื่อใช้เสร็จ (ไม่เรียกก็ได้ จะปลดตอน GC) */
  close(): void {
    this.#inner.close();
  }

  /** จำนวน layers ปัจจุบัน (ครบ 8 จะถูก compact อัตโนมัติเหลือ 1) */
  get layerCount(): number {
    return this.#inner.layerCount;
  }

  /** จำนวน entries ใน memtable ที่ยังไม่ได้ flush เป็น layer */
  get memtableLen(): number {
    return this.#inner.memtableLen;
  }

  /** ดัน memtable ลง layer ถาวร + ล้าง WAL (ปกติ auto ตาม flushEntries อยู่แล้ว) */
  flush(): void {
    this.#inner.flush();
  }

  /** มี background compaction กำลังรวอยู่หรือไม่ (เช็คได้ตอนจะปิดแอป) */
  get isCompacting(): boolean {
    return this.#inner.isCompacting;
  }

  /** เพิ่ม/แก้ค่า (upsert) — รับ Array / Map / Object, ไม่เรียงก็ได้, key ซ้ำตัวหลังชนะ */
  put(entries: BuildInput): void {
    this.#inner.put(normalize(entries));
  }

  /** ลบ keys (เขียน tombstone กดค่าเก่า) */
  delete(keys: KeyValue | KeyValue[]): void {
    const list = Array.isArray(keys) ? keys : [keys];
    this.#inner.delete(list.map((k) => (typeof k === "string" ? k : Buffer.from(k))));
  }

  /** คืนค่าเป็น Uint8Array หรือ null (ไม่มี หรือ ถูกลบ) */
  get(key: KeyValue): Uint8Array | null {
    const buf = this.#inner.get(toBuffer(key));
    return buf === null ? null : new Uint8Array(buf);
  }

  /** คืนค่าเป็น UTF-8 string หรือ null */
  getUtf8(key: KeyValue): string | null {
    return this.#inner.getUtf8(toBuffer(key));
  }

  has(key: KeyValue): boolean {
    return this.#inner.has(toBuffer(key));
  }

  /** รวมทุก layers เป็นไฟล์เดียวเอง (ปกติไม่ต้องเรียก — auto อยู่แล้ว) */
  compact(): number {
    return this.#inner.compact();
  }

  /** iterator มุมมองรวมทั้ง store — เรียงตาม key, ตัดที่ถูกลบ */
  iter(): XdbStoreIterator {
    return new XdbStoreIterator(this.#inner.iter());
  }

  /** iterator เริ่มที่ entry แรกที่ key >= start */
  seek(start: KeyValue): XdbStoreIterator {
    return new XdbStoreIterator(this.#inner.iter(toBuffer(start)));
  }

  /** ไล่ keys ในช่วง [start, end) — end exclusive */
  range(start: KeyValue, end: KeyValue): XdbStoreIterator {
    return new XdbStoreIterator(this.#inner.iter(toBuffer(start)), {
      kind: "end",
      bytes: bytesOf(end),
    });
  }

  /** ไล่ทุก entries ที่ key ขึ้นต้นด้วย prefix */
  prefix(p: KeyValue): XdbStoreIterator {
    return new XdbStoreIterator(this.#inner.iter(toBuffer(p)), {
      kind: "prefix",
      bytes: bytesOf(p),
    });
  }
}

// ---------------- seek / range / prefix ----------------

/** เงื่อนไขหยุดของ iterator ที่ถูก bound (range/prefix) */
type IterBound =
  | { kind: "end"; bytes: Uint8Array }
  | { kind: "prefix"; bytes: Uint8Array };

function bytesOf(v: KeyValue): Uint8Array {
  return typeof v === "string" ? new TextEncoder().encode(v) : v;
}

function compareBytes(a: Uint8Array, b: Uint8Array): number {
  return Buffer.compare(Buffer.from(a), Buffer.from(b));
}

function startsWith(haystack: Uint8Array, prefix: Uint8Array): boolean {
  if (haystack.length < prefix.length) return false;
  for (let i = 0; i < prefix.length; i++) {
    if (haystack[i] !== prefix[i]) return false;
  }
  return true;
}

/** entry จาก store iterator (มุมมองรวม — ตัดที่ถูกลบแล้ว) */
export interface StoreEntry {
  key: Uint8Array;
  value: Uint8Array;
}

/** ไล่มุมมองรวมของ store — เรียงตาม key, layer ใหม่ชนะ, ตัด tombstone */
export class XdbStoreIterator implements IterableIterator<StoreEntry> {
  readonly #inner: NativeStoreIterator;
  #bound: IterBound | null;

  constructor(inner: NativeStoreIterator, bound: IterBound | null = null) {
    this.#inner = inner;
    this.#bound = bound;
  }

  next(): IteratorResult<StoreEntry> {
    const entry = this.#inner.next();
    if (entry === null) return { value: undefined, done: true };
    const key = new Uint8Array(entry.key);
    if (this.#bound) {
      const { kind, bytes } = this.#bound;
      if (kind === "end" ? compareBytes(key, bytes) >= 0 : !startsWith(key, bytes)) {
        return { value: undefined, done: true };
      }
    }
    return { value: { key, value: new Uint8Array(entry.value) }, done: false };
  }

  [Symbol.iterator](): IterableIterator<StoreEntry> {
    return this;
  }
}

// ---------------- XdbSingleFile: ไฟล์ .xdb เดียวจบ (เขียน+อ่าน ไม่พัง) ----------------

import {
  copyFileSync,
  existsSync,
  mkdirSync,
  readdirSync,
  renameSync,
  rmSync,
  unlinkSync,
} from "node:fs";
import { join } from "node:path";

/** sleep แบบ sync (Atomics.wait — ใช้ได้ใน main thread ของ Node) */
function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * ใช้ไฟล์ .xdb **ไฟล์เดียว** ทำทุกอย่าง: สร้าง / อัพเดต / ลบ / อ่าน — ไม่พัง
 *
 * หลักการ: ภายนอกเห็นแค่ `data.xdb` ไฟล์เดียว ส่วนงานเขียน realtime ทำใน
 * ห้องเครื่องข้าง ๆ (`data.xdb.store/`) แล้ว `save()` บีบรวมทุกอย่างเขียนทับ
 * ไฟล์เดียวแบบ atomic (tmp + rename) — XdbReader ที่เปิดค้างอยู่ก็ไม่พัง
 * เพราะเห็น snapshot เดิมของตัวเองต่อไปได้
 *
 * ```ts
 * const db = new XdbSingleFile("./app.xdb");
 * db.put([["a", "1"]]);
 * db.save();                       // data.xdb ถูกแทนที่แบบ atomic
 * const r = new XdbReader("./app.xdb"); r.getUtf8("a"); // "1"
 * db.close();
 * ```
 */
export class XdbSingleFile {
  readonly #file: string;
  readonly #dir: string;
  readonly #store: XdbStore;

  constructor(path: string, options: XdbStoreOptions = {}) {
    this.#file = path;
    this.#dir = path + ".store";
    if (!existsSync(this.#dir)) {
      mkdirSync(this.#dir, { recursive: true });
      // มีไฟล์อยู่แล้ว (เคย save ไว้ / เอามาจากเครื่องอื่น) → ใช้เป็นฐาน layer แรก
      if (existsSync(this.#file)) {
        copyFileSync(this.#file, join(this.#dir, "000001.xdb"));
      }
    }
    this.#store = new XdbStore(this.#dir, options);
  }

  /** เพิ่ม/แก้ค่า (upsert) — รับ Array / Map / Object, batch ยิ่งใหญ่ยิ่งเร็ว */
  put(entries: BuildInput): void {
    this.#store.put(entries);
  }

  /** ลบ keys */
  delete(keys: KeyValue | KeyValue[]): void {
    this.#store.delete(keys);
  }

  get(key: KeyValue): Uint8Array | null {
    return this.#store.get(key);
  }

  getUtf8(key: KeyValue): string | null {
    return this.#store.getUtf8(key);
  }

  has(key: KeyValue): boolean {
    return this.#store.has(key);
  }

  /** มุมมองรวมทั้งหมด (รวมของที่ยังไม่ save) เรียงตาม key */
  iter(): ReturnType<XdbStore["iter"]> {
    return this.#store.iter();
  }

  seek(start: KeyValue): ReturnType<XdbStore["seek"]> {
    return this.#store.seek(start);
  }

  range(start: KeyValue, end: KeyValue): ReturnType<XdbStore["range"]> {
    return this.#store.range(start, end);
  }

  prefix(p: KeyValue): ReturnType<XdbStore["prefix"]> {
    return this.#store.prefix(p);
  }

  /**
   * บีบทุกอย่าง (ฐาน + memtable + layers) รวมเป็นไฟล์เดียวแล้ว**แทนที่ไฟล์เดิมแบบ atomic**
   * (tmp + rename) — ระหว่างนี้ XdbReader ตัวเก่าที่เปิดค้างอยู่ยังอ่าน snapshot เดิมได้ต่อ
   */
  save(): void {
    // compact รอบแรกเสมอ — flush memtable ลง layer ก่อน (กรณียังไม่มี layer เลย)
    this.#store.compact();
    // จากนั้น compact ซ้ำจนเหลือ layer เดียว (รอ background compaction ที่กำลังรวให้จบก่อน)
    for (;;) {
      while (this.#store.isCompacting) sleepSync(5);
      const layers = this.#layerFiles();
      if (layers.length <= 1) break;
      this.#store.compact();
    }

    const layers = this.#layerFiles();
    const tmp = this.#file + ".tmp";
    if (layers.length === 0) {
      // ยังไม่มีข้อมูลเลย → เขียนตารางเปล่าให้ไฟล์มีรูปแบบถูกต้องเสมอ
      writeTable(tmp, []);
    } else {
      copyFileSync(join(this.#dir, layers[0]), tmp);
    }
    try {
      renameSync(tmp, this.#file); // atomic replace (ทางเดียวจบ)
    } catch (e: unknown) {
      // Windows: ถ้ามี XdbReader ถือ mmap ของไฟล์เดิมอยู่ rename ทับไม่ได้ (EPERM)
      // → ลบไฟล์เดิมแบบ POSIX-delete (ชื่อว่างทันที แต่ reader เก่ายังอ่าน snapshot ของมันต่อได้)
      //   แล้ว rename ตัวใหม่เข้าที่ปกติ
      const code = (e as NodeJS.ErrnoException).code;
      if (code !== "EPERM" && code !== "EACCES" && code !== "ENOTEMPTY" && code !== "EEXIST") throw e;
      unlinkSync(this.#file);
      renameSync(tmp, this.#file);
    }
  }

  /** เปิด XdbReader บนไฟล์เดียวนั้น (snapshot ณ ตอน save() ล่าสุด) */
  openSnapshot(): XdbReader {
    if (!existsSync(this.#file)) {
      throw new Error("ยังไม่มีไฟล์ — เรียก save() ก่อน");
    }
    return new XdbReader(this.#file);
  }

  /** ปิด store (ข้อมูล durable ในห้องเครื่อง — เปิดใหม่ใช้ต่อได้) */
  close(): void {
    this.#store.close();
  }

  /**
   * ปิด + save + **ลบห้องเครื่อง** → เหลือ `data.xdb` ไฟล์เดียวจริง ๆ
   * พกไปเครื่องอื่น / แนบอีเมลได้เลย (เปิดครั้งหน้าจะ seed จากไฟล์นี้อัตโนมัติ)
   */
  exportAndClose(): void {
    this.save();
    this.#store.close();
    rmSync(this.#dir, { recursive: true, force: true });
  }

  #layerFiles(): string[] {
    return readdirSync(this.#dir).filter((f) => f.endsWith(".xdb"));
  }
}

// ---------------- XDB: API เดียวจบ (อ่าน/เขียน/อัพเดต/ลบ บนไฟล์เดียว) ----------------

/** ค่าที่ใส่ได้: string เก็บตรง ๆ / Uint8Array เก็บ bytes / object จะ JSON ให้อัตโนมัติ */
export type XDBValue = string | Uint8Array | Record<string, unknown> | unknown[];

/** ระดับความปลอดภัยของข้อมูล (แทนการตั้ง sync/syncIntervalMs มือ) */
export type XDBDurability =
  /** fsync ทุก operation — ไฟดับไม่เสียข้อมูลเลย (put ~1ms) */
  | "safe"
  /** put เร็ว (~5µs) + ซิงก์ดิสก์ทุก 200ms — ไฟดับเสียได้สูงสุด 200ms (แนะนำ) */
  | "balanced"
  /** เร็วสุด ไม่รอดิสก์ — เหมาะกับ cache/ข้อมูลสร้างใหม่ได้ */
  | "fast";

export interface XDBOptions {
  durability?: XDBDurability;
  /** entries ใน memtable ก่อน flush เป็น layer (default 4096) */
  flushEntries?: number;
  /** จำนวน layers ที่ trigger compact อัตโนมัติ (default 8, 0 = ปิด) */
  compactThreshold?: number;
}

/** ค่าที่ได้คืนจาก get/iter — object ถูก JSON.parse กลับมาให้แล้ว */
export type XDBDecoded = string | Uint8Array | Record<string, unknown> | unknown[];

function decodeValue(v: Uint8Array): XDBDecoded {
  try {
    const s = new TextDecoder("utf-8", { fatal: true }).decode(v);
    const c = s.charCodeAt(0);
    // { หรือ [ นำหน้า → ลอง JSON (เก็บผ่าน set แบบ object)
    if (c === 0x7b || c === 0x5b) {
      try {
        return JSON.parse(s) as Record<string, unknown> | unknown[];
      } catch {
        return s;
      }
    }
    return s;
  } catch {
    return v; // ไม่ใช่ UTF-8 → bytes ดิบ
  }
}

function encodeValue(v: XDBValue): string | Uint8Array {
  return typeof v === "string" || v instanceof Uint8Array ? v : JSON.stringify(v);
}

export interface XDBEntry {
  key: string | Uint8Array;
  value: XDBDecoded;
}

/**
 * XDB — API เดียวจบบน**ไฟล์ .xdb ไฟล์เดียว**
 * รวมทุกอย่างที่เคยแยกเป็น writeTable / XdbReader / XdbStore / XdbSingleFile ไว้ในคลาสเดียว
 *
 * ```ts
 * const db = new XDB("./app.xdb");         // หรือ XDB.open("./app.xdb")
 * db.set("user:1", { name: "สมชาย", age: 30 });
 * db.set("note", "hello");
 * db.get("user:1");                         // { name: "สมชาย", age: 30 }
 * db.set("user:1", { name: "สมชายใหม่" });  // update ได้บนไฟล์เดียวกัน
 * db.delete("note");
 * for (const e of db.prefix("user:")) { }
 * db.save();                                // บีบเป็นไฟล์เดียวแบบ atomic
 * db.close();
 * ```
 */
export class XDB {
  readonly #sf: XdbSingleFile;

  constructor(path: string, options: XDBOptions = {}) {
    const durability = options.durability ?? "safe";
    const storeOpts: XdbStoreOptions =
      durability === "safe"
        ? {}
        : durability === "balanced"
          ? { sync: false, syncIntervalMs: 200 }
          : { sync: false };
    this.#sf = new XdbSingleFile(path, {
      ...storeOpts,
      flushEntries: options.flushEntries,
      compactThreshold: options.compactThreshold,
    });
  }

  /** เปิด database (มาตรว่ากับ new XDB) */
  static open(path: string, options: XDBOptions = {}): XDB {
    return new XDB(path, options);
  }

  /** ตั้งค่า — string เก็บตรง / Uint8Array เก็บ bytes / object แปลง JSON ให้อัตโนมัติ */
  set(key: KeyValue, value: XDBValue): void {
    const encoded = encodeValue(value);
    this.#sf.put([[key, encoded] as [KeyValue, string | Uint8Array]]);
  }

  /** ตั้งหลายค่าในคำสั่งเดียว (batch — ยิ่งเยอะยิ่งเร็ว ~2-5µs/key) */
  setMany(entries: Array<[KeyValue, XDBValue]> | Record<string, XDBValue> | Map<KeyValue, XDBValue>): void {
    const list: Array<[KeyValue, string | Uint8Array]> = [];
    const push = (k: KeyValue, v: XDBValue): void => {
      list.push([k, encodeValue(v)] as [KeyValue, string | Uint8Array]);
    };
    if (entries instanceof Map) {
      for (const [k, v] of entries) push(k, v);
    } else if (Array.isArray(entries)) {
      for (const [k, v] of entries) push(k, v);
    } else {
      for (const [k, v] of Object.entries(entries)) push(k, v);
    }
    this.#sf.put(list);
  }

  /** อ่านค่า — object ที่เก็บไว้ได้กลับมาเป็น object (JSON ให้แล้ว) / bytes ถ้าไม่ใช่ UTF-8 */
  get<T = XDBDecoded>(key: KeyValue): T | null {
    const v = this.#sf.get(key);
    return v === null ? null : (decodeValue(v) as T);
  }

  /** อ่านค่าแบบ bytes ดิบเสมอ (ไม่ decode) */
  getBytes(key: KeyValue): Uint8Array | null {
    return this.#sf.get(key);
  }

  has(key: KeyValue): boolean {
    return this.#sf.has(key);
  }

  /** ลบ — รับกี่ key ก็ได้: del("a") หรือ del("a", "b", "c") */
  del(...keys: KeyValue[]): void {
    if (keys.length === 0) return;
    this.#sf.delete(keys);
  }

  /** ไล่ทั้งหมดเรียงตาม key */
  iter(): IterableIterator<XDBEntry> {
    return mapEntries(this.#sf.iter(), decodeValue);
  }

  /** ไล่เฉพาะ key ที่ขึ้นต้นด้วย prefix */
  prefix(p: KeyValue): IterableIterator<XDBEntry> {
    return mapEntries(this.#sf.prefix(p), decodeValue);
  }

  /** ไล่ช่วง [start, end) — end exclusive */
  range(start: KeyValue, end: KeyValue): IterableIterator<XDBEntry> {
    return mapEntries(this.#sf.range(start, end), decodeValue);
  }

  /** เริ่มไล่จาก key >= start */
  seek(start: KeyValue): IterableIterator<XDBEntry> {
    return mapEntries(this.#sf.seek(start), decodeValue);
  }

  /**
   * บีบทุกอย่างเข้าไฟล์ .xdb ไฟล์เดียวแบบ atomic —
   * หลังจากนี้ใครก็เปิดไฟล์นี้ด้วย XDBReader/writeTable ecosystem ได้
   * (reader ตัวเก่าที่เปิดค้างอยู่ก็ไม่พัง)
   */
  save(): void {
    this.#sf.save();
  }

  /** เปิดอ่านแบบ snapshot (เร็วสุด ~0.5-1.4µs/get) — เห็นข้อมูล ณ ตอน save() ล่าสุด */
  snapshot(): XdbReader {
    return this.#sf.openSnapshot();
  }

  /** ปิด + save + เหลือไฟล์เดียวพกไปไหนก็ได้ (เปิดครั้งหน้าข้อมูลอยู่ครบ) */
  close(): void {
    this.#sf.exportAndClose();
  }
}

function* mapEntries(
  it: IterableIterator<{ key: Uint8Array; value: Uint8Array }>,
  decode: (v: Uint8Array) => XDBDecoded,
): IterableIterator<XDBEntry> {
  for (const e of it) {
    yield { key: e.key, value: decode(e.value) };
  }
}
