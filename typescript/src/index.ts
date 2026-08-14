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
}

interface NativeApi {
  writeTable(path: string, entries: Entry[]): void;
  /** รวมหลายไฟล์ .xdb เป็นไฟล์เดียว — ไฟล์หลังสุดชนะเมื่อ key ซ้ำ คืนจำนวน entries ผลลัพธ์ */
  mergeTables(inputs: string[], output: string): number;
  XdbReader: new (path: string) => NativeReader;
  XdbStore: new (
    path: string,
    options?: { compactThreshold?: number; flushEntries?: number; sync?: boolean },
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
