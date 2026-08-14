/**
 * Mongo — document layer สไตล์ MongoDB Node.js driver บน XDB
 *
 * ```ts
 * import { XDB } from "xdb-native";
 *
 * const db = new XDB("./app.xdb");
 * const users = db.collection("users");
 *
 * users.insertOne({ name: "สมชาย", age: 30 });            // → { insertedId }
 * users.insertMany([{ name: "สมหญิง", age: 25 }, ...]);    // → { insertedCount }
 * users.findOne({ name: "สมชาย" });                        // doc | null
 * users.find({ age: { $gte: 18 } })
 *   .sort({ age: -1 }).skip(10).limit(5).toArray();
 * users.updateOne({ _id: id }, { $set: { age: 31 }, $inc: { login: 1 } });
 * users.deleteMany({ status: "inactive" });                // → { deletedCount }
 * users.countDocuments({ age: { $gt: 20 } });
 * ```
 *
 * หมายเหตุ: API ทำงานแบบ sync (engine เร็วอยู่แล้ว) — เขียน `await` ต่อท้ายก็ไม่พัง
 */
import { randomUUID } from "node:crypto";
import type { XDB, XDBValue } from "./index.js";

// ---- types ----

export interface MongoDoc {
  _id: string;
  [field: string]: unknown;
}

/** filter แบบ MongoDB: field เท่ากับค่า หรือใช้ operators */
export type Filter = {
  $and?: Filter[];
  $or?: Filter[];
  $nor?: Filter[];
} & Record<string, unknown>;

export interface FieldOps {
  $eq?: unknown;
  $ne?: unknown;
  $gt?: unknown;
  $gte?: unknown;
  $lt?: unknown;
  $lte?: unknown;
  $in?: unknown[];
  $nin?: unknown[];
  $exists?: boolean;
  $regex?: string | RegExp;
}

/** update แบบ MongoDB (ถ้าไม่มี $xxx = replace ทั้ง doc) */
export interface UpdateSpec {
  $set?: Record<string, unknown>;
  $unset?: Record<string, true>;
  $inc?: Record<string, number>;
  $push?: Record<string, unknown>;
  $addToSet?: Record<string, unknown>;
  $pull?: Record<string, unknown>;
  $pop?: Record<string, 1 | -1>;
  $rename?: Record<string, string>;
  $mul?: Record<string, number>;
  $min?: Record<string, unknown>;
  $max?: Record<string, unknown>;
  $setOnInsert?: Record<string, unknown>;
}

/** ตัวเลือกของ update operations */
export interface UpdateOptions {
  /** ไม่เจอ = insert ให้ (ค่า field ที่เท่ากันใน filter + update กลายเป็น doc ใหม่) */
  upsert?: boolean;
}

export type SortSpec = Record<string, 1 | -1>;

// ---- helpers ----

function deepEq(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

function cmp(a: unknown, b: unknown): number | null {
  if (typeof a === "number" && typeof b === "number") return a - b;
  if (typeof a === "string" && typeof b === "string") return a < b ? -1 : a > b ? 1 : 0;
  if (a === b) return 0;
  return null; // เทียบไม่ได้
}

/** ดึงค่าตาม path แบบ dot notation: "address.city" */
function getPath(doc: MongoDoc, path: string): unknown {
  let cur: unknown = doc;
  for (const part of path.split(".")) {
    if (cur === null || typeof cur !== "object") return undefined;
    cur = (cur as Record<string, unknown>)[part];
  }
  return cur;
}

/** ตั้งค่าตาม path แบบ dot notation (สร้าง object ตามทางให้) */
function setPath(obj: Record<string, unknown>, path: string, value: unknown): void {
  const parts = path.split(".");
  let cur: Record<string, unknown> = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (typeof cur[p] !== "object" || cur[p] === null) cur[p] = {};
    cur = cur[p] as Record<string, unknown>;
  }
  cur[parts[parts.length - 1]] = value;
}

function delPath(obj: Record<string, unknown>, path: string): void {
  const parts = path.split(".");
  let cur: unknown = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    if (typeof cur !== "object" || cur === null) return;
    cur = (cur as Record<string, unknown>)[parts[i]];
  }
  if (cur && typeof cur === "object") delete (cur as Record<string, unknown>)[parts[parts.length - 1]];
}

function isFieldOps(v: unknown): v is FieldOps {
  return typeof v === "object" && v !== null && !Array.isArray(v) &&
    Object.keys(v).length > 0 && Object.keys(v).every((k) => k.startsWith("$"));
}

function matchOps(value: unknown, ops: FieldOps): boolean {
  for (const [op, arg] of Object.entries(ops)) {
    switch (op) {
      case "$eq": if (!deepEq(value, arg)) return false; break;
      case "$ne": if (deepEq(value, arg)) return false; break;
      case "$gt": { const c = cmp(value, arg); if (c === null || c <= 0) return false; break; }
      case "$gte": { const c = cmp(value, arg); if (c === null || c < 0) return false; break; }
      case "$lt": { const c = cmp(value, arg); if (c === null || c >= 0) return false; break; }
      case "$lte": { const c = cmp(value, arg); if (c === null || c > 0) return false; break; }
      case "$in":
        if (!(arg as unknown[]).some((x) => deepEq(value, x))) return false;
        break;
      case "$nin":
        if ((arg as unknown[]).some((x) => deepEq(value, x))) return false;
        break;
      case "$exists":
        if ((value !== undefined) !== Boolean(arg)) return false;
        break;
      case "$regex": {
        const re = arg instanceof RegExp ? arg : new RegExp(String(arg));
        if (typeof value !== "string" || !re.test(value)) return false;
        break;
      }
      default:
        throw new Error(`Mongo: unsupported operator \`${op}\``);
    }
  }
  return true;
}

function matches(doc: MongoDoc, filter: Filter): boolean {
  for (const [k, v] of Object.entries(filter)) {
    switch (k) {
      case "$and":
        if (!(v as Filter[]).every((f) => matches(doc, f))) return false;
        break;
      case "$or":
        if (!(v as Filter[]).some((f) => matches(doc, f))) return false;
        break;
      case "$nor":
        if ((v as Filter[]).some((f) => matches(doc, f))) return false;
        break;
      default: {
        const value = getPath(doc, k);
        if (isFieldOps(v)) {
          if (!matchOps(value, v)) return false;
        } else if (!deepEq(value, v)) {
          return false;
        }
      }
    }
  }
  return true;
}

/** ดึง path เป็น array (ไม่ใช่ array = เริ่มใหม่) — ใช้กับ $push/$addToSet */
function asArray(doc: MongoDoc, path: string): unknown[] {
  const cur = getPath(doc, path);
  return Array.isArray(cur) ? [...cur] : [];
}

/** $pull: condition เป็น field-ops ก็กรองตาม ops ได้ (subset ของ MongoDB) */
function pullMatches(item: unknown, cond: unknown): boolean {
  if (isFieldOps(cond)) return matchOps(item, cond);
  return deepEq(item, cond);
}

function applyUpdate(doc: MongoDoc, update: UpdateSpec, isInsert = false): MongoDoc {
  const hasOps = Object.keys(update).some((k) => k.startsWith("$"));
  if (!hasOps) {
    // ไม่มี $xxx = replace ทั้ง doc (คง _id เดิมตาม semantics ของ MongoDB)
    return { ...(update as Record<string, unknown>), _id: doc._id } as MongoDoc;
  }
  const next: MongoDoc = { ...doc };
  for (const [op, spec] of Object.entries(update)) {
    switch (op) {
      case "$set":
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) setPath(next, p, v);
        break;
      case "$unset":
        for (const p of Object.keys(spec as Record<string, true>)) delPath(next, p);
        break;
      case "$inc":
        for (const [p, d] of Object.entries(spec as Record<string, number>)) {
          const cur = getPath(next, p);
          // field ที่ยังไม่มี = 0 (ตาม semantics MongoDB — สำคัญตอน upsert)
          if (cur !== undefined && typeof cur !== "number") {
            throw new Error(`Mongo $inc: \`${p}\` ไม่ใช่ตัวเลข`);
          }
          setPath(next, p, (typeof cur === "number" ? cur : 0) + d);
        }
        break;
      case "$push": {
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) {
          const arr = asArray(next, p);
          arr.push(v);
          setPath(next, p, arr);
        }
        break;
      }
      case "$setOnInsert": {
        if (!isInsert) break;
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) setPath(next, p, v);
        break;
      }
      case "$addToSet": {
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) {
          const arr = asArray(next, p);
          if (!arr.some((x) => deepEq(x, v))) arr.push(v);
          setPath(next, p, arr);
        }
        break;
      }
      case "$pull": {
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) {
          const cur = getPath(next, p);
          if (!Array.isArray(cur)) continue;
          setPath(next, p, cur.filter((x) => !pullMatches(x, v)));
        }
        break;
      }
      case "$pop": {
        for (const [p, dir] of Object.entries(spec as Record<string, 1 | -1>)) {
          const cur = getPath(next, p);
          if (!Array.isArray(cur) || cur.length === 0) continue;
          setPath(next, p, dir === -1 ? cur.slice(1) : cur.slice(0, -1));
        }
        break;
      }
      case "$rename": {
        for (const [from, to] of Object.entries(spec as Record<string, string>)) {
          const v = getPath(next, from);
          if (v === undefined) continue;
          delPath(next, from);
          setPath(next, to, v);
        }
        break;
      }
      case "$mul": {
        for (const [p, m] of Object.entries(spec as Record<string, number>)) {
          const cur = getPath(next, p);
          if (typeof cur !== "number") throw new Error(`Mongo $mul: \`${p}\` ไม่ใช่ตัวเลข`);
          setPath(next, p, cur * m);
        }
        break;
      }
      case "$min": {
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) {
          const cur = getPath(next, p);
          if (cur === undefined || (cmp(cur, v) ?? 0) > 0) setPath(next, p, v);
        }
        break;
      }
      case "$max": {
        for (const [p, v] of Object.entries(spec as Record<string, unknown>)) {
          const cur = getPath(next, p);
          if (cur === undefined || (cmp(cur, v) ?? 0) < 0) setPath(next, p, v);
        }
        break;
      }
      default:
        throw new Error(`Mongo: unsupported update operator \`${op}\``);
    }
  }
  return next;
}

// ---- Cursor ----

export class FindCursor<T extends MongoDoc> implements Iterable<T> {
  readonly #iter: () => Iterable<{ key: string; value: unknown }>;
  readonly #filter: Filter;
  #skip = 0;
  #limit = Infinity;
  #sort: SortSpec | null = null;

  constructor(iter: () => Iterable<{ key: string; value: unknown }>, filter: Filter) {
    this.#iter = iter;
    this.#filter = filter;
  }

  sort(spec: SortSpec): this {
    this.#sort = spec;
    return this;
  }

  skip(n: number): this {
    this.#skip = n;
    return this;
  }

  limit(n: number): this {
    this.#limit = n;
    return this;
  }

  toArray(): T[] {
    return [...this];
  }

  forEach(fn: (doc: T) => void): void {
    for (const doc of this) fn(doc);
  }

  count(): number {
    return this.toArray().length;
  }

  [Symbol.iterator](): Iterator<T> {
    let docs: T[] = [];
    if (this.#sort) {
      // มี sort → materialize ก่อนเรียง (จากนั้นค่อย skip/limit)
      docs = this.#collect();
      const entries = Object.entries(this.#sort);
      docs.sort((a, b) => {
        for (const [field, dir] of entries) {
          const c = cmp(getPath(a, field), getPath(b, field)) ?? 0;
          if (c !== 0) return c * dir;
        }
        return 0;
      });
      docs = docs.slice(this.#skip, this.#skip + this.#limit);
      let i = 0;
      return { next: () => (i < docs.length ? { value: docs[i++], done: false } : { value: undefined, done: true }) };
    }
    // ไม่มี sort → lazy: ไล่ทีละตัว กรอง + skip + limit
    const src = this.#iter()[Symbol.iterator]();
    let skipped = 0;
    let yielded = 0;
    return {
      next: () => {
        for (;;) {
          const { value, done } = src.next();
          if (done) return { value: undefined, done: true };
          const doc = value.value as T;
          if (!matches(doc, this.#filter)) continue;
          if (skipped < this.#skip) { skipped++; continue; }
          if (yielded >= this.#limit) return { value: undefined, done: true };
          yielded++;
          return { value: doc, done: false };
        }
      },
    };
  }

  #collect(): T[] {
    const out: T[] = [];
    for (const { value } of this.#iter()) {
      if (matches(value as T, this.#filter)) out.push(value as T);
    }
    return out;
  }
}

export class Collection<T extends MongoDoc = MongoDoc> {
  readonly #db: XDB;
  readonly #name: string;

  constructor(db: XDB, name: string) {
    this.#db = db;
    this.#name = name;
  }

  #keyOf(id: string): string {
    return `${this.#name}:${encodeURIComponent(id)}`;
  }

  #docIter(): Iterable<{ key: string; value: unknown }> {
    const prefix = `${this.#name}:`;
    const iter = this.#db.prefix(prefix);
    return {
      [Symbol.iterator]() {
        const it = iter[Symbol.iterator]();
        return {
          next: () => {
            for (;;) {
              const { value, done } = it.next();
              if (done) return { value: undefined, done: true };
              const doc = value.value as T;
              const id = decodeURIComponent(
                Buffer.from(value.key).toString("utf8").slice(prefix.length),
              );
              if (doc && typeof doc === "object" && !("_id" in (doc as object))) {
                (doc as MongoDoc)._id = id; // กันของเก่าที่ไม่มี _id
              }
              return { value: { key: id, value: doc }, done: false };
            }
          },
        };
      },
    };
  }

  // ── insert ──

  insertOne(doc: Record<string, unknown>): { insertedId: string } {
    const _id = typeof doc._id === "string" ? doc._id : randomUUID();
    const full = { ...doc, _id } as T;
    this.#db.set(this.#keyOf(_id), full as unknown as XDBValue);
    return { insertedId: _id };
  }

  insertMany(docs: Array<Record<string, unknown>>): { insertedCount: number; insertedIds: string[] } {
    const entries: Array<[string, XDBValue]> = [];
    const ids: string[] = [];
    for (const doc of docs) {
      const _id = typeof doc._id === "string" ? doc._id : randomUUID();
      ids.push(_id);
      entries.push([this.#keyOf(_id), { ...doc, _id } as unknown as XDBValue]);
    }
    this.#db.setMany(entries);
    return { insertedCount: entries.length, insertedIds: ids };
  }

  // ── query ──

  findOne(filter: Filter = {}): T | null {
    for (const { value } of this.#docIter()) {
      if (matches(value as T, filter)) return value as T;
    }
    return null;
  }

  findById(id: string): T | null {
    const v = this.#db.get<Record<string, unknown>>(this.#keyOf(id));
    return (v as T) ?? null;
  }

  find(filter: Filter = {}): FindCursor<T> {
    return new FindCursor<T>(() => this.#docIter(), filter);
  }

  countDocuments(filter: Filter = {}): number {
    if (Object.keys(filter).length === 0) {
      let n = 0;
      for (const _d of this.#docIter()) n++;
      return n;
    }
    return this.find(filter).count();
  }

  // ── update ──

  updateOne(
    filter: Filter,
    update: UpdateSpec,
    options: UpdateOptions = {},
  ): { matchedCount: number; modifiedCount: number; upsertedId?: string } {
    for (const { key: id, value } of this.#docIter()) {
      if (!matches(value as T, filter)) continue;
      const updated = applyUpdate(value as T, update);
      this.#db.set(this.#keyOf(id), updated as unknown as XDBValue);
      return { matchedCount: 1, modifiedCount: 1 };
    }
    if (options.upsert) return { matchedCount: 0, modifiedCount: 0, ...this.#upsert(filter, update) };
    return { matchedCount: 0, modifiedCount: 0 };
  }

  updateMany(
    filter: Filter,
    update: UpdateSpec,
    options: UpdateOptions = {},
  ): { matchedCount: number; modifiedCount: number; upsertedId?: string } {
    let matched = 0;
    const updated: Array<[string, XDBValue]> = [];
    for (const { key: id, value } of this.#docIter()) {
      if (!matches(value as T, filter)) continue;
      matched++;
      updated.push([this.#keyOf(id), applyUpdate(value as T, update) as unknown as XDBValue]);
    }
    if (updated.length > 0) this.#db.setMany(updated);
    if (matched === 0 && options.upsert) {
      return { matchedCount: 0, modifiedCount: 0, ...this.#upsert(filter, update) };
    }
    return { matchedCount: matched, modifiedCount: matched };
  }

  /** แทนที่ทั้ง doc (ต่างจาก update = ไม่สน operators) — คง _id เดิม */
  replaceOne(
    filter: Filter,
    doc: Record<string, unknown>,
    options: UpdateOptions = {},
  ): { matchedCount: number; modifiedCount: number; upsertedId?: string } {
    for (const { key: id, value } of this.#docIter()) {
      if (!matches(value as T, filter)) continue;
      this.#db.set(this.#keyOf(id), { ...doc, _id: id } as unknown as XDBValue);
      return { matchedCount: 1, modifiedCount: 1 };
    }
    if (options.upsert) {
      return { matchedCount: 0, modifiedCount: 0, ...this.#upsert(filter, { $set: doc }) };
    }
    return { matchedCount: 0, modifiedCount: 0 };
  }

  /** upsert: สร้าง doc ใหม่จาก field ที่เท่ากันใน filter + update (ตาม semantics MongoDB) */
  #upsert(filter: Filter, update: UpdateSpec): { upsertedId: string } {
    const seed: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(filter)) {
      if (k.startsWith("$")) continue;
      if (!isFieldOps(v)) seed[k] = v; // field-ops/$regex ฯลฯ ข้าม (แบบ MongoDB)
    }
    const doc = applyUpdate({ ...seed } as MongoDoc, update, true);
    return { upsertedId: this.insertOne(doc).insertedId };
  }

  // ── delete ──

  deleteOne(filter: Filter): { deletedCount: number } {
    for (const { key: id, value } of this.#docIter()) {
      if (!matches(value as T, filter)) continue;
      this.#db.del(this.#keyOf(id));
      return { deletedCount: 1 };
    }
    return { deletedCount: 0 };
  }

  deleteMany(filter: Filter): { deletedCount: number } {
    const ids: string[] = [];
    for (const { key: id, value } of this.#docIter()) {
      if (matches(value as T, filter)) ids.push(this.#keyOf(id));
    }
    if (ids.length > 0) this.#db.del(...ids);
    return { deletedCount: ids.length };
  }

  /** ลบ collection ทั้งหมด (เหมือน drop()) */
  drop(): { deletedCount: number } {
    return this.deleteMany({});
  }
}
