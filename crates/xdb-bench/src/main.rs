//! เทียบ x-db กับ database อื่น: SQLite, redb และ HashMap (in-RAM baseline)
//! รัน: cargo run --release -p xdb-bench
//!
//! หมายเหตุความเป็นธรรม: x-db เป็น immutable read-only store (เขียนครั้งเดียว อ่านเยอะ)
//! ส่วน SQLite/redb เป็น read-write database (มี transaction/locking ในตัว)
//! ทุกตัววัดหลัง page cache ร้อนแล้ว ใช้ key/value ชุดเดียวกัน 100,000 entries × 100B

use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::time::Instant;
use x_db::{XDBReader, XDBWriter};

const N: usize = 100_000;
const VAL_SIZE: usize = 100;
const LOOKUPS: usize = 100_000;

fn entries() -> Vec<(String, Vec<u8>)> {
    (0..N)
        .map(|i| (format!("key:{:012}", i), vec![(i % 251) as u8; VAL_SIZE]))
        .collect()
}

/// 7919 เป็นจำนวนเฉพาะ → ลำดับ key กระจายทั่วทั้งตาราง
fn hit_keys() -> Vec<String> {
    (0..LOOKUPS).map(|i| format!("key:{:012}", (i * 7919) % N)).collect()
}

fn miss_keys() -> Vec<String> {
    (0..LOOKUPS).map(|i| format!("zzz:{:012}", i)).collect()
}

struct Row {
    name: &'static str,
    build_ms: f64,
    open_ms: f64,
    hit_ns: f64,
    miss_ns: f64,
    iterate_ms: f64,
    file_mb: f64,
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

fn ns_per(t: Instant, ops: usize) -> f64 {
    t.elapsed().as_secs_f64() * 1e9 / ops as f64
}

fn file_mb(path: &Path) -> f64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) as f64 / 1024.0 / 1024.0
}

// ---------------- x-db ----------------

fn bench_xdb(dir: &Path, ents: &[(String, Vec<u8>)], hits: &[String], misses: &[String]) -> Result<Row, Box<dyn Error>> {
    let path = dir.join("xdb.xdb");
    let refs: Vec<(&[u8], &[u8])> = ents.iter().map(|(k, v)| (k.as_bytes(), v.as_slice())).collect();

    let t = Instant::now();
    XDBWriter::write_table(&path, &refs)?;
    let build_ms = ms(t);

    // เปิดครั้งแรก (จ่ายค่า first-touch ของ OS) แล้วจับเวลา open รอบสอง
    let _warm = XDBReader::open(&path)?;
    drop(_warm);
    let t = Instant::now();
    let reader = XDBReader::open(&path)?;
    let open_ms = ms(t);

    let mut found = 0;
    let t = Instant::now();
    for k in hits {
        if reader.get(k.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, hits.len());

    let t = Instant::now();
    let mut rejected = 0;
    for k in misses {
        if reader.get(k.as_bytes())?.is_none() {
            rejected += 1;
        }
    }
    let miss_ns = ns_per(t, misses.len());

    let t = Instant::now();
    let mut count = 0usize;
    for e in reader.iter() {
        e?;
        count += 1;
    }
    let iterate_ms = ms(t);

    assert_eq!(found, hits.len());
    assert_eq!(rejected, misses.len());
    assert_eq!(count, N);

    Ok(Row { name: "x-db", build_ms, open_ms, hit_ns, miss_ns, iterate_ms, file_mb: file_mb(&path) })
}

// ---------------- HashMap (in-RAM baseline) ----------------

fn bench_hashmap(ents: &[(String, Vec<u8>)], hits: &[String], misses: &[String]) -> Row {
    let t = Instant::now();
    let mut map: HashMap<&str, &[u8]> = HashMap::with_capacity(N);
    for (k, v) in ents {
        map.insert(k.as_str(), v.as_slice());
    }
    let build_ms = ms(t);

    let mut found = 0;
    let t = Instant::now();
    for k in hits {
        if map.contains_key(k.as_str()) {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, hits.len());

    let t = Instant::now();
    let mut rejected = 0;
    for k in misses {
        if !map.contains_key(k.as_str()) {
            rejected += 1;
        }
    }
    let miss_ns = ns_per(t, misses.len());

    let t = Instant::now();
    let count = map.values().count();
    let iterate_ms = ms(t);

    assert_eq!(found, hits.len());
    assert_eq!(rejected, misses.len());
    assert_eq!(count, N);

    Row { name: "HashMap(RAM)", build_ms, open_ms: f64::NAN, hit_ns, miss_ns, iterate_ms, file_mb: f64::NAN }
}

// ---------------- SQLite ----------------

fn bench_sqlite(dir: &Path, ents: &[(String, Vec<u8>)], hits: &[String], misses: &[String]) -> Result<Row, Box<dyn Error>> {
    let path = dir.join("sqlite.db");
    let _ = std::fs::remove_file(&path);

    let t = Instant::now();
    {
        let mut conn = Connection::open(&path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=OFF;
             PRAGMA synchronous=OFF;
             CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID;",
        )?;
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO kv (k, v) VALUES (?, ?)")?;
            for (k, v) in ents {
                stmt.execute(params![k.as_bytes(), v.as_slice()])?;
            }
        }
        tx.commit()?;
    } // ปิด connection ก่อนจับเวลาเปิดใหม่
    let build_ms = ms(t);

    let _warm = Connection::open(&path)?;
    drop(_warm);
    let t = Instant::now();
    let conn = Connection::open(&path)?;
    let open_ms = ms(t);

    let mut found = 0;
    {
        let mut stmt = conn.prepare("SELECT v FROM kv WHERE k = ?")?;
        let t = Instant::now();
        for k in hits {
            let result: Result<Vec<u8>, _> = stmt.query_row(params![k.as_bytes()], |r| r.get(0));
            if result.is_ok() {
                found += 1;
            }
        }
        let hit_ns = ns_per(t, hits.len());
        assert_eq!(found, hits.len());

        let t = Instant::now();
        let mut rejected = 0;
        for k in misses {
            let result: Result<Vec<u8>, _> = stmt.query_row(params![k.as_bytes()], |r| r.get(0));
            if result.is_err() {
                rejected += 1;
            }
        }
        let miss_ns = ns_per(t, misses.len());
        assert_eq!(rejected, misses.len());

        let t = Instant::now();
        let mut count = 0usize;
        let mut scan = conn.prepare("SELECT k, v FROM kv")?;
        let mut rows = scan.query([])?;
        while rows.next()?.is_some() {
            count += 1;
        }
        let iterate_ms = ms(t);
        assert_eq!(count, N);

        return Ok(Row { name: "SQLite", build_ms, open_ms, hit_ns, miss_ns, iterate_ms, file_mb: file_mb(&path) });
    }
}

// ---------------- redb ----------------

fn bench_redb(dir: &Path, ents: &[(String, Vec<u8>)], hits: &[String], misses: &[String]) -> Result<Row, Box<dyn Error>> {
    use redb::{Database, TableDefinition};
    const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

    let path = dir.join("redb.db");
    let _ = std::fs::remove_file(&path);

    let t = Instant::now();
    {
        let db = Database::create(&path)?;
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table(TABLE)?;
            for (k, v) in ents {
                table.insert(k.as_bytes(), v.as_slice())?;
            }
        }
        txn.commit()?;
    } // drop database handle ก่อนจับเวลาเปิดใหม่
    let build_ms = ms(t);

    let _warm = Database::open(&path)?;
    drop(_warm);
    let t = Instant::now();
    let db = Database::open(&path)?;
    let open_ms = ms(t);

    let read = db.begin_read()?;
    let table = read.open_table(TABLE)?;

    let mut found = 0;
    let t = Instant::now();
    for k in hits {
        if table.get(k.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, hits.len());
    assert_eq!(found, hits.len());

    let t = Instant::now();
    let mut rejected = 0;
    for k in misses {
        if table.get(k.as_bytes())?.is_none() {
            rejected += 1;
        }
    }
    let miss_ns = ns_per(t, misses.len());
    assert_eq!(rejected, misses.len());

    let t = Instant::now();
    let count = table.range::<&[u8]>(..)?.count();
    let iterate_ms = ms(t);
    assert_eq!(count, N);

    Ok(Row { name: "redb", build_ms, open_ms, hit_ns, miss_ns, iterate_ms, file_mb: file_mb(&path) })
}

// ---------------- main ----------------

fn main() -> Result<(), Box<dyn Error>> {
    let dir = std::env::temp_dir().join("xdb_compare_bench");
    std::fs::create_dir_all(&dir)?;

    println!("เตรียมข้อมูล: {N} entries × {VAL_SIZE}B ...");
    let ents = entries();
    let hits = hit_keys();
    let misses = miss_keys();

    let rows = vec![
        bench_xdb(&dir, &ents, &hits, &misses)?,
        bench_hashmap(&ents, &hits, &misses),
        bench_sqlite(&dir, &ents, &hits, &misses)?,
        bench_redb(&dir, &ents, &hits, &misses)?,
    ];

    println!();
    println!("=== เปรียบเทียบ (page cache ร้อน, Windows x64, release build) ===");
    println!(
        "{:<13} {:>10} {:>9} {:>12} {:>13} {:>12} {:>9}",
        "database", "build(ms)", "open-warm(ms)", "get-hit(ns)", "get-miss(ns)", "iterate(ms)", "file(MB)"
    );
    for r in &rows {
        println!(
            "{:<13} {:>10.0} {:>9.2} {:>12.0} {:>13.0} {:>12.1} {:>9.1}",
            r.name,
            r.build_ms,
            r.open_ms,
            r.hit_ns,
            r.miss_ns,
            r.iterate_ms,
            r.file_mb
        );
    }
    println!();
    println!("หมายเหตุ:");
    println!("- x-db เป็น immutable read-only store — SQLite/redb เป็น read-write (จ่ายค่า transaction/locking)");
    println!("- get-miss ของ x-db โดน bloom filter ตัดก่อนแตะข้อมูลเลยเร็วเป็นพิเศษ");
    println!("- HashMap เป็น baseline บน RAM ล้วน (ไม่มีไฟล์ ไม่ต้องเปิด)");

    // เก็บไฟล์ไว้ใน temp — ลบทิ้ง
    for f in ["xdb.xdb", "sqlite.db", "redb.db"] {
        let _ = std::fs::remove_file(dir.join(f));
    }

    oltp_main()?;
    Ok(())
}

// =====================================================================
// OLTP benchmark: เขียน / อัพเดต / ลบ / อ่าน ราย operation
// =====================================================================

const OP_N: usize = 10_000; // จำนวน ops ต่อ phase (fsync-heavy จึงไม่เยอะ)
const BATCH: usize = 1_000; // ขนาด batch

struct OltpRow {
    name: String,
    write_ns: f64,
    batch_ns: f64, // ns/key เมื่อเขียนแบบ batch
    update_ns: f64,
    delete_ns: f64,
    hit_ns: f64,
    miss_ns: f64,
}

fn base_key(i: usize) -> Vec<u8> {
    format!("k:{i:07}").into_bytes()
}
fn op_val(i: usize) -> Vec<u8> {
    format!("value-of-{i}-xxxxxxxxxx").into_bytes()
}

fn oltp_main() -> Result<(), Box<dyn Error>> {
    let dir = std::env::temp_dir().join("xdb_oltp_bench");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    println!();
    println!("=== OLTP: เขียน/อัพเดต/ลบ/อ่าน ราย operation ({OP_N} ops/phase) ===");
    let rows = vec![
        oltp_xdb(&dir, true, "x-db(sync)")?,
        oltp_xdb(&dir, false, "x-db(nosync)")?,
        oltp_sqlite(&dir)?,
        oltp_redb(&dir)?,
        oltp_hashmap(),
    ];

    println!();
    println!(
        "{:<14} {:>11} {:>14} {:>11} {:>11} {:>9} {:>9}",
        "database", "put(ns)", "batch(ns/k)", "update(ns)", "delete(ns)", "get(ns)", "miss(ns)"
    );
    for r in &rows {
        println!(
            "{:<14} {:>11.0} {:>14.1} {:>11.0} {:>11.0} {:>9.0} {:>9.0}",
            r.name, r.write_ns, r.batch_ns, r.update_ns, r.delete_ns, r.hit_ns, r.miss_ns
        );
    }
    println!();
    println!("หมายเหตุ:");
    println!("- x-db(sync) = fsync WAL ทุก op / (nosync) = ปิด fsync (ยอมรับเสีย op ล่าสุดถ้าพังกลางทาง)");
    println!("- SQLite ใช้ WAL + synchronous=NORMAL (ค่าที่แนะนำ) — autocommit ต่อ statement");
    println!("- redb = 1 transaction ต่อ op (durability เต็ม)");
    println!("- ทุก phase ตรวจ correctness (อ่านคืน/นับ) ก่อนขึ้น phase ถัดไป");

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn oltp_xdb(dir: &Path, sync: bool, label: &str) -> Result<OltpRow, Box<dyn Error>> {
    use x_db::store::{StoreOptions, XDBStore};
    let d = dir.join(if sync { "xdb_sync" } else { "xdb_nosync" });
    let store = XDBStore::open_opts(&d, StoreOptions { compact_threshold: 8, flush_entries: 4096, sync })?;

    // preload base keys (batch)
    let base: Vec<(Vec<u8>, Vec<u8>)> = (0..OP_N).map(|i| (base_key(i), op_val(i))).collect();
    {
        let refs: Vec<(&[u8], &[u8])> = base.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        store.put(&refs)?;
    }

    // write: single-key inserts (keys ใหม่)
    let t = Instant::now();
    for i in 0..OP_N {
        let k = format!("w:{i:07}").into_bytes();
        let v = op_val(i);
        store.put(&[(k.as_slice(), v.as_slice())])?;
    }
    let write_ns = ns_per(t, OP_N);
    assert_eq!(store.get(format!("w:{:07}", OP_N - 1).as_bytes())?, Some(op_val(OP_N - 1)));

    // batch write
    let t = Instant::now();
    for b in 0..OP_N / BATCH {
        let batch: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH)
            .map(|j| (format!("b:{:07}", b * BATCH + j).into_bytes(), op_val(j)))
            .collect();
        let refs: Vec<(&[u8], &[u8])> = batch.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
        store.put(&refs)?;
    }
    let batch_ns = ns_per(t, OP_N);
    assert_eq!(store.get(format!("b:{:07}", OP_N - 1).as_bytes())?, Some(op_val(BATCH - 1)));

    // update: overwrite base keys
    let t = Instant::now();
    for i in 0..OP_N {
        let v = format!("updated-{i}").into_bytes();
        store.put(&[(base_key(i).as_slice(), v.as_slice())])?;
    }
    let update_ns = ns_per(t, OP_N);
    assert_eq!(store.get(&base_key(0))?, Some(b"updated-0".to_vec()));

    // get hit / miss
    let t = Instant::now();
    let mut found = 0;
    for i in 0..OP_N {
        if store.get(&base_key(i * 7919 % OP_N))?.is_some() {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, OP_N);
    assert_eq!(found, OP_N);
    let t = Instant::now();
    let mut rejected = 0;
    for i in 0..OP_N {
        if store.get(format!("zzz:{i:07}").as_bytes())?.is_none() {
            rejected += 1;
        }
    }
    let miss_ns = ns_per(t, OP_N);
    assert_eq!(rejected, OP_N);

    // delete
    let t = Instant::now();
    for i in 0..OP_N {
        store.delete(&[base_key(i).as_slice()])?;
    }
    let delete_ns = ns_per(t, OP_N);
    assert_eq!(store.get(&base_key(5))?, None);

    drop(store);
    let _ = std::fs::remove_dir_all(&d);
    Ok(OltpRow {
        name: label.to_string(),
        write_ns,
        batch_ns,
        update_ns,
        delete_ns,
        hit_ns,
        miss_ns,
    })
}

fn oltp_sqlite(dir: &Path) -> Result<OltpRow, Box<dyn Error>> {
    let path = dir.join("sqlite.db");
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         CREATE TABLE kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID;",
    )?;

    // preload (batch)
    {
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO kv VALUES (?, ?)")?;
            for i in 0..OP_N {
                stmt.execute(params![base_key(i), op_val(i)])?;
            }
        }
        tx.commit()?;
    }

    // write: single inserts (autocommit)
    let t = Instant::now();
    for i in 0..OP_N {
        conn.execute("INSERT INTO kv VALUES (?, ?)", params![format!("w:{i:07}"), op_val(i)])?;
    }
    let write_ns = ns_per(t, OP_N);

    // batch write
    let t = Instant::now();
    for b in 0..OP_N / BATCH {
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare("INSERT INTO kv VALUES (?, ?)")?;
            for j in 0..BATCH {
                stmt.execute(params![format!("b:{:07}", b * BATCH + j), op_val(j)])?;
            }
        }
        tx.commit()?;
    }
    let batch_ns = ns_per(t, OP_N);

    // update
    let t = Instant::now();
    for i in 0..OP_N {
        conn.execute("UPDATE kv SET v = ? WHERE k = ?", params![format!("updated-{i}").into_bytes(), base_key(i)])?;
    }
    let update_ns = ns_per(t, OP_N);

    // get hit / miss
    let mut stmt = conn.prepare("SELECT v FROM kv WHERE k = ?")?;
    let t = Instant::now();
    let mut found = 0;
    for i in 0..OP_N {
        let r: Result<Vec<u8>, _> = stmt.query_row(params![base_key(i * 7919 % OP_N)], |row| row.get(0));
        if r.is_ok() {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, OP_N);
    assert_eq!(found, OP_N);
    let t = Instant::now();
    let mut rejected = 0;
    for i in 0..OP_N {
        let r: Result<Vec<u8>, _> = stmt.query_row(params![format!("zzz:{i:07}")], |row| row.get(0));
        if r.is_err() {
            rejected += 1;
        }
    }
    let miss_ns = ns_per(t, OP_N);
    assert_eq!(rejected, OP_N);

    // delete
    let t = Instant::now();
    for i in 0..OP_N {
        conn.execute("DELETE FROM kv WHERE k = ?", params![base_key(i)])?;
    }
    let delete_ns = ns_per(t, OP_N);
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM kv", [], |r| r.get(0))?;
    assert_eq!(count, (OP_N * 2) as i64); // เหลือ w: + b: keys

    drop(stmt);
    drop(conn);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(dir.join("sqlite.db-wal"));
    let _ = std::fs::remove_file(dir.join("sqlite.db-shm"));
    Ok(OltpRow {
        name: "SQLite(WAL)".into(),
        write_ns,
        batch_ns,
        update_ns,
        delete_ns,
        hit_ns,
        miss_ns,
    })
}

fn oltp_redb(dir: &Path) -> Result<OltpRow, Box<dyn Error>> {
    use redb::{Database, TableDefinition};
    const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");
    let path = dir.join("redb.db");
    let _ = std::fs::remove_file(&path);
    let db = Database::create(&path)?;

    // preload (batch ใหญ่)
    {
        let tx = db.begin_write()?;
        {
            let mut t = tx.open_table(TABLE)?;
            for i in 0..OP_N {
                t.insert(base_key(i).as_slice(), op_val(i).as_slice())?;
            }
        }
        tx.commit()?;
    }

    // write: 1 txn ต่อ op
    let t = Instant::now();
    for i in 0..OP_N {
        let tx = db.begin_write()?;
        {
            let mut tb = tx.open_table(TABLE)?;
            tb.insert(format!("w:{i:07}").as_bytes(), op_val(i).as_slice())?;
        }
        tx.commit()?;
    }
    let write_ns = ns_per(t, OP_N);

    // batch write
    let t = Instant::now();
    for b in 0..OP_N / BATCH {
        let tx = db.begin_write()?;
        {
            let mut tb = tx.open_table(TABLE)?;
            for j in 0..BATCH {
                tb.insert(format!("b:{:07}", b * BATCH + j).as_bytes(), op_val(j).as_slice())?;
            }
        }
        tx.commit()?;
    }
    let batch_ns = ns_per(t, OP_N);

    // update
    let t = Instant::now();
    for i in 0..OP_N {
        let tx = db.begin_write()?;
        {
            let mut tb = tx.open_table(TABLE)?;
            tb.insert(base_key(i).as_slice(), format!("updated-{i}").as_bytes())?;
        }
        tx.commit()?;
    }
    let update_ns = ns_per(t, OP_N);

    // get hit / miss (read txn รวม)
    let read = db.begin_read()?;
    let table = read.open_table(TABLE)?;
    let t = Instant::now();
    let mut found = 0;
    for i in 0..OP_N {
        if table.get(base_key(i * 7919 % OP_N).as_slice())?.is_some() {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, OP_N);
    assert_eq!(found, OP_N);
    let t = Instant::now();
    let mut rejected = 0;
    for i in 0..OP_N {
        if table.get(format!("zzz:{i:07}").as_bytes())?.is_none() {
            rejected += 1;
        }
    }
    let miss_ns = ns_per(t, OP_N);
    assert_eq!(rejected, OP_N);
    drop(table);
    drop(read);

    // delete
    let t = Instant::now();
    for i in 0..OP_N {
        let tx = db.begin_write()?;
        {
            let mut tb = tx.open_table(TABLE)?;
            tb.remove(base_key(i).as_slice())?;
        }
        tx.commit()?;
    }
    let delete_ns = ns_per(t, OP_N);
    {
        let read = db.begin_read()?;
        let table = read.open_table(TABLE)?;
        assert!(table.get(base_key(5).as_slice())?.is_none());
        let count = table.range::<&[u8]>(..)?.count();
        assert_eq!(count, OP_N * 2); // เหลือ w: + b:
    }

    drop(db);
    let _ = std::fs::remove_file(&path);
    Ok(OltpRow {
        name: "redb".into(),
        write_ns,
        batch_ns,
        update_ns,
        delete_ns,
        hit_ns,
        miss_ns,
    })
}

fn oltp_hashmap() -> OltpRow {
    let mut map: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(OP_N * 3);
    for i in 0..OP_N {
        map.insert(base_key(i), op_val(i));
    }

    let t = Instant::now();
    for i in 0..OP_N {
        map.insert(format!("w:{i:07}").into_bytes(), op_val(i));
    }
    let write_ns = ns_per(t, OP_N);

    let t = Instant::now();
    for i in 0..OP_N {
        map.insert(base_key(i), format!("updated-{i}").into_bytes());
    }
    let update_ns = ns_per(t, OP_N);

    let t = Instant::now();
    let mut found = 0;
    for i in 0..OP_N {
        if map.contains_key(&base_key(i * 7919 % OP_N)) {
            found += 1;
        }
    }
    let hit_ns = ns_per(t, OP_N);
    assert_eq!(found, OP_N);

    let t = Instant::now();
    for i in 0..OP_N {
        map.remove(&base_key(i));
    }
    let delete_ns = ns_per(t, OP_N);
    assert!(map.contains_key(&format!("w:{:07}", 0).into_bytes()));

    OltpRow {
        name: "HashMap(RAM)".into(),
        write_ns,
        batch_ns: f64::NAN,
        update_ns,
        delete_ns,
        hit_ns,
        miss_ns: f64::NAN,
    }
}
