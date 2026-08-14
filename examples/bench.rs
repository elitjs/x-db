//! Benchmark พื้นฐาน: build / point lookup / iteration
//! รัน: cargo run --release --example bench
use std::io;
use std::time::Instant;
use x_db::{XDBReader, XDBStore, XDBWriter};

fn main() -> io::Result<()> {
    const N: usize = 100_000;
    const VAL_SIZE: usize = 100;
    const LOOKUPS: usize = 100_000;

    let dir = std::env::temp_dir().join("xdb_bench");
    std::fs::create_dir_all(&dir)?;
    let path = dir.join("bench.xdb");

    // --- build ---
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
        .map(|i| (format!("key:{:012}", i).into_bytes(), vec![(i % 251) as u8; VAL_SIZE]))
        .collect();
    let refs: Vec<(&[u8], &[u8])> = entries.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();

    let t = Instant::now();
    XDBWriter::write_table(&path, &refs)?;
    let build_ms = t.elapsed().as_millis();
    let file_mb = std::fs::metadata(&path)?.len() as f64 / 1024.0 / 1024.0;

    let reader = XDBReader::open(&path)?;
    println!("build   : {N} entries ({VAL_SIZE}B each) ใน {build_ms} ms  (ไฟล์ {file_mb:.1} MB, {} blocks)", reader.block_count());

    // --- point lookup: hit ---
    let t = Instant::now();
    let mut found = 0usize;
    for i in 0..LOOKUPS {
        let key = format!("key:{:012}", (i * 7919) % N); // 7919 เป็น prime → กระจายทั่วตาราง
        if reader.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let hit_ns = t.elapsed().as_nanos() as f64 / LOOKUPS as u64 as f64;
    println!("get hit : {found}/{} พบ  เฉลี่ย {:.0} ns/op (รวม first-touch CRC/page faults)", LOOKUPS, hit_ns);

    // --- point lookup: hit อีกรอบ (warm cache) ---
    let t = Instant::now();
    let mut found2 = 0usize;
    for i in 0..LOOKUPS {
        let key = format!("key:{:012}", (i * 7919) % N);
        if reader.get(key.as_bytes())?.is_some() {
            found2 += 1;
        }
    }
    let warm_ns = t.elapsed().as_nanos() as f64 / LOOKUPS as u64 as f64;
    println!("get warm: {found2}/{} พบ  เฉลี่ย {:.0} ns/op (block cache ร้อนแล้ว)", LOOKUPS, warm_ns);

    // --- point lookup: same-key ซ้ำ ๆ (วัดค่าคงที่ของ hot path ล้วน ๆ) ---
    let hot = b"key:000000050000";
    let t = Instant::now();
    for _ in 0..LOOKUPS {
        if reader.get(hot).unwrap().is_none() {
            panic!("hot key must be found");
        }
    }
    let same_ns = t.elapsed().as_nanos() as f64 / LOOKUPS as u64 as f64;
    println!("same-key: เฉลี่ย {:.0} ns/op (key เดิมซ้ำ ๆ = ทุกอย่างร้อนสุด)", same_ns);

    // --- point lookup: miss (bloom filter ต้องตัดได้เร็ว) ---
    let t = Instant::now();
    let mut rejected = 0usize;
    for i in 0..LOOKUPS {
        let key = format!("zzz:{:012}", i); // ไม่มีในตารางแน่นอน
        if reader.get(key.as_bytes())?.is_none() {
            rejected += 1;
        }
    }
    let miss_ns = t.elapsed().as_nanos() as f64 / LOOKUPS as u64 as f64;
    println!("get miss: {rejected}/{} ตัดแล้ว เฉลี่ย {:.0} ns/op", LOOKUPS, miss_ns);

    // --- full iteration ---
    let t = Instant::now();
    let mut count = 0usize;
    let mut checksum = 0usize;
    for entry in reader.iter() {
        let (k, v) = entry?;
        checksum = checksum.wrapping_add(k.len() + v.unwrap().len());
        count += 1;
    }
    let iter_ms = t.elapsed().as_millis();
    println!("iterate : {count} entries ใน {iter_ms} ms (checksum {checksum:#x})");

    // --- range scan: keys 50000..=50999 ---
    let t = Instant::now();
    let start = b"key:000000050000".as_slice();
    let end = b"key:000000050999".as_slice();
    let range_count = reader
        .iter()
        .map(|r| r.unwrap())
        .skip_while(|(k, _)| *k < start)
        .take_while(|(k, _)| *k <= end)
        .count();
    let range_us = t.elapsed().as_micros();
    println!("range   : {range_count} entries ใน {range_us} µs");

    // --- XDBStore: realtime put/get latency ---
    let store_dir = std::env::temp_dir().join("xdb_bench_store");
    let _ = std::fs::remove_dir_all(&store_dir);
    let store = XDBStore::open(&store_dir)?;

    // โหลดข้อมูลเริ่มต้น 100k entries ลง store
    store.put(&refs)?;
    let t = Instant::now();
    for i in 0..100u32 {
        let key = format!("key:{:012}", (i * 7919) as usize % N);
        let val = format!("realtime-{i}").into_bytes();
        store.put(&[(key.as_bytes(), val.as_slice())])?;
    }
    let put_ms = t.elapsed().as_secs_f64() * 1000.0 / 100.0;
    println!("store   : single-key put เฉลี่ย {put_ms:.2} ms/op (รวม fsync) — layer_count = {}", store.layer_count());

    let t = Instant::now();
    let mut found = 0;
    for i in 0..100_000usize {
        let key = format!("key:{:012}", (i * 7919) % N);
        if store.get(key.as_bytes())?.is_some() {
            found += 1;
        }
    }
    let store_get_ns = t.elapsed().as_secs_f64() * 1e9 / 100_000.0;
    println!("store   : get เจอ {found}/100000 เฉลี่ย {store_get_ns:.0} ns/op (ข้าม layers)");

    let _ = std::fs::remove_dir_all(&store_dir);
    let _ = std::fs::remove_file(&path);
    Ok(())
}

