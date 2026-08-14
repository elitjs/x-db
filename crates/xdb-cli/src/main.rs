//! xdb — CLI tools สำหรับ x-db
//!
//!   xdb check  <file.xdb>            ตรวจความถูกต้องของไฟล์ทั้งหมด (CRC ทุก block)
//!   xdb stats  <file.xdb>            สถิติตาราง (entries, blocks, bloom, อัตราบีบอัด)
//!   xdb dump   <file.xdb> [options]  แสดง entries (--prefix P, --limit N, --keys-only, --start K)
//!   xdb get    <file.xdb> <key>      ค้นหา key เดียว
//!   xdb merge  <out.xdb> <in...>     รวมหลายตาราง (--compress)

use std::io::{self, Write};
use std::process::ExitCode;
use x_db::{merge_tables_with, XDBReader};

const USAGE: &str = "\
xdb — CLI tools สำหรับ x-db

USAGE:
    xdb check  <file.xdb>                 ตรวจไฟล์: CRC ทุก block + key เรียงถูกต้อง + จำนวนตรง footer
    xdb stats  <file.xdb>                 สถิติ: entries / blocks / bloom / อัตราบีบอัด
    xdb dump   <file.xdb> [options]       แสดง entries เรียงตาม key
        --start <key>     เริ่มที่ key >= K (seek)
        --prefix <p>      เฉพาะ key ที่ขึ้นต้นด้วย P
        --limit <n>       แสดงไม่เกิน N entries (default 100)
        --keys-only       แสดงแค่ key
    xdb get    <file.xdb> <key>           ค้นหา key เดียว (คืนค่าเป็น text, ถ้าไม่ใช่ UTF-8 จะแสดงความยาว)
    xdb merge  <out.xdb> <in1.xdb> [...]  รวมหลายตาราง (ตารางหลังสุดชนะ) --compress = บีบอัด LZ4
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let Some(cmd) = args.first() else {
        print!("{USAGE}");
        return Ok(());
    };
    match cmd.as_str() {
        "check" => cmd_check(arg(args, 1)?),
        "stats" => cmd_stats(arg(args, 1)?),
        "dump" => cmd_dump(&args[1..]),
        "get" => {
            let file = arg(args, 1)?;
            let key = arg(args, 2)?;
            cmd_get(file, key.as_bytes())
        }
        "merge" => cmd_merge(&args[1..]),
        "help" | "--help" | "-h" => {
            print!("{USAGE}");
            Ok(())
        }
        other => Err(format!("unknown command `{other}` — try `xdb help`")),
    }
}

fn arg(args: &[String], i: usize) -> Result<&str, String> {
    args.get(i).map(String::as_str).ok_or_else(|| "missing argument — try `xdb help`".to_string())
}

fn flag_value(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

// ---------------- check ----------------

fn cmd_check(path: &str) -> Result<(), String> {
    let reader = XDBReader::open(path).map_err(|e| format!("cannot open: {e}"))?;
    println!("ตรวจไฟล์: {path}");

    // ไล่ทุก entries — CRC ของแต่ละ block จะถูกตรวจตอนแตะครั้งแรก
    let mut count: u64 = 0;
    let mut prev: Option<Vec<u8>> = None;
    let mut tombstones = 0u64;
    for entry in reader.iter() {
        let (key, value) = entry.map_err(|e| format!("CORRUPT at entry #{count}: {e}"))?;
        if let Some(p) = &prev {
            if key.as_slice() <= p.as_slice() {
                return Err(format!("ORDER VIOLATION at entry #{count}: key ไม่เรียงจากน้อยไปมาก"));
            }
        }
        if value.is_none() {
            tombstones += 1;
        }
        prev = Some(key);
        count += 1;
    }

    if count != reader.len() {
        return Err(format!(
            "COUNT MISMATCH: ไล่ได้ {count} entries แต่ footer บอก {}",
            reader.len()
        ));
    }

    println!("  entries        : {count} (tombstone {tombstones})");
    println!("  blocks         : {}", reader.block_count());
    println!("  ✓ ทุก block ผ่าน CRC32, keys เรียงถูกต้อง, จำนวนตรง footer");
    Ok(())
}

// ---------------- stats ----------------

fn cmd_stats(path: &str) -> Result<(), String> {
    let reader = XDBReader::open(path).map_err(|e| format!("cannot open: {e}"))?;
    let file_size = std::fs::metadata(path).map_err(|e| e.to_string())?.len();
    let (compressed_blocks, blocks, raw, stored) = reader.compression_stats();

    println!("ไฟล์        : {path}");
    println!("ขนาดไฟล์    : {} ({:.2} MB)", file_size, file_size as f64 / 1048576.0);
    println!("entries     : {}", reader.len());
    println!("blocks      : {blocks} (~{} bytes/block)", if blocks > 0 { (raw / blocks as u64) as u64 } else { 0 });
    println!("bloom filter: {} KB", reader.bloom_len() / 1024);
    if blocks > 0 {
        println!("บีบอัด      : {compressed_blocks}/{blocks} blocks (LZ4)");
        if raw > 0 {
            println!("  payload   : {} → {} bytes (บีบอัดได้ {:.1}x)", raw, stored, raw as f64 / stored as f64);
        }
    }
    Ok(())
}

// ---------------- dump ----------------

fn cmd_dump(args: &[String]) -> Result<(), String> {
    let path = args.first().ok_or("missing <file.xdb>")?;
    let reader = XDBReader::open(path).map_err(|e| format!("cannot open: {e}"))?;

    let limit: usize = flag_value(args, "--limit").and_then(|v| v.parse().ok()).unwrap_or(100);
    let keys_only = has_flag(args, "--keys-only");
    let prefix = flag_value(args, "--prefix");
    let start = flag_value(args, "--start");

    let iter: Box<dyn Iterator<Item = io::Result<(Vec<u8>, Option<Vec<u8>>)>>> = if let Some(p) =
        &prefix
    {
        Box::new(reader.prefix(p.as_bytes()))
    } else if let Some(s) = &start {
        Box::new(reader.iter_from(s.as_bytes()))
    } else {
        Box::new(reader.iter())
    };

    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    let mut shown = 0usize;
    let mut total = 0usize;
    for entry in iter {
        let (key, value) = entry.map_err(|e| e.to_string())?;
        total += 1;
        if shown >= limit {
            continue;
        }
        shown += 1;
        let key = String::from_utf8_lossy(&key);
        match (&value, keys_only) {
            (Some(v), false) if String::from_utf8_lossy(v).len() <= 200 => {
                writeln!(out, "{key}\t{}", String::from_utf8_lossy(v)).map_err(|e| e.to_string())?;
            }
            (Some(v), false) => {
                writeln!(out, "{key}\t<{} bytes>", v.len()).map_err(|e| e.to_string())?;
            }
            _ => {
                writeln!(out, "{key}\t<tombstone>").map_err(|e| e.to_string())?;
            }
        }
    }
    out.flush().ok();
    if total > shown {
        eprintln!("... แสดง {shown} จาก {total} entries (ใช้ --limit เพิ่มได้)");
    }
    Ok(())
}

// ---------------- get ----------------

fn cmd_get(path: &str, key: &[u8]) -> Result<(), String> {
    let reader = XDBReader::open(path).map_err(|e| format!("cannot open: {e}"))?;
    match reader.get_entry(key).map_err(|e| e.to_string())? {
        Some(Some(v)) => {
            match String::from_utf8(v.clone()) {
                Ok(s) => println!("{s}"),
                Err(_) => {
                    let mut stdout = io::stdout();
                    stdout.write_all(&v).map_err(|e| e.to_string())?;
                    stdout.write_all(b"\n").ok();
                }
            }
            Ok(())
        }
        Some(None) => Err("<tombstone — คีย์ถูกลบ>".to_string()),
        None => Err("not found".to_string()),
    }
}

// ---------------- merge ----------------

fn cmd_merge(args: &[String]) -> Result<(), String> {
    let compress = has_flag(args, "--compress");
    let positional: Vec<&String> = args.iter().filter(|a| !a.starts_with("--")).collect();
    if positional.len() < 2 {
        return Err("merge ต้องมี <out.xdb> และ input อย่างน้อย 1 ไฟล์".to_string());
    }
    let output = positional[0];
    let inputs: Vec<&str> = positional[1..].iter().map(|s| s.as_str()).collect();

    let t = std::time::Instant::now();
    let written = merge_tables_with(&inputs, output, compress).map_err(|e| e.to_string())?;
    let size = std::fs::metadata(output).map_err(|e| e.to_string())?.len();
    println!(
        "รวม {} ไฟล์ → {output}: {written} entries, {} bytes ({:.0} ms{})",
        inputs.len(),
        size,
        t.elapsed().as_millis(),
        if compress { ", LZ4" } else { "" }
    );
    Ok(())
}
