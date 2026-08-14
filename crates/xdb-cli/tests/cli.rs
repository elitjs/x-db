//! Integration tests — รัน binary `xdb` จริงผ่าน CARGO_BIN_EXE
use std::path::PathBuf;
use std::process::Command;
use x_db::{TableBuilder, XDBWriter};

fn exe() -> Command {
    Command::new(env!("CARGO_BIN_EXE_xdb"))
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("xdb_cli_{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// สร้างตารางทดสอบ: key:000..key:N (ค่าอัดง่ายถ้าอยากให้บีบอัดได้)
fn make_table(path: &std::path::Path, n: usize, compress: bool) {
    let mut b = TableBuilder::create_with(path, n, compress).unwrap();
    for i in 0..n {
        b.add(format!("key:{:06}", i).as_bytes(), format!("value-of-{}", i).repeat(4).as_bytes())
            .unwrap();
    }
    b.finish().unwrap();
}

fn output(cmd: &mut Command) -> (bool, String) {
    let out = cmd.output().unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

#[test]
fn check_passes_on_valid_file() {
    let dir = temp_dir("check_ok");
    let table = dir.join("t.xdb");
    make_table(&table, 500, false);

    let (ok, out) = output(exe().arg("check").arg(&table));
    assert!(ok, "exit must be 0, got: {out}");
    assert!(out.contains("500"), "entries count in output: {out}");
    assert!(out.contains("✓"), "success marker: {out}");
}

#[test]
fn check_fails_on_corrupted_file() {
    let dir = temp_dir("check_bad");
    let table = dir.join("t.xdb");
    make_table(&table, 500, false);

    // flip ไบต์ใน block แรก
    let raw = std::fs::read(&table).unwrap();
    let mut corrupted = raw.clone();
    corrupted[32 + 10] ^= 0xFF;
    std::fs::write(&table, &corrupted).unwrap();

    let (ok, out) = output(exe().arg("check").arg(&table));
    assert!(!ok, "exit must be non-zero on corrupt file");
    assert!(out.contains("CORRUPT"), "corruption message: {out}");
}

#[test]
fn check_passes_on_compressed_file() {
    let dir = temp_dir("check_lz4");
    let table = dir.join("t.xdb");
    make_table(&table, 500, true);

    let (ok, out) = output(exe().arg("check").arg(&table));
    assert!(ok, "compressed table must pass check: {out}");
}

#[test]
fn stats_shows_compression_ratio() {
    let dir = temp_dir("stats");
    let table = dir.join("t.xdb");
    make_table(&table, 5000, true);

    let (ok, out) = output(exe().arg("stats").arg(&table));
    assert!(ok, "{out}");
    assert!(out.contains("entries     : 5000"), "{out}");
    assert!(out.contains("LZ4"), "{out}");
    assert!(out.contains("บีบอัดได้"), "{out}");
}

#[test]
fn dump_supports_prefix_and_limit() {
    let dir = temp_dir("dump");
    let table = dir.join("t.xdb");
    make_table(&table, 100, false);

    // prefix
    let (ok, out) = output(exe().arg("dump").arg(&table).arg("--prefix").arg("key:00009"));
    assert!(ok, "{out}");
    assert!(out.contains("key:000090"), "{out}");
    assert!(out.contains("key:000099"), "{out}");
    assert!(!out.contains("key:00008"), "{out}");

    // limit
    let (ok, out) = output(exe().arg("dump").arg(&table).arg("--limit").arg("3"));
    assert!(ok, "{out}");
    let lines = out.lines().filter(|l| l.starts_with("key:")).count();
    assert_eq!(lines, 3, "{out}");
    assert!(out.contains("แสดง 3 จาก 100"), "{out}");

    // start/seek
    let (ok, out) = output(exe().arg("dump").arg(&table).arg("--start").arg("key:000098"));
    assert!(ok, "{out}");
    assert!(out.contains("key:000098"), "{out}");
    assert!(!out.contains("key:000097"), "{out}");
}

#[test]
fn get_finds_and_reports_missing() {
    let dir = temp_dir("get");
    let table = dir.join("t.xdb");
    make_table(&table, 10, false);

    let (ok, out) = output(exe().arg("get").arg(&table).arg("key:000003"));
    assert!(ok, "{out}");
    assert!(out.contains("value-of-3"), "{out}");

    let (ok, out) = output(exe().arg("get").arg(&table).arg("nope"));
    assert!(!ok, "missing key must exit non-zero");
    assert!(out.contains("not found"), "{out}");
}

#[test]
fn merge_combines_tables() {
    let dir = temp_dir("merge");
    let t1 = dir.join("t1.xdb");
    let t2 = dir.join("t2.xdb");
    let out_path = dir.join("merged.xdb");

    let refs1: Vec<(&[u8], &[u8])> = vec![(b"a", b"1"), (b"b", b"old")];
    let refs2: Vec<(&[u8], &[u8])> = vec![(b"b", b"new"), (b"c", b"3")];
    XDBWriter::write_table(&t1, &refs1).unwrap();
    XDBWriter::write_table(&t2, &refs2).unwrap();

    let (ok, out) = output(exe().arg("merge").arg(&out_path).arg(&t1).arg(&t2));
    assert!(ok, "{out}");
    assert!(out.contains("3 entries"), "{out}");

    // ตรวจผลลัพธ์ — t2 (หลังสุด) ชนะ
    let (ok, out) = output(exe().arg("get").arg(&out_path).arg("b"));
    assert!(ok, "{out}");
    assert!(out.contains("new"), "{out}");
}

#[test]
fn help_shows_usage() {
    let (ok, out) = output(exe().arg("help"));
    assert!(ok, "{out}");
    assert!(out.contains("xdb check"), "{out}");

    let (ok2, out2) = output(&mut exe());
    assert!(ok2, "{out2}");
    assert!(out2.contains("USAGE"), "{out2}");
}
