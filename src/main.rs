use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, RwLock},
};
use x_db::{XDBReader, XDBWriter};

type SharedTables = Arc<RwLock<HashMap<String, XDBReader>>>;
type AppState = (Arc<PathBuf>, SharedTables);

fn load_tables(data_dir: &std::path::Path) -> HashMap<String, XDBReader> {
    let mut map = HashMap::new();
    let Ok(rd) = std::fs::read_dir(data_dir) else {
        return map;
    };
    for entry in rd.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("xdb") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        match XDBReader::open(&path) {
            Ok(reader) => {
                map.insert(stem.to_string(), reader);
            }
            Err(e) => eprintln!("skipping {}: {}", path.display(), e),
        }
    }
    map
}

fn decode_opt(decoder: &str, value: &str) -> Result<Vec<u8>, io::Error> {
    B64.decode(value).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid base64 in `{decoder}`: {e}"),
        )
    })
}

/// Accepts either a plain UTF-8 `key` or a base64 `keyB64` (สำหรับ binary keys)
fn decode_field(name_b64: &str, name_plain: &str, b64: &Option<String>, plain: &Option<String>) -> Result<Vec<u8>, io::Error> {
    if let Some(v) = b64 {
        return decode_opt(name_b64, v);
    }
    if let Some(v) = plain {
        return Ok(v.as_bytes().to_vec());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("each entry needs `{name_b64}` or `{name_plain}`"),
    ))
}

fn valid_table_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse { error: msg.into() }),
    )
}

fn not_found(msg: impl Into<String>) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse { error: msg.into() }),
    )
}

// ---------------- GET /api/tables ----------------

#[derive(Serialize)]
struct TablesResponse {
    tables: Vec<String>,
}

async fn list_tables(State((_, tables)): State<AppState>) -> Json<TablesResponse> {
    let mut names: Vec<String> = tables.read().unwrap().keys().cloned().collect();
    names.sort();
    Json(TablesResponse { tables: names })
}

// ---------------- GET /api/get ----------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetParams {
    key: Option<String>,
    key_b64: Option<String>,
    table: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GetResponse {
    table: String,
    key_b64: String,
    value_b64: String,
    value_utf8: Option<String>,
}

async fn get_key(
    State((_, tables)): State<AppState>,
    Query(p): Query<GetParams>,
) -> Result<Json<GetResponse>, (StatusCode, Json<ErrorResponse>)> {
    let key = decode_field("keyB64", "key", &p.key_b64, &p.key).map_err(|e| bad_request(e.to_string()))?;

    let map = tables.read().unwrap();
    // ถ้าไม่ระบุ table ให้หาจากทุก table (เรียงชื่อจากมาไปน้อย = table ใหม่ชนะ)
    let order: Vec<String> = match &p.table {
        Some(t) => {
            if !map.contains_key(t) {
                return Err(not_found(format!("table `{t}` not found")));
            }
            vec![t.clone()]
        }
        None => {
            let mut names: Vec<String> = map.keys().cloned().collect();
            names.sort();
            names.reverse();
            names
        }
    };

    for name in order {
        let value = match map.get(&name).map(|r| r.get(&key)) {
            Some(Ok(v)) => v,
            Some(Err(e)) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse { error: format!("corrupt table `{name}`: {e}") }),
                ))
            }
            None => continue,
        };
        if let Some(value) = value {
            return Ok(Json(GetResponse {
                table: name,
                key_b64: B64.encode(&key),
                value_b64: B64.encode(&value),
                value_utf8: String::from_utf8(value).ok(),
            }));
        }
    }
    Err(not_found("key not found"))
}

// ---------------- POST /api/build ----------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BuildEntry {
    key: Option<String>,
    key_b64: Option<String>,
    value: Option<String>,
    value_b64: Option<String>,
}

#[derive(Deserialize)]
struct BuildRequest {
    table: String,
    entries: Vec<BuildEntry>,
}

#[derive(Serialize)]
struct BuildResponse {
    table: String,
    count: usize,
}

async fn build_table(
    State((data_dir, tables)): State<AppState>,
    Json(req): Json<BuildRequest>,
) -> Result<Json<BuildResponse>, (StatusCode, Json<ErrorResponse>)> {
    if !valid_table_name(&req.table) {
        return Err(bad_request(
            "table name must be 1-128 chars of [A-Za-z0-9_-]",
        ));
    }

    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(req.entries.len());
    for e in &req.entries {
        let key = decode_field("keyB64", "key", &e.key_b64, &e.key).map_err(|err| bad_request(err.to_string()))?;
        let value = decode_field("valueB64", "value", &e.value_b64, &e.value).map_err(|err| bad_request(err.to_string()))?;
        entries.push((key, value));
    }

    // เรียง key + กรอง key ซ้ำ (ตัวหลังสุดชนะ)
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut unique: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        if let Some(last) = unique.last_mut() {
            if last.0 == k {
                last.1 = v;
                continue;
            }
        }
        unique.push((k, v));
    }
    let count = unique.len();

    let tmp = data_dir.join(format!("{}.xdb.tmp", req.table));
    let dst = data_dir.join(format!("{}.xdb", req.table));
    let table_name = req.table.clone();

    std::fs::create_dir_all(data_dir.as_path()).map_err(|e| {
        (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() }))
    })?;

    tokio::task::spawn_blocking({
        let dst = dst.clone();
        move || -> io::Result<()> {
            let refs: Vec<(&[u8], &[u8])> = unique.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
            XDBWriter::write_table(&tmp, &refs)?;
            // Windows: rename ไม่สามารถทับไฟล์เดิมได้ ต้องลบก่อน
            if dst.exists() {
                std::fs::remove_file(&dst)?;
            }
            std::fs::rename(&tmp, &dst)?;
            Ok(())
        }
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;

    let reader = XDBReader::open(&dst).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e.to_string() }),
        )
    })?;
    tables.write().unwrap().insert(table_name, reader);

    Ok(Json(BuildResponse { table: req.table, count }))
}

// ---------------- POST /api/reload ----------------

#[derive(Serialize)]
struct ReloadResponse {
    tables: usize,
}

async fn reload(State((data_dir, tables)): State<AppState>) -> Json<ReloadResponse> {
    let loaded = tokio::task::spawn_blocking({
        let data_dir = data_dir.clone();
        move || load_tables(&data_dir)
    })
    .await
    .unwrap_or_default();
    let n = loaded.len();
    *tables.write().unwrap() = loaded;
    Json(ReloadResponse { tables: n })
}

#[tokio::main]
async fn main() {
    let data_dir = PathBuf::from(
        std::env::var("XDB_DATA_DIR").unwrap_or_else(|_| "data".to_string()),
    );
    let addr: SocketAddr = std::env::var("XDB_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7878".to_string())
        .parse()
        .expect("invalid XDB_ADDR");

    std::fs::create_dir_all(&data_dir).expect("cannot create data dir");
    let tables = Arc::new(RwLock::new(load_tables(&data_dir)));

    let app = Router::new()
        .route("/api/tables", get(list_tables))
        .route("/api/get", get(get_key))
        .route("/api/build", post(build_table))
        .route("/api/reload", post(reload))
        .with_state((Arc::new(data_dir.clone()), tables));

    let listener = tokio::net::TcpListener::bind(addr).await.expect("cannot bind");
    println!("x-db server listening on http://{addr} (data dir: {})", data_dir.display());
    axum::serve(listener, app).await.unwrap();
}
