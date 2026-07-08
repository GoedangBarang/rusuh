use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::env;
use tower_http::cors::{Any, CorsLayer};

#[derive(Deserialize)]
struct WebhookPayload {
    action: String,
    id: String,
    #[serde(rename = "type")]
    signal_type: Option<String>,
    entry: Option<f64>,
    tp: Option<f64>,
    sl: Option<f64>, // TAMBAHAN: Menerima SL
    reason: Option<String>,
}

#[derive(Serialize, sqlx::FromRow)]
struct SignalData {
    id: String,
    signal_type: String,
    entry_price: bigdecimal::BigDecimal,
    tp_price: bigdecimal::BigDecimal,
    sl_price: bigdecimal::BigDecimal, // TAMBAHAN: Mengirim SL
    is_tp_hit: bool, // TAMBAHAN: Status Take Profit
}

#[tokio::main]
async fn main() {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL wajib diisi");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Gagal terhubung ke Supabase");

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/api/signals", get(get_signals))
        .route("/api/webhook", post(handle_webhook))
        .layer(cors)
        .with_state(pool);

    let port = env::var("PORT").unwrap_or_else(|_| "10000".to_string());
    let addr = format!("0.0.0.0:{}", port);
    
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("🚀 Backend berjalan di {}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn handle_webhook(
    State(pool): State<Pool<Postgres>>,
    Json(payload): Json<WebhookPayload>,
) -> StatusCode {
    if payload.action == "new_signal" {
        let result = sqlx::query(
            "INSERT INTO active_signals (id, signal_type, entry_price, tp_price, sl_price, is_tp_hit) 
             VALUES ($1, $2, $3, $4, $5, false) 
             ON CONFLICT (id) DO NOTHING"
        )
        .bind(&payload.id)
        .bind(payload.signal_type.unwrap_or_default())
        .bind(payload.entry.unwrap_or(0.0) as f64)
        .bind(payload.tp.unwrap_or(0.0) as f64)
        .bind(payload.sl.unwrap_or(0.0) as f64)
        .execute(&pool)
        .await;

        match result {
            Ok(_) => StatusCode::CREATED,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    } else if payload.action == "tp_hit" {
        // PERINTAH BARU: Update status TP menjadi TRUE
        let result = sqlx::query("UPDATE active_signals SET is_tp_hit = true WHERE id = $1")
            .bind(&payload.id)
            .execute(&pool)
            .await;

        match result {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    } else if payload.action == "delete_signal" {
        let result = sqlx::query("DELETE FROM active_signals WHERE id = $1")
            .bind(&payload.id)
            .execute(&pool)
            .await;

        match result {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    } else {
        // Jika perintah tidak dikenali, kirim Error 400
        StatusCode::BAD_REQUEST
    }
}

async fn get_signals(State(pool): State<Pool<Postgres>>) -> Json<Vec<SignalData>> {
    let signals = sqlx::query_as::<_, SignalData>(
        // PERUBAHAN: ORDER BY id DESC agar terurut kronologis sesuai kalender
        "SELECT id, signal_type, entry_price, tp_price, sl_price, is_tp_hit FROM active_signals ORDER BY id DESC"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    Json(signals)
}
