use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, Responder, get, post, web};
use chrono;
use dotenvy::dotenv;
use std::{env, fs::File, io::BufReader, sync::Mutex, time::Duration};

// DB Interaction
use sea_orm::{ConnectOptions, Database, DatabaseConnection, EntityTrait, entity::prelude::*};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

// Web Socket
use futures_util::StreamExt;
use tokio::sync::broadcast;

/* #region DB ENTITIES */

pub mod temperature {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "temperatures")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub date: Date,
        pub temperature: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod humidity {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
    #[sea_orm(table_name = "humidities")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub date: Date,
        pub humidity: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/* #endregion */

/* #region INTERFACE STRUCTS */

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Temperature {
    date: Date,
    temperature: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Humidity {
    date: Date,
    humidity: f64,
}

#[derive(Deserialize)]
struct DateRange {
    start_date: Date,
    end_date: Date,
}

#[derive(Deserialize)]
struct Info {
    username: String,
}

/* #endregion */

struct AppState {
    app_name: String,
    conn: Option<DatabaseConnection>,
    tx: broadcast::Sender<Vec<u8>>,
    counter: Mutex<i32>, // <- Mutex is necessary to mutate safely across threads
}

async fn db_connection() -> Option<DatabaseConnection> {
    let db_url = env::var("DATABASE_URL").unwrap_or_else(|_| "DB URL NOT SET".to_string());

    let mut opt = ConnectOptions::new(db_url);

    // Configure connection behavior
    opt.max_connections(10)
        .min_connections(1)
        .connect_timeout(Duration::from_secs(8))
        .acquire_timeout(Duration::from_secs(8))
        .idle_timeout(Duration::from_secs(8));

    // Panic on connection fail
    // let conn = Database::connect(opt).await.unwrap_or_else(|err| {
    //     eprintln!("Warning: Initial connection failed: {err}");
    //     panic!("Could not connect to database");
    // });

    let conn = Database::connect(opt).await;

    return conn.ok();
}

fn tls_config() -> rustls::ServerConfig {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    let mut certs_file = BufReader::new(File::open("cert.pem").unwrap());
    let mut key_file = BufReader::new(File::open("key.pem").unwrap());

    // load TLS certs and key -> create self signed
    let tls_certs = rustls_pemfile::certs(&mut certs_file)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let tls_key = rustls_pemfile::pkcs8_private_keys(&mut key_file)
        .next()
        .unwrap()
        .unwrap();

    // set up TLS config options
    let tls_config: rustls::ServerConfig = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(tls_certs, rustls::pki_types::PrivateKeyDer::Pkcs8(tls_key))
        .unwrap();

    return tls_config;
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());

    // DB Connection
    let conn = db_connection().await;

    // App State
    let (tx, _) = broadcast::channel::<Vec<u8>>(500);

    let state = web::Data::new(AppState {
        app_name: String::from("Sentinel API"),
        conn,
        tx,
        counter: Mutex::new(0),
    });

    // TLS / HTTPS
    let tls_config = tls_config();

    // Start Server
    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            // Set max payload size to 10MB for all WebSocket routes
            .app_data(web::PayloadConfig::new(10 * 1024 * 1024))
            // Main Routes
            .service(hello)
            .service(get_date)
            .service(echo)
            .service(path_test)
            .service(query_test)
            .service(submit)
            .route("/hey", web::get().to(manual_hello))
            // DB Routes
            .service(add_temperature)
            .service(add_temperature_test)
            .service(get_temperature_range)
            .service(add_humidity)
            .service(get_humiditie_range)
            // Web Socket Routes
            .service(publish_stream)
            .service(subscribe_stream)
            .service(ws_publish)
    })
    .keep_alive(Duration::from_secs(75))
    .bind_rustls_0_23(("127.0.0.1", port.parse().unwrap()), tls_config)?
    .run()
    .await
}

/* #region MAIN ROUTES */

#[get("/")]
async fn hello(data: web::Data<AppState>) -> impl Responder {
    let app_name = &data.app_name;
    let mut counter = data.counter.lock().unwrap(); // <- get counter's MutexGuard
    *counter += 1; // <- access counter inside MutexGuard
    println!("Counter = {}", counter);
    HttpResponse::Ok().body(format!("Hello {}, {}", app_name, counter))
}

#[get("/date")]
async fn get_date(state: web::Data<AppState>) -> impl Responder {
    let current_date = chrono::Utc::now().naive_utc().date();
    HttpResponse::Ok().body(format!("current date : {}", current_date.to_string()))
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

/// extract path info from "/users/{user_id}/{friend}" url
/// {user_id} - deserializes to a u32
/// {friend} - deserializes to a String
#[get("/users/{user_id}/{friend}")] // <- define path parameters
async fn path_test(path: web::Path<(u32, String)>) -> actix_web::Result<String> {
    let (user_id, friend) = path.into_inner();
    Ok(format!("Welcome {}, user_id {}!", friend, user_id))
}

#[get("/query")]
async fn query_test(info: web::Query<Info>) -> String {
    format!("Welcome {}!", info.username)
}

/// deserialize `Info` from request's body
#[post("/submit")]
async fn submit(info: web::Json<Info>) -> actix_web::Result<String> {
    Ok(format!("Welcome {}!", info.username))
}

/* #endregion */

/* #region DB ROUTES */

#[post("/temperature")]
async fn add_temperature(
    state: web::Data<AppState>,
    payload: web::Json<Temperature>,
) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    let new_entry = temperature::ActiveModel {
        date: sea_orm::Set(payload.date),
        temperature: sea_orm::Set(payload.temperature),
    };

    match new_entry.insert(conn).await {
        Ok(inserted) => HttpResponse::Created().json(inserted),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

#[post("/temperature_test")]
async fn add_temperature_test(state: web::Data<AppState>, payload: String) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    println!("Received Raw Payload: '{}'", payload);

    // Placeholder data
    let new_entry = temperature::ActiveModel {
        date: sea_orm::Set(chrono::NaiveDate::from_ymd_opt(2024, 6, 1).unwrap()),
        temperature: sea_orm::Set(25.0),
    };

    match new_entry.insert(conn).await {
        Ok(inserted) => HttpResponse::Created().json(inserted),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

/// Get the first Temperature in the Date Range
#[get("/temperature")]
async fn get_temperature(
    state: web::Data<AppState>,
    date_range: web::Query<DateRange>,
) -> web::Json<Temperature> {
    let Some(conn) = &state.conn else {
        let current_date = chrono::Utc::now().naive_utc().date();
        return web::Json(Temperature {
            date: current_date,
            temperature: 20.0,
        });
    };

    let start_date = date_range.start_date;
    let end_date = date_range.end_date;

    // Query between date range
    let temperatures = temperature::Entity::find()
        .filter(temperature::Column::Date.between(start_date, end_date))
        .all(conn)
        .await
        .expect("Failed to fetch temperature data");

    // return first
    if let Some(temp) = temperatures.first() {
        web::Json(Temperature {
            date: temp.date,
            temperature: temp.temperature,
        })
    } else {
        web::Json(Temperature {
            date: start_date,
            temperature: 0.0,
        })
    }
}

/// Get all Temperatures in the Date Range
#[get("/temperature")]
async fn get_temperature_range(
    state: web::Data<AppState>,
    date_range: web::Query<DateRange>,
) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    let start_date = date_range.start_date;
    let end_date = date_range.end_date;

    // Query between date range
    let temperatures = temperature::Entity::find()
        .filter(temperature::Column::Date.between(start_date, end_date))
        .all(conn)
        .await
        .expect("Failed to fetch temperature data");

    let mut map = Map::new();

    for (index, record) in temperatures.into_iter().enumerate() {
        let i = index + 1;
        map.insert(format!("date{}", i), Value::String(record.date.to_string()));
        map.insert(format!("temperature{}", i), json!(record.temperature));
    }

    HttpResponse::Ok().json(Value::Object(map))
}

// HUMIDITY ROUTES

#[post("/humidity")]
async fn add_humidity(state: web::Data<AppState>, payload: web::Json<Humidity>) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    let new_entry = humidity::ActiveModel {
        date: sea_orm::Set(payload.date),
        humidity: sea_orm::Set(payload.humidity),
    };

    match new_entry.insert(conn).await {
        Ok(inserted) => HttpResponse::Created().json(inserted),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

/// Get all Humidities in the Date Range
#[get("/humidity")]
async fn get_humiditie_range(
    state: web::Data<AppState>,
    date_range: web::Query<DateRange>,
) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    let start_date = date_range.start_date;
    let end_date = date_range.end_date;

    // Query between date range
    let humidities = humidity::Entity::find()
        .filter(humidity::Column::Date.between(start_date, end_date))
        .all(conn)
        .await
        .expect("Failed to fetch humidity data");

    let response: Vec<Value> = humidities
        .into_iter()
        .map(|record| {
            json!({
                "date": record.date.to_string(),
                "humidity": record.humidity,
            })
        })
        .collect();

    HttpResponse::Ok().json(response)
}

/* #endregion */

/* #region WEB SOCKETS ROUTES */

/// Connect to a Socket and Send Text Messages
#[get("/message")]
pub async fn ws_publish(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    println!("Connection Attempt");
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            println!("Message");
            match msg {
                actix_ws::Message::Text(text) => println!("Received: {text}"),
                actix_ws::Message::Ping(bytes) => {
                    if session.pong(&bytes).await.is_err() {
                        println!("Ping Error");
                        break;
                    }
                }
                actix_ws::Message::Close(reason) => break,
                _ => (),
            }
        }
    });

    Ok(response)
}

/// Connect to Socket and Send Bytes (Video Stream)
#[get("/stream")]
async fn publish_stream(
    req: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    println!("Stream Connection");
    let (response, session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    let tx = state.tx.clone();

    actix_web::rt::spawn(async move {
        let mut session = session;

        while let Some(res) = msg_stream.next().await {
            match res {
                Ok(actix_ws::Message::Ping(bytes)) => {
                    if session.pong(&bytes).await.is_err() {
                        println!("Ping Error");
                        break;
                    }
                }
                Ok(actix_ws::Message::Binary(bytes)) => {
                    // Broadcast raw video chunk to all active consumers
                    println!("Received video chunk: {} bytes", bytes.len());
                    let _ = tx.send(bytes.to_vec());
                }
                Ok(actix_ws::Message::Close(reason)) => {
                    let _ = session.close(reason).await;
                    break;
                }
                Err(err) => {
                    println!("WebSocket Frame Error: {:?}", err);
                    break;
                }
                _ => {}
            }
        }

        println!("Stream disconnected");
    });

    Ok(response)
}

#[get("/watch")]
async fn subscribe_stream(
    req: HttpRequest,
    stream: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let (res, session, _msg_stream) = actix_ws::handle(&req, stream)?;

    let mut rx = data.tx.subscribe();

    actix_web::rt::spawn(async move {
        let mut session = session;

        while let Ok(bytes) = rx.recv().await {
            if session.binary(bytes).await.is_err() {
                println!("Stopped Watching");
                break;
            }
        }
    });

    Ok(res)
}

/* #endregion */
