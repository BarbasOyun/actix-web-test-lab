use actix_web::{App, Error, HttpRequest, HttpResponse, HttpServer, Responder, get, post, web};
use actix_ws::{AggregatedMessage, Message, Session};
use chrono;
use dotenvy::dotenv;
use std::{env, fs::File, io::BufReader, sync::Mutex, time::Duration};
use tokio::sync::broadcast::error::RecvError;

// DB Interaction
use sea_orm::{
    ConnectOptions, Database, DatabaseConnection, EntityTrait, QueryOrder, entity::prelude::*,
};
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
        pub date_time: DateTimeUtc,
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
        pub date_time: DateTimeUtc,
        pub humidity: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

/* #endregion */

/* #region INTERFACE STRUCTS */

#[derive(Deserialize)]
struct Info {
    username: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Temperature {
    date_time: DateTimeUtc,
    temperature: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Humidity {
    date_time: DateTimeUtc,
    humidity: f64,
}

#[derive(Deserialize)]
struct DateRange {
    start_date: DateTimeUtc,
    end_date: DateTimeUtc,
}

/* #endregion */

struct AppState {
    app_name: String,
    conn: Option<DatabaseConnection>,
    counter: Mutex<i32>, // <- Mutex is necessary to mutate safely across threads
    // Websocket
    // session: Session
    latest_keyframe: Mutex<Option<Vec<u8>>>,
    tx: broadcast::Sender<Vec<u8>>,
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

    if let Err(err) = conn {
        println!("Failed to connect to DB : {}", err);
        return Option::None;
    }

    return conn.ok();
}

fn tls_config() -> rustls::ServerConfig {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .unwrap();

    // Get Certificates
    // let mut certs_file = BufReader::new(File::open("cert.pem").unwrap());
    // let mut key_file = BufReader::new(File::open("key.pem").unwrap());
    let mut certs_file = BufReader::new(File::open("certs/cert.pem").unwrap());
    let mut key_file = BufReader::new(File::open("certs/key.pem").unwrap());

    // load TLS certs and key
    let tls_certs = match rustls_pemfile::certs(&mut certs_file).collect::<Result<Vec<_>, _>>() {
        Ok(cert) => cert,
        Err(err) => panic!("Error : {}", err.to_string()),
    };

    let tls_key = match rustls_pemfile::pkcs8_private_keys(&mut key_file)
        .next()
        .unwrap()
    {
        Ok(key) => key,
        Err(err) => panic!("Error : {}", err.to_string()),
    };

    // set up TLS config options
    let tls_config: rustls::ServerConfig = match rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(tls_certs, rustls::pki_types::PrivateKeyDer::Pkcs8(tls_key))
    {
        Ok(conf) => conf,
        Err(err) => panic!("Error : {}", err.to_string()),
    };

    println!("Certificates loaded");

    return tls_config;
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    println!("---APP STARTING---");
    dotenv().ok();
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

    // DB Connection
    let conn = db_connection().await;

    // App State
    let (tx, _) = broadcast::channel::<Vec<u8>>(500);

    let state = web::Data::new(AppState {
        app_name: String::from("Actix TestLab"),
        conn,
        tx,
        counter: Mutex::new(0),
        latest_keyframe: Mutex::new(Option::None),
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
            .route("/hey", web::get().to(manual_hello))
            .service(get_date)
            .service(echo)
            .service(path_test)
            .service(query_test)
            .service(submit)
            // DB Routes
            .service(add_temperature)
            .service(get_temperature)
            .service(get_temperature_range)
            .service(add_humidity)
            .service(get_humiditie_range)
            // Web Socket Routes
            .service(publish_stream)
            .service(subscribe_stream)
            .service(ws_publish)
    })
    .keep_alive(Duration::from_secs(75))
    .bind_rustls_0_23((host.to_string(), port.parse().unwrap()), tls_config)?
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

/// Manual Route
async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[get("/date")]
async fn get_date() -> impl Responder {
    let date_time = chrono::Utc::now();
    let date = chrono::Utc::now().naive_utc().date();
    let time = chrono::Utc::now().time();

    let local_date_time = chrono::Local::now();
    let local_date = chrono::Local::now().naive_utc().date();
    let local_time = chrono::Local::now().time();

    HttpResponse::Ok().body(
        format!("UTC :\ncurrent date + time : {}\ncurrent date : {}\ncurrent time : {}\n\nLocal : \ncurrent date + time : {}\ncurrent date : {}\ncurrent time : {}",
        date_time.to_string(), date.to_string(), time.to_string(),
        local_date_time.to_string(), local_date.to_string(), local_time.to_string()))
}

/// String -> JSON
#[get("/json")]
async fn parse_json() -> impl Responder {
    let json_str = r#"{"temperature": 10}"#;
    let parsed: Option<Value> = serde_json::from_str(json_str).ok();

    let Some(parsed) = parsed else {
        return HttpResponse::InternalServerError().body("Failed to parse Sensor JSON");
    };

    // Check if the root value is a JSON Object
    if let Some(map) = parsed.as_object() {
        for (key, value) in map {
            println!("Key: {}, Value: {}", key, value);
        }
    }

    return HttpResponse::Ok().json(parsed);
}

/// Receive String
#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

/// extract path info from "/users/{user_id}/{friend}" url
#[get("/users/{user_id}/{friend}")] // <- define path parameters
async fn path_test(path: web::Path<(u32, String)>) -> actix_web::Result<String> {
    let (user_id, friend) = path.into_inner();
    Ok(format!("Welcome {}, user_id {}!", friend, user_id))
}

#[get("/query")]
async fn query_test(info: web::Query<Info>) -> String {
    format!("Welcome {}!", info.username)
}

/// Receive JSON
#[post("/submit")]
async fn submit(info: web::Json<Info>) -> actix_web::Result<String> {
    Ok(format!("Welcome {}!", info.username))
}

/* #endregion */

/* #region DB ROUTES */

// CRUD operations per tables

/// Receive a JSON -> Create in DB
#[post("/temperature")]
async fn add_temperature(
    state: web::Data<AppState>,
    payload: web::Json<Temperature>,
) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    let new_entry = temperature::ActiveModel {
        date_time: sea_orm::Set(payload.date_time),
        temperature: sea_orm::Set(payload.temperature),
    };

    match new_entry.insert(conn).await {
        Ok(inserted) => HttpResponse::Created().json(inserted),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

/// Get Latest temperature
#[get("/temperature")]
async fn get_temperature(state: web::Data<AppState>) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body("No DB Connection");
    };

    let latest_temp = temperature::Entity::find()
        .order_by_desc(temperature::Column::DateTime)
        .one(conn)
        .await;

    let Ok(latest_temp) = latest_temp else {
        return HttpResponse::InternalServerError().body("DB Fail");
    };

    let Some(latest_temp) = latest_temp else {
        return HttpResponse::InternalServerError().body("DB Fail");
    };

    return HttpResponse::Ok().json(latest_temp);
}

/// Get all Temperatures in the Date Range
#[get("/temperature/range")]
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
        .filter(temperature::Column::DateTime.between(start_date, end_date))
        .all(conn)
        .await
        .expect("Failed to fetch temperature data");

    let mut map = Map::new();

    for (index, record) in temperatures.into_iter().enumerate() {
        let i = index + 1;
        map.insert(
            format!("date{}", i),
            Value::String(record.date_time.to_string()),
        );
        map.insert(format!("temperature{}", i), json!(record.temperature));
    }

    HttpResponse::Ok().json(Value::Object(map))
}

#[post("/humidity")]
async fn add_humidity(state: web::Data<AppState>, payload: web::Json<Humidity>) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body(format!("No DB Connection"));
    };

    let new_entry = humidity::ActiveModel {
        date_time: sea_orm::Set(payload.date_time),
        humidity: sea_orm::Set(payload.humidity),
    };

    match new_entry.insert(conn).await {
        Ok(inserted) => HttpResponse::Created().json(inserted),
        Err(err) => HttpResponse::InternalServerError().body(err.to_string()),
    }
}

/// Get Latest humidity
#[get("/humidity")]
async fn get_humidity(state: web::Data<AppState>) -> impl Responder {
    let Some(conn) = &state.conn else {
        return HttpResponse::InternalServerError().body("No DB Connection");
    };

    let humidity = temperature::Entity::find()
        .order_by_desc(temperature::Column::DateTime)
        .one(conn)
        .await;

    let Ok(humidity) = humidity else {
        return HttpResponse::InternalServerError().body("DB Fail");
    };

    let Some(humidity) = humidity else {
        return HttpResponse::InternalServerError().body("DB Fail");
    };

    return HttpResponse::Ok().json(humidity);
}

/// Get all Humidities in the Date Range
#[get("/humidity/range")]
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
        .filter(humidity::Column::DateTime.between(start_date, end_date))
        .all(conn)
        .await
        .expect("Failed to fetch humidity data");

    let response: Vec<Value> = humidities
        .into_iter()
        .map(|record| {
            json!({
                "date": record.date_time.to_string(),
                "humidity": record.humidity,
            })
        })
        .collect();

    HttpResponse::Ok().json(response)
}

// TODO : Users

// TODO : simple items

/* #endregion */

/* #region WEB SOCKETS ROUTES */

/// Connect to a Socket and Send Text Messages
#[get("/message")]
pub async fn ws_publish(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    println!("Connection to Message");
    let (response, mut session, mut msg_stream) = actix_ws::handle(&req, stream)?;

    actix_web::rt::spawn(async move {
        while let Some(Ok(msg)) = msg_stream.next().await {
            println!("Message");
            match msg {
                Message::Text(text) => println!("Received: {text}"),
                Message::Ping(bytes) => {
                    if session.pong(&bytes).await.is_err() {
                        println!("Ping Error");
                        break;
                    }
                }
                Message::Close(reason) => break,
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
    println!("1] Stream Connection");

    // Set Max frame size :
    // Help : https://github.com/actix/actix-extras/discussions/491
    // Example : https://github.com/actix/actix-extras/blob/18f94fa8b54453ac79cfe2ea04bcb8d7c10acb81/actix-ws/examples/chat.rs#L71
    // StackOverFlow : https://stackoverflow.com/questions/77207464/how-to-set-message-size-limit-for-actix-websocket-actorless

    let (response, session, msg_stream) = actix_ws::handle(&req, stream)?;
    let mut msg_stream = msg_stream
        .max_frame_size(1024 * 1024) // 128 *
        .aggregate_continuations();
    // let mut msg_stream = msg_stream.max_frame_size(128 * 1024);
    let tx = state.tx.clone();

    // state.insert(session.clone()).await; // Save Session

    actix_web::rt::spawn(async move {
        let mut session = session;

        while let Some(res) = msg_stream.next().await {
            match res {
                Ok(AggregatedMessage::Ping(bytes)) => {
                    if session.pong(&bytes).await.is_err() {
                        println!("Ping Error");
                        break;
                    }
                }
                Ok(AggregatedMessage::Binary(bytes)) => {
                    let is_keyframe = bytes.len() >= 5_000;

                    if is_keyframe {
                        if let Ok(mut latest_keyframe_guard) = state.latest_keyframe.lock() {
                            println!("Save Keyframe");
                            *latest_keyframe_guard = Some(bytes.to_vec().clone());
                        }
                    }

                    // Broadcast raw video bytes to channels
                    // println!("Received video chunk: {} bytes", bytes.len());
                    let _ = tx.send(bytes.to_vec());
                }
                Ok(AggregatedMessage::Close(reason)) => {
                    let _ = session.close(reason).await;
                    break;
                }
                Err(err) => {
                    println!("WebSocket Error: {:?}", err);
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
    state: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    println!("2] Watch Connection");
    let (res, session, _msg_stream) = actix_ws::handle(&req, stream)?;
    let mut rx = data.tx.subscribe();

    let latest_keyframe = state.latest_keyframe.lock().ok().and_then(|h| h.clone());

    actix_web::rt::spawn(async move {
        let mut session = session;

        // Send latest_keyframe
        if let Some(latest_keyframe) = latest_keyframe {
            println!("Sending latest_keyframe");

            if let Err(err) = session.binary(latest_keyframe).await {
                println!("Session latest_keyframe error : {}", err);
                return;
            }
        }

        loop {
            match rx.recv().await {
                Ok(bytes) => {
                    if let Err(err) = session.binary(bytes).await {
                        println!("Error sending Image: {}", err);
                        break;
                    }
                }
                Err(RecvError::Lagged(skipped)) => {
                    println!("Watcher lagged behind, skipped {} frames", skipped);
                    continue;
                }
                Err(RecvError::Closed) => {
                    println!("Streamer channel closed");
                    break;
                }
            }
        }
    });

    Ok(res)
}

/* #endregion */
