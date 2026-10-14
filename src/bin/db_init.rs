use sqlx::postgres::PgPoolOptions;
use std::env;
use dotenvy::dotenv;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("DB Init...");
    dotenv().ok();
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL env not set");

    let pool = loop {
        match PgPoolOptions::new().connect(&database_url).await {
            Ok(pool) => break pool,
            Err(e) => {
                println!("DB not ready: {}. Retry in 2 seconds...", e);
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    };

    println!("Successfully connected! Running database initialization...");

    // Create users + posts tables
    // sqlx::query(
    //     r#"
    //     CREATE TABLE IF NOT EXISTS users (
    //         id SERIAL PRIMARY KEY,
    //         username VARCHAR(50) NOT NULL UNIQUE,
    //         created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    //     );

    //     CREATE TABLE IF NOT EXISTS posts (
    //         id SERIAL PRIMARY KEY,
    //         user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    //         title TEXT NOT NULL,
    //         body TEXT
    //     );
    //     "#,
    // )
    // .execute(&pool)
    // .await?;

    // temperatures table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS temperatures (
            id SERIAL PRIMARY KEY,
            date_time TIMESTAMP WITH TIME ZONE UNIQUE NOT NULL,
            temperature DOUBLE PRECISION NOT NULL
        );
        "#,
    )
    .execute(&pool)
    .await?;

    println!("DB tables initialized");

    Ok(())
}
