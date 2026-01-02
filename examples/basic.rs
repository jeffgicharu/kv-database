//! Basic usage example for rustdb.
//!
//! Run with: cargo run --example basic

use rustdb::Database;
use tempfile::TempDir;

fn main() -> rustdb::Result<()> {
    // Create a temporary directory for the database
    let dir = TempDir::new().expect("failed to create temp dir");

    // Open the database
    let db = Database::open(dir.path())?;

    println!("Database opened successfully!");

    // Put some values
    db.put(b"name", b"Alice")?;
    db.put(b"city", b"San Francisco")?;
    db.put(b"country", b"USA")?;

    println!("Stored 3 key-value pairs");

    // Get values
    if let Some(name) = db.get(b"name")? {
        println!("name = {}", String::from_utf8_lossy(&name));
    }

    if let Some(city) = db.get(b"city")? {
        println!("city = {}", String::from_utf8_lossy(&city));
    }

    // Check for non-existent key
    match db.get(b"nonexistent")? {
        Some(_) => println!("Found nonexistent key (unexpected)"),
        None => println!("Key 'nonexistent' not found (expected)"),
    }

    // Update a value
    db.put(b"city", b"New York")?;
    if let Some(city) = db.get(b"city")? {
        println!("Updated city = {}", String::from_utf8_lossy(&city));
    }

    // Delete a value
    db.delete(b"country")?;
    match db.get(b"country")? {
        Some(_) => println!("Country still exists (unexpected)"),
        None => println!("Country deleted successfully"),
    }

    // Close the database
    db.close()?;
    println!("Database closed successfully!");

    Ok(())
}
