//! Iterator and range query example for rustdb.
//!
//! Run with: cargo run --example iterators

use rustdb::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> rustdb::Result<()> {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db = Arc::new(Database::open(dir.path())?);

    println!("=== Iterator Example ===\n");

    // Insert sample data with prefixes
    println!("Inserting sample data...");
    let users = [
        ("user:001", "Alice"),
        ("user:002", "Bob"),
        ("user:003", "Charlie"),
        ("user:004", "Diana"),
        ("user:005", "Eve"),
    ];
    for (key, value) in &users {
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let products = [
        ("product:laptop", "1299.99"),
        ("product:mouse", "29.99"),
        ("product:keyboard", "79.99"),
    ];
    for (key, value) in &products {
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    let orders = [
        ("order:1001", "user:001,product:laptop"),
        ("order:1002", "user:002,product:mouse"),
        ("order:1003", "user:001,product:keyboard"),
    ];
    for (key, value) in &orders {
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    println!("Inserted {} records\n", users.len() + products.len() + orders.len());

    // Iterate all keys
    println!("--- All Keys (sorted) ---");
    let iter = db.iter()?;
    for (key, value) in iter {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Prefix iteration - users only
    println!("\n--- Users Only (prefix: 'user:') ---");
    let user_iter = db.prefix_iter(b"user:")?;
    for (key, value) in user_iter {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Prefix iteration - products only
    println!("\n--- Products Only (prefix: 'product:') ---");
    let product_iter = db.prefix_iter(b"product:")?;
    for (key, value) in product_iter {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Range iteration
    println!("\n--- Range Query (user:002 to user:004) ---");
    let range_iter = db.range(b"user:002", b"user:005")?;
    for (key, value) in range_iter {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // Count records by prefix
    println!("\n--- Record Counts ---");
    let user_count = db.prefix_iter(b"user:")?.count();
    let product_count = db.prefix_iter(b"product:")?.count();
    let order_count = db.prefix_iter(b"order:")?.count();
    println!("  Users: {}", user_count);
    println!("  Products: {}", product_count);
    println!("  Orders: {}", order_count);

    println!("\n=== Iterator Example Complete ===");
    Ok(())
}
