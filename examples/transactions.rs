//! Transaction example for rustdb.
//!
//! Run with: cargo run --example transactions

use rustdb::Database;
use std::sync::Arc;
use tempfile::TempDir;

fn main() -> rustdb::Result<()> {
    let dir = TempDir::new().expect("failed to create temp dir");
    let db = Arc::new(Database::open(dir.path())?);

    println!("=== Transaction Example ===\n");

    // Initialize account balances
    db.put(b"account:alice", b"1000")?;
    db.put(b"account:bob", b"500")?;
    println!("Initial balances:");
    print_balances(&db)?;

    // Successful transaction: transfer $200 from Alice to Bob
    println!("\n--- Transfer $200 from Alice to Bob ---");
    {
        let txn = db.begin_transaction()?;

        // Read current balances
        let alice_balance: i64 = get_balance(&txn, b"account:alice")?;
        let bob_balance: i64 = get_balance(&txn, b"account:bob")?;

        // Update balances
        txn.put(b"account:alice", (alice_balance - 200).to_string().as_bytes())?;
        txn.put(b"account:bob", (bob_balance + 200).to_string().as_bytes())?;

        // Commit the transaction
        txn.commit()?;
        println!("Transaction committed successfully!");
    }
    print_balances(&db)?;

    // Transaction with rollback
    println!("\n--- Attempt transfer $2000 (will rollback) ---");
    {
        let txn = db.begin_transaction()?;

        let alice_balance: i64 = get_balance(&txn, b"account:alice")?;

        if alice_balance < 2000 {
            println!("Insufficient funds! Rolling back...");
            txn.rollback()?;
            println!("Transaction rolled back!");
        } else {
            // This won't execute
            txn.commit()?;
        }
    }
    println!("Balances unchanged after rollback:");
    print_balances(&db)?;

    // Snapshot isolation demonstration
    println!("\n--- Snapshot Isolation Demo ---");
    {
        // Start a transaction
        let txn1 = db.begin_transaction()?;
        let initial = get_balance(&txn1, b"account:alice")?;
        println!("Txn1 reads Alice's balance: {}", initial);

        // Another write happens outside the transaction
        db.put(b"account:alice", b"999")?;
        println!("Direct write changed Alice's balance to 999");

        // Txn1 still sees the old value (snapshot isolation)
        let snapshot_value = get_balance(&txn1, b"account:alice")?;
        println!("Txn1 still sees: {} (snapshot isolation)", snapshot_value);

        // Don't commit to avoid conflicts
        drop(txn1);
    }

    println!("\n=== Transaction Example Complete ===");
    Ok(())
}

fn get_balance(txn: &rustdb::Transaction, key: &[u8]) -> rustdb::Result<i64> {
    let value = txn.get(key)?.unwrap_or_default();
    let s = String::from_utf8_lossy(&value);
    Ok(s.parse().unwrap_or(0))
}

fn print_balances(db: &Database) -> rustdb::Result<()> {
    if let Some(alice) = db.get(b"account:alice")? {
        println!("  Alice: ${}", String::from_utf8_lossy(&alice));
    }
    if let Some(bob) = db.get(b"account:bob")? {
        println!("  Bob: ${}", String::from_utf8_lossy(&bob));
    }
    Ok(())
}
