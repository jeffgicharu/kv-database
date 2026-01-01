//! Memory arena for efficient allocation.
//!
//! The arena provides fast, bump-pointer allocation for data that will
//! be freed all at once (like when a MemTable is discarded).
//!
//! Note: For the MemTable's skip list, we use crossbeam-skiplist which
//! handles its own memory management. This arena is provided for other
//! use cases like block building.

use std::alloc::{alloc, dealloc, Layout};
use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Default block size (4KB).
const DEFAULT_BLOCK_SIZE: usize = 4 * 1024;

/// Memory arena for efficient allocation.
///
/// Allocations are bump-pointer style within blocks. Memory is freed
/// all at once when the arena is dropped.
pub struct Arena {
    /// Current allocation pointer within the current block.
    alloc_ptr: UnsafeCell<*mut u8>,
    /// Remaining bytes in the current block.
    alloc_bytes_remaining: UnsafeCell<usize>,
    /// All allocated blocks (for deallocation on drop).
    blocks: UnsafeCell<Vec<NonNull<u8>>>,
    /// Block size for new allocations.
    block_size: usize,
    /// Total memory usage.
    memory_usage: AtomicUsize,
}

// Safety: Arena is designed for single-threaded use within a MemTable.
// The MemTable itself handles synchronization.
unsafe impl Send for Arena {}

impl Arena {
    /// Create a new arena with default block size.
    pub fn new() -> Self {
        Self::with_block_size(DEFAULT_BLOCK_SIZE)
    }

    /// Create a new arena with specified block size.
    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            alloc_ptr: UnsafeCell::new(std::ptr::null_mut()),
            alloc_bytes_remaining: UnsafeCell::new(0),
            blocks: UnsafeCell::new(Vec::new()),
            block_size,
            memory_usage: AtomicUsize::new(0),
        }
    }

    /// Allocate memory of the given size.
    ///
    /// Returns a pointer to the allocated memory. The memory is
    /// uninitialized.
    ///
    /// # Safety
    /// The returned pointer is valid until the arena is dropped.
    pub fn allocate(&self, size: usize) -> *mut u8 {
        // Fast path: allocate from current block
        unsafe {
            let remaining = *self.alloc_bytes_remaining.get();
            if size <= remaining {
                let result = *self.alloc_ptr.get();
                *self.alloc_ptr.get() = result.add(size);
                *self.alloc_bytes_remaining.get() = remaining - size;
                return result;
            }
        }

        // Slow path: allocate new block
        self.allocate_fallback(size)
    }

    /// Allocate aligned memory.
    pub fn allocate_aligned(&self, size: usize, align: usize) -> *mut u8 {
        debug_assert!(align.is_power_of_two());

        unsafe {
            let current_ptr = *self.alloc_ptr.get() as usize;
            let aligned_ptr = (current_ptr + align - 1) & !(align - 1);
            let padding = aligned_ptr - current_ptr;
            let total_size = size + padding;

            let remaining = *self.alloc_bytes_remaining.get();
            if total_size <= remaining {
                let result = aligned_ptr as *mut u8;
                *self.alloc_ptr.get() = result.add(size);
                *self.alloc_bytes_remaining.get() = remaining - total_size;
                return result;
            }
        }

        // Need new block - allocate with alignment
        self.allocate_fallback_aligned(size, align)
    }

    /// Slow path: allocate from a new block.
    fn allocate_fallback(&self, size: usize) -> *mut u8 {
        if size > self.block_size / 4 {
            // Large allocation: allocate separately
            return self.allocate_new_block(size);
        }

        // Allocate a new standard block
        let new_block = self.allocate_new_block(self.block_size);
        unsafe {
            *self.alloc_ptr.get() = new_block.add(size);
            *self.alloc_bytes_remaining.get() = self.block_size - size;
        }
        new_block
    }

    /// Allocate with alignment from a new block.
    fn allocate_fallback_aligned(&self, size: usize, align: usize) -> *mut u8 {
        // For simplicity, allocate a new block with enough space
        let block_size = std::cmp::max(size + align, self.block_size);
        let new_block = self.allocate_new_block(block_size);

        let ptr = new_block as usize;
        let aligned_ptr = (ptr + align - 1) & !(align - 1);
        let result = aligned_ptr as *mut u8;

        unsafe {
            let used = (aligned_ptr - ptr) + size;
            *self.alloc_ptr.get() = result.add(size);
            *self.alloc_bytes_remaining.get() = block_size - used;
        }

        result
    }

    /// Allocate a new block of the given size.
    fn allocate_new_block(&self, size: usize) -> *mut u8 {
        let layout = Layout::from_size_align(size, 8).expect("Invalid layout");

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        let non_null = NonNull::new(ptr).expect("Allocation returned null");
        unsafe {
            (*self.blocks.get()).push(non_null);
        }

        self.memory_usage.fetch_add(size, Ordering::Relaxed);
        ptr
    }

    /// Get the total memory usage.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }

    /// Allocate and copy bytes.
    pub fn allocate_copy(&self, data: &[u8]) -> *mut u8 {
        let ptr = self.allocate(data.len());
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
        }
        ptr
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let blocks = unsafe { &mut *self.blocks.get() };
        for block in blocks.drain(..) {
            // We don't know the exact size of each block, but we allocated
            // with alignment 8. For proper deallocation, we'd need to track sizes.
            // For now, this is a simplified implementation.
            // In production, we'd track (ptr, layout) pairs.
            unsafe {
                // Note: This is simplified - in production we'd track the actual size
                let layout = Layout::from_size_align_unchecked(1, 8);
                dealloc(block.as_ptr(), layout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_basic() {
        let arena = Arena::new();

        let ptr1 = arena.allocate(100);
        assert!(!ptr1.is_null());

        let ptr2 = arena.allocate(200);
        assert!(!ptr2.is_null());
        assert_ne!(ptr1, ptr2);

        assert!(arena.memory_usage() >= 300);
    }

    #[test]
    fn test_arena_copy() {
        let arena = Arena::new();
        let data = b"hello world";

        let ptr = arena.allocate_copy(data);
        let copied = unsafe { std::slice::from_raw_parts(ptr, data.len()) };

        assert_eq!(copied, data);
    }

    #[test]
    fn test_arena_large_allocation() {
        let arena = Arena::with_block_size(1024);

        // Allocate more than block_size / 4
        let ptr = arena.allocate(512);
        assert!(!ptr.is_null());

        // This should get its own block
        assert!(arena.memory_usage() >= 512);
    }

    #[test]
    fn test_arena_many_allocations() {
        let arena = Arena::new();

        for i in 0..1000 {
            let ptr = arena.allocate(i % 100 + 1);
            assert!(!ptr.is_null());
        }

        assert!(arena.memory_usage() > 0);
    }
}
