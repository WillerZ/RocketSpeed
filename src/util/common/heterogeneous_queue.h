// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include "src/util/common/thread_check.h"

namespace rocketspeed {

/**
 * Single producer, single consumer queue that stores heterogeneous types
 * in situ within a contiguous buffer. Supports write transactions of multiple
 * types that succeed or fail atomically. Readers will never see a
 * partially-completed transaction.
 *
 * No type information is stored within the queue, so it is the responsibility
 * of the user to ensure that the same type is read when written, and that
 * all entries are read on destruction.
 *
 * It is assumed that a typical use case will write a type-tag before each
 * entry, e.g.:
 *
 * HeterogeneousQueue queue(4096);
 *
 * HeterogeneousQueue::Transaction tx(&queue);
 * tx.Write('s');  // 's' => string
 * tx.Write(std::string("foo"));
 * RS_ASSERT(tx.Commit());
 *
 * HeterogeneousQueue::Transaction tx(&queue);
 * tx.Write('i');  // 'i' => int
 * tx.Write(1234);
 * RS_ASSERT(tx.Commit());
 *
 * The reader can then read the tag before deciding what type to read.
 *
 * char tag;
 * queue.Read(&tag);
 *
 * if (tag == 's') {
 *   std::string s;
 *   queue.Read(&s);
 *   // do something with s
 * } else if (tag == 'i') {
 *   int i;
 *   queue.Read(&i);
 *   // do something with i
 * }
 */
class HeterogeneousQueue {
 public:
  /**
   * Construct a HeterogeneousQueue with a fixed buffer size.
   * The buffer size will be rounded up to a power-of-two.
   *
   * @param buffer_size Size of the queue in bytes.
   */
  explicit HeterogeneousQueue(uint64_t buffer_size)
  : buffer_size_(buffer_size)
  , write_index_(0)
  , read_index_cached_(0)
  , committed_index_(0)
  , committed_index_cached_(0)
  , read_index_(0) {
    if (buffer_size & (buffer_size - 1)) {
      // Not power-of-two, find next biggest power of two.
      buffer_size_ = 1;
      while (buffer_size_ < buffer_size) {
        buffer_size_ <<= 1;
      }
    }
    buffer_.reset(new char[buffer_size_]);
  }

  // non-copyable, non-moveable.
  HeterogeneousQueue(const HeterogeneousQueue&) = delete;
  HeterogeneousQueue& operator=(const HeterogeneousQueue&) = delete;
  HeterogeneousQueue(HeterogeneousQueue&&) = delete;
  HeterogeneousQueue& operator=(HeterogeneousQueue&&) = delete;

  ~HeterogeneousQueue() {
    // It's up to the user to consume all entries before destruction.
    RS_ASSERT(read_index_.load(std::memory_order_acquire) ==
           committed_index_.load(std::memory_order_acquire));
  }

  /**
   * Write a single value.
   */
  template <typename Value>
  bool Write(Value&& value) {
    write_check_.Check();
    const uint64_t rollback = write_index_;
    bool result = WriteUncommitted(std::forward<Value>(value));
    if (result) {
      committed_index_.store(write_index_, std::memory_order_release);
    } else {
      write_index_ = rollback;
    }
    return result;
  }

  /**
   * Reads a value at the front of the queue to given variable.
   */
  template <typename Value>
  bool Read(Value* value) {
    read_check_.Check();
    RS_ASSERT(value);

    // Current read offset.
    auto const current = read_index_.load(std::memory_order_relaxed);

    // Test against cached commit index first.
    if (current == committed_index_cached_) {
      // Cached committed index too low, so update from source.
      const uint64_t committed =
        committed_index_.load(std::memory_order_acquire);
      committed_index_cached_ = committed;
      if (current == committed) {
        // Still too low. Queue is empty.
        return false;
      }
    }

    const uint64_t read = AdjustIndex<Value>(current);
    const uint64_t next_index = read + sizeof(Value);
    const uint64_t mask = buffer_size_ - 1;
    Value* read_loc = reinterpret_cast<Value*>(&buffer_[read & mask]);
    *value = std::move(*read_loc);
    read_loc->~Value();
    read_index_.store(next_index, std::memory_order_release);
    return true;
  }

  /**
   * RAII-style wrapper that encapsulates a write transaction.
   * This allows multiple writes to occur atomically. If any single write,
   * all writes will roll back.
   *
   * Writes must be manually committed before the transaction is destroyed.
   */
  struct Transaction {
   public:
    /** Constructs a transaction on a queue */
    explicit Transaction(HeterogeneousQueue* queue)
    : queue_(queue)
    , rollback_index_(queue->write_index_)
    , success_(true)
    , uncommitted_writes_(false) {}

    /** Attempt to write a value in the transaction */
    template <typename Value>
    void Write(Value&& value) {
      uncommitted_writes_ = true;
      success_ =
        success_ && queue_->WriteUncommitted(std::forward<Value>(value));
    }

    /** End the transaction, previous writes must be committed */
    ~Transaction() {
      RS_ASSERT(!uncommitted_writes_);
    }

    /** Commit writes. Returns true iff successful. */
    bool Commit() {
      bool success = success_;
      if (success) {
        queue_->committed_index_.store(queue_->write_index_,
                                       std::memory_order_release);
        rollback_index_ = queue_->write_index_;
      } else {
        queue_->write_index_ = rollback_index_;
      }
      uncommitted_writes_ = false;
      success_ = true;
      return success;
    }

   private:
    HeterogeneousQueue* queue_;
    uint64_t rollback_index_;
    bool success_;
    bool uncommitted_writes_;
  };

private:
  /**
   * Attempts to write all values in a single transaction. Either all will
   * succeed, or all will fail.
   */
  template <class Value>
  bool WriteUncommitted(Value value) {
    // Compute write index for value.
    const uint64_t write = AdjustIndex<Value>(write_index_);

    // Next write offset.
    const uint64_t next_index = write + sizeof(Value);

    // Check that we have space to write.
    if (next_index >= read_index_cached_ + buffer_size_) {
      const uint64_t read = read_index_.load(std::memory_order_acquire);
      read_index_cached_ = read;
      if (next_index >= read + buffer_size_) {
        // Out of space.
        return false;
      }
    }

    // Write the command.
    const uint64_t mask = buffer_size_ - 1;
    new (&buffer_[write & mask]) Value(std::move(value));

    // Update write index.
    write_index_ = next_index;
    return true;
  }

  /**
   * Aligns a value upwards.
   *
   * @param index Value to align.
   * @param align Alignment. Must be power-of-two.
   * @return Smallest uint64_t that is >= index, where
   *         index & (align - 1) == 0.
   */
  inline uint64_t Align(uint64_t index, uint64_t align) {
    RS_ASSERT((align & (align - 1)) == 0);  // align must be power-of-two.
    return (index + align - 1) & ~(align - 1);
  }

  /**
   * Adjust a index so that a value of type T can be written, correctly aligned,
   * and so that the write doesn't cross the buffer boundary.
   *
   * @param index The unwrapped index of the write.
   * @param size The number of bytes to write.
   */
  template <typename T>
  inline uint64_t AdjustIndex(uint64_t index) {
    // First, align index for T.
    index = Align(index, alignof(T));

    // Check if the write crosses buffer boundary.
    const uint64_t last_index = index + sizeof(T) - 1;
    const uint64_t mask = ~(buffer_size_ - 1);
    if ((last_index & mask) != (index & mask)) {
      // Crosses buffer boundary, so move the index up to the buffer size.
      index = Align(index, buffer_size_);
    }
    return index;
  }

  // Pre-allocated contiguous circular buffer and size (in bytes).
  std::unique_ptr<char[]> buffer_;
  uint64_t buffer_size_;

  // Read/write indices. Note that these are unwrapped, so indices will
  // typically be > buffer_size_. Index i corresponds to
  // buffer_[i & (buffer_size_ - 1)].
  //
  // The cached indices are there to avoid atomic reads on every operation,
  // e.g. if the read size knows we have written 100 bytes ahead, we don't need
  // to read from committed_index_ again until we have read those 100 bytes.
  uint64_t write_index_;
  uint64_t read_index_cached_;
  std::atomic<uint64_t> committed_index_;
  uint64_t committed_index_cached_;
  std::atomic<uint64_t> read_index_;

  // HeterogeneousQueue can only be read from one thread, and written by one
  // thread (but can be different threads). These check for correct usage.
  ThreadCheck read_check_;
  ThreadCheck write_check_;
};

}  // namespace rocketspeed
