//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#ifndef STORAGE_ROCKETSPEED_PORT_PORT_POSIX_H_
#define STORAGE_ROCKETSPEED_PORT_PORT_POSIX_H_

#undef PLATFORM_IS_LITTLE_ENDIAN
#if defined(OS_MACOSX)
  #include <machine/endian.h>
  #if defined(__DARWIN_LITTLE_ENDIAN) && defined(__DARWIN_BYTE_ORDER)
    #define PLATFORM_IS_LITTLE_ENDIAN \
        (__DARWIN_BYTE_ORDER == __DARWIN_LITTLE_ENDIAN)
  #endif
#elif defined(OS_SOLARIS)
  #include <sys/isa_defs.h>
  #ifdef _LITTLE_ENDIAN
    #define PLATFORM_IS_LITTLE_ENDIAN true
  #else
    #define PLATFORM_IS_LITTLE_ENDIAN false
  #endif
#elif defined(OS_FREEBSD) || defined(OS_OPENBSD) || defined(OS_NETBSD) ||\
      defined(OS_DRAGONFLYBSD) || defined(OS_ANDROID)
  #include <sys/types.h>
  #include <sys/endian.h>
#else
  #include <endian.h>
#endif
#include <pthread.h>
#ifdef SNAPPY
#include <snappy.h>
#endif

#ifdef ZLIB
#include <zlib.h>
#endif

#ifdef BZIP2
#include <bzlib.h>
#endif

#if defined(LZ4)
#include <lz4.h>
#include <lz4hc.h>
#endif

#include <semaphore.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <chrono>
#include <memory>

#include <string>

#if defined(OS_MACOSX)
typedef uint64_t eventfd_t;
#else
#include <sys/eventfd.h>
#include <unistd.h>
#include <fcntl.h>
#endif

#include "src/port/atomic_pointer.h"

#ifndef PLATFORM_IS_LITTLE_ENDIAN
#define PLATFORM_IS_LITTLE_ENDIAN (__BYTE_ORDER == __LITTLE_ENDIAN)
#endif

#if defined(OS_MACOSX) || defined(OS_SOLARIS) || defined(OS_FREEBSD) ||\
    defined(OS_NETBSD) || defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD) ||\
    defined(OS_ANDROID)
// Use fread/fwrite/fflush on platforms without _unlocked variants
#define fread_unlocked fread
#define fwrite_unlocked fwrite
#define fflush_unlocked fflush
#endif

#if defined(OS_MACOSX) || defined(OS_FREEBSD) ||\
    defined(OS_OPENBSD) || defined(OS_DRAGONFLYBSD)
// Use fsync() on platforms without fdatasync()
#define fdatasync fsync
#endif

#if defined(OS_ANDROID) && __ANDROID_API__ < 9
// fdatasync() was only introduced in API level 9 on Android. Use fsync()
// when targetting older platforms.
#define fdatasync fsync
#endif

#if defined(OS_ANDROID)
// std::to_string(...) is missing in <string> on Android gcc
// https://code.google.com/p/android/issues/detail?id=53460

namespace std {
extern string to_string(int value);
extern string to_string(unsigned int value);
extern string to_string(long value);
extern string to_string(unsigned long value);
extern string to_string(long long value);
extern string to_string(unsigned long long value);
extern string to_string(float value);
extern string to_string(double value);
extern string to_string(long double value);
}
#endif

#if defined(OS_ANDROID)
// libevent_global_shutdown not available in libevent 2.0.*
extern void libevent_global_shutdown(void);
#endif
#if defined(OS_MACOSX)
// libevent_global_shutdown not available in libevent 2.0.*
extern void ld_libevent_global_shutdown(void);
#endif

namespace rocketspeed {
namespace port {

static const bool kLittleEndian = PLATFORM_IS_LITTLE_ENDIAN;
#undef PLATFORM_IS_LITTLE_ENDIAN

class CondVar;

class Mutex {
 public:
  /* implicit */ Mutex(bool adaptive = false);
  ~Mutex();

  void Lock();
  void Unlock();
  // this will assert if the mutex is not locked
  // it does NOT verify that mutex is held by a calling thread
  void AssertHeld();

 private:
  friend class CondVar;
  pthread_mutex_t mu_;
#ifndef NO_RS_ASSERT
  bool locked_;
#endif

  // No copying
  Mutex(const Mutex&);
  void operator=(const Mutex&);
};

class RWMutex {
 public:
  RWMutex();
  ~RWMutex();

  void ReadLock();
  void WriteLock();
  void ReadUnlock();
  void WriteUnlock();
  void AssertHeld() { }

 private:
  pthread_rwlock_t mu_; // the underlying platform mutex

  // No copying allowed
  RWMutex(const RWMutex&);
  void operator=(const RWMutex&);
};

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();
  void Wait();
  // Timed condition wait.  Returns true if timeout occurred.
  bool TimedWait(uint64_t abs_time_us);
  void Signal();
  void SignalAll();
 private:
  pthread_cond_t cv_;
  Mutex* mu_;
};

/**
 * Wrapper around POSIX semaphores.  Features:
 *
 * (1) RAII semantics.
 * (2) Waiting methods swallow EINTR.
 * (3) Internally dynamically allocates the raw sem_t to work around a glibc
 * bug.  A common pattern where one thread does wait+destroy while another
 * posts is not safe because the post can unblock the waiter+destroyer and
 * then try to access sem_t data again.  Details:
 * https://sourceware.org/bugzilla/show_bug.cgi?id=12674
 */

namespace detail {
// Internal wrapper that we can use with an std::shared_ptr
class RawSemaphore {
 public:
  explicit RawSemaphore(unsigned initial_value);
  ~RawSemaphore();
  sem_t* sem_;
 private:
#if !defined(OS_MACOSX)
  sem_t buffer_;
#endif
  std::string name_;
};
}  // namespace detail

class Semaphore {
 public:
  explicit Semaphore(unsigned int initial_value = 0);

  void Wait();

  /**
   * @return true on success, false if semaphore could not
   *         be decremented before @param deadline.
   */
  bool TimedWait(std::chrono::system_clock::time_point deadline);

  template <typename Duration>
  bool TimedWait(Duration timeout) {
    return TimedWait(std::chrono::system_clock::now() +
      std::chrono::duration_cast<std::chrono::system_clock::duration>(timeout));
  }
  void Post();

  // Not copyable but movable
  Semaphore(const Semaphore &other) = delete;
  Semaphore& operator=(const Semaphore &other) = delete;
  Semaphore(Semaphore &&other) = default;
  Semaphore& operator=(Semaphore &&other) = default;

 private:
  std::shared_ptr<detail::RawSemaphore> sem_;

  sem_t* rawsem() const {
    return sem_->sem_;
  }
};

/*
 * Port of Linux's eventfd() on various platforms.
 */
class Eventfd {
 public:
  // Create an eventfd descriptor. If nonblock is true,
  // then the underling socket is marked as non-blocking.
  Eventfd(bool non_block, bool close_on_exec);

  // Return the current status of the eventfd.
  // Returns -1 on error, otherwise valid.
  int status() const;

  // Close the eventfd. Returns 0 on success, otherwise error.
  int closefd();

  // Returns the read descriptor of this eventfd
  int readfd() const;

  // Returns the write descriptor of this eventfd
  int writefd() const;

  // Read an event.  Returns 0 if the correct number of bytes
  // was transferred, or -1 otherwise.
  int read_event(eventfd_t *value);

  // Write an event.  Returns 0 if the correct number of bytes
  // was transferred, or -1 otherwise.
  int write_event(eventfd_t value);

 private:
  int fd_[2];
#if defined(OS_MACOSX)
  int status_;
  std::unique_ptr<Mutex> mutex_;
  eventfd_t value_ = 0;
#endif
};

typedef pthread_once_t OnceType;
#define ROCKETSPEED_ONCE_INIT PTHREAD_ONCE_INIT
extern void InitOnce(OnceType* once, void (*initializer)());

// Compression options for different compression algorithms like Zlib
class CompressionOptions {
 public:
  int window_bits;
  int level;
  int strategy;
  CompressionOptions() : window_bits(-14), level(-1), strategy(0) {}
  CompressionOptions(int wbits, int _lev, int _strategy)
      : window_bits(wbits), level(_lev), strategy(_strategy) {}
};

inline bool Snappy_Compress(const CompressionOptions& opts, const char* input,
                            size_t length, ::std::string* output) {
#ifdef SNAPPY
  output->resize(snappy::MaxCompressedLength(length));
  size_t outlen;
  snappy::RawCompress(input, length, &(*output)[0], &outlen);
  output->resize(outlen);
  return true;
#endif

  return false;
}

inline bool Snappy_GetUncompressedLength(const char* input, size_t length,
                                         size_t* result) {
#ifdef SNAPPY
  return snappy::GetUncompressedLength(input, length, result);
#else
  return false;
#endif
}

inline bool Snappy_Uncompress(const char* input, size_t length,
                              char* output) {
#ifdef SNAPPY
  return snappy::RawUncompress(input, length, output);
#else
  return false;
#endif
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#pragma GCC diagnostic ignored "-Wsign-conversion"
inline bool Zlib_Compress(const CompressionOptions& opts, const char* input,
                          size_t length, ::std::string* output) {
#ifdef ZLIB
  // The memLevel parameter specifies how much memory should be allocated for
  // the internal compression state.
  // memLevel=1 uses minimum memory but is slow and reduces compression ratio.
  // memLevel=9 uses maximum memory for optimal speed.
  // The default value is 8. See zconf.h for more details.
  static const int memLevel = 8;
  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));
  int st = deflateInit2(&_stream, opts.level, Z_DEFLATED, opts.window_bits,
                        memLevel, opts.strategy);
  if (st != Z_OK) {
    return false;
  }

  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(length);

  // Compress the input, and put compressed data in output.
  _stream.next_in = (Bytef *)input;
  _stream.avail_in = length;

  // Initialize the output size.
  _stream.avail_out = length;
  _stream.next_out = (Bytef *)&(*output)[0];

  int old_sz =0, new_sz =0, new_sz_delta =0;
  bool done = false;
  while (!done) {
    st = deflate(&_stream, Z_FINISH);
    switch (st) {
      case Z_STREAM_END:
        done = true;
        break;
      case Z_OK:
        // No output space. Increase the output space by 20%.
        // (Should we fail the compression since it expands the size?)
        old_sz = output->size();
        new_sz_delta = (int)(output->size() * 0.2);
        new_sz = output->size() + (new_sz_delta < 10 ? 10 : new_sz_delta);
        output->resize(new_sz);
        // Set more output.
        _stream.next_out = (Bytef *)&(*output)[old_sz];
        _stream.avail_out = new_sz - old_sz;
        break;
      case Z_BUF_ERROR:
      default:
        deflateEnd(&_stream);
        return false;
    }
  }

  output->resize(output->size() - _stream.avail_out);
  deflateEnd(&_stream);
  return true;
#endif
  return false;
}

inline char* Zlib_Uncompress(const char* input_data, size_t input_length,
    int* decompress_size, int windowBits = -14) {
#ifdef ZLIB
  z_stream _stream;
  memset(&_stream, 0, sizeof(z_stream));

  // For raw inflate, the windowBits should be -8..-15.
  // If windowBits is bigger than zero, it will use either zlib
  // header or gzip header. Adding 32 to it will do automatic detection.
  int st = inflateInit2(&_stream,
      windowBits > 0 ? windowBits + 32 : windowBits);
  if (st != Z_OK) {
    return nullptr;
  }

  _stream.next_in = (Bytef *)input_data;
  _stream.avail_in = input_length;

  // Assume the decompressed data size will 5x of compressed size.
  int output_len = input_length * 5;
  char* output = new char[output_len];
  int old_sz = output_len;

  _stream.next_out = (Bytef *)output;
  _stream.avail_out = output_len;

  char* tmp = nullptr;
  int output_len_delta;
  bool done = false;

  //while(_stream.next_in != nullptr && _stream.avail_in != 0) {
  while (!done) {
    st = inflate(&_stream, Z_SYNC_FLUSH);
    switch (st) {
      case Z_STREAM_END:
        done = true;
        break;
      case Z_OK:
        // No output space. Increase the output space by 20%.
        old_sz = output_len;
        output_len_delta = (int)(output_len * 0.2);
        output_len += output_len_delta < 10 ? 10 : output_len_delta;
        tmp = new char[output_len];
        memcpy(tmp, output, old_sz);
        delete[] output;
        output = tmp;

        // Set more output.
        _stream.next_out = (Bytef *)(output + old_sz);
        _stream.avail_out = output_len - old_sz;
        break;
      case Z_BUF_ERROR:
      default:
        delete[] output;
        inflateEnd(&_stream);
        return nullptr;
    }
  }

  *decompress_size = output_len - _stream.avail_out;
  inflateEnd(&_stream);
  return output;
#endif

  return nullptr;
}

inline bool BZip2_Compress(const CompressionOptions& opts, const char* input,
                           size_t length, ::std::string* output) {
#ifdef BZIP2
  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  // Block size 1 is 100K.
  // 0 is for silent.
  // 30 is the default workFactor
  int st = BZ2_bzCompressInit(&_stream, 1, 0, 30);
  if (st != BZ_OK) {
    return false;
  }

  // Resize output to be the plain data length.
  // This may not be big enough if the compression actually expands data.
  output->resize(length);

  // Compress the input, and put compressed data in output.
  _stream.next_in = (char *)input;
  _stream.avail_in = length;

  // Initialize the output size.
  _stream.next_out = (char *)&(*output)[0];
  _stream.avail_out = length;

  int old_sz =0, new_sz =0;
  while(_stream.next_in != nullptr && _stream.avail_in != 0) {
    st = BZ2_bzCompress(&_stream, BZ_FINISH);
    switch (st) {
      case BZ_STREAM_END:
        break;
      case BZ_FINISH_OK:
        // No output space. Increase the output space by 20%.
        // (Should we fail the compression since it expands the size?)
        old_sz = output->size();
        new_sz = (int)(output->size() * 1.2);
        output->resize(new_sz);
        // Set more output.
        _stream.next_out = (char *)&(*output)[old_sz];
        _stream.avail_out = new_sz - old_sz;
        break;
      case BZ_SEQUENCE_ERROR:
      default:
        BZ2_bzCompressEnd(&_stream);
        return false;
    }
  }

  output->resize(output->size() - _stream.avail_out);
  BZ2_bzCompressEnd(&_stream);
  return true;
#endif
  return false;
}

inline char* BZip2_Uncompress(const char* input_data, size_t input_length,
                              int* decompress_size) {
#ifdef BZIP2
  bz_stream _stream;
  memset(&_stream, 0, sizeof(bz_stream));

  int st = BZ2_bzDecompressInit(&_stream, 0, 0);
  if (st != BZ_OK) {
    return nullptr;
  }

  _stream.next_in = (char *)input_data;
  _stream.avail_in = input_length;

  // Assume the decompressed data size will be 5x of compressed size.
  int output_len = input_length * 5;
  char* output = new char[output_len];
  int old_sz = output_len;

  _stream.next_out = (char *)output;
  _stream.avail_out = output_len;

  char* tmp = nullptr;

  while(_stream.next_in != nullptr && _stream.avail_in != 0) {
    st = BZ2_bzDecompress(&_stream);
    switch (st) {
      case BZ_STREAM_END:
        break;
      case BZ_OK:
        // No output space. Increase the output space by 20%.
        old_sz = output_len;
        output_len = (int)(output_len * 1.2);
        tmp = new char[output_len];
        memcpy(tmp, output, old_sz);
        delete[] output;
        output = tmp;

        // Set more output.
        _stream.next_out = (char *)(output + old_sz);
        _stream.avail_out = output_len - old_sz;
        break;
      default:
        delete[] output;
        BZ2_bzDecompressEnd(&_stream);
        return nullptr;
    }
  }

  *decompress_size = output_len - _stream.avail_out;
  BZ2_bzDecompressEnd(&_stream);
  return output;
#endif
  return nullptr;
}

inline bool LZ4_Compress(const CompressionOptions &opts, const char *input,
                         size_t length, ::std::string* output) {
#ifdef LZ4
  int compressBound = LZ4_compressBound(length);
  output->resize(8 + compressBound);
  char *p = const_cast<char *>(output->c_str());
  memcpy(p, &length, sizeof(length));
  size_t outlen;
  outlen = LZ4_compress_limitedOutput(input, p + 8, length, compressBound);
  if (outlen == 0) {
    return false;
  }
  output->resize(8 + outlen);
  return true;
#endif
  return false;
}

inline char* LZ4_Uncompress(const char* input_data, size_t input_length,
                            int* decompress_size) {
#ifdef LZ4
  if (input_length < 8) {
    return nullptr;
  }
  int output_len;
  memcpy(&output_len, input_data, sizeof(output_len));
  char *output = new char[output_len];
  *decompress_size = LZ4_decompress_safe_partial(
      input_data + 8, output, input_length - 8, output_len, output_len);
  if (*decompress_size < 0) {
    delete[] output;
    return nullptr;
  }
  return output;
#endif
  return nullptr;
}

inline bool LZ4HC_Compress(const CompressionOptions &opts, const char* input,
                           size_t length, ::std::string* output) {
#ifdef LZ4
  int compressBound = LZ4_compressBound(length);
  output->resize(8 + compressBound);
  char *p = const_cast<char *>(output->c_str());
  memcpy(p, &length, sizeof(length));
  size_t outlen;
#ifdef LZ4_VERSION_MAJOR  // they only started defining this since r113
  outlen = LZ4_compressHC2_limitedOutput(input, p + 8, length, compressBound,
                                         opts.level);
#else
  outlen = LZ4_compressHC_limitedOutput(input, p + 8, length, compressBound);
#endif
  if (outlen == 0) {
    return false;
  }
  output->resize(8 + outlen);
  return true;
#endif
  return false;
}
#pragma GCC diagnostic pop


void BackTraceHandler(int sig);

#define CACHE_LINE_SIZE 64U

#define PREFETCH(addr, rw, locality) __builtin_prefetch(addr, rw, locality)

} // namespace port
} // namespace rocketspeed

#endif  // STORAGE_ROCKETSPEED_PORT_PORT_POSIX_H_
