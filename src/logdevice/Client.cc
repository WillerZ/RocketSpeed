// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
#pragma GCC diagnostic ignored "-Wshadow"

#include "logdevice/include/Client.h"
#include <algorithm>
#include <chrono>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include "include/Assert.h"
#include "include/Slice.h"
#include "src/port/Env.h"
#include "logdevice/include/Record.h"
#include "src/logdevice/AsyncReader.h"
#include "src/logdevice/Common.h"
#include "src/util/scoped_file_lock.h"

namespace facebook { namespace logdevice {

class ClientImpl : public Client {
 public:
  ClientImpl() {}

 private:
  friend class Client;

  lsn_t GetLogSeqno(logid_t logid);

  rocketspeed::Env* env_;
  std::unique_ptr<ClientSettings> settings_;
  std::chrono::milliseconds timeout_;
  std::default_random_engine rng_;
};

lsn_t ClientImpl::GetLogSeqno(logid_t logid) {
  // Seqno is stored in its own file.
  std::string fname = SeqnoFilename(logid);
  rocketspeed::FileLock* fileLock;

  while (!(env_->LockFile(fname, &fileLock).ok() && fileLock)) {
    // Another thread or process has the lock, so yield to let them finish.
    std::this_thread::yield();
  }

  // Lock acquired, now try to open the file.
  rocketspeed::EnvOptions opts;
  opts.use_mmap_writes = false;  // PosixRandomRWFile doesn't support this
  std::unique_ptr<rocketspeed::RandomRWFile> file;
  if (!(env_->NewRandomRWFile(fname, &file, opts).ok() && file)) {
    env_->UnlockFile(fileLock);
    return LSN_INVALID;
  }

  // Get the file size to use as the write offset (to append).
  lsn_t lsn = LastSeqnoWritten(fname, file, env_);

  // Write new seqno.
  lsn_t newlsn = lsn + 1;
  uint8_t buff[sizeof(lsn_t)];
  for (int i = static_cast<int>(sizeof(buff)) - 1; i >= 0; --i) {
    buff[i] = newlsn & 0xff;
    newlsn >>= 8;
  }
  file->Write(0, rocketspeed::Slice(reinterpret_cast<char*>(buff),
                                    sizeof(buff)));
  env_->UnlockFile(fileLock);
  return lsn;
}

std::shared_ptr<Client> Client::create(
  std::string cluster_name,
  std::string config_url,
  std::string credentials,
  std::chrono::milliseconds timeout,
  std::unique_ptr<ClientSettings> &&settings) noexcept {
  // ignore cluster_name, config_url, and credentials.
  ClientImpl* impl = new ClientImpl();
  impl->env_ = rocketspeed::Env::Default();
  impl->settings_ = std::move(settings);
  impl->timeout_ = timeout;

  // Make sure log directory exists
  impl->env_->CreateDirIfMissing(MOCK_LOG_DIR);

  return std::shared_ptr<Client>(impl);
}

lsn_t Client::appendSync(logid_t logid, const Payload& payload) noexcept {
  using std::chrono::duration_cast;
  using std::chrono::microseconds;
  using std::chrono::milliseconds;

  // Try to get a lock on the log file.
  std::string fname = LogFilename(logid);
  rocketspeed::FileLock* fileLock;

  // Keep trying to lock the file forever!
  // Append should not fail just because someone is reading the file.
  while (!(impl()->env_->LockFile(fname, &fileLock).ok() && fileLock)) {
    // Another thread or process has the lock, so yield to let them finish.
    std::this_thread::yield();
  }

  // Lock acquired, now try to open the file.
  rocketspeed::EnvOptions opts;
  opts.use_mmap_writes = false;  // PosixRandomRWFile doesn't support this
  std::unique_ptr<rocketspeed::RandomRWFile> file;
  if (!(impl()->env_->NewRandomRWFile(fname, &file, opts).ok() && file)) {
    impl()->env_->UnlockFile(fileLock);
    return LSN_INVALID;
  }

  auto now = std::chrono::system_clock::now().time_since_epoch();

  RecordHeader header;
  header.lsn = impl()->GetLogSeqno(logid);
  header.timestamp = duration_cast<milliseconds>(now).count();
  header.datasize = payload.size;

  // Get the file size to use as the write offset (to append).
  uint64_t offset;
  if (!impl()->env_->GetFileSize(fname, &offset).ok()) {
    impl()->env_->UnlockFile(fileLock);
    return LSN_INVALID;
  }

  // Write header.
  rocketspeed::Slice headerSlice(reinterpret_cast<const char*>(&header),
                                 sizeof(RecordHeader));
  file->Write(offset, headerSlice);

  // Write payload.
  rocketspeed::Slice payloadSlice(reinterpret_cast<const char*>(payload.data),
                                  payload.size);
  file->Write(offset + sizeof(RecordHeader), payloadSlice);

  // Clean up.
  impl()->env_->UnlockFile(fileLock);
  return header.lsn;
}

int Client::append(logid_t logid,
                   const Payload& payload,
                   append_callback_t cb) noexcept {
  // Synchronous for now.
  // TODO(pja) 1 : Create some sort of event loop to for async handling.
  lsn_t lsn = appendSync(logid, payload);
  DataRecord record(logid, payload, lsn);
  if (lsn == LSN_INVALID) {
    cb(E::FAILED, record);
  } else {
    cb(E::OK, record);
  }
  return 0;
}

std::unique_ptr<Reader> Client::createReader(
  size_t max_logs,
  ssize_t buffer_size) noexcept {
  RS_ASSERT(false);  // not implemented
  return std::unique_ptr<Reader>(nullptr);
}

std::unique_ptr<AsyncReader> Client::createAsyncReader() noexcept {
  return std::unique_ptr<AsyncReader>(new AsyncReaderImpl());
}

void Client::setTimeout(std::chrono::milliseconds timeout) noexcept {
  impl()->timeout_ = timeout;
}

int Client::trimSync(logid_t logid, lsn_t lsn) noexcept {
  // Find file offset to trim to.
  std::string fname = LogFilename(logid);
  uint64_t offset = 0;
  bool found = false;
  {
    LogFile file(logid, true);
    while (file.Next()) {
      if (file.GetLSN() > lsn) {
        // File offset will be at end of header here, so rewind a little.
        RS_ASSERT(file.GetOffset() >= sizeof(RecordHeader));
        offset = file.GetOffset() - sizeof(RecordHeader);
        found = true;
        break;
      }
    }
  }

  if (!found) {
    // Didn't find any LSN after the trim point, so delete the whole file.
    impl()->env_->DeleteFile(fname);
  }

  if (offset == 0) {
    // Nothing to trim
    return 0;
  }

  // Get a lock. Didn't need this above as LogFile has its own lock.
  rocketspeed::ScopedFileLock fileLock(impl()->env_, fname, false);

  // Get file size
  uint64_t fileSize;
  if (!impl()->env_->GetFileSize(fname, &fileSize).ok()) {
    return 1;
  }

  rocketspeed::EnvOptions opts;
  opts.use_mmap_writes = false;  // PosixRandomRWFile doesn't support this

  // Read tail of the file into temporary buffer
  std::vector<char> buffer;
  rocketspeed::Slice tailSlice;
  {
    uint64_t tailSize = fileSize - offset;
    buffer.resize(fileSize - offset);
    std::unique_ptr<rocketspeed::RandomAccessFile> file;
    if (!(impl()->env_->NewRandomAccessFile(fname, &file, opts).ok() && file)) {
      return 1;
    }
    if (!file->Read(offset, tailSize, &tailSlice, buffer.data()).ok()) {
      return 1;
    }
  }

  // Write tail over the old file
  {
    std::unique_ptr<rocketspeed::WritableFile> file;
    if (!(impl()->env_->NewWritableFile(fname, &file, opts).ok() && file)) {
      return 1;
    }
    if (!file->Append(tailSlice).ok()) {
      return 1;
    }
  }

  return 0;
}

lsn_t Client::findTimeSync(logid_t logid,
                           std::chrono::milliseconds timestamp,
                           Status *status_out) noexcept {
  // Opens the log and reads through until we find the timestamp.
  // If not found, returns the last LSN + 1 (i.e. the next LSN).
  // If log empty, returns LSN_OLDEST.
  LogFile file(logid, true);
  lsn_t lsn = LSN_OLDEST;
  while (file.Next()) {
    lsn = file.GetLSN();
    if (file.GetTimestamp() >= timestamp) {
      break;
    }
    ++lsn;
  }
  if (status_out) {
    // E::PARTIAL should be accepted as success also.
    // Fuzzing here to get more code coverage.
    *status_out = impl()->rng_() & 1 ? E::OK : E::PARTIAL;
  }
  return lsn;
}

int Client::findTime(logid_t logid,
                     std::chrono::milliseconds timestamp,
                     find_time_callback_t cb) noexcept {
  // Do it synchronously.
  Status err;
  lsn_t lsn = findTimeSync(logid, timestamp, &err);
  cb(err, lsn);
  return 0;
}

std::pair<logid_t, logid_t> Client::getLogRangeByName(
  const std::string& name) noexcept {
  RS_ASSERT(false);  // not implemented
  return std::make_pair(LOGID_INVALID, LOGID_INVALID);
}

logid_t Client::getLogIdFromRange(const std::string& range_name,
                                  off_t offset) noexcept {
  RS_ASSERT(false);  // not implemented
  return LOGID_INVALID;
}

ClientSettings& Client::settings() {
  return *impl()->settings_.get();
}

ClientImpl *Client::impl() {
  return static_cast<ClientImpl*>(this);
}


}  // namespace logdevice
}  // namespace facebook
