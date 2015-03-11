#ifndef YUKINO_LSM_LOG_H_
#define YUKINO_LSM_LOG_H_

#include "base/io.h"
#include "base/crc32.h"

namespace yukino {

namespace base {

class Writer;

} // namespace base

namespace lsm {

class LogWriter;
class LogReader;

struct Log {

    typedef LogWriter Writer;
    typedef LogReader Reader;

    enum RecordType {
        // Zero is reserved for preallocated files
        kZeroType = 0,

        kFullType = 1,

        // For fragments
        kFirstType = 2,
        kMiddleType = 3,
        kLastType = 4
    };

    static const int kMaxRecordType = kLastType;

    static const auto kHeaderSize = 4 + 2 + 1;

    static const int kDefaultBlockSize = 32768;
};

/*
 * +---------+-------+
 * |         | crc32 | 4 bytes
 * |         +-------+
 * | header  | len   | 2 bytes
 * |         +-------+
 * |         | type  | 1 bytes
 * +---------+-------+
 * | payload | data  | len bytes
 * +---------+-------+
 */

class LogWriter {
public:
    LogWriter(base::Writer *writer, size_t block_size);

    base::Status Append(const base::Slice &record);

private:
    base::Status EmitPhysicalRecord(const void *buf, size_t len, Log::RecordType type);

    const size_t block_size_;
    int block_offset_ = 0;

    base::CRC32::DigestTy typed_checksums_[Log::kMaxRecordType + 1];
    base::Writer *writer_;
};

class LogReader {
public:
    LogReader(const void *buf, size_t len, bool checksum, size_t block_size);

    bool Read(base::Slice *slice);

    const base::Status &status() const { return status_; }

private:

    Log::RecordType ReadPhysicalRecord(base::Slice *slice, int *fail);

    const size_t block_size_;
    int block_offset_ = 0;

    bool checksum_;
    base::Status status_;
    base::BufferedReader reader_;
};

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_LOG_H_