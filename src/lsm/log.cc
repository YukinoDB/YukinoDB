#include "lsm/log.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

LogWriter::LogWriter(base::Writer *writer, size_t block_size)
    : block_size_(block_size)
    , writer_(writer) {
    for (auto i = 0; i <= Log::kMaxRecordType; i++) {
        auto c = static_cast<uint8_t>(i);
        typed_checksums_[i] = ::crc32(0, &c, 1);
    }
}

base::Status LogWriter::Append(const base::Slice &record) {
    auto left = record.size();
    auto p = record.data();
    base::Status rs;

    auto begin = true;
    do {
        auto left_over = block_size_ - block_offset_;
        DCHECK_GE(left_over, 0);

        if (left_over < Log::kHeaderSize) {
            if (left_over > 0) {
                rs = writer_->Write("\x00\x00\x00\x00\x00\x00\x00",
                                    left_over, nullptr);
                if (!rs.ok()) {
                    break;
                }
            }
            block_offset_ = 0;
        }

        DCHECK_GE(block_size_ - block_offset_ - Log::kHeaderSize, 0);

        const size_t avail = block_size_ - block_offset_ - Log::kHeaderSize;
        const size_t fragment_length = (left < avail) ? left : avail;

        Log::RecordType type;
        const bool end = (left == fragment_length);
        if (begin && end) {
            type = Log::kFullType;
        } else if (begin) {
            type = Log::kFirstType;
        } else if (end) {
            type = Log::kLastType;
        } else {
            type = Log::kMiddleType;
        }

        rs = EmitPhysicalRecord(p, fragment_length, type);
        p += fragment_length;
        left -= fragment_length;
        begin = false;
    } while (rs.ok() && left > 0);

    return rs;
}

base::Status LogWriter::EmitPhysicalRecord(const void *buf, size_t len,
                                   Log::RecordType type) {
    DCHECK_LE(len, UINT16_MAX);
    DCHECK_LE(block_offset_ + Log::kHeaderSize + len, block_size_);

    auto checksum = ::crc32(typed_checksums_[type], buf, len);
    auto rs = writer_->WriteFixed32(checksum);
    if (!rs.ok()) {
        return rs;
    }
    rs = writer_->WriteFixed16(static_cast<uint16_t>(len));
    if (!rs.ok()) {
        return rs;
    }
    rs = writer_->WriteByte(static_cast<uint8_t>(type));
    if (!rs.ok()) {
        return rs;
    }

    rs = writer_->Write(buf, len, nullptr);
    if (!rs.ok()) {
        return rs;
    }

    block_offset_ += (Log::kHeaderSize + len);
    return base::Status::OK();
}

LogReader::LogReader(const void *buf, size_t len, bool checksum,
                     size_t block_size)
    : block_size_(block_size)
    , checksum_(checksum)
    , reader_(buf, len) {
}

bool LogReader::Read(base::Slice *slice, std::string* scratch) {
    if (reader_.active() == 0) {
        return false;
    }
    DCHECK_GT(reader_.active(), Log::kHeaderSize);

    auto fail = 0, segment = 0;
    Log::RecordType type = Log::kZeroType;
    do {
        auto left_over = block_size_ - block_offset_;
        DCHECK_GE(left_over, 0);

        if (left_over < Log::kHeaderSize) {
            if (left_over > 0) {
                reader_.Skip(left_over);
            }
            block_offset_ = 0;
        }

        if (segment > 0) {
            if (segment == 1) {
                scratch->clear();
            }
            scratch->append(slice->data(), slice->size());
        }

        type = ReadPhysicalRecord(slice, &fail);
        segment++;
    } while (type == Log::kMiddleType ||
             type == Log::kFirstType);

    if (fail > 0) {
        status_ = base::Status::IOError("crc32 checksum fail.");
    } else {
        status_ = base::Status::OK();
    }

    if (segment > 1) {
        scratch->append(slice->data(), slice->size());
        *slice = *scratch;
    }
    return true;
}

Log::RecordType LogReader::ReadPhysicalRecord(base::Slice *slice, int *fail) {

    auto checksum = reader_.ReadFixed32();
    auto len = reader_.ReadFixed16();
    auto type = reader_.ReadByte();

    *slice = reader_.Read(len);

    if (checksum_) {
        base::CRC32 crc32;

        crc32.Update(&type, 1);
        crc32.Update(slice->data(), slice->size());

        if (crc32.digest() != checksum) {
            (*fail)++;
        }
    }
    block_offset_ += (Log::kHeaderSize + len);
    return static_cast<Log::RecordType>(type);
}

} // namespace lsm
    
} // namespace yukino