#include "base/io.h"
#include "base/varint_encoding.h"
#include <stdio.h>

namespace yukino {

namespace base {

Writer::Writer() {
}

Writer::~Writer() {
}

Status Writer::WriteVarint32(uint32_t value, size_t *written) {
    char buf[Varint32::kMaxLen];

    auto len = Varint32::Encode(buf, value);
    return Write(buf, len, written);
}

Status Writer::WriteVarint64(uint64_t value, size_t *written) {
    char buf[Varint64::kMaxLen];

    auto len = Varint64::Encode(buf, value);
    return Write(buf, len, written);
}

uint32_t BufferedReader::ReadVarint32() {
    size_t read = 0;
    auto rv = Varint32::Decode(buf_, &read);
    DCHECK_GE(active_, read);
    Advance(read);
    return rv;
}

uint64_t BufferedReader::ReadVarint64() {
    size_t read = 0;
    auto rv = Varint64::Decode(buf_, &read);
    DCHECK_GE(active_, read);
    Advance(read);
    return rv;
}

MappedMemory::MappedMemory(const std::string &file_name, void *buf, size_t len)
    : file_name_(file_name)
    , buf_(static_cast<uint8_t*>(buf))
    , len_(len) {
}

MappedMemory::MappedMemory(MappedMemory &&other)
    : file_name_(std::move(other.file_name_))
    , buf_(other.buf_)
    , len_(other.len_) {

    other.buf_ = nullptr;
    other.len_ = 0;
}

MappedMemory::~MappedMemory() {
}

base::Status MappedMemory::Close() {
    return base::Status::OK();
}

base::Status MappedMemory::Sync(size_t, size_t) {
    return base::Status::OK();
}

Status BufferedWriter::Write(const void *data, size_t size, size_t *written) {
    if (!Advance(size)) {
        return Status::Corruption("not enough memory.");
    }

    ::memcpy(tail(), data, size);
    len_ += size;

    if (written)
        *written = size;
    return Status::OK();
}

Status BufferedWriter::Skip(size_t count) {
    if (!Advance(count)) {
        return Status::Corruption("not enough memory.");
    }

    len_ += count;
    return Status::OK();
}

bool BufferedWriter::Reserve(size_t size) {
    if (size < len())
        return false;

    std::unique_ptr<char[]> buf(new char[size]);
    if (!buf)
        return false;
    cap_ = size;
    buf_ = std::move(buf);

    return true;
}

bool BufferedWriter::Advance(size_t add) {

    if (len() + add > cap()) {

        auto res = cap();
        while (res < len() + add) {
            res = res * 2 + 128;
        }

        std::unique_ptr<char[]> buf(new char[res]);
        if (!buf)
            return false;

        ::memcpy(buf.get(), buf_.get(), len_);
        cap_ = res;
        buf_ = std::move(buf);
    }
    return true;
}

AppendFile::~AppendFile() {
}

} // namespace base
    
} // namespace yukino