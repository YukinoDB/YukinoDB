#include "base/io.h"
#include "base/varint_encoding.h"

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

} // namespace base
    
} // namespace yukino