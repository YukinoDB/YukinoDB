#ifndef YUKINO_BASE_IO_INL_H_
#define YUKINO_BASE_IO_INL_H_

#include "base/io.h"

namespace yukino {

namespace base {

//==============================================================================
// class VerifiedWriter<T>
//==============================================================================

template<class T>
Status VerifiedWriter<T>::Write(const void *data, size_t size,
                         size_t *written) {
    checker_.Update(data, size);
    return delegated_->Write(data, size, written);;
}

template<class T>
Status VerifiedWriter<T>::Skip(size_t count) {
    return delegated_->Skip(count);
}

//==============================================================================
// class VerifiedReader<T>
//==============================================================================

template<class T>
Status VerifiedReader<T>::Read(void *buf, size_t size) {
    auto rs = delegated_->Read(buf, size);
    if (rs.ok()) {
        checker_.Update(buf, size);
    }
    return rs;
}

template<class T>
int VerifiedReader<T>::ReadByte() {
    auto rv = delegated_->ReadByte();
    if (rv != EOF) {
        auto byte = static_cast<uint8_t>(rv);
        checker_.Update(&byte, sizeof(byte));
    }
    return rv;
}

//==============================================================================
// class Reader
//==============================================================================

inline Status Reader::ReadString(std::string *str) {
    uint32_t len = 0;
    auto rs = ReadVarint32(&len, nullptr);
    if (!rs.ok()) {
        return rs;
    }
    str->resize(len);
    return Read(&str->at(0), len);
}

inline Status Reader::ReadLargeString(std::string *str) {
    uint64_t len = 0;
    auto rs = ReadVarint64(&len, nullptr);
    if (!rs.ok()) {
        return rs;
    }
    str->resize(len);
    return Read(&str->at(0), len);
}

//==============================================================================
// class BufferedWriter
//==============================================================================

inline BufferedWriter::BufferedWriter(void *buf, size_t size)
    : buf_(static_cast<char*>(buf))
    , cap_(size)
    , ownership_(false) {
}

inline Status BufferedWriter::Write(char ch) {
    if (!Advance(1)) {
        return Status::Corruption("not enough memory.");
    }
    buf_[len_++] = ch;
    return Status::OK();
}

inline char *BufferedWriter::Drop() {
    auto droped = buf_;
    len_ = 0;
    cap_ = 0;
    buf_ = nullptr;
    return droped;
}

inline void BufferedWriter::Clear() {
    delete[] buf_;
    len_ = 0;
    cap_ = 0;
    buf_ = nullptr;
}

//==============================================================================
// class BufferedReader
//==============================================================================

inline BufferedReader::BufferedReader(const void *buf, size_t len)
    : buf_(static_cast<const uint8_t *>(DCHECK_NOTNULL(buf)))
    , active_(len) {
    DCHECK_LT(0, active_);
}

inline Slice BufferedReader::Read(size_t count) {
    DCHECK_GE(active_, count);
    Slice rv(typed<char>(), count);
    Advance(count);
    return rv;
}

inline uint16_t BufferedReader::ReadFixed16() {
    DCHECK_GE(active_, sizeof(uint16_t));
    auto rv = *typed<uint16_t>();
    Advance(sizeof(uint16_t));
    return rv;
}

inline uint32_t BufferedReader::ReadFixed32() {
    DCHECK_GE(active_, sizeof(uint32_t));
    auto rv = *typed<uint32_t>();
    Advance(sizeof(uint32_t));
    return rv;
}

inline uint64_t BufferedReader::ReadFixed64() {
    DCHECK_GE(active_, sizeof(uint64_t));
    auto rv = *typed<uint64_t>();
    Advance(sizeof(uint64_t));
    return rv;
}

inline uint8_t BufferedReader::ReadByte() {
    DCHECK_GE(active_, sizeof(uint8_t));
    auto rv = *buf_;
    Advance(sizeof(uint8_t));
    return rv;
}

inline void BufferedReader::Ignore(size_t count) {
    DCHECK_GE(active_, count);
    Advance(count);
}

//==============================================================================
// class MappedMemory
//==============================================================================

inline const uint8_t *MappedMemory::buf(size_t offset) const {
    DCHECK_LT(offset, len_);
    return buf_ + offset;
}

/*static*/
inline MappedMemory MappedMemory::Attach(std::string *buf) {
    return Attach(&buf->at(0), buf->length());
}

/*static*/
inline MappedMemory MappedMemory::Attach(void *buf, size_t len) {
    return MappedMemory(":memory:", buf, len);
}
} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_IO_INL_H_