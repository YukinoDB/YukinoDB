#ifndef YUKINO_BASE_IO_H_
#define YUKINO_BASE_IO_H_

#include "base/status.h"
#include "base/slice.h"
#include "base/base.h"
#include "glog/logging.h"

namespace yukino {

namespace base {

/**
 * The sequence wirter.
 *
 */
class Writer : public DisableCopyAssign {
public:
    Writer();
    virtual ~Writer();

    Status Write(const Slice &buf, size_t *written) {
        return Write(buf.data(), buf.size(), written);
    }

    Status WriteVarint32(uint32_t value, size_t *written);

    Status WriteVarint64(uint64_t value, size_t *written);

    Status WriteFixed32(uint32_t value) {
        return Write(&value, sizeof(value), nullptr);
    }

    Status WriteFixed64(uint64_t value) {
        return Write(&value, sizeof(value), nullptr);
    }

    virtual Status Write(const void *data, size_t size, size_t *written) = 0;

    virtual Status Skip(size_t count) = 0;

    size_t active() const { return active_; }

protected:
    size_t active_ = 0;
};


class BufferedWriter : public Writer {
public:
    BufferedWriter() = default;
    BufferedWriter(size_t size) { Reserve(size); }
    virtual ~BufferedWriter() = default;

    virtual Status Write(const void *data, size_t size, size_t *written) override;

    virtual Status Skip(size_t count) override;

    const char *buf() const { return buf_.get(); }
    size_t len() const { return len_; }
    size_t cap() const { return cap_; }

    char *mutable_buf() { return buf_.get(); }

    char *Drop() {
        auto droped = buf_.release();
        len_ = 0;
        cap_ = 0;
        return droped;
    }

    char *tail() { return mutable_buf() + len_; }

    bool Reserve(size_t size);
private:
    bool Advance(size_t add);

    std::unique_ptr<char[]> buf_;
    size_t len_ = 0;
    size_t cap_ = 0;
};

template<class Checksum>
class VerifiedWriter : public Writer {
public:
    VerifiedWriter(Writer *delegated) : delegated_(delegated) {}
    virtual ~VerifiedWriter() {}

    virtual Status Write(const void *data, size_t size,
                         size_t *written) override {
        checker_.Update(data, size);
        return delegated_->Write(data, size, written);
    }

    virtual Status Skip(size_t count) override {
        return delegated_->Skip(count);
    }

    void Reset() {
        checker_.Reset();
    }

    typename Checksum::DigestTy digest() const {
        return checker_.digest();
    }

    Writer *delegated() const {
        return delegated_;
    }

private:
    Writer *delegated_;
    Checksum checker_;
};

class BufferedReader : public DisableCopyAssign {
public:
    BufferedReader(const void *buf, size_t len)
        : buf_(static_cast<const uint8_t *>(DCHECK_NOTNULL(buf)))
        , active_(len) {
        DCHECK_LT(0, active_);
    }

    Slice Read(size_t count) {
        DCHECK_GE(active_, count);
        Slice rv(typed<char>(), count);
        Advance(count);
        return rv;
    }

    uint32_t ReadFixed32() {
        DCHECK_GE(active_, sizeof(uint32_t));
        auto rv = *typed<uint32_t>();
        Advance(sizeof(uint32_t));
        return rv;
    }

    uint64_t ReadFixed64() {
        DCHECK_GE(active_, sizeof(uint64_t));
        auto rv = *typed<uint64_t>();
        Advance(sizeof(uint64_t));
        return rv;
    }

    uint32_t ReadVarint32();

    uint64_t ReadVarint64();

    uint8_t ReadByte() {
        DCHECK_GE(active_, sizeof(uint8_t));
        auto rv = *buf_;
        Advance(sizeof(uint8_t));
        return rv;
    }

    void Skip(size_t count) {
        DCHECK_GE(active_, count);
        Advance(count);
    }

    size_t active() const { return active_; }

    const uint8_t *current() const { return buf_; }

private:
    void Advance(size_t count) {
        buf_    += count;
        active_ -= count;
    }

    template<class T>
    inline const T *typed() {
        return reinterpret_cast<const T*>(buf_);
    }

    const uint8_t *buf_;
    size_t active_;
};

class MappedMemory : public DisableCopyAssign {
public:
    MappedMemory(const std::string &file_name, void *buf, size_t len);
    MappedMemory(MappedMemory &&other);
    virtual ~MappedMemory();

    bool Valid() const { return buf_ != nullptr && len_ > 0; }

    size_t size() const { return len_; }

    const uint8_t *buf() const { return buf(0); }
    const uint8_t *buf(size_t offset) const {
        DCHECK_LT(offset, len_);
        return buf_ + offset;
    }

    const std::string &file_name() const { return file_name_; }

    static MappedMemory Attach(std::string *buf) {
        return Attach(&buf->at(0), buf->length());
    }

    static MappedMemory Attach(void *buf, size_t len) {
        return MappedMemory(":memory:", buf, len);
    }

protected:
    uint8_t *buf_;
    size_t   len_;

private:
    std::string file_name_;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_IO_H_