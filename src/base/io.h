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

    // Write string with length
    Status WriteString(const Slice &str, size_t *written);

    Status WriteVarint32(uint32_t value, size_t *written);

    Status WriteVarint64(uint64_t value, size_t *written);

    Status WriteByte(uint8_t value) {
        return Write(&value, sizeof(value), nullptr);
    }

    Status WriteFixed16(uint16_t value) {
        return Write(&value, sizeof(value), nullptr);
    }

    Status WriteFixed32(uint32_t value) {
        return Write(&value, sizeof(value), nullptr);
    }

    Status WriteFixed64(uint64_t value) {
        return Write(&value, sizeof(value), nullptr);
    }

    virtual Status Write(const void *data, size_t size, size_t *written) = 0;

    virtual Status Skip(size_t count) = 0;

    virtual size_t active() const { return active_; }

protected:
    size_t active_ = 0;
};

/**
 * The sequence reader
 */
class Reader : public DisableCopyAssign {
public:
    Reader();
    virtual ~Reader();

    Status ReadString(std::string *str) {
        uint32_t len = 0;
        auto rs = ReadVarint32(&len, nullptr);
        if (!rs.ok()) {
            return rs;
        }
        str->resize(len);
        return Read(&str->at(0), len);
    }

    Status ReadLargeString(std::string *str) {
        uint64_t len = 0;
        auto rs = ReadVarint64(&len, nullptr);
        if (!rs.ok()) {
            return rs;
        }
        str->resize(len);
        return Read(&str->at(0), len);
    }

    Status ReadVarint32(uint32_t *value, size_t *read);

    Status ReadVarint64(uint64_t *value, size_t *read);

    Status ReadFixed16(uint16_t *value) { return Read(value, sizeof(*value)); }

    Status ReadFixed32(uint32_t *value) { return Read(value, sizeof(*value)); }

    Status ReadFixed64(uint64_t *value) { return Read(value, sizeof(*value)); }

    virtual Status Read(void *buf, size_t size) = 0;

    virtual int ReadByte() = 0;

    /**
     * Ignore count bytes for read.
     *
     */
    virtual Status Ignore(size_t count) = 0;
};


class BufferedWriter : public Writer {
public:
    BufferedWriter() = default;
    BufferedWriter(size_t size) { Reserve(size); }
    virtual ~BufferedWriter() = default;

    virtual Status Write(const void *data, size_t size, size_t *written) override;

    virtual Status Skip(size_t count) override;

    Status Write(char ch) {
        if (!Advance(1)) {
            return Status::Corruption("not enough memory.");
        }
        buf_[len_++] = ch;
        return Status::OK();
    }

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

    void Clear() {
        std::unique_ptr<char[]>().swap(buf_);
        len_ = 0;
        cap_ = 0;
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
        return delegated_->Write(data, size, written);;
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

    virtual size_t active() const override { return delegated_->active(); }

private:
    Writer *delegated_;
    Checksum checker_;
};

template<class Checksum>
class VerifiedReader : public Reader {
public:
    VerifiedReader(Reader *delegated) : delegated_(delegated) {}
    virtual ~VerifiedReader() {}

    virtual Status Read(void *buf, size_t size) {
        auto rs = delegated_->Read(buf, size);
        if (rs.ok()) {
            checker_.Update(buf, size);
        }
        return rs;
    }

    virtual int ReadByte() {
        auto rv = delegated_->ReadByte();
        if (rv != EOF) {
            auto byte = static_cast<uint8_t>(rv);
            checker_.Update(&byte, sizeof(byte));
        }
        return rv;
    }

    virtual Status Ignore(size_t count) {
        return delegated_->Ignore(count);
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
    Reader *delegated_;
    Checksum checker_;
};

/**
 * Read from a buffer.
 *
 */
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

    Slice ReadString() { return Read(ReadVarint32()); }

    Slice ReadLargeString() { return Read(ReadVarint64()); }

    char Read() {
        auto ch = *typed<char>();
        Advance(1);
        return ch;
    }

    uint16_t ReadFixed16() {
        DCHECK_GE(active_, sizeof(uint16_t));
        auto rv = *typed<uint16_t>();
        Advance(sizeof(uint16_t));
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

    virtual base::Status Close();
    virtual base::Status Sync(size_t offset, size_t len);

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

class AppendFile : public Writer {
public:
    virtual ~AppendFile();

    virtual Status Close() = 0;
    virtual Status Flush() = 0;
    virtual Status Sync() = 0;
};

/**
 * Complete file IO. contains: reader, writer, flush ...
 */
class FileIO : public AppendFile
             , public Reader {
public:

    virtual Status Truncate(uint64_t offset) = 0;
    virtual Status Seek(uint64_t offset) = 0;
};

class FileLock : public DisableCopyAssign {
public:
    virtual ~FileLock();

    virtual Status Lock() const = 0;
    virtual Status Unlock() const = 0;
    virtual std::string name() const = 0;
    virtual bool locked() const = 0;
};

base::Status WriteAll(const std::string &file_name, const base::Slice &buf,
                      size_t *written);

base::Status ReadAll(const std::string &file_name, std::string *buf);

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_IO_H_