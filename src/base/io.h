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

    inline Status ReadString(std::string *str);

    inline Status ReadLargeString(std::string *str);

    Status ReadFixed16(uint16_t *value) { return Read(value, sizeof(*value)); }

    Status ReadFixed32(uint32_t *value) { return Read(value, sizeof(*value)); }

    Status ReadFixed64(uint64_t *value) { return Read(value, sizeof(*value)); }

    Status ReadVarint32(uint32_t *value, size_t *read);

    Status ReadVarint64(uint64_t *value, size_t *read);

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

    explicit BufferedWriter(size_t size) { Reserve(size); }

    inline BufferedWriter(void *buf, size_t size);

    virtual ~BufferedWriter();

    virtual Status Write(const void *data, size_t size, size_t *written) override;

    virtual Status Skip(size_t count) override;

    inline Status Write(char ch);

    const char *buf() const { return buf_; }
    size_t len() const { return len_; }
    size_t cap() const { return cap_; }

    char *mutable_buf() { return buf_; }

    inline char *Drop();

    inline void Clear();

    char *tail() { return mutable_buf() + len_; }

    bool Reserve(size_t size);

    virtual size_t active() const override { return len(); }
private:
    bool Advance(size_t add);

    char *buf_ = nullptr;
    size_t len_ = 0;
    size_t cap_ = 0;
    const bool ownership_ = true;
};

template<class Checksum>
class VerifiedWriter : public Writer {
public:
    VerifiedWriter(Writer *delegated) : delegated_(delegated) {}
    virtual ~VerifiedWriter() {}

    virtual Status Write(const void *data, size_t size,
                         size_t *written) override;

    virtual Status Skip(size_t count) override;

    void Reset() { checker_.Reset(); }

    typename Checksum::DigestTy digest() const { return checker_.digest(); }

    Writer *delegated() const { return delegated_; }

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

    virtual Status Read(void *buf, size_t size);

    virtual int ReadByte();

    virtual Status Ignore(size_t count) { return delegated_->Ignore(count); }

    void Reset() { checker_.Reset(); }

    typename Checksum::DigestTy
    digest() const { return checker_.digest(); }

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
    inline BufferedReader(const void *buf, size_t len);

    inline Slice Read(size_t count);

    Slice ReadString() { return Read(ReadVarint32()); }

    Slice ReadLargeString() { return Read(ReadVarint64()); }

    inline uint8_t ReadByte();
    inline uint16_t ReadFixed16();
    inline uint32_t ReadFixed32();
    inline uint64_t ReadFixed64();

    uint32_t ReadVarint32();
    uint64_t ReadVarint64();

    inline void Ignore(size_t count);

    size_t active() const { return active_; }

    const uint8_t *current() const { return buf_; }

private:
    void Advance(size_t count) {
        buf_    += count;
        active_ -= count;
    }

    template<class T>
    inline const T *typed() { return reinterpret_cast<const T*>(buf_); }

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
    inline const uint8_t *buf(size_t offset) const;

    uint8_t *mutable_buf() { return mutable_buf(0); }
    inline uint8_t *mutable_buf(size_t offset);

    const std::string &file_name() const { return file_name_; }

    inline static MappedMemory Attach(std::string *buf);
    inline static MappedMemory Attach(void *buf, size_t len);

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