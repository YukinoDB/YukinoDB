#ifndef YUKINO_BASE_MEM_IO_H_
#define YUKINO_BASE_MEM_IO_H_

#include "base/io-inl.h"
#include "base/io.h"
#include <string>

namespace yukino {

namespace base {

class StringWriter : public Writer {
public:
    virtual Status Write(const void *data, size_t size,
                         size_t *written) override;

    virtual Status Skip(size_t count) override;

    const std::string &buf() const { return buf_; }

    std::string *mutable_buf() { return &buf_; }

    std::string &&move_buf() { return std::move(buf_); }

private:
    std::string buf_;
};

class StringIO : public FileIO {
public:
    virtual Status Write(const void *data, size_t size,
                         size_t *written) override;
    virtual Status Skip(size_t count) override;

    virtual Status Read(void *buf, size_t size) override;
    virtual int ReadByte() override;
    virtual Status Ignore(size_t count) override;

    virtual base::Status Close() override;
    virtual base::Status Flush() override;
    virtual base::Status Sync() override;

    virtual base::Status Truncate(uint64_t offset) override;
    virtual base::Status Seek(uint64_t offset) override;

    const std::string &buf() const { return buf_; }
    std::string *mutable_buf() { return &buf_; }
    std::string &&move_buf() { return std::move(buf_); }

    void Reset() { Seek(0); buf_.clear(); }
private:
    std::string buf_;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_MEM_IO_H_