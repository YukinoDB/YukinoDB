#include "base/mem_io.h"

namespace yukino {

namespace base {

Status StringWriter::Write(const void *data, size_t size, size_t *written) {
    buf_.append(static_cast<const char *>(data), size);
    if (written)
        *written = size;
    active_ += size;
    return Status::OK();
}

Status StringWriter::Skip(size_t count) {
    buf_.append(count, 0);
    active_ += count;
    return Status::OK();
}

Status StringIO::Write(const void *data, size_t size,
                       size_t *written) {
    if (buf_.size() - active_ < size) {
        buf_.resize(active_ + size);
    }
    ::memcpy(&buf_[active_], data, size);

    active_ += size;
    if (written) *written = size;
    return Status::OK();
}

Status StringIO::Skip(size_t count)  {
    if (buf_.size() - active_ < count) {
        buf_.resize(active_ + count, 0);
    }
    active_ += count;
    return Status::OK();
}

Status StringIO::Read(void *buf, size_t size)  {
    Status rs;
    if (active_ + size > buf_.size()) {
        rs = Status::IOError("EOF");
        size = buf_.size() - active_;
    }
    ::memcpy(buf, &buf_[active_], size);
    return rs;
}

int StringIO::ReadByte() {
    if (active_ + 1 > buf_.size()) {
        return EOF;
    }
    return static_cast<uint8_t>(buf_[active_++]);
}

Status StringIO::Ignore(size_t count) {
    active_ += count;
    return Status::OK();
}

Status StringIO::Close() {
    // Nothing.
    return Status::OK();
}

Status StringIO::Flush() {
    // Nothing.
    return Status::OK();
}

Status StringIO::Sync() {
    // Nothing.
    return Status::OK();
}

Status StringIO::Truncate(uint64_t offset) {
    if (active_ > offset) {
        active_ = offset;
    }
    buf_.resize(offset);
    return Status::OK();
}

Status StringIO::Seek(uint64_t offset) {
    if (offset > buf_.size()) {
        return Status::IOError("Seek out of range.");
    }
    active_ = offset;
    return Status::OK();
}

} // namespace base
    
} // namespace yukino