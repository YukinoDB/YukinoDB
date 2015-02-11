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

} // namespace base
    
} // namespace yukino