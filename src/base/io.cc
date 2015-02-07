#include "base/io.h"
#include "base/varint_encoding.h"

namespace yukino {

namespace base {

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

} // namespace base
    
} // namespace yukino