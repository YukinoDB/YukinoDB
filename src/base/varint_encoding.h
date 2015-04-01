#ifndef YUKINO_BASE_VARINT_ENCODING_H_
#define YUKINO_BASE_VARINT_ENCODING_H_

#include "base/base.h"
#include <stdint.h>
#include <stddef.h>

namespace yukino {

namespace base {

class Varint64 {
public:
    Varint64() = delete;
    ~Varint64() = delete;

    static size_t Encode(void *buf, uint64_t value);

    static uint64_t Decode(const void *buf, size_t *len);

    static size_t Sizeof(uint64_t value) {
        if (value == 0) {
            return 1;
        } else {
            return (64 - Bits::CountLeadingZeros64(value) + 6) / 7;
        }
    }

    static const size_t kMaxLen = 10;
};

class Varint32 {
public:
    Varint32() = delete;
    ~Varint32() = delete;

    static size_t Encode(void *buf, uint32_t value);

    static uint32_t Decode(const void *buf, size_t *len) {
        return static_cast<uint32_t>(Varint64::Decode(buf, len));
    }

    static size_t Sizeof(uint32_t value) {
        if (value == 0) {
            return 1;
        } else {
            return (32 - Bits::CountLeadingZeros32(value) + 6) / 7;
        }
    }

    static const size_t kMaxLen =  5;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_VARINT_ENCODING_H_
