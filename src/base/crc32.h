#ifndef YUKINO_BASE_CRC32_H_
#define YUKINO_BASE_CRC32_H_

#include "base/base.h"
#include <stddef.h>
#include <stdint.h>


// in file: base/crc32.c
// from Apple Inc.
extern "C" uint32_t crc32(uint32_t crc, const void *buf, size_t size);

namespace yukino {

namespace base {

class CRC32 : public DisableCopyAssign {
public:
    typedef uint32_t DigestTy;

    explicit CRC32(DigestTy initial_value)
        : digest_(initial_value) {
    }

    CRC32();

    void Update(const void *data, size_t size) {
        digest_ = ::crc32(digest_, data, size);
    }

    void Reset() { digest_ = 0U; }

    DigestTy digest() const { return digest_; }

private:
    DigestTy digest_ = 0;

}; // class CRC32


} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_CRC32_H_