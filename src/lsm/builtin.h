#ifndef YUKINO_LSM_BUILTIN_H_
#define YUKINO_LSM_BUILTIN_H_

#include <stddef.h>
#include <stdint.h>

namespace yukino {

namespace lsm {

static const uint32_t kTrailerSize = sizeof(uint8_t) // type
    + sizeof(uint32_t); // crc32 check sum

static const uint32_t kBlockFixedSize = sizeof(uint32_t) // number of restarts
    + kTrailerSize;

static const char kTypeData = 0;
static const char kTypeIndex = 1;

static const uint32_t kFileVersion = 0x00010001;
static const uint32_t kMagicNumber = 0xa000000a;
static const int kRestartInterval = 32;

static const size_t kFooterFixedSize = 512;
static const uint8_t kPaddingByte = 0xff;

static const size_t kBottomFixedSize = sizeof(uint32_t); // magic number


} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_BUILTIN_H_