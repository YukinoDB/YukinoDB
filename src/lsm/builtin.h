#ifndef YUKINO_LSM_BUILTIN_H_
#define YUKINO_LSM_BUILTIN_H_

#include <stddef.h>
#include <stdint.h>

#define LSM_FILE_VERSION 0x00010001
#define LSM_MAGIC_NUMBER 0xa000000a
#define LSM_RESTART_INTERVAL 32

namespace yukino {

namespace lsm {

static const uint32_t kBlockFixedSize = sizeof(uint32_t) // number of restarts
    + sizeof(uint8_t)   // type
    + sizeof(uint32_t); // crc32 checksum

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_BUILTIN_H_