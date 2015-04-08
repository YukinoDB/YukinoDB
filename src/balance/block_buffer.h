#ifndef YUKINO_BALANCE_BLOCK_BUFFER_H_
#define YUKINO_BALANCE_BLOCK_BUFFER_H_

#include "base/io-inl.h"
#include "base/io.h"
#include "base/base.h"

namespace yukino {

namespace balance {

struct Block final {

    static const size_t kHeaderSize
        = sizeof(uint32_t)  // crc32
        + sizeof(uint8_t)   // type
        + sizeof(uint32_t); // len


    Block() = delete;
    ~Block() = delete;

}; // struct Block

class BlockBuffer : public base::DisableCopyAssign {
public:
    inline BlockBuffer(size_t block_size);

    uint64_t Append(const base::Slice &record);

    inline base::Slice Read(uint64_t addr);

    inline void Clear();

    inline base::Slice buf() const;

private:
    inline uint64_t ToPhysical(size_t offset) const;
    inline size_t   ToRelactive(uint64_t addr) const;

    const size_t block_size_; // origin block size;
    const size_t payload_size_; // real block size;
    size_t block_offset_ = 0;
    base::BufferedWriter writer_;

}; // class BlockBuffer

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_BLOCK_BUFFER_H_