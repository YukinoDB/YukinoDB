#ifndef YUKINO_BALANCE_BLOCK_BUFFER_INL_H_
#define YUKINO_BALANCE_BLOCK_BUFFER_INL_H_

#include "balance/block_buffer.h"
#include "base/varint_encoding.h"

namespace yukino {

namespace balance {

inline BlockBuffer::BlockBuffer(size_t block_size)
    : block_size_(block_size)
    , payload_size_(block_size - Block::kHeaderSize) {
    DCHECK_GT(block_size_, Block::kHeaderSize);
}

inline base::Slice BlockBuffer::Read(uint64_t addr) {
    auto offset = ToRelactive(addr);
    auto p = buf().data() + offset;

    size_t len = 0;
    auto size = base::Varint32::Decode(p, &len);
    return {p + len, size};
}

inline void BlockBuffer::Clear() {
    writer_.Clear();
    block_offset_ = 0;
}

inline base::Slice BlockBuffer::buf() const {
    return {writer_.buf(), writer_.len()};
}

inline uint64_t BlockBuffer::ToPhysical(size_t offset) const {
    DCHECK_LT(offset, buf().size());
    return (block_size_ * (offset / payload_size_)) + (offset % payload_size_);
}

inline size_t BlockBuffer::ToRelactive(uint64_t addr) const {
    auto index_block = addr / block_size_;
    auto offset = index_block * payload_size_ + addr % block_size_;
    DCHECK_LT(offset, buf().size());
    return offset;
}

} // namespace balance

} // namespace yukino


#endif // YUKINO_BALANCE_BLOCK_BUFFER_INL_H_