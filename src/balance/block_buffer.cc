#include "balance/block_buffer-inl.h"
#include "balance/block_buffer.h"

namespace yukino {

namespace balance {

uint64_t BlockBuffer::Append(const base::Slice &record) {
    auto avil = payload_size_ - block_offset_;
    if (avil < base::Varint32::kMaxLen) {
        writer_.Skip(avil);
        block_offset_ = payload_size_;
    }

    auto position = writer_.active();
    size_t written = 0;
    writer_.WriteString(record, &written);
    block_offset_ += written;

    if (block_offset_ >= payload_size_) {
        block_offset_ %= payload_size_;
    }
    return ToPhysical(position);
}


} // namespace balance

} // namespace yukino