#ifndef YUKINO_LSM_BLOCK_H_
#define YUKINO_LSM_BLOCK_H_

#include "lsm/builtin.h"
#include "base/io.h"
#include "base/status.h"
#include "base/slice.h"
#include "base/base.h"
#include <vector>

namespace yukino {

namespace base {

class Writer;

} // namespace base

namespace lsm {

class BlockHandle;
class Chunk;

class BlockBuilder : public base::DisableCopyAssign {
public:
    BlockBuilder(base::Writer *writer, size_t block_size, int restart_interval);

    bool CanAppend(const Chunk &chunk);

    base::Status Append(const Chunk &chunk);

    base::Status Finalize(char type, BlockHandle *handle);

    uint32_t CalcSharedSize(const base::Slice &key, bool *should_restart);
private:

    void Reset();

    std::unique_ptr<base::Writer> writer_;
    size_t block_size_;
    const size_t fixed_block_size_;
    const int restart_interval_;

    uint32_t active_size_;
    int restart_count_;

    uint32_t last_shared_size_;
    base::Slice last_key_;

    std::vector<uint32_t> index_;
};

class BlockHandle {
public:
    explicit BlockHandle(uint64_t offset) : offset_(offset) {};
    BlockHandle(const BlockHandle &) = default;

    BlockHandle &operator = (const BlockHandle &) = default;

    uint64_t offset() { return offset_; }
    uint64_t size() { return size_; }

    uint64_t NumberOfBlocks(size_t block_size) {
        return (size() + block_size - 1) / block_size;
    }

    friend class BlockBuilder;
private:
    // void set_offset(uint64_t offset) { offset_ = offset; }
    void set_size(uint64_t size) { size_ = size; }

    uint64_t offset_ = 0;
    uint64_t size_ = 0;
};

} // namespace lsm

} // namespace yukino


#endif // YUKINO_LSM_BLOCK_H_