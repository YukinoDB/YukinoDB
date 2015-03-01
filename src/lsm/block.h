#ifndef YUKINO_LSM_BLOCK_H_
#define YUKINO_LSM_BLOCK_H_

#include "lsm/builtin.h"
#include "yukino/iterator.h"
#include "base/io.h"
#include "base/status.h"
#include "base/slice.h"
#include "base/base.h"
#include <vector>

namespace yukino {

namespace base {

class Writer;

} // namespace base

class Comparator;

namespace lsm {

class BlockHandle;
class Chunk;

class BlockBuilder : public base::DisableCopyAssign {
public:
    BlockBuilder(base::Writer *writer, size_t block_size, int restart_interval);

    bool CanAppend(const Chunk &chunk) const;

    base::Status Append(const Chunk &chunk);

    base::Status Finalize(char type, BlockHandle *handle);

    size_t CalcChunkSize(const Chunk &chunk) const;

    uint32_t CalcSharedSize(const base::Slice &key, bool *should_restart) const;

    base::Writer *writer() const { return writer_.get(); }

    bool unlimited() const { return unlimited_; }
    void SetUnlimited(bool unlimited) { unlimited_ = unlimited; }

    void SetOffset(uint64_t offset) { offset_ = offset; }

private:

    void Reset();

    std::unique_ptr<base::Writer> writer_;
    size_t block_size_;
    const size_t fixed_block_size_;
    const int restart_interval_;

    uint32_t active_size_;
    uint64_t offset_ = 0;
    int restart_count_;

    uint32_t last_shared_size_;
    base::Slice last_key_;

    std::vector<uint32_t> index_;

    bool unlimited_;
};

class BlockHandle {
public:
    explicit BlockHandle(uint64_t offset) : offset_(offset) {};
    BlockHandle() : BlockHandle(0) {}
    BlockHandle(const BlockHandle &) = default;

    BlockHandle &operator = (const BlockHandle &) = default;

    uint64_t offset() const { return offset_; }
    uint64_t size() const { return size_; }

    uint64_t NumberOfBlocks(size_t block_size) {
        return (size() + block_size - 1) / block_size;
    }

    void set_size(uint64_t size) { size_ = size; }
private:
    // void set_offset(uint64_t offset) { offset_ = offset; }

    uint64_t offset_;
    uint64_t size_ = 0;
};

class BlockIterator : public Iterator {
public:
    BlockIterator(const Comparator *comparator, const void *base, size_t size);
    virtual ~BlockIterator();

    virtual bool Valid() const override;
    virtual void SeekToFirst() override;
    virtual void SeekToLast() override;
    virtual void Seek(const base::Slice& target) override;
    virtual void Next() override;
    virtual void Prev() override;
    virtual base::Slice key() const override;
    virtual base::Slice value() const override;
    virtual base::Status status() const override;

private:
    struct Pair {
        Pair() {}
        Pair(Pair &&other) : key(std::move(other.key)), value(other.value) {}

        std::string key;
        base::Slice value;
    };

    const uint8_t *PrepareRead(size_t i);
    const uint8_t *Read(const std::string &prev, const uint8_t *p, Pair *rv);

    const Comparator *comparator_;
    const uint8_t *base_;
    const uint8_t *data_end_;
    const uint32_t *restarts_;
    size_t num_restarts_;
    base::Status status_;

    int64_t curr_restart_ = 0;
    int64_t curr_local_ = 0;
    std::vector<Pair> local_;
};


} // namespace lsm

} // namespace yukino


#endif // YUKINO_LSM_BLOCK_H_