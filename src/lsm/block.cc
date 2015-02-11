#include "lsm/block.h"
#include "lsm/chunk.h"
#include "yukino/comparator.h"
#include "base/io.h"
#include "base/crc32.h"
#include "base/slice.h"
#include "base/varint_encoding.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

/*
 block:
 +-----------+
 | chunk0    |
 +-----------+
 | chunk1    |
 +-----------+
 | .....     |
 +-----------+
 | chunkN    |
 +-----------+
 | index     | 4 bytes * (num_index)
 +-----------+
 | num_index | 4 bytes
 +-----------+
 | trailer   |
 +-----------+
 
 trailer
 +-------+
 | type  | 1 byte
 +-------+
 | crc32 | 4 bytes
 +-------+
 */

BlockBuilder::BlockBuilder(base::Writer *writer, size_t block_size,
                           int restart_interval)
    : writer_(new base::VerifiedWriter<base::CRC32>(DCHECK_NOTNULL(writer)))
    , fixed_block_size_(block_size)
    , restart_interval_(restart_interval) {

    Reset();
}

/*
 chunk:
 +-------------------+
 | shared_size       | varint32-encoding
 +-------------------+
 | unshared_size     | varint32-encoding
 +-------------------+
 | value_size        | varint64-encoding
 +-------------------+
 | unshared_key_data | unshared_size
 +-------------------+
 | value_data        | value_size
 +-------------------+
 */
bool BlockBuilder::CanAppend(const Chunk &chunk) const {
    auto add_size = CalcChunkSize(chunk);

    // chunk size biger than block size, make it to large block.
    if (add_size > fixed_block_size_) {
        return block_size_ <= fixed_block_size_;
    }

    return active_size_ + add_size < block_size_;
}

base::Status BlockBuilder::Append(const Chunk &chunk) {
    size_t add_size = 0, len = 0;
    auto should_restart = false;

    auto shared_size = CalcSharedSize(chunk.key_slice(), &should_restart);
    auto rs = writer_->WriteVarint32(shared_size, &len);
    if (!rs.ok())
        return rs;
    add_size += len;

    auto unshared_size = chunk.key_size() - shared_size;
    rs = writer_->WriteVarint32(unshared_size, &len);
    if (!rs.ok())
        return rs;
    add_size += len;

    rs = writer_->WriteVarint64(chunk.value_size(), &len);
    if (!rs.ok())
        return rs;
    add_size += len;

    rs = writer_->Write(chunk.key() + shared_size, unshared_size, &len);
    if (!rs.ok())
        return rs;
    add_size += len;

    rs = writer_->Write(chunk.value_slice(), &len);
    if (!rs.ok())
        return rs;
    add_size += len;

    if (should_restart) {
        auto restart_index = static_cast<uint32_t>(writer_->active());

        add_size += sizeof(restart_index);
        index_.push_back(restart_index);

        restart_count_ = 1;
    } else {
        restart_count_++;
    }
    last_shared_size_ = shared_size;
    last_key_ = chunk.key_slice();

    // chunk size is too big, we should make block to large block.
    // ( > 1 fixed_block_size_)
    if (add_size > fixed_block_size_) {
        while (block_size_ < add_size + kBlockFixedSize) {
            block_size_ += fixed_block_size_;
        }
    }

    active_size_ += add_size;
    return base::Status::OK();
}

base::Status BlockBuilder::Finalize(char type, BlockHandle *handle) {
    index_.push_back(static_cast<uint32_t>(index_.size()));

    auto rs = writer_->Write(index_.data(), index_.size() * sizeof(index_[0]),
                             nullptr);
    if (!rs.ok())
        return rs;

    rs = writer_->Write(&type, sizeof(type), nullptr);
    if (!rs.ok())
        return rs;

    auto proxy = static_cast<base::VerifiedWriter<base::CRC32> *>(writer_.get());
    uint32_t crc32_digest = proxy->digest();
    rs = proxy->delegated()->Write(&crc32_digest, sizeof(crc32_digest), nullptr);
    if (!rs.ok())
        return rs;

    DCHECK_NOTNULL(handle)->set_size(active_size_);
    Reset();
    return base::Status::OK();
}

void BlockBuilder::Reset() {
    block_size_ = fixed_block_size_;
    active_size_ = kBlockFixedSize;
    restart_count_ = 0;
    last_shared_size_ = 0;
    last_key_ = base::Slice();

    index_.clear();
    static_cast<base::VerifiedWriter<base::CRC32> *>(writer_.get())->Reset();
}

size_t BlockBuilder::CalcChunkSize(const Chunk &chunk) const {
    bool should_restart = false;

    auto shared_size = CalcSharedSize(chunk.key_slice(), &should_restart);
    auto unshared_size = chunk.key_size() - shared_size;

    size_t add_size = 0;
    add_size += base::Varint32::Sizeof(shared_size);
    add_size += base::Varint32::Sizeof(unshared_size);
    add_size += base::Varint64::Sizeof(chunk.value_size());
    add_size += unshared_size;
    add_size += chunk.value_size();

    if (should_restart) {
        add_size += sizeof(uint32_t); // one index's size
    }
    return add_size;
}

uint32_t BlockBuilder::CalcSharedSize(const base::Slice &key,
                                      bool *should_restart) const {
    if (restart_count_ % restart_interval_ == 0) {
        *should_restart = true;
        return 0;
    }

    size_t size = std::min(key.size(), last_key_.size());
    uint32_t i = 0;
    for (i = 0; i < size; ++i) {
        if (key.data()[i] != last_key_.data()[i]) {
            break;
        }
    }
    
    if (i == 0 || i < last_shared_size_) {
        *should_restart = true;
        return 0; // no shared part. explicit
    }

    return i;
}


BlockIterator::BlockIterator(const Comparator *comparator, const void *base,
                             size_t size)
    : comparator_(comparator)
    , status_(base::Status::OK()) {

    base_ = static_cast<const uint8_t *>(base);

    num_restarts_ = *reinterpret_cast<const uint32_t *>(base_ + size
                                                        - kBlockFixedSize);
    restarts_ = reinterpret_cast<const uint32_t *>(base_ + size
                                                   - kBlockFixedSize
                                                   - num_restarts_
                                                   * sizeof(uint32_t));
    data_end_ = reinterpret_cast<const uint8_t *>(restarts_);

    SeekToFirst();
}

BlockIterator::~BlockIterator() {

}

bool BlockIterator::Valid() const {
    return status_.ok() && current_ >= base_ && current_ <= data_end_;
}

void BlockIterator::SeekToFirst() {
    current_ = base_;
    Next();
}

void BlockIterator::SeekToLast() {
    status_ = base::Status::NotSupported("SeekToLast()");
}

void BlockIterator::Seek(const base::Slice& target) {
    int32_t i;
    for (i = static_cast<int32_t>(num_restarts_) - 1; i != 0; i--) {
        auto entry = base_ + restarts_[i];

        base::BufferedReader reader(entry, -1);
        auto shared_size = reader.ReadVarint32(); // Ignore
        DCHECK_EQ(0, shared_size);
        auto unshared_size = reader.ReadVarint32();
        reader.ReadVarint64(); // Ignore

        auto unshared = reader.Read(unshared_size);
        if (comparator_->Compare(target, unshared) >= 0) {
            break;
        }
    }

    if (i == num_restarts_) {
        status_ = base::Status::NotFound("Seek()");
        return;
    }

    auto entry = base_ + restarts_[i];
    auto end   = (i == num_restarts_ - 1) ? data_end_ : base_ + restarts_[i+1];

    std::string key;
    while (entry < end) {
        base::BufferedReader reader(entry, -1);
        auto shared_size = reader.ReadVarint32();
        auto unshared_size = reader.ReadVarint32();
        auto value_size = reader.ReadVarint64();

        auto last_key = key;
        auto unshared_key = reader.Read(unshared_size);
        key.assign(last_key.data(), shared_size);
        key.append(unshared_key.data(), unshared_key.size());
        if (target.compare(key) == 0) {
            key_ = key;
            value_ = reader.Read(value_size);
            current_ = reader.current();
            return;
        }

        reader.Skip(value_size);
        entry = reader.current();
    }

    status_ = base::Status::NotFound("Seek()");
}

void BlockIterator::Next() {
    base::BufferedReader reader(current_, -1);
    uint32_t shared_size = reader.ReadVarint32();
    uint32_t unshared_size = reader.ReadVarint32();
    uint64_t value_size = reader.ReadVarint64();

    auto last_key = key_;
    auto unshared_key = reader.Read(unshared_size);
    key_.assign(last_key.data(), shared_size);
    key_.append(unshared_key.data(), unshared_key.size());
    value_ = reader.Read(value_size);

    current_ = reader.current();
}

void BlockIterator::Prev() {
    status_ = base::Status::NotSupported("Prev()");
}

base::Slice BlockIterator::key() const {
    return base::Slice(key_);
}

base::Slice BlockIterator::value() const {
    return value_;
}

base::Status BlockIterator::status() const {
    return status_;
}


} // namespace lsm

} // namespace yukino