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
    if (unlimited_) {
        return true;
    }

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
    auto head = writer_->active() - offset_;

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
        auto restart_index = static_cast<uint32_t>(head);

        add_size += sizeof(restart_index);
        index_.push_back(restart_index);

        restart_count_ = 1;
    } else {
        restart_count_++;
    }
    last_shared_size_ = shared_size;
    last_key_ = chunk.key_slice();

    if (unlimited_) {
        while (block_size_ < add_size + active_size_) {
            block_size_ += fixed_block_size_;
        }
    } else {
        // chunk size is too big, we should make block to large block.
        // ( > 1 fixed_block_size_)
        if (add_size > fixed_block_size_) {
            while (block_size_ < add_size + kBlockFixedSize) {
                block_size_ += fixed_block_size_;
            }
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
    unlimited_ = false;

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
}

BlockIterator::~BlockIterator() {

}

bool BlockIterator::Valid() const {
    return status_.ok() &&
            (curr_restart_ >= 0 && curr_restart_ < num_restarts_) &&
            (curr_local_ >= 0 && curr_local_ < local_.size());
}

void BlockIterator::SeekToFirst() {
    PrepareRead(0);
    curr_local_   = 0;
    curr_restart_ = 0;
}

void BlockIterator::SeekToLast() {
    PrepareRead(num_restarts_ - 1);
    curr_local_   = static_cast<int64_t>(local_.size()) - 1;
    curr_restart_ = static_cast<int64_t>(num_restarts_) - 1;
}

void BlockIterator::Seek(const base::Slice& target) {
    bool found = false;
    int32_t i;
    Pair pair;
    for (i = static_cast<int32_t>(num_restarts_) - 1; i >= 0; i--) {
        auto entry = base_ + restarts_[i];

        Read("", entry, &pair);
        if (comparator_->Compare(target, pair.key) >= 0) {
            found = true;
            break;
        }
    }

    if (!found) {
        i = 0;
    }

    PrepareRead(i);
    for (auto j = 0; j < local_.size(); j++) {
        if (comparator_->Compare(target, local_[j].key) <= 0) {
            curr_local_   = j;
            curr_restart_ = i;
            return;
        }
    }

    // 0 6
    // 1 5
    // 1 4
    // 1 3
    // ----> 2 6
    // 2 2
    // 2 1
    status_ = base::Status::NotFound("Seek()");
}

void BlockIterator::Next() {
    if (curr_local_ >= local_.size() - 1) {
        if (curr_restart_ < num_restarts_ - 1) {
            PrepareRead(++curr_restart_);
        } else {
            ++curr_restart_;
        }
        curr_local_ = 0;
        return;
    }

    curr_local_++;
}

void BlockIterator::Prev() {
    if (curr_local_ == 0) {
        if (curr_restart_ > 0) {
            PrepareRead(--curr_restart_);
        } else {
            --curr_restart_;
        }
        curr_local_ = static_cast<int64_t>(local_.size()) - 1;
        return;
    }

    --curr_local_;
}

base::Slice BlockIterator::key() const {
    DCHECK(Valid());
    return base::Slice(local_[curr_local_].key);
}

base::Slice BlockIterator::value() const {
    DCHECK(Valid());
    return local_[curr_local_].value;
}

base::Status BlockIterator::status() const {
    return status_;
}

const uint8_t *BlockIterator::PrepareRead(size_t i) {
    DCHECK_LT(i, num_restarts_);

    auto entry = base_ + restarts_[i];
    auto end   = (i == num_restarts_ - 1) ? data_end_ : base_ + restarts_[i+1];

    Pair pair;
    std::string last_key;
    local_.clear();
    while (entry < end) {
        entry = Read(last_key, entry, &pair);
        last_key.assign(pair.key);

        local_.push_back(std::move(pair));
    }

    return entry;
}

const uint8_t *BlockIterator::Read(const std::string &prev, const uint8_t *p,
                                   Pair *rv) {
    base::BufferedReader reader(p, -1);
    auto shared_size = reader.ReadVarint32();
    rv->key = prev.substr(0, shared_size);
    auto unshared_size = reader.ReadVarint32();
    auto value_size = reader.ReadVarint64();

    auto unshared = reader.Read(unshared_size);
    rv->key.append(unshared.data(), unshared.size());
    rv->value = reader.Read(value_size);

    return reader.current();
}


} // namespace lsm

} // namespace yukino