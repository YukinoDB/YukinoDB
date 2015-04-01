#include "lsm/table.h"
#include "lsm/builtin.h"
#include "yukino/comparator.h"
#include "base/io-inl.h"
#include "base/io.h"
#include "base/varint_encoding.h"
#include "base/slice.h"
#include "base/crc32.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

Table::Table(const Comparator *comparator, base::MappedMemory *mmap)
    : mmap_(DCHECK_NOTNULL(mmap))
    , comparator_(comparator) {

    DCHECK(mmap_->Valid());
}

Table::~Table() {
}

base::Status Table::Init() {
    if (mmap_->size() < kFooterFixedSize) {
        return base::Status::IOError("SST file is too small.");
    }

    const uint8_t *magic_number = mmap_->buf(mmap_->size() - sizeof(uint32_t));
    if (*reinterpret_cast<const uint32_t *>(magic_number) != kMagicNumber) {
        return base::Status::IOError("Not valid SST file(bad magic number).");
    }

    const uint8_t *footer = mmap_->buf(mmap_->size() - kFooterFixedSize);
    base::BufferedReader reader(footer, kFooterFixedSize);

    file_version_ = reader.ReadVarint32();
    restart_interval_ = reader.ReadVarint32();
    block_size_ = reader.ReadVarint32();

    auto index_handle = ReadHandle(&reader);
    if (index_handle.offset() + index_handle.size() > mmap_->size()) {
        return base::Status::IOError("Not valid SST file(bad index handle).");
    }

    return LoadIndex(index_handle, &index_);
}

BlockHandle Table::ReadHandle(base::BufferedReader *reader) {
    uint64_t offset = reader->ReadVarint64();

    BlockHandle handle(offset);
    handle.set_size(reader->ReadVarint64());

    return handle;
}

bool Table::VerifyBlock(const BlockHandle &handle, char *type) const {
    base::CRC32 crc32;

    crc32.Update(mmap_->buf(handle.offset()),
                 handle.size() - sizeof(base::CRC32::DigestTy));

    auto verified = crc32.digest();

    base::BufferedReader reader(mmap_->buf(handle.offset() + handle.size() -
                                           kTrailerSize), kTrailerSize);
    *type = reader.ReadByte();

    return verified == reader.ReadFixed32();
}

base::Status Table::LoadIndex(const BlockHandle &index_handle,
                              std::vector<Table::IndexEntry> *index) {

    char type = 0;
    if (!VerifyBlock(index_handle, &type)) {
        return base::Status::IOError("Block CRC32 checksum fail!");
    }
    BlockIterator iter(comparator_, mmap_->buf(index_handle.offset()),
                       index_handle.size());
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        size_t len = 0;

        BlockHandle handle(base::Varint64::Decode(iter.value().data(), &len));
        handle.set_size(base::Varint64::Decode(iter.value().data() + len,
                                               &len));

        IndexEntry entry {
            iter.key().ToString(),
            handle,
        };
        index->push_back(entry);
    }
    return base::Status::OK();
}

TableIterator::TableIterator(const Table *table)
    : owned_(DCHECK_NOTNULL(table)) {
}

TableIterator::~TableIterator() {
}

bool TableIterator::Valid() const{
    return status_.ok() &&
        (block_idx_ >= 0 && block_idx_ < owned_->index_.size()) &&
        block_iter_ && block_iter_->Valid();
}

void TableIterator::SeekToFirst() {
    block_idx_ = 0;
    direction_ = kForward;
    SeekByHandle(owned_->index_[block_idx_].handle, true);
}

void TableIterator::SeekToLast() {
    block_idx_ = owned_->index_.size() - 1;
    direction_ = kReserve;

    if (block_idx_ >= 0) {
        SeekByHandle(owned_->index_[block_idx_].handle, false);
    }
}

void TableIterator::Seek(const base::Slice& target) {
    direction_ = kForward;
    block_idx_ = -1;

    int64_t left = 0, right = owned_->index_.size() - 1, middle = 0;
    while (left <= right) {
        middle = (left + right) / 2;

        const auto &entry = owned_->index_[middle];
        auto rv = owned_->comparator_->Compare(target, entry.key);
        if (rv < 0) {
            right = middle - 1;
        } else if (rv > 0) {
            left = middle + 1;
        } else {
            SeekByHandle(entry.handle, true);
            block_idx_ = middle;
            block_iter_->Seek(target);
            return;
        }
    }

    for (auto i = middle; i < owned_->index_.size(); ++i) {
        const auto &entry = owned_->index_[i];
        auto rv = owned_->comparator_->Compare(target, entry.key);

        if (rv < 0) {
            SeekByHandle(entry.handle, true);
            block_idx_ = i;
            block_iter_->Seek(target);
            return;
        }
    }
}

void TableIterator::Next() {
    DCHECK(Valid());

    direction_ = kForward;
    block_iter_->Next();

    if (!block_iter_->Valid()) {

        // test not eof
        if (block_idx_ < owned_->index_.size() - 1) {
            const auto &handle = owned_->index_[++block_idx_].handle;
            SeekByHandle(handle, true);
        } else {
            block_idx_++;
        }
    }
}

void TableIterator::Prev() {
    DCHECK(Valid());

    direction_ = kReserve;
    block_iter_->Prev();

    if (!block_iter_->Valid()) {

        if (block_idx_ > 0) {
            const auto &handle = owned_->index_[--block_idx_].handle;
            SeekByHandle(handle, false);
        } else {
            block_idx_--;
        }
    }
}

base::Slice TableIterator::key() const {
    DCHECK(Valid());
    return block_iter_->key();
}

base::Slice TableIterator::value() const {
    DCHECK(Valid());
    return block_iter_->value();
}

base::Status TableIterator::status() const {
    return status_;
}

void TableIterator::SeekByHandle(const BlockHandle &handle, bool to_first) {
    char type = 0;
    if (!owned_->VerifyBlock(handle, &type) || type != kTypeData) {
        status_ = base::Status::IOError("Block CRC32 checksum fail!");
        return;
    }

    auto block_base = owned_->mmap_->buf(handle.offset());

    std::unique_ptr<Iterator> block_iter(new BlockIterator(owned_->comparator_,
                                                           block_base,
                                                           handle.size()));
    block_iter_.swap(block_iter);

    if (to_first) {
        block_iter_->SeekToFirst();
    } else {
        block_iter_->SeekToLast();
    }
}

} // namespace lsm

} // namespace yukino