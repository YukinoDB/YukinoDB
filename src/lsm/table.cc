#include "lsm/table.h"
#include "lsm/builtin.h"
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

    const uint8_t *magic_number = mmap_->buf(sizeof(uint32_t));
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

    char type = 0;
    if (!VerifyBlock(index_handle, &type)) {
        return base::Status::IOError("Block CRC32 check sum fail!");
    }
    BlockIterator iter(comparator_, mmap_->buf(index_handle.offset()),
                       index_handle.size());
    for (; iter.Valid(); iter.Next()) {
        size_t len = 0;

        BlockHandle handle(base::Varint64::Decode(iter.value().data(), &len));
        handle.set_size(base::Varint64::Decode(iter.value().data() + len,
                                               &len));
        IndexEntry entry {
            iter.key().ToString(),
            handle,
        };
        index_.push_back(entry);
    }
    return base::Status::OK();
}

BlockHandle Table::ReadHandle(base::BufferedReader *reader) {
    uint64_t offset = reader->ReadVarint64();

    BlockHandle handle(offset);
    handle.set_size(reader->ReadVarint64());

    return handle;
}

bool Table::VerifyBlock(const BlockHandle &handle, char *type) {
    base::CRC32 crc32;

    crc32.Update(mmap_->buf(handle.offset()),
                 handle.size() - sizeof(base::CRC32::DigestTy));

    auto verified = crc32.digest();

    base::BufferedReader reader(mmap_->buf(handle.offset() + handle.size() - kTrailerSize), kTrailerSize);
    *type = reader.ReadByte();

    return verified == reader.ReadFixed32();
}

} // namespace lsm

} // namespace yukino