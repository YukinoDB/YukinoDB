#include "lsm/table_builder.h"
#include "lsm/chunk.h"
#include "lsm/block.h"
#include "lsm/builtin.h"
#include "base/varint_encoding.h"
#include "base/crc32.h"
#include "base/io.h"
#include "glog/logging.h"
#include <unistd.h>
#include <vector>

namespace yukino {

namespace lsm {

TableOptions::TableOptions()
    : file_version(kFileVersion)
    , magic_number(kMagicNumber)
    // set block size to page size(normal: 8kb)
    , block_size(static_cast<uint32_t>(sysconf(_SC_PAGESIZE)))
    , restart_interval(kRestartInterval) {
}

class TableBuilder::Core {
public:
    Core(const TableOptions &options, base::Writer *writer)
        : builder_(writer, options.block_size, options.restart_interval)
        , active_blocks_(0)
        , options_(options) {
    }

    uint64_t ActiveSize() const {
        return active_blocks_ * options_.block_size;
    }

    void AddIndex(const base::Slice &key, const BlockHandle &handle) {
        char buf[base::Varint64::kMaxLen * 2];
        size_t len = 0;

        len += base::Varint64::Encode(buf + len, handle.offset());
        len += base::Varint64::Encode(buf + len, handle.size());

        index_.push_back(Chunk::CreateKeyValue(key, base::Slice(buf, len)));
    }

    base::Status CloseBlock(BlockHandle *rv) {
        BlockHandle handle(ActiveSize());
        auto rs = builder_.Finalize(kTypeData, &handle);
        if (!rs.ok())
            return rs;
        DCHECK_LT(0, handle.size());

        AddIndex(splite_key_, handle);

        active_blocks_ += handle.NumberOfBlocks(options_.block_size);
        block_close_ = true;

        auto skipped = handle.NumberOfBlocks(options_.block_size) *
            options_.block_size - handle.size();
        builder_.writer()->Skip(skipped);

        if (rv)
            *rv = handle;
        return base::Status::OK();
    }

    /*
     * Footer:
     *
     +----------------+
     | file-version   | varint32-encoding
     +----------------+
     | restart-interval  | varint32-enoding
     +----------------|
     | block-size     | varint32-encoding
     +----------------+
     | index-offset   | varint64-encoding
     +----------------+
     | index-size     | varint64-encoding
     +----------------+
     | padding bytes  |
     +----------------+
     | magic-number   | 4 bytes
     +----------------+
     */
    base::Status WriteFooter(const BlockHandle &index_handle) {
        auto writer = builder_.writer();

        size_t len = 0, written = 0;
        auto rs = writer->WriteVarint32(options_.file_version, &written);
        if (!rs.ok())
            return rs;
        len += written;

        rs = writer->WriteVarint32(options_.restart_interval, &written);
        if (!rs.ok())
            return rs;
        len += written;

        rs = writer->WriteVarint32(options_.block_size, &written);
        if (!rs.ok())
            return rs;
        len += written;

        rs = writer->WriteVarint64(index_handle.offset(), &written);
        if (!rs.ok())
            return rs;
        len += written;

        rs = writer->WriteVarint64(index_handle.size(), &written);
        if (!rs.ok())
            return rs;
        len += written;

        writer->Skip(kFooterFixedSize - len - kBottomFixedSize);

        rs = writer->WriteFixed32(options_.magic_number);
        if (!rs.ok())
            return rs;

        return base::Status::OK();
    }

    BlockBuilder builder_;
    base::Slice splite_key_;

    bool block_close_ = false;
    uint64_t active_blocks_;
    std::vector<Chunk> index_;
    const TableOptions options_;
};

TableBuilder::TableBuilder(const TableOptions &options, base::Writer *writer)
    : writer_(DCHECK_NOTNULL(writer))
    , core_(new Core{options, writer}) {
}

TableBuilder::~TableBuilder() {
}

base::Status TableBuilder::Append(const Chunk &chunk) {
    if (core_->builder_.CanAppend(chunk)) {
        core_->block_close_ = false;
        core_->splite_key_ = chunk.key_slice();
        return core_->builder_.Append(chunk);
    }

    auto rs = core_->CloseBlock(nullptr);
    if (!rs.ok())
        return rs;

    return core_->builder_.Append(chunk);
}

base::Status TableBuilder::Finalize() {
    if (!core_->block_close_) {
        auto rs = core_->CloseBlock(nullptr);
        if (!rs.ok())
            return rs;
    }

    for (const auto &chunk : core_->index_) {
        auto rs = Append(chunk);
        if (!rs.ok())
            return rs;
    }
    BlockHandle handle;
    auto rs = core_->CloseBlock(&handle);
    if (!rs.ok())
        return rs;

    return core_->WriteFooter(handle);
}

} // namespace lsm

} // namespace yukino