#include "lsm/compaction.h"
#include "lsm/table_cache.h"
#include "lsm/table_builder.h"
#include "lsm/merger.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "lsm/chunk.h"
#include "yukino/env.h"
#include "yukino/options.h"
#include "yukino/read_options.h"
#include "yukino/iterator.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

Compaction::Compaction(const std::string &db_name,
                       const InternalKeyComparator &comparator,
                       TableCache *cache)
    : db_name_(db_name)
    , comparator_(comparator)
    , cache_(DCHECK_NOTNULL(cache)) {
}

Compaction::~Compaction() {
}

base::Status Compaction::AddOriginFile(uint64_t number, uint64_t size) {
    std::unique_ptr<Iterator> iter(cache_->CreateIterator(ReadOptions(), number,
                                                          size));
    if (iter->status().ok()) {
        AddOriginIterator(iter.release());
    }
    return iter->status();
}

base::Status Compaction::Compact(TableBuilder *builder) {
    DCHECK_NOTNULL(builder);

    std::unique_ptr<Iterator> merger(CreateMergingIterator(&comparator_,
                                                           &origin_iters_[0],
                                                           origin_iters_.size()));
    if (!merger->status().ok()) {
        return merger->status();
    }

    if (compaction_point_.empty()) {
        merger->SeekToFirst();
    } else {
        merger->Seek(compaction_point_);
    }

    origin_size_ = 0;
    target_size_ = 0;
    for (; merger->Valid(); merger->Next()) {
        DCHECK_GE(merger->key().size(), Tag::kTagSize);

        origin_size_ += (merger->key().size() + merger->value().size());

        auto p = merger->key().data() + merger->key().size() - Tag::kTagSize;
        auto tag = Tag::Decode(*reinterpret_cast<const Tag::EncodedType*>(p));

        if (tag.version < oldest_version_ || tag.flag == kFlagDeletion) {
            continue;
        }

        auto chunk = Chunk::CreateKeyValue(merger->key(), merger->value());
        target_size_ += chunk.size();

        auto rs = builder->Append(chunk);
        if (!rs.ok()) {
            origin_iters_.clear();
            return rs;
        }
    }

    origin_iters_.clear();
    return builder->Finalize();
}

} // namespace lsm
    
} // namespace yukino