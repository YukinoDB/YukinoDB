#include "lsm/compaction.h"
#include "lsm/table_cache.h"
#include "lsm/table_builder.h"
#include "lsm/merger.h"
#include "lsm/format.h"
#include "lsm/chunk.h"
#include "yukino/env.h"
#include "yukino/options.h"
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
    std::for_each(origin_iters_.begin(), origin_iters_.end(),
    [](Iterator *iter){
        delete iter;
    });
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
    std::unique_ptr<Iterator> merger(CreateMergingIterator(&comparator_,
                                                           &origin_iters_[0],
                                                           origin_iters_.size()));
    if (!merger->status().ok()) {
        return merger->status();
    }
    if (origin_iters_.size() == 1) {
        origin_iters_.clear();
    }

    if (compaction_point_.empty()) {
        merger->SeekToFirst();
    } else {
        merger->Seek(compaction_point_);
    }

    for (; merger->Valid(); merger->Next()) {
        // TODO:
    }

    return base::Status::OK();
}

} // namespace lsm
    
} // namespace yukino