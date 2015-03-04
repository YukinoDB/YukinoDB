#include "lsm/compactor.h"
#include "lsm/table_builder.h"
#include "lsm/chunk.h"
#include "lsm/merger.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "base/status.h"
#include "glog/logging.h"
#include <list>

namespace yukino {

namespace lsm {

Compactor::Compactor(InternalKeyComparator comparator, uint64_t oldest)
    : comparator_(comparator)
    , oldest_(oldest) {
}


base::Status Compactor::Compact(Iterator **children, size_t n,
                                TableBuilder *builder) {
    std::unique_ptr<Iterator> merger(CreateMergingIterator(&comparator_,
                                                           children, n));
    std::list<Chunk> stored;

    for (merger->SeekToFirst(); merger->Valid(); merger->Next()) {
        DCHECK_GE(merger->key().size(), Tag::kTagSize);

        auto p = merger->key().data() + merger->key().size() - Tag::kTagSize;
        auto tag = Tag::Decode(*reinterpret_cast<const Tag::EncodedType*>(p));

        if (tag.version < oldest_ || tag.flag == kFlagDeletion) {
            continue;
        }

        auto chunk = Chunk::CreateKeyValue(merger->key(), merger->value());
        stored.push_back(std::move(chunk));

        auto rs = DCHECK_NOTNULL(builder)->Append(stored.back());
        if (!rs.ok()) {
            return rs;
        }
    }
    builder->Finalize();
    return base::Status::OK();
}

} // namespace lsm

} // namespace yukino