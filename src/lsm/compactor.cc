#include "lsm/compactor.h"
#include "lsm/table_builder.h"
#include "lsm/chunk.h"
#include "lsm/merger.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "base/status.h"

namespace yukino {

namespace lsm {

Compactor::Compactor(InternalKeyComparator comparator)
    : comparator_(comparator) {
}


base::Status Compactor::Compact(Iterator **children, size_t n,
                                TableBuilder *builder) {
    std::unique_ptr<Iterator> merger(CreateMergingIterator(&comparator_,
                                                           children, n));

    for (merger->SeekToFirst(); merger->Valid(); merger->Next()) {
        DCHECK_GE(merger->key().size(), Tag::kTagSize);

        auto p = merger->key().data() + merger->key().size() - Tag::kTagSize;
        auto tag = Tag::Decode(*reinterpret_cast<const Tag::RawType*>(p));

        if (tag.flag == kFlagDeletion) {
            continue;
        }

        auto chunk = Chunk::CreateKeyValue(merger->key(), merger->value());
        auto rs = DCHECK_NOTNULL(builder)->Append(chunk);
        if (!rs.ok()) {
            return rs;
        }
    }
    builder->Finalize();
    return base::Status::OK();
}

} // namespace lsm

} // namespace yukino