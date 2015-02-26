#ifndef YUKINO_LSM_MEMORY_TABLE_H_
#define YUKINO_LSM_MEMORY_TABLE_H_

#include "lsm/skiplist.h"
#include "base/base.h"

namespace yukino {

class Iterator;

namespace base {

class Slice;

} // namespace base

namespace lsm {

class Chunk;
class InternalKey;
class InternalKeyComparator;

class MemoryTable : public base::DisableCopyAssign {
public:
    MemoryTable(const InternalKeyComparator *comparator);

    void Put(const base::Slice &key, const base::Slice &value, uint64_t version,
             uint8_t flag);

    Iterator *NewIterator();

    struct KeyComparator {

        KeyComparator(const InternalKeyComparator *comparator)
            : comparator_(comparator) {
        }
        
        int operator()(const InternalKey &a, const InternalKey &b) const;

    private:
        const InternalKeyComparator *const comparator_;
    };

    typedef SkipList<InternalKey, KeyComparator> Table;

private:
    const InternalKeyComparator *const comparator_;
    Table table_;
}; // class MemoryTable

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_MEMORY_TABLE_H_