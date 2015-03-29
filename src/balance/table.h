#ifndef YUKINO_BALANCE_TABLE_H_
#define YUKINO_BALANCE_TABLE_H_

#include "balance/format.h"
#include "util/btree.h"
#include "base/status.h"
#include "base/base.h"
#include <vector>
#include <map>
#include <unordered_map>

namespace yukino {

class Iterator;

namespace base {

class MappedMemory;
class FileIO;
class Slice;

} // namespace base

namespace balance {

class Table {
public:
    Table(InternalKeyComparator comparator);
    ~Table();

    base::Status Create(uint32_t page_size, uint32_t version, int order,
                        base::FileIO *file);

    base::Status Open(base::FileIO *file, size_t file_size);

    bool Put(const base::Slice &key, uint64_t tx_id, uint8_t flag,
             const base::Slice &value,
             std::string *old_value);

    base::Status Flush(bool sync);

    Iterator *CreateIterator() const;

private:
    struct Comparator {

        Comparator(InternalKeyComparator c) : comparator(c) {}

        inline int operator () (const char *a, const char *b);

        InternalKeyComparator comparator;
    };

    typedef util::detail::Page<const char*, Comparator> PageTy;
    typedef util::detail::Entry<const char*, Comparator> EntryTy;

    struct Allocator {

        Allocator(Table *t) : owns_(t) {}

        PageTy *Allocate(int num_entries) {
            return owns_->AllocatePage(num_entries);
        }

        void Free(const PageTy *page) { owns_->FreePage(page); }

        Table *owns_;
    };

    typedef util::BTree<const char*, Comparator, Allocator> TreeTy;

    struct PageMetadata {
        uint64_t parent;
        uint64_t addr;
        uint64_t ts;
        PageTy *page = nullptr;
    };

    base::Status InitFile(int order);
    base::Status LoadTree();

    base::Status ReadTreePage(std::map<uint64_t, PageMetadata> *metadatas,
                              uint64_t id, PageTy **rv);

    base::Status WritePage(const PageTy *page);

    base::Status MakeRoomForPage(uint64_t id, uint64_t *addr);
    base::Status FreeRoomForPage(uint64_t id);

    inline PageTy *AllocatePage(int num_entries);
    inline void FreePage(const PageTy *page);

    inline uint64_t Addr2Index(uint64_t addr);
    inline bool TestUsed(uint64_t addr);
    inline void SetUsed(uint64_t addr);
    inline void ClearUsed(uint64_t addr);

    uint32_t page_size_ = 0;
    uint32_t version_ = 0;
    uint64_t file_size_ = 0;
    uint64_t next_page_id_ = 0;

    std::vector<uint32_t> bitmap_;

    // page_id -> physical address
    std::unordered_map<uint64_t, uint64_t> id_map_;

    InternalKeyComparator comparator_;
    
    std::unique_ptr<TreeTy> tree_;
    base::FileIO *file_ = nullptr;
}; // class Table

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_TABLE_H_