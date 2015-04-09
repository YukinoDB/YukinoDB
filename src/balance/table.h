#ifndef YUKINO_BALANCE_TABLE_H_
#define YUKINO_BALANCE_TABLE_H_

#include "balance/format.h"
#include "util/btree.h"
#include "util/bloom_filter.h"
#include "base/ref_counted.h"
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

class Table : public base::ReferenceCounted<Table> {
public:
    Table(InternalKeyComparator comparator, size_t max_cache_size);
    ~Table();

    base::Status Create(uint32_t page_size, uint32_t version, int order,
                        base::FileIO *file);

    base::Status Open(base::FileIO *file, size_t file_size);

    /**
     * Put the key into b+tree table
     *
     * @param key key for putting.
     * @param tx_id transaction id of key.
     * @param flag key flag.
     * @param value value for putting.
     * @param old old value if exists.
     * @return true - key already exists; false - new key.
     */
    bool Put(const base::Slice &key, uint64_t tx_id, KeyFlag flag,
             const base::Slice &value,
             std::string *old_value);

    bool Get(const base::Slice &key, uint64_t tx_id, std::string *value);

    /**
     * Flush all page to disk.
     *
     * @param sync synchronous operation.
     */
    base::Status Flush(bool sync);

    /**
     * Create internal iterator.
     *
     * @return the table's internal iterator.
     */
    Iterator *CreateIterator() const;

    /**
     * Approximate the large page ratio: used-blocks / pages.
     * This means:
     *     == 1: good page-size setting.
     *     >  1: page-size too small.
     * @return used-blocks / pages
     */
    inline float ApproximateLargeRatio() const;

    /**
     * Approximate the space usage ratio: all-blocks / used-blocs
     * This means:
     *     == 1: all blocks be used.
     *     >  1: not all blocks be used.
     */
    inline float ApproximateUsageRatio() const;

    //--------------------------------------------------------------------------
    // Testing:
    //--------------------------------------------------------------------------
    base::Status TEST_WriteChunk(const char *buf, size_t len, uint64_t *addr) {
        return WriteChunk(buf, len, addr);
    }

    base::Status TEST_ReadChunk(uint64_t addr, std::string *buf) {
        return ReadChunk(addr, buf);
    }

    //--------------------------------------------------------------------------
    // Types:
    //--------------------------------------------------------------------------
    struct Comparator {

        Comparator(InternalKeyComparator c) : comparator(c) {}

        inline int operator () (const char *a, const char *b) const;

        InternalKeyComparator comparator;
    };

    typedef util::detail::Page<const char*, Comparator> Page;
    typedef util::detail::Entry<const char*, Comparator> Entry;

    struct Allocator {

        Allocator(Table *t) : owns_(t) {}

        Page *Allocate(int num_entries) {
            return owns_->AllocatePage(num_entries);
        }

        void Free(Page *page) {
            owns_->FreePage(page);
        }

        const char *Duplicate(const char *key) {
            return owns_->DuplicateKey(key);
        }

        Page *Get(uint64_t id, bool cached) const {
            return owns_->GetPage(id, cached);
        }

        Table *owns_;
    };

    typedef util::BTree<const char*, Comparator, Allocator> Tree;

    struct CacheEntry {
        CacheEntry *next;
        CacheEntry *prev;
        base::Handle<Page> page;

        CacheEntry(Page *p)
            : page(p) {
            next = this;
            prev = this;
        }
    };

private:
    struct PageMetadata {
        uint64_t parent;
        uint64_t addr;
        uint64_t ts;
    };

    base::Status InitFile(int order);
    base::Status LoadTree();
    base::Status ScanPage(uint64_t addr);
    base::Status ReadPage(uint64_t id, Page **rv);

    base::Status WritePage(const Page *page);
    base::Status WriteChunk(const char *buf, size_t len, uint64_t *addr);
    base::Status WriteBlock(const char *buf, uint16_t len, uint8_t type,
                            uint64_t addr, uint64_t next);
    base::Status ReadChunk(uint64_t addr, std::string *buf);

    base::Status MakeRoomForPage(uint64_t *addr);
    base::Status FreeRoomForPage(uint64_t id);

    inline Page *AllocatePage(int num_entries);
    inline void FreePage(Page *page);
    inline const char *DuplicateKey(const char *key);
    inline Page *GetPage(uint64_t id, bool cached);

    base::Status CachedGet(uint64_t page_id, Page **rv, bool cached);
    base::Status CachedActivity(Page *page, bool cached);

    inline void ClearPage(const Page *page) const;

    inline uint64_t Addr2Index(uint64_t addr);
    inline bool TestUsed(uint64_t addr);
    inline void SetUsed(uint64_t addr);
    inline void ClearUsed(uint64_t addr);

    uint32_t page_size_ = 0;
    uint32_t version_ = 0;
    uint64_t file_size_ = 0;
    uint64_t next_page_id_ = 1;

    // file usage bitmap
    util::Bitmap<uint32_t> bitmap_;

    // page_id -> physical address
    std::unordered_map<uint64_t, uint64_t> id_map_;

    // page_id -> page metadatas
    std::map<uint64_t, PageMetadata> metadata_;

    // cache
    std::map<uint64_t, CacheEntry*> cache_map_;
    CacheEntry cache_dummy_;
    CacheEntry cache_purge_;
    size_t cache_size_ = 0;
    const size_t max_cache_size_;

    InternalKeyComparator comparator_;

    std::unique_ptr<Tree> tree_;
    base::FileIO *file_ = nullptr;

}; // class Table

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_TABLE_H_