#include "lsm/table_cache.h"
#include "lsm/table.h"
#include "yukino/options.h"
#include "yukino/env.h"

namespace yukino {

namespace lsm {

TableCache::TableCache(const std::string &db_name, const Options &options)
    : env_(options.env)
    , db_name_(db_name)
    , comparator_(options.comparator) {
}

Iterator *TableCache::CreateIterator(const ReadOptions &options,
                                     uint64_t file_number, uint64_t file_size) {
    base::Handle<CacheEntry> entry;

    auto found = cached_.find(file_number);
    if (found == cached_.end()) {
        char buf[1024];
        ::snprintf(buf, sizeof(buf), "%s/%llu.%s", db_name_.c_str(),
                   file_number, kTableExtendName);

        entry = new CacheEntry;
        auto rs = env_->CreateRandomAccessFile(buf, &entry->mmap);
        if (!rs.ok()) {
            return CreateErrorIterator(rs);
        }
        entry->file_name = buf;
        entry->table = new Table(&comparator_, entry->mmap);

        cached_.emplace(file_number, entry);
    } else {
        entry = found->second;
    }

    auto iter = new Table::Iterator(entry->table);
    entry->AddRef();
    iter->RegisterCleanup([entry]() { entry->Release(); });
    return iter;
}


TableCache::CacheEntry::~CacheEntry() {
    if (table)
        delete table;
    if (mmap)
        delete mmap;
}

} // namespace lsm
    
} // namespace yukino