#include "lsm/table_cache.h"
#include "lsm/table.h"
#include "lsm/version.h"
#include "lsm/chunk.h"
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
        entry = new CacheEntry;
        entry->file_name = TableFileName(db_name_, file_number);

        auto rs = env_->CreateRandomAccessFile(entry->file_name, &entry->mmap);
        if (!rs.ok()) {
            return CreateErrorIterator(rs);
        }
        entry->table = new Table(&comparator_, entry->mmap);
        rs = entry->table->Init();
        if (!rs.ok()) {
            return CreateErrorIterator(rs);
        }

        cached_.emplace(file_number, entry);
    } else {
        entry = found->second;
    }

    auto iter = new Table::Iterator(entry->table);
    entry->AddRef();
    iter->RegisterCleanup([entry]() { entry->Release(); });
    return iter;
}

base::Status TableCache::GetFileMetadata(uint64_t file_number, FileMetadata *rv) {
    auto file_name = TableFileName(db_name_, file_number);

    if (!env_->FileExists(file_name)) {
        return base::Status::IOError(base::Strings::Sprintf("SST file %s not exist.",
                                                            file_name.c_str()));
    }

    auto rs = env_->GetFileSize(file_name, &rv->size);
    if (!rs.ok()) {
        return rs;
    }

    std::unique_ptr<Iterator> iter(CreateIterator(ReadOptions(), file_number,
                                                  rv->size));
    if (!iter->status().ok()) {
        return iter->status();
    }
    iter->SeekToFirst();
    DCHECK(iter->Valid());
    rv->smallest_key = InternalKey::CreateKey(iter->key());

    iter->SeekToLast();
    DCHECK(iter->Valid());
    rv->largest_key = InternalKey::CreateKey(iter->key());

    return base::Status::OK();
}

TableCache::CacheEntry::~CacheEntry() {
    if (table)
        delete table;
    if (mmap)
        delete mmap;
}

} // namespace lsm
    
} // namespace yukino