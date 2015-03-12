#ifndef YUKINO_LSM_TABLE_CACHE_H_
#define YUKINO_LSM_TABLE_CACHE_H_

#include "lsm/format.h"
#include "base/status.h"
#include "base/ref_counted.h"
#include "base/base.h"
#include <stdint.h>
#include <string>
#include <unordered_map>

namespace yukino {

class Options;
class ReadOptions;
class Iterator;
class Comparator;
class Env;

namespace base {

class MappedMemory;

} // namespace base

namespace lsm {

class Table;
struct FileMetadata;

class TableCache {
public:
    TableCache(const std::string &db_name, const Options &options);

    Iterator *CreateIterator(const ReadOptions &options, uint64_t file_number,
                             uint64_t file_size);

    void Invalid(uint64_t file_number) { cached_.erase(file_number); }

    base::Status GetFileMetadata(uint64_t file_number, FileMetadata *rv);

    Env *env() const { return env_; }

private:
    Env *env_;
    std::string db_name_;
    InternalKeyComparator comparator_;

    struct CacheEntry : public base::ReferenceCounted<CacheEntry> {
        std::string file_name;
        base::MappedMemory *mmap = nullptr;
        Table *table = nullptr;

        ~CacheEntry();
    };
    std::unordered_map<uint64_t, base::Handle<CacheEntry>> cached_;
};

} // namespace lsm
    
} // namespace yukino

#endif // YUKINO_LSM_TABLE_CACHE_H_