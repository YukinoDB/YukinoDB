#ifndef YUKINO_LSM_COMPACTION_H_
#define YUKINO_LSM_COMPACTION_H_

#include "lsm/format.h"
#include "lsm/chunk.h"
#include "base/status.h"
#include "base/base.h"
#include <string>
#include <vector>
#include <set>

namespace yukino {

class Iterator;

namespace base {

class Writer;

} // namespace base

namespace lsm {

class TableCache;
class TableBuilder;
class FileMetadata;
class InternalKeyComparator;

class Compaction : public base::DisableCopyAssign {
public:
    Compaction(const std::string &db_name,
               const InternalKeyComparator &comparator, TableCache *cache);
    ~Compaction();

    base::Status AddOriginFile(uint64_t number, uint64_t size);

    void AddOriginIterator(Iterator *iter) { origin_iters_.push_back(iter); }

    /**
     * Compact starts >= compaction_point
     * Compact version < oldest_version
     *
     * REQUIRES: AddOriginIterator or AddOriginFile
     * REQUIRES: set_target
     * OPTIONAL: set_compaction_point
     * OPTIONAL: set_oldest_version
     */
    base::Status Compact(TableBuilder *builder);

    void set_target(uint64_t number) { target_file_number_ = number; }

    void set_oldest_version(uint64_t version) { oldest_version_ = version; }

    void set_compaction_point(const base::Slice &key) { compaction_point_ = key; }

    const std::set<uint64_t> &origin_files() const {
        return origin_file_numbers_;
    }

    // REQUIRES: Compact
    uint64_t origin_size() const { return origin_size_; }

    // REQUIRES: Compact
    uint64_t target_size() const { return target_size_; }

private:
    std::string db_name_;
    TableCache *cache_;

    uint64_t target_file_number_ = 0;
    std::set<uint64_t> origin_file_numbers_;
    std::vector<Iterator*> origin_iters_;

    uint64_t oldest_version_ = 0;
    base::Slice compaction_point_;

    uint64_t origin_size_ = 0;
    uint64_t target_size_ = 0;

    InternalKeyComparator comparator_;
};
    
} // namespace lsm
    
} // namespace yukino

#endif // YUKINO_LSM_COMPACTION_H_