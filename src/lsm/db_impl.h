#ifndef YUKINO_LSM_DB_IMPL_H_
#define YUKINO_LSM_DB_IMPL_H_

#include "lsm/memory_table.h"
#include "yukino/db.h"
#include "base/status.h"
#include <mutex>

namespace yukino {

class Env;

namespace lsm {

class VersionSet;
class TableCache;
class InternalKeyComparator;

class DBImpl : public DB {
public:
    DBImpl();

    base::Status Open(const Options &opt, const std::string &name);

    virtual ~DBImpl() override;
    virtual base::Status Put(const WriteOptions& options,
                             const base::Slice& key,
                             const base::Slice& value) override;
    virtual base::Status Delete(const WriteOptions& options,
                                const base::Slice& key) override;
    virtual base::Status Write(const WriteOptions& options,
                               WriteBatch* updates) override;
    virtual base::Status Get(const ReadOptions& options,
                             const base::Slice& key, std::string* value) override;
    virtual Iterator* NewIterator(const ReadOptions& options) override;
    virtual const Snapshot* GetSnapshot() override;
    virtual void ReleaseSnapshot(const Snapshot* snapshot) override;

    base::Status NewDB(const Options &opt);

    constexpr static const auto kName = "lsm";

    class WritingHandler;
private:
    Env *env_ = nullptr;
    std::string db_name_;

    base::Handle<MemoryTable> mutable_;
    base::Handle<MemoryTable> immtable_;

    std::unique_ptr<VersionSet> versions_;
    std::unique_ptr<TableCache> table_cache_;
    std::unique_ptr<InternalKeyComparator> internal_comparator_;

    std::mutex mutex_;
};

} // namespace lsm

} // namespace yukino

#endif //