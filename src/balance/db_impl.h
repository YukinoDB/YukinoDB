#ifndef YUKINO_BALANCE_DB_IMPL_H_
#define YUKINO_BALANCE_DB_IMPL_H_

#include "balance/format.h"
#include "base/status.h"
#include "base/base.h"
#include "yukino/db.h"
#include "yukino/options.h"

namespace yukino {

class Iterator;

namespace base {

class Slice;

} // namespace base

namespace balance {

class DBImpl : public DB {
public:
    DBImpl(const Options &options, const std::string &name);
    ~DBImpl();

    base::Status Open();

    //==========================================================================
    // Override methods:
    //==========================================================================
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

    // Then engine's name
    constexpr static const auto kName = "yukino.balance";

private:
    const Options options_;
    const std::string name_;

    Env * const env_;
    const InternalKeyComparator comparator_;
}; // class DBImpl

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_DB_IMPL_H_