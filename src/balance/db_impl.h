#ifndef YUKINO_BALANCE_DB_IMPL_H_
#define YUKINO_BALANCE_DB_IMPL_H_

#include "balance/format.h"
#include "balance/snapshot_impl.h"
#include "base/ref_counted.h"
#include "base/status.h"
#include "base/base.h"
#include "yukino/db.h"
#include "yukino/options.h"
#include <mutex>
#include <thread>

namespace yukino {

class Iterator;

namespace base {

class Slice;
class AppendFile;
class FileLock;

} // namespace base

namespace util {

class LogWriter;

} // namespace util

namespace balance {

class Table;
class VersionSet;

class DBImpl : public DB {
public:
    DBImpl(const Options &options, const std::string &name);
    ~DBImpl();

    base::Status Open();

    //--------------------------------------------------------------------------
    // Override Methods:
    //--------------------------------------------------------------------------

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

    //--------------------------------------------------------------------------
    // Proprietary Methods:
    //--------------------------------------------------------------------------

    base::Status CreateDB();
    base::Status Recover();
    base::Status Redo(uint64_t log_file_number, uint64_t startup_tx_id);

    void AddCheckpointRate(int rate) { checkpoint_rate_ += rate; }
    void ScheduleCheckpoint();

    base::Status PurgingStep(uint64_t startup_tx_id);

    //--------------------------------------------------------------------------
    // For Testing
    //--------------------------------------------------------------------------

    inline void TEST_WaitForCheckpoint() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (background_active_) {
            background_cv_.wait(lock);
        }
    }

    // Then engine's name
    constexpr static const auto kName = "yukino.balance";

private:
    void BackgroundCheckpoint();

    bool CatchError(const base::Status &status);
    base::Status NewTable();
    base::Status NewLog(uint64_t log_file_number);

    const Options options_;
    const std::string name_;

    Env * const env_;
    const InternalKeyComparator comparator_;
    std::mutex mutex_;
    std::unique_ptr<base::FileLock> db_lock_;

    std::unique_ptr<VersionSet> versions_;
    SnapshotImpl snapshot_dummy_;

    bool background_active_ = false;
    std::atomic<DBImpl*> shutting_down_;
    base::Status background_status_;
    std::condition_variable background_cv_;

    int checkpoint_rate_ = 0;
    int checkpoint_threshold_ = Config::kCheckpointThreshold;
    std::string purging_point_;
    int purging_count_ = Config::kPurgingStepCount;

    std::unique_ptr<base::FileIO> storage_io_;
    base::Handle<Table> table_; // The b+tree table with disk storage.

    std::unique_ptr<base::AppendFile> log_file_; // redo-log's file
    std::unique_ptr<util::LogWriter> log_; // redo-log writer
    uint64_t log_file_number_ = 0;

    const Files files_;
}; // class DBImpl

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_DB_IMPL_H_