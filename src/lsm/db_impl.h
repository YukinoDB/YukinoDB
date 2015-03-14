#ifndef YUKINO_LSM_DB_IMPL_H_
#define YUKINO_LSM_DB_IMPL_H_

#include "lsm/memory_table.h"
#include "yukino/db.h"
#include "base/status.h"
#include "base/base.h"
#include <mutex>
#include <thread>

namespace yukino {

class Env;
class Options;
class ReadOptions;
class WriteOptions;

namespace base {

class AppendFile;

} // namespace base

namespace lsm {

class VersionSet;
class VersionPatch;
class Version;
struct FileMetadata;
class TableCache;
class LogWriter;
class InternalKeyComparator;

class DBImpl : public DB {
public:
    DBImpl(const Options &opt, const std::string &name);
    virtual ~DBImpl() override;

    base::Status Open(const Options &opt);

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
    base::Status Recovery();
    base::Status ReplayVersions(uint64_t file_number,
                                std::vector<uint64_t> *version);
    base::Status Redo(uint64_t log_file_number, uint64_t last_version);

    base::Status MakeRoomForWrite(bool force, std::unique_lock<std::mutex> *lock);
    void MaybeScheduleCompaction();
    void BackgroundWork();
    void BackgroundCompaction();
    base::Status CompactMemoryTable();
    base::Status WriteLevel0Table(const Version *current, VersionPatch *patch,
                                  MemoryTable *table);
    base::Status BuildTable(Iterator *iter, FileMetadata *metadata);

    // For testing:
    void TEST_WaitForBackground();

    constexpr static const auto kName = "lsm";

    class WritingHandler;
private:

    Env *env_ = nullptr;
    std::string db_name_;

    base::Handle<MemoryTable> mutable_;
    base::Handle<MemoryTable> immtable_;

    size_t write_buffer_size_ = 0;
    base::Status background_error_;
    std::condition_variable background_cv_;
    bool background_active_ = false;
    std::atomic<DBImpl*> shutting_down_;

    std::unique_ptr<TableCache> table_cache_;
    std::unique_ptr<VersionSet> versions_;
    std::unique_ptr<InternalKeyComparator> internal_comparator_;
    std::unique_ptr<LogWriter> log_;
    std::unique_ptr<base::AppendFile> log_file_;
    uint64_t log_file_number_ = 0;

    std::mutex mutex_;
};

} // namespace lsm

} // namespace yukino

#endif //