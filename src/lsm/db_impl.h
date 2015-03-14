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
class FileLock;

} // namespace base

namespace lsm {

class VersionSet;
class VersionPatch;
class Version;
struct FileMetadata;
class TableCache;
class LogWriter;
class InternalKeyComparator;
class SnapshotList;
class SnapshotImpl;

class SnapshotImpl : public Snapshot {
public:
    SnapshotImpl(uint64_t version, SnapshotList *owns)
        : version_(version)
        , owns_(owns) {
    }

    virtual ~SnapshotImpl() override;

    void InsertTail(SnapshotImpl *x) {
        x->prev_ = prev_;
        x->prev_->next_ = x;
        x->next_ = this;
        prev_ = x;
    }

    void InsertHead(SnapshotImpl *x) {
        x->next_ = next_;
        x->next_->prev_ = x;
        x->prev_ = this;
        next_ = x;
    }

    void Remove(SnapshotList *owns) const {
        DCHECK_EQ(owns_, owns);
        prev_->next_ = next_;
        next_->prev_ = prev_;
    }

    template<class T>
    static const SnapshotImpl *DownCast(const T *x) {
        if (!x) {
            return nullptr;
        }
        DCHECK(dynamic_cast<const SnapshotImpl*>(x));
        return static_cast<const SnapshotImpl*>(x);
    }

    uint64_t version() const { return version_; }

    SnapshotImpl *next() const { return next_; }
    SnapshotImpl *prev() const { return prev_; }

private:
    uint64_t version_;

    SnapshotList *owns_ = nullptr;
    SnapshotImpl *prev_ = this;
    SnapshotImpl *next_ = this;
};

class SnapshotList : public base::DisableCopyAssign {
public:
    SnapshotList() : dummy_(0, this) {}

    ~SnapshotList() {
        auto iter = dummy_.next();
        auto p = iter;
        while (iter != &dummy_) {
            iter = iter->next();
            delete p;
            p = iter;
        }
    }

    SnapshotImpl *CreateSnapshot(uint64_t version) {
        auto rv = new SnapshotImpl(version, this);
        dummy_.InsertTail(rv);
        return rv;
    }

    void DeleteSnapshot(const SnapshotImpl *x) {
        x->Remove(this);
        delete x;
    }

private:
    SnapshotImpl dummy_;
};

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

    SnapshotList snapshots_;
    std::unique_ptr<base::FileLock> db_lock_;
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