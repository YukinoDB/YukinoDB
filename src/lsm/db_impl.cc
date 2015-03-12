#include "lsm/db_impl.h"
#include "lsm/builtin.h"
#include "lsm/format.h"
#include "lsm/table_cache.h"
#include "lsm/version.h"
#include "lsm/log.h"
#include "yukino/write_batch.h"
#include "yukino/options.h"
#include "yukino/env.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

class DBImpl::WritingHandler : public WriteBatch::Handler {
public:
    WritingHandler(uint64_t last_version, MemoryTable *table)
        : last_version_(last_version)
        , mutable_(table) {
    }

    virtual ~WritingHandler() override {
    }

    virtual void Put(const base::Slice& key, const base::Slice& value) override {
        mutable_->Put(key, value, version(), kFlagValue);
        ++counting_version_;

        counting_size_ += key.size() + sizeof(uint32_t) + sizeof(uint64_t);
        counting_size_ += value.size();
    }

    virtual void Delete(const base::Slice& key) override {
        mutable_->Put(key, "", version(), kFlagDeletion);
        ++counting_version_;

        counting_size_ += key.size() + sizeof(uint32_t) + sizeof(uint64_t);
    }

    uint64_t counting_version() const { return counting_version_; }

    uint64_t counting_size() const { return counting_size_; }

private:
    uint64_t version() const { return last_version_ + counting_version_; }

    const uint64_t last_version_;
    uint64_t counting_version_ = 0;

    uint64_t counting_size_ = 0;

    MemoryTable *mutable_;
};

DBImpl::DBImpl() {
}

base::Status DBImpl::Open(const Options &opt, const std::string &name) {
    env_ = DCHECK_NOTNULL(opt.env);
    db_name_ = name;
    internal_comparator_ = std::unique_ptr<InternalKeyComparator>(
                                     new InternalKeyComparator(opt.comparator));

    table_cache_ = std::unique_ptr<TableCache>(new TableCache(db_name_, opt));

    versions_ = std::unique_ptr<VersionSet>(new VersionSet(db_name_, opt,
                                                           table_cache_.get()));

    mutable_ = new MemoryTable(*internal_comparator_);

    write_buffer_size_ = opt.write_buffer_size;
    if (write_buffer_size_ <= 1 * base::kMB) {
        return base::Status::InvalidArgument("options.write_buffer_size too "
                                             "small, should be > 1 MB");
    }

    auto current_file_name = db_name_ + "/" + kCurrentFileName;
    if (!env_->FileExists(current_file_name)) {

        if (opt.create_if_missing) {
            return NewDB(opt);
        } else {
            return base::Status::InvalidArgument("db miss and "
                                                 "create_if_missing is false.");
        }
    }

    return base::Status::OK();
}

DBImpl::~DBImpl() {
    {
        std::unique_lock<std::mutex> lock(mutex_);

        shutting_down_.store(this, std::memory_order_release);
        while (background_active_) {
            background_cv_.wait(lock);
        }
    }
}

base::Status DBImpl::Put(const WriteOptions& options, const base::Slice& key,
                         const base::Slice& value) {
    WriteBatch batch;
    batch.Put(key, value);
    return Write(options, &batch);
}

base::Status DBImpl::Delete(const WriteOptions& options,
                            const base::Slice& key) {
    WriteBatch batch;
    batch.Delete(key);
    return Write(options, &batch);
}

base::Status DBImpl::Write(const WriteOptions& options,
                           WriteBatch* updates) {
    uint64_t last_version = 0;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        last_version = versions_->last_version();

        auto rs = MakeRoomForWrite(false, &lock);
        if (!rs.ok()) {
            return rs;
        }

        rs = log_->Append(updates->buf());
        if (!rs.ok()) {
            return rs;
        }

        rs = log_file_->Sync();
        if (!rs.ok()) {
            return rs;
        }
    }

    WritingHandler handler(last_version + 1, mutable_.get());
    updates->Iterate(&handler);

    mutex_.lock();
    versions_->AdvanceVersion(handler.counting_version());
    mutex_.unlock();
    return base::Status::OK();
}

base::Status DBImpl::Get(const ReadOptions& options,
                         const base::Slice& key, std::string* value) {
    // TODO:
    return base::Status::OK();
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    // TODO:
    return nullptr;
}

const Snapshot* DBImpl::GetSnapshot() {
    // TODO:
    return nullptr;
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {

}

base::Status DBImpl::NewDB(const Options &opt) {
    auto rs = env_->CreateDir(db_name_);
    if (!rs.ok()) {
        return rs;
    }

    log_file_number_ = versions_->GenerateFileNumber();

    base::AppendFile *file = nullptr;
    rs = env_->CreateAppendFile(LogFileName(db_name_, log_file_number_), &file);
    if (!rs.ok()) {
        return rs;
    }

    log_file_ = std::unique_ptr<base::AppendFile>(file);
    log_ = std::unique_ptr<LogWriter>(new Log::Writer(file,
                                                      Log::kDefaultBlockSize));

    // check log file ok:
    rs = log_->Append("YukinoDB");
    if (!rs.ok()) {
        return rs;
    }
    rs = log_file_->Sync();
    if (!rs.ok()) {
        return rs;
    }

    return base::Status::OK();
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is the current logger
// That's means, the function must be lock.
base::Status DBImpl::MakeRoomForWrite(bool force,
                                      std::unique_lock<std::mutex> *lock) {
    bool allow_delay = !force;
    base::Status rs;

    while (true) {

        if (!background_error_.ok()) {
            rs = background_error_;
            break;
        } else if (allow_delay &&
                   versions_->NumberLevelFiles(0) >= kMaxNumberLevel0File) {
            mutex_.unlock();

            std::this_thread::sleep_for(std::chrono::seconds(1));
            allow_delay = false;

            mutex_.lock();
        } else if (!force &&
                   mutable_->memory_usage_size() <= write_buffer_size_) {
            break;
        } else if (immtable_.get()) {
            background_cv_.wait(*lock);
        } else if (versions_->NumberLevelFiles(0) >= kMaxNumberLevel0File) {
            LOG(INFO) << "Level-0 files: " << versions_->NumberLevelFiles(0)
                << " wait...";
            background_cv_.wait(*lock);
        } else {
            DCHECK_EQ(0, versions_->prev_log_number());

            auto new_log_number = versions_->GenerateFileNumber();
            base::AppendFile *file = nullptr;
            rs = env_->CreateAppendFile(LogFileName(db_name_, new_log_number),
                                        &file);
            if (!rs.ok()) {
                break;
            }

            log_file_number_ = new_log_number;
            log_file_ = std::unique_ptr<base::AppendFile>(file);
            log_ = std::unique_ptr<LogWriter>(new Log::Writer(file,
                                                       Log::kDefaultBlockSize));
            immtable_ = mutable_;
            mutable_ = new MemoryTable(*internal_comparator_);
            force = false;
            MaybeScheduleCompaction();
        }
    }
    
    return rs;
}

void DBImpl::MaybeScheduleCompaction() {
    if (background_active_) {
        return; // Compaction is running.
    }

    if (shutting_down_.load(std::memory_order_acquire)) {
        return; // Is shutting down, ignore schedule
    }

    if (immtable_.get() == nullptr && !versions_->NeedsCompaction()) {
        return; // Compaction is no need
    }

    background_active_ = true;
    std::thread([this]() {
        this->BackgroundWork();
    }).detach();
}

void DBImpl::BackgroundWork() {
    std::unique_lock<std::mutex> lock(mutex_);

    DCHECK(background_active_);
    if (!shutting_down_.load(std::memory_order_acquire)) {
        BackgroundCompaction();
    }
    background_active_ = false;

    MaybeScheduleCompaction();
    background_cv_.notify_all();
}

void DBImpl::BackgroundCompaction() {
    // TODO:
}

} // namespace lsm

} // namespace yukino