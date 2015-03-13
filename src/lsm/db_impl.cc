#include "lsm/db_impl.h"
#include "lsm/builtin.h"
#include "lsm/format.h"
#include "lsm/table_cache.h"
#include "lsm/table_builder.h"
#include "lsm/version.h"
#include "lsm/log.h"
#include "lsm/compaction.h"
#include "yukino/iterator.h"
#include "yukino/write_batch.h"
#include "yukino/options.h"
#include "yukino/read_options.h"
#include "yukino/write_options.h"
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
        //DLOG(INFO) << "key: " << key.ToString() << " version: " << version();
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

DBImpl::DBImpl(const Options &opt, const std::string &name)
    : env_(DCHECK_NOTNULL(opt.env))
    , db_name_(name)
    , internal_comparator_(new InternalKeyComparator(opt.comparator))
    , table_cache_(new TableCache(db_name_, opt))
    , versions_(new VersionSet(db_name_, opt, table_cache_.get()))
    , write_buffer_size_(opt.write_buffer_size) {

    mutable_ = new MemoryTable(*internal_comparator_);
}

base::Status DBImpl::Open(const Options &opt) {

    if (write_buffer_size_ <= 1 * base::kMB) {
        return base::Status::InvalidArgument("options.write_buffer_size too "
                                             "small, should be > 1 MB");
    }

    if (!env_->FileExists(CurrentFileName(db_name_))) {

        if (opt.create_if_missing) {
            return NewDB(opt);
        } else {
            return base::Status::InvalidArgument("db miss and "
                                                 "create_if_missing is false.");
        }
    }

    if (opt.error_if_exists) {
        return base::Status::InvalidArgument("db exists and "
                                             "error_if_exists is true");
    }

    return Recovery();
}

DBImpl::~DBImpl() {
    DLOG(INFO) << "Shutting down, last_version: " << versions_->last_version();
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
    uint64_t last_version = 0;

    std::unique_lock<std::mutex> lock(mutex_);
    if (options.snapshot) {
        // TODO:
    } else {
        last_version = versions_->last_version();
    }

    base::Handle<MemoryTable> mut(mutable_);
    base::Handle<MemoryTable> imm(immtable_);

    base::Status rs;
    mutex_.unlock();

    InternalKey internal_key = InternalKey::CreateKey(key, last_version);
    rs = mut->Get(internal_key, value);
    if (rs.IsNotFound()) {
        if (imm.get()) {
            rs = imm->Get(internal_key, value);
        }
    }

    mutex_.lock();

    if (rs.ok() || !rs.IsNotFound()) {
        return rs;
    }

    base::Handle<Version> current(versions_->current());
    return current->Get(options, internal_key, value);
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

    VersionPatch patch(internal_comparator_->delegated()->Name());
    patch.set_prev_log_number(0);
    patch.set_redo_log_number(log_file_number_);

    return versions_->Apply(&patch, &mutex_);
}

base::Status DBImpl::Recovery() {

    std::string buf;
    auto rs = base::ReadAll(CurrentFileName(db_name_), &buf);
    if (!rs.ok()) {
        return rs;
    }

    if (buf.back() != '\n') {
        return base::Status::Corruption("CURRENT file is not with newline.");
    }
    buf[buf.size() - 1] = '\0';

    auto manifest_file_number = atoll(buf.c_str());
    std::vector<uint64_t> logs;
    rs = ReplayVersions(manifest_file_number, &logs);
    if (!rs.ok()) {
        return rs;
    }

    DCHECK_GE(logs.size(), 2);
    rs = Redo(versions_->redo_log_number(), logs[logs.size() - 2]);
    if (!rs.ok()) {
        return rs;
    }
    DLOG(INFO) << "Replay ok, last version: " << versions_->last_version();

    log_file_number_ = versions_->redo_log_number();
    base::AppendFile *file = nullptr;
    rs = env_->CreateAppendFile(LogFileName(db_name_, log_file_number_), &file);
    if (!rs.ok()) {
        return rs;
    }

    log_file_ = std::unique_ptr<base::AppendFile>(file);
    log_ = std::unique_ptr<LogWriter>(new Log::Writer(file,
                                                      Log::kDefaultBlockSize));
    return base::Status::OK();
}

base::Status DBImpl::ReplayVersions(uint64_t file_number,
                                    std::vector<uint64_t> *logs) {
    base::MappedMemory *rv = nullptr;
    auto rs = env_->CreateRandomAccessFile(ManifestFileName(db_name_,
                                                            file_number), &rv);
    if (!rs.ok()) {
        return rs;
    }
    std::unique_ptr<base::MappedMemory> file(rv);
    Log::Reader reader(file->buf(), file->size(), true, Log::kDefaultBlockSize);

    VersionPatch patch("");
    VersionSet::Builder builder(versions_.get(), versions_->current());
    base::Slice record;
    std::string buf;
    while (reader.Read(&record, &buf)) {
        if (!reader.status().ok()) {
            break;
        }
        patch.Reset();
        patch.Decode(record);
        if (patch.has_field(VersionPatch::kComparator)) {
            if (patch.comparator() != internal_comparator_->delegated()->Name()) {
                return base::Status::Corruption("difference comparators");
            }
        }
        logs->push_back(patch.last_version());

        DLOG(INFO) << "Replay apply: " << patch.last_version();
        builder.Apply(patch);
        versions_->Append(builder.Build());
    }
    return reader.status();
}

base::Status DBImpl::Redo(uint64_t file_number, uint64_t last_version) {
    base::MappedMemory *rv = nullptr;
    auto rs = env_->CreateRandomAccessFile(LogFileName(db_name_, file_number),
                                           &rv);
    if (!rs.ok()) {
        return rs;
    }

    std::unique_ptr<base::MappedMemory> file(rv);
    Log::Reader reader(file->buf(), file->size(), true, Log::kDefaultBlockSize);
    base::Slice record;
    std::string buf;

    WritingHandler handler(last_version + 1, mutable_.get());
    while (reader.Read(&record, &buf)) {
        if (!reader.status().ok()) {
            break;
        }

        rs = WriteBatch::Iterate(record.data(), record.size(), &handler);
        if (!rs.ok()) {
            return rs;
        }
    }

    versions_->AdvanceVersion(handler.counting_version());
    return reader.status();
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

    // Need compact memory table first.
    if (immtable_.get() != nullptr) {
        CompactMemoryTable();
        return;
    }


}

base::Status DBImpl::CompactMemoryTable() {
    DCHECK_NOTNULL(immtable_.get());

    VersionPatch patch(internal_comparator_->delegated()->Name());
    {
        base::Handle<Version> current(versions_->current());
        auto rs = WriteLevel0Table(current.get(), &patch, immtable_.get());
        if (!rs.ok()) {
            return rs;
        }
    }

    if (shutting_down_.load(std::memory_order_acquire)) {
        return base::Status::IOError("Deleting DB during memtable compaction");
    }

    patch.set_prev_log_number(0);
    patch.set_redo_log_number(log_file_number_);
    auto rs = versions_->Apply(&patch, &mutex_);
    if (!rs.ok()) {
        return rs;
    }

    // compact finish, should release the immtable_
    immtable_ = nullptr;

    // TODO: DeleteObsoleteFiles
    return base::Status::OK();
}

base::Status DBImpl::WriteLevel0Table(const Version *current,
                                      VersionPatch *patch,
                                      MemoryTable *table) {

    base::Handle<FileMetadata> metadata(new FileMetadata(versions_->GenerateFileNumber()));
    std::unique_ptr<Iterator> iter(table->NewIterator());
    if (!iter->status().ok()) {
        return iter->status();
    }
    LOG(INFO) << "Level0 table compaction start, target file number: "
              << metadata->number;

    {
        mutex_.unlock();
        auto rs = BuildTable(iter.release(), metadata.get());
        mutex_.lock();

        if (!rs.ok()) {
            return rs;
        }
    }

    patch->CreateFile(0, metadata.get());
    return base::Status::OK();
}

base::Status DBImpl::BuildTable(Iterator *iter, FileMetadata *metadata) {
    base::AppendFile *rv = nullptr;
    std::string file_name(TableFileName(db_name_, metadata->number));
    auto rs = env_->CreateAppendFile(file_name, &rv);
    if (!rs.ok()) {
        return rs;
    }

    std::unique_ptr<base::AppendFile> file(rv);
    TableBuilder builder(TableOptions(), file.get());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rs = builder.Append(Chunk::CreateKeyValue(iter->key(), iter->value()));
        if (!rs.ok()) {
            break;
        }
    }
    if (rs.ok()) {
        rs = builder.Finalize();
    }

    file->Close();
    if (!rs.ok()) {
        env_->DeleteFile(file_name, false);
    }
    return table_cache_->GetFileMetadata(metadata->number, metadata);
}

} // namespace lsm

} // namespace yukino