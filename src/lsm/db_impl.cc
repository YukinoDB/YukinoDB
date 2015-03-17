#include "lsm/db_impl.h"
#include "lsm/db_iter.h"
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
#include <chrono>
#include <map>

namespace yukino {

namespace lsm {

namespace {

inline uint64_t now_microseconds() {
    using namespace std::chrono;

    auto now = high_resolution_clock::now();
    return duration_cast<microseconds>(now.time_since_epoch()).count();
}

} // namespace

SnapshotImpl::~SnapshotImpl() {
}

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
    , block_size_(opt.block_size)
    , block_restart_interval_(opt.block_restart_interval)
    , db_name_(name)
    , internal_comparator_(new InternalKeyComparator(opt.comparator))
    , table_cache_(new TableCache(db_name_, opt))
    , versions_(new VersionSet(db_name_, opt, table_cache_.get()))
    , write_buffer_size_(opt.write_buffer_size) {

    mutable_ = new MemoryTable(*internal_comparator_);
}

base::Status DBImpl::Open(const Options &opt) {

    if (block_size_ == 0 || block_size_ > INT32_MAX) {
        return base::Status::InvalidArgument("block_size out of range");
    }

    if (block_restart_interval_ <= 0) {
        return base::Status::InvalidArgument("block_restart_interval out of range");
    }

//    if (write_buffer_size_ <= 1 * base::kMB) {
//        return base::Status::InvalidArgument("options.write_buffer_size too "
//                                             "small, should be > 1 MB");
//    }

    base::Status rs;
    shutting_down_.store(nullptr, std::memory_order_release);
    if (!env_->FileExists(CurrentFileName(db_name_))) {
        if (!opt.create_if_missing) {

            return base::Status::InvalidArgument("db miss and "
                                                 "create_if_missing is false.");
        }

        rs = NewDB(opt);
    } else {
        if (opt.error_if_exists) {
            return base::Status::InvalidArgument("db exists and "
                                                 "error_if_exists is true");
        }

        rs = Recovery();
    }

    return rs;
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

    if (db_lock_.get()) {
        auto rs = db_lock_->Unlock();
        if (!rs.ok()) {
            LOG(ERROR) << "Can not unlock file: " << db_lock_->name()
                       << " cause: " << rs.ToString();
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

        if (options.sync) {
            rs = log_file_->Sync();
            if (!rs.ok()) {
                return rs;
            }
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
        last_version = SnapshotImpl::DownCast(options.snapshot)->version();
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
    uint64_t version = 0;

    std::unique_lock<std::mutex> lock(mutex_);
    if (options.snapshot) {
        version = SnapshotImpl::DownCast(options.snapshot)->version();
    } else {
        version = versions_->last_version();
    }

    std::vector<Iterator*> children;
    children.push_back(mutable_->NewIterator());

    if (immtable_.get()) {
        children.push_back(immtable_->NewIterator());
    }

    auto rs = versions_->AddIterators(options, &children);
    if (!rs.ok()) {
        for (auto iter : children) {
            delete iter;
        }
        return CreateErrorIterator(rs);
    }

    auto rv = CreateDBIterator(internal_comparator_.get(), &children[0],
                               children.size(), version);
    mutable_->AddRef();
    rv->RegisterCleanup([this]() {
        mutable_->Release();
    });

    if (immtable_.get()) {
        immtable_->AddRef();
        rv->RegisterCleanup([this] () {
            immtable_->Release();
        });
    }
    return rv;
}

const Snapshot* DBImpl::GetSnapshot() {
    std::unique_lock<std::mutex> lock;
    return snapshots_.CreateSnapshot(versions_->last_version());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
    std::unique_lock<std::mutex> lock;
    snapshots_.DeleteSnapshot(SnapshotImpl::DownCast(snapshot));
}

base::Status DBImpl::NewDB(const Options &opt) {
    auto rs = env_->CreateDir(db_name_);
    if (!rs.ok()) {
        return rs;
    }

    base::FileLock *lock = nullptr;
    rs = env_->LockFile(LockFileName(db_name_), &lock);
    if (!rs.ok()) {
        return rs;
    }
    db_lock_ = std::unique_ptr<base::FileLock>(lock);

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

    base::FileLock *lock = nullptr;
    auto rs = env_->LockFile(LockFileName(db_name_), &lock);
    if (!rs.ok()) {
        return rs;
    }
    db_lock_ = std::unique_ptr<base::FileLock>(lock);

    std::string buf;
    rs = base::ReadAll(CurrentFileName(db_name_), &buf);
    if (!rs.ok()) {
        return rs;
    }

    if (buf.back() != '\n') {
        return base::Status::Corruption("CURRENT file is not with newline.");
    }
    buf[buf.size() - 1] = '\0';

    auto manifest_file_number = atoll(buf.c_str());
    std::vector<uint64_t> logs;
    rs = versions_->Recovery(manifest_file_number, &logs);
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

void DBImpl::DeleteObsoleteFiles() {
    std::vector<std::string> children;

    auto rs = env_->GetChildren(db_name_, &children);
    if (!rs.ok()) {
        LOG(ERROR) << "Can not open db: " << db_name_
                   << ", cause: " << rs.ToString();
        return;
    }

    std::map<uint64_t, std::string> exists;
    for (const auto &child : children) {
        auto rv = Files::ParseName(child);

        switch (std::get<0>(rv)) {
        case Files::kLog:
        case Files::kTable:
        case Files::kManifest:
            exists.emplace(std::get<1>(rv), child);
            break;

        default:
            break;
        }
    }

    exists.erase(versions_->redo_log_number());
    exists.erase(versions_->manifest_file_number());
    exists.erase(log_file_number_);
    for (auto i = 0; i < kMaxLevel; i++) {
        auto files = versions_->current()->file(i);

        for (const auto &metadata : files) {
            exists.erase(metadata->number);
        }
    }

    for (const auto &entry : exists) {
        auto rs = env_->DeleteFile(db_name_ + "/" + entry.second, false);
        if (rs.ok()) {
            DLOG(INFO) << "Delete obsolete file: " << entry.second;
        } else {
            DLOG(INFO) << "Delete obsolete file: " << entry.second
                       << " fail, cause: " << rs.ToString();
        }
        table_cache_->Invalid(entry.first);
    }
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
        } else if (allow_delay && background_active_ &&
                   versions_->NumberLevelFiles(0) >= kMaxNumberLevel0File) {
            mutex_.unlock();

            std::this_thread::sleep_for(std::chrono::seconds(1));
            allow_delay = false;

            mutex_.lock();
        } else if (!force &&
                   mutable_->memory_usage_size() <= write_buffer_size_) {
            break;
        } else if (immtable_.get()) {
            if (background_active_) {
                background_cv_.wait(*lock);
            }
        } else if (background_active_ &&
                   versions_->NumberLevelFiles(0) >= kMaxNumberLevel0File) {
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
    DLOG(INFO) << "Background work on...";
    std::unique_lock<std::mutex> lock(mutex_);

    DCHECK(background_active_);
    if (!shutting_down_.load(std::memory_order_acquire)) {
        BackgroundCompaction();
    }
    background_active_ = false;

    MaybeScheduleCompaction();
    background_cv_.notify_all();
}

// REQUIRES: mutex_.lock()
void DBImpl::BackgroundCompaction() {
    using namespace std::chrono;
    auto start = high_resolution_clock::now();
    auto defer = base::Defer([&start] () {
        auto epch = high_resolution_clock::now() - start;
        LOG(INFO) << "Compaction epch: "
                  << duration_cast<milliseconds>(epch).count() << " ms";
    });

    // Need compact memory table first.
    if (immtable_.get() != nullptr) {
        auto rs = CompactMemoryTable();
        if (!rs.ok()) {
            DLOG(ERROR) << rs.ToString();
        }
        background_error_ = rs;
        return;
    }


    if (versions_->NeedsCompaction()) {
        Compaction *rv_cpt;
        VersionPatch patch;
        auto rs = versions_->GetCompaction(&patch, &rv_cpt);
        if (!rs.ok()) {
            background_error_ = rs;
            return;
        }

        mutex_.unlock();

        std::unique_ptr<Compaction> compaction(rv_cpt);

        base::AppendFile *rv_file = nullptr;
        std::string file_name(TableFileName(db_name_,
                                            compaction->target_file_number()));
        rs = env_->CreateAppendFile(file_name, &rv_file);
        if (!rs.ok()) {
            background_error_ = rs;
            return;
        }

        std::unique_ptr<base::AppendFile> file(rv_file);
        TableOptions options;
        options.block_size       = static_cast<uint32_t>(block_size_);
        options.restart_interval = block_restart_interval_;
        TableBuilder builder(options, file.get());
        rs = compaction->Compact(&builder);
        if (!rs.ok()) {
            background_error_ = rs;
            return;
        }

        file->Close();
        mutex_.lock();

        base::Handle<FileMetadata> metadata(new FileMetadata(compaction->target_file_number()));
        rs = table_cache_->GetFileMetadata(metadata->number, metadata.get());
        if (!rs.ok()) {
            background_error_ = rs;
            return;
        }

        patch.CreateFile(compaction->target_level(), metadata.get());
        rs = versions_->Apply(&patch, &mutex_);
        if (!rs.ok()) {
            background_error_ = rs;
            return;
        }
        DeleteObsoleteFiles();
    }
}

// REQUIRES: mutex_.lock()
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

    DeleteObsoleteFiles();
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

    auto counter = 0;
    std::unique_ptr<base::AppendFile> file(rv);
    TableOptions options;
    options.block_size       = static_cast<uint32_t>(block_size_);
    options.restart_interval = block_restart_interval_;
    TableBuilder builder(options, file.get());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        rs = builder.Append(Chunk::CreateKeyValue(iter->key(), iter->value()));
        if (!rs.ok()) {
            break;
        }

//        {
//            auto show_key = InternalKey::CreateKey(iter->key());
//            DLOG(INFO) << "dump key: " << show_key.user_key_slice().ToString()
//                       << " version: " << show_key.tag().version;
//        }

        counter++;
    }
    if (rs.ok()) {
        rs = builder.Finalize();
    }

    file->Close();
    if (!rs.ok()) {
        DLOG(ERROR) << "Build table fail: " << rs.ToString();
        env_->DeleteFile(file_name, false);
        return rs;
    }
//    DLOG(INFO) << "Build table: " << metadata->number
//               << " ok, count: " << counter;
    metadata->ctime = now_microseconds();
    return table_cache_->GetFileMetadata(metadata->number, metadata);
}

void DBImpl::TEST_WaitForBackground() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (background_active_) {
        background_cv_.wait_for(lock, std::chrono::seconds(1));
    }
}

void DBImpl::TEST_DumpVersions() {
    for (auto i = 0; i < kMaxLevel; ++i) {
        std::string text;

        for (const auto &metadata : versions_->current()->file(i)) {
            text.append(base::Strings::Sprintf("[%llu.sst %llu] ",
                                               metadata->number,
                                               metadata->size));
        }

        DLOG(INFO) << "level-" << i << " " << text;
    }
}

} // namespace lsm

} // namespace yukino