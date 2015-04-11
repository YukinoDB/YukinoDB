#include "balance/db_impl.h"
#include "balance/version_set.h"
#include "balance/table-inl.h"
#include "balance/table.h"
#include "util/linked_queue.h"
#include "util/log.h"
#include "yukino/iterator.h"
#include "yukino/write_batch.h"
#include "yukino/env.h"
#include "glog/logging.h"
#include <list>
#include <chrono>

#if defined(CHECK_OK)
#   undef CHECK_OK
#endif
#   define CHECK_OK(expr) rs = (expr); if (!rs.ok()) return rs

namespace yukino {

namespace balance {

DBImpl::DBImpl(const Options &options, const std::string &name)
    : options_(options)
    , name_(name)
    , env_(DCHECK_NOTNULL(options.env))
    , comparator_(options_.comparator)
    , snapshot_dummy_(0)
    , versions_(new VersionSet(name, DCHECK_NOTNULL(options.comparator),
                               DCHECK_NOTNULL(options.env)))
    , shutting_down_(nullptr)
    , files_(name) {
}

DBImpl::~DBImpl() {
    DLOG(INFO) << "Shutting down, last_tx_id: " << versions_->last_tx_id();
    {
        std::unique_lock<std::mutex> lock(mutex_);

        shutting_down_.store(this, std::memory_order_release);
        while (background_active_) {
            background_cv_.wait(lock);
            //std::this_thread::yield();
        }
    }

    if (db_lock_.get()) {
        auto rs = db_lock_->Unlock();
        if (!rs.ok()) {
            LOG(ERROR) << "Can not unlock file: " << db_lock_->name()
            << " cause: " << rs.ToString();
        }
    }
    env_->DeleteFile(files_.LockFile(), false);

    while (!util::Dll::Empty(&snapshot_dummy_)) {
        auto snapshot = util::Dll::Head(&snapshot_dummy_);
        util::Dll::Remove(snapshot);
        delete snapshot;
    }
}

base::Status DBImpl::Open() {
    base::Status rs;

    if (options_.block_size < Config::kMinPageSize ||
        options_.block_size > Config::kMaxPageSize) {
        return base::Status::InvalidArgument("block_size out of range");
    }

    if (!env_->FileExists(files_.CurrentFile())) {
        if (!options_.create_if_missing) {

            return base::Status::InvalidArgument("db miss and "
                                                 "create_if_missing is false.");
        }

        rs = CreateDB();
    } else {
        if (options_.error_if_exists) {
            return base::Status::InvalidArgument("db exists and "
                                                 "error_if_exists is true");
        }

        rs = Recover();
    }

    return rs;
}

//==========================================================================
// Override methods:
//==========================================================================
base::Status DBImpl::Put(const WriteOptions& options,
                         const base::Slice& key,
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

namespace {

class WritingHandler : public WriteBatch::Handler {
public:
    WritingHandler(uint64_t last_tx_id, Table *table)
        : last_tx_id_(last_tx_id)
        , table_(table) {
    }

    virtual ~WritingHandler() override {
    }

    virtual void Put(const base::Slice& key,
                     const base::Slice& value) override {
        table_->Put(key, tx_id(), kFlagValue, value, nullptr);
        ++counting_tx_;

        counting_size_ += key.size() + sizeof(uint32_t) + sizeof(uint64_t);
        counting_size_ += value.size();
    }

    virtual void Delete(const base::Slice& key) override {
        table_->Put(key, tx_id(), kFlagDeletion, "", nullptr);
        ++counting_tx_;

        counting_size_ += key.size() + sizeof(uint32_t) + sizeof(uint64_t);
    }

    uint64_t counting_tx() const { return counting_tx_; }

    uint64_t counting_size() const { return counting_size_; }
    
private:
    uint64_t tx_id() const { return last_tx_id_ + counting_tx_; }
    
    const uint64_t last_tx_id_;
    uint64_t counting_tx_ = 0;
    
    uint64_t counting_size_ = 0;
    
    Table *table_;
}; // class WritingHandler

} // namespace

base::Status DBImpl::Write(const WriteOptions& options,
                           WriteBatch* updates) {
    base::Status rs;
    // TODO: Wait for checkpoint

    // Write-ahead-log fisrt:
    CHECK_OK(log_->Append(updates->buf()));
    if (options.sync) {
        CHECK_OK(log_file_->Sync());
    }

    std::unique_lock<std::mutex> lock(mutex_);
    uint64_t tx_id = versions_->last_tx_id();

    WritingHandler handler(tx_id, table_.get());
    updates->Iterate(&handler);
    if (!table_->status().ok()) {
        return table_->status();
    }
    versions_->AdvacneTxId(handler.counting_tx());

    AddCheckpointRate(::rand() % static_cast<int>(updates->buf().size()));
    if (checkpoint_rate_ > checkpoint_threshold_) {
        ScheduleCheckpoint();
    }
    return rs;
}

base::Status DBImpl::Get(const ReadOptions& options,
                         const base::Slice& key, std::string* value) {
    base::Status rs;
    uint64_t tx_id = 0;

    std::unique_lock<std::mutex> lock(mutex_);
    if (options.snapshot) {
        tx_id = static_cast<const SnapshotImpl *>(options.snapshot)->tx_id();
    } else {
        tx_id = versions_->last_tx_id();
    }

    if (table_->Get(key, tx_id, value)) {
        // Key be find.
    } else {
        rs = base::Status::NotFound("");
    }
    return rs;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
    // TODO:
    return nullptr;
}

const Snapshot* DBImpl::GetSnapshot() {
    std::unique_lock<std::mutex> lock(mutex_);

    auto snapshot = new SnapshotImpl(versions_->last_tx_id());
    util::Dll::InsertTail(&snapshot_dummy_, snapshot);
    return snapshot;
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
#if defined(DEBUG)
    if (snapshot) {
        DCHECK(nullptr != dynamic_cast<const SnapshotImpl *>(snapshot));
    }
#endif
    auto impl = static_cast<const SnapshotImpl *>(snapshot);

    std::unique_lock<std::mutex> lock(mutex_);
    util::Dll::Remove(impl);
    delete impl;
}

base::Status DBImpl::CreateDB() {
    base::Status rs;

    CHECK_OK(env_->CreateDir(name_));

    base::FileLock *lock = nullptr;
    CHECK_OK(env_->LockFile(files_.LockFile(), &lock));
    db_lock_ = std::unique_ptr<base::FileLock>(lock);

    CHECK_OK(NewLog(versions_->NextFileNumber()));
    CHECK_OK(NewTable());
    CHECK_OK(table_->Create(static_cast<uint32_t>(options_.block_size),
                            Config::kBtreeFileVersion,
                            Config::kBtreeOrder, storage_io_.get()));

    VersionPatch patch;
    patch.set_comparator(comparator_.delegated()->Name());
    patch.set_log_file_number(log_file_number_);
    CHECK_OK(versions_->Apply(&patch, nullptr));

    return rs;
}

base::Status DBImpl::Recover() {
    base::Status rs;

    base::FileLock *lock = nullptr;
    CHECK_OK(env_->LockFile(files_.LockFile(), &lock));
    db_lock_ = std::unique_ptr<base::FileLock>(lock);

    std::string buf;
    CHECK_OK(base::ReadAll(files_.CurrentFile(), &buf));
    if (buf.back() != '\n') {
        return base::Status::Corruption("CURRENT file is not with newline.");
    }
    buf[buf.size() - 1] = '\0';

    auto manifest_file_number = atoll(buf.c_str());
    CHECK_OK(versions_->Recover(manifest_file_number));

    uint64_t storage_size = 0;
    CHECK_OK(env_->GetFileSize(files_.DataFile(), &storage_size));
    CHECK_OK(NewTable());
    CHECK_OK(table_->Open(storage_io_.get(), storage_size));
    CHECK_OK(Redo(versions_->log_file_number(), versions_->startup_tx_id()));

    CHECK_OK(NewLog(versions_->NextFileNumber()));
    return rs;
}

base::Status DBImpl::Redo(uint64_t log_file_number, uint64_t startup_tx_id) {
    base::Status rs;
    base::MappedMemory *rv = nullptr;
    CHECK_OK(env_->CreateRandomAccessFile(files_.LogFile(log_file_number), &rv));

    std::unique_ptr<base::MappedMemory> file(rv);
    util::Log::Reader reader(file->buf(), file->size(), true,
                             util::Log::kDefaultBlockSize);
    base::Slice record;
    std::string buf;

    WritingHandler handler(startup_tx_id, table_.get());
    while (reader.Read(&record, &buf) && reader.status().ok()) {
        CHECK_OK(WriteBatch::Iterate(record.data(), record.size(), &handler));
        CHECK_OK(table_->status());
    }

    versions_->AdvacneTxId(handler.counting_tx());
    return reader.status();
}

void DBImpl::ScheduleCheckpoint() {
    background_active_ = true;
    checkpoint_rate_   = 0;

    std::thread([this]() {
        this->BackgroundCheckpoint();
    }).detach();
}

void DBImpl::BackgroundCheckpoint() {
    using namespace std::chrono;

    DCHECK(background_active_);

    if (shutting_down_.load(std::memory_order_acquire)) {
        return;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    auto start = high_resolution_clock::now();
    auto defer = base::Defer([this, &start]() {
        background_active_ = false;
        background_cv_.notify_one();

        auto epch = high_resolution_clock::now() - start;
        LOG(INFO) << "Checkpoint epch: "
                  << duration_cast<milliseconds>(epch).count() << " ms";
    });


    if (!CatchError(table_->Flush(true))) {
        return;
    }
    if (!CatchError(PurgingStep(versions_->startup_tx_id()))) {
        return;
    }

    // switch new log-file
    auto prev_log_number = log_file_number_;
    if (!CatchError(NewLog(versions_->NextFileNumber()))) {
        return;
    }

    VersionPatch patch;
    patch.set_log_file_number(log_file_number_);
    patch.set_prev_log_file_number(prev_log_number);

    CatchError(versions_->Apply(&patch, &mutex_));
}

base::Status DBImpl::PurgingStep(uint64_t startup_tx_id) {
    base::Status rs;

    std::unique_ptr<Iterator> iter(table_->CreateIterator());
    CHECK_OK(iter->status());

    if (purging_point_.empty()) {
        iter->SeekToFirst();
    } else {
        iter->Seek(purging_point_);
    }

    auto count = purging_count_;
    std::list<std::string> collection;
    std::string deletion_key;
    while (count-- > 0 && iter->Valid() && deletion_key.empty()) {
        auto parsed = InternalKey::PartialParse(iter->key().data(),
                                                iter->key().size());
        if (!deletion_key.empty()) {
            if (deletion_key == parsed.user_key.ToString()) {
                std::string key(parsed.key().data(), parsed.key().size());

                collection.push_back(std::move(key));
            } else {
                deletion_key.clear();
            }
        }
        if (parsed.tx_id < startup_tx_id) {
            std::string key(parsed.key().data(), parsed.key().size());

            collection.push_back(std::move(key));
        } else if (parsed.flag == kFlagDeletion) {
            deletion_key.assign(parsed.user_key.data(), parsed.user_key.size());
            std::string key(parsed.key().data(), parsed.key().size());

            collection.push_back(std::move(key));
        }
        iter->Next();
    }

    if (iter->Valid()) {
        purging_point_.assign(iter->key().data(), iter->key().size());
    } else {
        purging_point_.clear();
    }

    for (const auto &key : collection) {
        auto parsed = InternalKey::PartialParse(key.data(), key.size());
        table_->Purge(parsed.user_key, parsed.tx_id, nullptr);

        CHECK_OK(table_->status());
    }
    return rs;
}

bool DBImpl::CatchError(const base::Status &status) {
    if (background_status_.ok() && !status.ok()) {
        background_status_ = status;
    }
    if (!status.ok()) {
        DLOG(ERROR) << "background error: " << status.ToString();
    }
    return status.ok();
}

base::Status DBImpl::NewTable() {
    base::Status rs;
    base::FileIO *io = nullptr;
    CHECK_OK(env_->CreateFileIO(files_.DataFile(), &io));
    storage_io_ = base::make_unique_ptr(io);

    table_ = new Table(comparator_, options_.write_buffer_size);
    return rs;
}

base::Status DBImpl::NewLog(uint64_t log_file_number) {
    base::Status rs;
    log_file_number_ = log_file_number;

    base::AppendFile *file = nullptr;
    CHECK_OK(env_->CreateAppendFile(files_.LogFile(log_file_number_), &file));

    log_file_ = base::make_unique_ptr(file);
    log_      = base::make_unique_ptr(new util::Log::Writer(file,
                                                            util::Log::kDefaultBlockSize));
    return rs;
}

} // namespace balance
    
} // namespace yukino