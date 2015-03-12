#ifndef YUKINO_LSM_VERSION_H_
#define YUKINO_LSM_VERSION_H_

#include "lsm/chunk.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "base/ref_counted.h"
#include "base/status.h"
#include "glog/logging.h"
#include <stdint.h>
#include <string>
#include <vector>
#include <numeric>
#include <set>

namespace yukino {

class Env;
class Options;
class ReadOptions;

namespace base {

class AppendFile;

} // namespace base

namespace lsm {

class VersionSet;
class TableCache;
class LogWriter;
class VersionBuilder;

struct FileMetadata : public base::ReferenceCounted<FileMetadata> {

    uint64_t number;

    InternalKey smallest_key;
    InternalKey largest_key;

    uint64_t size = 0;
    uint64_t ctime = 0;

    FileMetadata(uint64_t file_number) : number(file_number) {}
};

class VersionPatch : public base::DisableCopyAssign {
public:
    enum Field {
        kComparator,
        kLastVersion,
        kNextFileNumber,
        kRedoLogNumber,
        kPrevLogNumber,
        kCompactionPoint,
        kDeletion,
        kCreation,
        kMaxFields,
    };

    typedef std::vector<std::pair<int, base::Handle<FileMetadata>>> CreationTy;
    typedef std::set<std::pair<int, uint64_t>> DeletionTy;

    static const auto kNum32Bits = (kMaxFields + 31) / 32;

    explicit VersionPatch(const std::string &comparator)
        : comparator_(comparator) {
        ::memset(bits_, 0, kNum32Bits * sizeof(uint32_t));
    }

    uint64_t last_version() const {
        DCHECK(has_field(kLastVersion));
        return last_version_;
    }

    void set_last_version(uint64_t version) {
        set_field(kLastVersion);
        last_version_ = version;
    }

    uint64_t next_file_number() const {
        DCHECK(has_field(kNextFileNumber));
        return next_file_number_;
    }

    void set_next_file_number(uint64_t number) {
        set_field(kNextFileNumber);
        next_file_number_ = number;
    }

    uint64_t redo_log_number() const {
        DCHECK(has_field(kRedoLogNumber));
        return redo_log_number_;
    }

    void set_redo_log_number(uint64_t number) {
        set_field(kRedoLogNumber);
        redo_log_number_ = number;
    }

    uint64_t prev_log_number() const {
        DCHECK(has_field(kPrevLogNumber));
        return prev_log_number_;
    }

    void set_prev_log_number(uint64_t number) {
        set_field(kPrevLogNumber);
        prev_log_number_ = number;
    }

    const DeletionTy &deletion() const {
        //DCHECK(has_field(kDeletion));
        return deletion_;
    }

    const CreationTy &creation() const {
        //DCHECK(has_field(kCreation));
        return creation_;
    }

    void DeleteFile(int level, uint64_t file_number) {
        set_field(kDeletion);
        deletion_.emplace(level, file_number);
    }

    void CreateFile(int level, FileMetadata *metadata) {
        set_field(kCreation);
        creation_.emplace_back(level, base::Handle<FileMetadata>(metadata));
    }

    void CreateFile(int level, uint64_t file_number,
                    const base::Slice &smallest_key,
                    const base::Slice &largest_key,
                    size_t size, uint64_t ctime);

    bool has_field(Field field) const {
        auto i = static_cast<int>(field);
        DCHECK_GE(i, 0); DCHECK_LT(i, kMaxFields);
        return bits_[i / 32] & (1 << (i % 32));
    }

    base::Status Decode(const base::Slice &buf);
    base::Status Encode(std::string *buf) const;

private:
    void set_field(Field field) {
        auto i = static_cast<int>(field);
        DCHECK_GE(i, 0); DCHECK_LT(i, kMaxFields);
        bits_[i / 32] |= (1 << (i % 32));
    }

    std::string comparator_;
    uint64_t last_version_ = 0;
    uint64_t next_file_number_ = 0;
    uint64_t redo_log_number_ = 0;
    uint64_t prev_log_number_ = 0;

    int compaction_level_ = 0;
    InternalKey compaction_key_;

    // Which files will be delete.
    DeletionTy deletion_;

    // Which files will be create.
    CreationTy creation_;

    uint32_t bits_[kNum32Bits];
};

class Version : public base::ReferenceCounted<Version> {
public:
    explicit Version(VersionSet *owned);
    ~Version();

    base::Status Get(const ReadOptions &options, const InternalKey &key,
                     std::string *value);

    Version *next() const { return next_; }
    Version *prev() const { return prev_; }

    void set_next(Version *node) { next_ = node; }
    void set_prev(Version *node) { prev_ = node; }

    VersionSet *owned() const { return owned_; }

    void InsertTail(Version *x) {
        x->prev_ = prev_;
        x->prev_->next_ = x;
        x->next_ = this;
        prev_ = x;
    }

    void InsertHead(Version *x) {
        x->next_ = next_;
        x->next_->prev_ = x;
        x->prev_ = this;
        next_ = x;
    }

    const std::vector<base::Handle<FileMetadata>> &file(size_t i) const {
        return files_[i];
    }

    std::vector<base::Handle<FileMetadata>> *mutable_file(size_t i) {
        return &files_[i];
    }

    size_t NumberLevelFiles(size_t level) const {
        return file(level).size();
    }

    uint64_t SizeLevelFiles(size_t level) const {
        return std::accumulate(file(level).begin(), file(level).end(), 0ULL,
                               [](uint64_t sum, const base::Handle<FileMetadata> &fmd) {
                                   return sum + fmd->size;
                               });
    }

    friend class VersionBuilder;
private:
    VersionSet *owned_;
    Version *next_ = this;
    Version *prev_ = this;

    std::vector<base::Handle<FileMetadata>> files_[kMaxLevel];
};

class VersionSet : public base::DisableCopyAssign {
public:
    typedef VersionBuilder Builder;

    VersionSet(const std::string &db_name, const Options &options,
               TableCache *table_cache);
    ~VersionSet();

    uint64_t AdvanceVersion(uint64_t add) {
        last_version_ += add;
        return last_version_;
    }

    void Append(Version *version) {
        version->AddRef();
        version_dummy_.InsertHead(version);
        current_ = version;
    }

    size_t NumberLevelFiles(size_t level) {
        return current()->NumberLevelFiles(level);
    }

    uint64_t SizeLevelFiles(size_t level) {
        return current()->SizeLevelFiles(level);
    }

    uint64_t GenerateFileNumber() {
        return next_file_number_ ++;
    }

    bool NeedsCompaction() const {
        // TODO:
        return false;
    }

    base::Status Apply(VersionPatch *patch, std::mutex *mutex);

    base::Status CreateManifestFile();

    base::Status WriteSnapshot();

    base::Status WritePatch(const VersionPatch &patch);

    uint64_t last_version() const { return last_version_; }

    uint64_t redo_log_number() const { return redo_log_number_; }

    uint64_t prev_log_number() const { return prev_log_number_; }

    Version *current() const { return current_; }

    friend class Version;
    friend class VersionBuilder;
private:
    uint64_t last_version_ = 0;
    uint64_t next_file_number_ = 0;
    uint64_t redo_log_number_ = 0;
    uint64_t prev_log_number_ = 0;
    uint64_t manifest_file_number_ = 0;

    const std::string db_name_;
    Env *env_;
    InternalKeyComparator comparator_;

    Version version_dummy_;
    Version *current_;

    TableCache *table_cache_;

    std::unique_ptr<base::AppendFile> log_file_;
    std::unique_ptr<LogWriter> log_;
};

class VersionBuilder : public base::DisableCopyAssign {
public:
    VersionBuilder(VersionSet *versions, Version *current);
    ~VersionBuilder();

    void Apply(const VersionPatch &patch);

    Version *Build();

private:
    struct BySmallestKey {

        InternalKeyComparator comparator;

        bool operator ()(const base::Handle<FileMetadata> &a,
                         const base::Handle<FileMetadata> &b) const {
            auto rv = comparator.Compare(a->smallest_key.key_slice(),
                                         b->smallest_key.key_slice());
            if (rv != 0) {
                return rv < 0;
            } else {
                return a->number < b->number;
            }
        }
    };

    struct FileEntry {
        std::set<uint64_t> deletion;
        std::set<base::Handle<FileMetadata>, BySmallestKey> creation;
    };
    
    FileEntry levels_[kMaxLevel];
    
    VersionSet *owns_;
    base::Handle<Version> current_;
};

} // namespace lsm
    
} // namespace yukino

#endif // YUKINO_LSM_VERSION_H_