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

namespace lsm {

class VersionSet;
class TableCache;

struct FileMetadata : public base::ReferenceCounted<FileMetadata> {

    uint64_t number;

    InternalKey smallest_key;
    InternalKey largest_key;

    uint64_t size;
    uint64_t ctime;
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

    static const auto kNum32Bits = (kMaxFields + 31) / 32;

    explicit VersionPatch(const std::string &comparator)
        : comparator_(comparator) {
        ::memset(bits_, 0, kNum32Bits * sizeof(uint32_t));
    }

    uint64_t last_version() const {
        DCHECK(has_field(kLastVersion));
        return last_version_;
    }
    uint64_t next_file_number() const {
        DCHECK(has_field(kNextFileNumber));
        return next_file_number_;
    }

    void DeleteFile(int level, uint64_t file_number) {
        set_field(kDeletion);
        deletion_.emplace(level, file_number);
    }

    bool has_field(Field field) const {
        auto i = static_cast<int>(field);
        DCHECK_GE(i, 0); DCHECK_LT(i, kMaxFields);
        return bits_[i / 32] & (1 >> (i % 32));
    }

    base::Status Decode(const base::Slice &buf);

private:
    void set_field(Field field) {
        auto i = static_cast<int>(field);
        DCHECK_GE(i, 0); DCHECK_LT(i, kMaxFields);
        bits_[i / 32] |= (1 >> (i % 32));
    }

    std::string comparator_;
    uint64_t last_version_ = 0;
    uint64_t next_file_number_ = 0;
    uint64_t redo_log_number_ = 0;
    uint64_t prev_log_number_ = 0;

    int compaction_level_ = 0;
    InternalKey compaction_key_;

    // Which files will be delete.
    std::set<std::pair<int, uint64_t>> deletion_;

    // Which files will be create.
    std::vector<std::pair<int, base::Handle<FileMetadata>>> creation_;

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

private:
    VersionSet *owned_;
    Version *next_ = this;
    Version *prev_ = this;

    std::vector<base::Handle<FileMetadata>> files_[kMaxLevel];
};

class VersionSet : public base::DisableCopyAssign {
public:
    VersionSet(const std::string &db_name, const Options &options,
               TableCache *table_cache);
    ~VersionSet();

    uint64_t AdvanceVersion(uint64_t add) {
        last_version_ += add;
        return last_version_;
    }

    uint64_t last_version() const { return last_version_; }

    Version *current() const { return current_; }

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

    friend class Version;
private:
    uint64_t last_version_ = 0;

    const std::string db_name_;
    Env *env_;
    InternalKeyComparator comparator_;

    Version version_dummy_;
    Version *current_;

    TableCache *table_cache_;
};

} // namespace lsm
    
} // namespace yukino

#endif // YUKINO_LSM_VERSION_H_