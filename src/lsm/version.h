#ifndef YUKINO_LSM_VERSION_H_
#define YUKINO_LSM_VERSION_H_

#include "lsm/chunk.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "base/ref_counted.h"
#include "base/status.h"
#include <stdint.h>
#include <string>
#include <vector>

namespace yukino {

class Env;
class Options;
class ReadOptions;

namespace lsm {

class VersionSet;
struct FileMetadata;
class TableCache;

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

    const std::vector<FileMetadata*> &file(size_t i) const {
        return files_[i];
    }

    std::vector<FileMetadata*> *mutable_file(size_t i) {
        return &files_[i];
    }

private:
    VersionSet *owned_;
    Version *next_ = this;
    Version *prev_ = this;

    std::vector<FileMetadata*> files_[kMaxLevel];
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

struct FileMetadata {
    uint64_t number;

    InternalKey smallest_key;
    InternalKey largest_key;

    uint64_t size;
    uint64_t ctime;
};

} // namespace lsm
    
} // namespace yukino

#endif // YUKINO_LSM_VERSION_H_