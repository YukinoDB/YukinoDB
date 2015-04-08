#include "balance/db_impl.h"
#include "glog/logging.h"

#if defined(CHECK_OK)
#   undef CHECK_OK
#else
#   define CHECK_OK(expr) rs = (expr); if (!rs.ok()) return rs
#endif

namespace yukino {

namespace balance {

DBImpl::DBImpl(const Options &options, const std::string &name)
    : options_(options)
    , name_(name)
    , env_(DCHECK_NOTNULL(options.env))
    , comparator_(options_.comparator) {
}

DBImpl::~DBImpl() {
}

base::Status DBImpl::Open() {
    base::Status rs;

    if (options_.block_size < Config::kMinPageSize ||
        options_.block_size > Config::kMaxPageSize) {
        return base::Status::InvalidArgument("block_size out of range");
    }
    return rs;
}

//==========================================================================
// Override methods:
//==========================================================================
base::Status DBImpl::Put(const WriteOptions& options,
                         const base::Slice& key,
                         const base::Slice& value) {
    base::Status rs;
    // TODO:
    return rs;
}

base::Status DBImpl::Delete(const WriteOptions& options,
                            const base::Slice& key) {
    base::Status rs;
    // TODO:
    return rs;
}

base::Status DBImpl::Write(const WriteOptions& options,
                           WriteBatch* updates) {
    base::Status rs;
    // TODO:
    return rs;
}

base::Status DBImpl::Get(const ReadOptions& options,
                         const base::Slice& key, std::string* value) {
    base::Status rs;
    // TODO:
    return rs;
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
    // TODO:
}

} // namespace balance
    
} // namespace yukino