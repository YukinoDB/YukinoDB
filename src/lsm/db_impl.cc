#include "lsm/db_impl.h"
#include "yukino/write_batch.h"

namespace yukino {

namespace lsm {

DBImpl::DBImpl() {

}

base::Status DBImpl::Open(const Options &opt, const std::string &name) {
    // TODO:
    return base::Status::OK();
}

DBImpl::~DBImpl() {

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
    // TODO:
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

} // namespace lsm

} // namespace yukino