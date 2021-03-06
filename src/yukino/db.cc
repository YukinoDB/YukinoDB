#include "yukino/db.h"
#include "lsm/db_impl.h"
#include "balance/db_impl.h"
#include "yukino/options.h"
#include "base/slice.h"
#include <memory>

namespace yukino {

/*static*/ base::Status DB::Open(const Options& options, const std::string& name,
                           DB** dbptr) {
    if (::strcmp(options.engine_name, lsm::DBImpl::kName) == 0) {
        std::unique_ptr<lsm::DBImpl> db(new lsm::DBImpl(options, name));

        auto rs = db->Open(options);
        if (!rs.ok()) {
            return rs;
        }

        *dbptr = db.release();
        return rs;
    } else if (::strcmp(options.engine_name, balance::DBImpl::kName) == 0) {
        std::unique_ptr<balance::DBImpl> db(new balance::DBImpl(options, name));

        auto rs = db->Open();
        if (!rs.ok()) {
            return rs;
        }

        *dbptr = db.release();
        return rs;
    } else {
        return base::Status::NotSupported("not supported engine!");
    }
}

/*virtual*/ DB::~DB() {
}

/*virtual*/ Snapshot::~Snapshot() {
}

// auto tx = db->BeginTransaction(write_options);
//     writing...
// db->Commit(tx);
// or
// db->Rollback(tx);
//
/*virtual*/ Transaction::~Transaction() {
}

} // namespace yukino
