#include "yukino/db.h"
#include "base/slice.h"

namespace yukino {

/*static*/ base::Status DB::Open(const Options& options, const std::string& name,
                           DB** dbptr) {
// TODO:
    return base::Status::NotSupported("//TODO");
}

/*virtual*/ DB::~DB() {
}

} // namespace yukino
