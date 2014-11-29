#include "yukinodb/db.h"
#include "yukinodb/slice.h"

namespace yukinodb {

/*static*/ Status DB::Open(const Options& options, const std::string& name,
                           DB** dbptr) {
    // TODO:
    return Status::NotSupported("//TODO");
}

/*virtual*/ DB::~DB() {
}

} // namespace yukinodb