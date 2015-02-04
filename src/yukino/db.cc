#include "yukino/db.h"
#include "yukino/slice.h"

namespace yukino {

/*static*/ Status DB::Open(const Options& options, const std::string& name,
                           DB** dbptr) {
    // TODO:
    return Status::NotSupported("//TODO");
}

/*virtual*/ DB::~DB() {
}

} // namespace yukino
