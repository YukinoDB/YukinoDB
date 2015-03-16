#include "yukino/read_options.h"

namespace yukino {

ReadOptions::ReadOptions()
    : verify_checksums(true)
    , fill_cache(true)
    , snapshot(nullptr) {
}

} // namespace yukino
