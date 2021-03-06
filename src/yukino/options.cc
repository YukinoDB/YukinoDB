#include "yukino/options.h"
#include "yukino/comparator.h"
#include "yukino/env.h"

namespace yukino {

Options::Options()
    : engine_name(nullptr)
    , comparator(BytewiseCompartor())
    , create_if_missing(false)
    , error_if_exists(false)
    , env(Env::Default())
    , write_buffer_size(4 * base::kMB)
    , block_size(4 * base::kKB)
    , block_restart_interval(16)
    , max_open_files(1000) {
}

ReadOptions::ReadOptions()
    : verify_checksums(true)
    , fill_cache(true)
    , snapshot(nullptr) {
}

WriteOptions::WriteOptions()
    : sync(false) {
}

} // namespace yukino
