#include "lsm/table_builder.h"
#include "lsm/builtin.h"
#include "base/crc32.h"
#include "glog/logging.h"
#include <unistd.h>

namespace yukino {

namespace lsm {

TableOptions::TableOptions()
    : file_version(LSM_FILE_VERSION)
    , magic_number(LSM_MAGIC_NUMBER)
    // set block size to page size(normal: 8kb)
    , block_size(static_cast<uint32_t>(sysconf(_SC_PAGESIZE)))
    , restart_interval(LSM_RESTART_INTERVAL) {
}

class TableBuilder::Core {
public:
    Core(const TableOptions &options)
        : restart_count_(options.restart_interval)
        , options_(options) {
    }

    int restart_count_;
    const TableOptions options_;
};

TableBuilder::TableBuilder(const TableOptions &options, base::Writer *writer)
    : writer_(DCHECK_NOTNULL(writer))
    , core_(new Core{options}) {
}

base::Status TableBuilder::Append(const Chunk &chunk) {

    return base::Status::OK();
}

base::Status TableBuilder::Finalize() {

    return base::Status::OK();
}

} // namespace lsm

} // namespace yukino