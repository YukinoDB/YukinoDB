#ifndef YUKINO_LSM_TABLE_BUILDER_H_
#define YUKINO_LSM_TABLE_BUILDER_H_

#include "base/status.h"
#include "base/base.h"
#include <stdint.h>
#include <memory>

namespace yukino {

namespace base {

class Writer;

} // namespace base

namespace lsm {

class Chunk;

struct TableOptions {
    TableOptions();
    TableOptions(const TableOptions &) = default;

    uint32_t file_version;
    uint32_t magic_number;
    uint32_t block_size;
    int restart_interval;
};

class TableBuilder : public base::DisableCopyAssign {
public:
    TableBuilder(const TableOptions &options, base::Writer *writer);
    ~TableBuilder();

    base::Status Append(const Chunk &chunk);

    base::Status Finalize();

private:
    class Core;

    std::unique_ptr<Core> core_;
    base::Writer *writer_;
};

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_TABLE_BUILDER_H_