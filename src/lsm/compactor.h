#ifndef YUKINO_LSM_COMPACTOR_H_
#define YUKINO_LSM_COMPACTOR_H_

#include "lsm/format.h"
#include "base/status.h"
#include "base/base.h"

namespace yukino {

class Iterator;
class Comparator;

namespace lsm {

class TableBuilder;

class Compactor : public base::DisableCopyAssign {
public:

    Compactor(InternalKeyComparator comparator);

    base::Status Compact(Iterator **children, size_t n, TableBuilder *builder);

private:
    InternalKeyComparator comparator_;
};

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_COMPACTOR_H_