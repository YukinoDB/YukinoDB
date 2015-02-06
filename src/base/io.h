#ifndef YUKINO_BASE_IO_H_
#define YUKINO_BASE_IO_H_

#include "base/base.h"
#include "base/status.h"
#include "base/slice.h"

namespace yukino {

namespace base {

/**
 * The sequence wirter.
 *
 */
class Writer : public DisableCopyAssign {
public:
    Writer() {}

    virtual ~Writer() {}

    virtual Status Write(const Slice &buf, size_t *written) = 0;

    virtual Status Skip(size_t count) = 0;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_IO_H_