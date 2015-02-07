#ifndef YUKINO_BASE_MEM_IO_H_
#define YUKINO_BASE_MEM_IO_H_

#include "base/io.h"
#include <string>

namespace yukino {

namespace base {

class StringWriter : public Writer {
public:
    virtual Status Write(const void *data, size_t size,
                         size_t *written) override;

    virtual Status Skip(size_t count) override;

    const std::string &buf() const { return buf_; }

private:
    std::string buf_;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_MEM_IO_H_