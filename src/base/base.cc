#include "base/base.h"
#include <stdarg.h>
#include <memory>

namespace yukino {

namespace base {


/*static*/ std::string Strings::Vsprintf(const char *fmt, va_list ap) {
    va_list copied;
    int len = kInitialSize, rv = len;
    std::unique_ptr<char[]> buf;
    do {
        len = rv + kInitialSize;
        buf.reset(new char[len]);
        va_copy(copied, ap);
        rv = vsnprintf(buf.get(), len, fmt, ap);
        va_copy(ap, copied);
    } while (rv > len);
    return std::string(buf.get());
}

/*static*/ std::string Strings::Sprintf(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::string str(Vsprintf(fmt, ap));
    va_end(ap);
    return str;
}

} // namespace base

} // namespace yukino