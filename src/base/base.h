#ifndef YUKI_BASE_BASE_H
#define YUKI_BASE_BASE_H

#include <utility>
#include <string>

namespace yukino {

namespace base {

class DisableCopyAssign {
public:
    DisableCopyAssign() = default;
    DisableCopyAssign(const DisableCopyAssign &) = delete;
    void operator = (const DisableCopyAssign &) = delete;

}; // class DisableCopyAssign

template<class Callback>
class AtScope : public DisableCopyAssign {
public:
    AtScope(Callback callback, int) : callback_(callback) {}
    AtScope(AtScope &&other) : callback_(std::move(callback_)) {}
    ~AtScope() { callback_(); }

private:
    Callback callback_;
};

template<class Callback>
inline AtScope<Callback> Defer(Callback &&callback) {
    return AtScope<Callback>( callback, 0 );
}

const static size_t kKB = 1024;
const static size_t kMB = 1024 * kKB;
const static size_t kGB = 1024 * kMB;
const static size_t kTB = 1024 * kGB;

struct Strings {

    __attribute__ (( __format__ (__printf__, 1, 2)))
    static std::string Sprintf(const char *fmt, ...);

    static std::string Vsprintf(const char *fmt, va_list ap);

    static const size_t kInitialSize = 128;

    Strings() = delete;
    ~Strings() = delete;
};

// clz - count leading zero
#define YK_CLZ64(n)  \
    (!(( n ) & 0xffffffff00000000ull) ? \
        32 + YK_CLZ32(( n )) : YK_CLZ32((( n ) & 0xffffffff00000000ull) >> 32))

#define YK_CLZ32(n) __builtin_clz((uint32_t)( n ))

// Fast find first zero, right to left
// Base on binary searching
inline int FindFirstZero32(uint32_t x) {
    static const int zval[] = {
        0, /* 0 */ 1, /* 1 */ 0, /* 2 */ 2, /* 3 */
        0, /* 4 */ 1, /* 5 */ 0, /* 6 */ 3, /* 7 */
        0, /* 8 */ 1, /* 9 */ 0, /* a */ 2, /* b */
        0, /* c */ 1, /* d */ 0, /* e */ 4, /* f */
    };

    int base = 0;
    if ((x & 0xffff) == 0xffffu) {
        base += 16;
        x >>= 16;
    }
    if ((x & 0xff) == 0xffu) {
        base += 8;
        x >>= 8;
    }
    if ((x & 0xf) == 0xfu) {
        base += 4;
        x >>= 4;
    }
    return base + zval[x & 0xfu];
}

} // namespace base

} // namespace yukinodb

#endif // YUKI_BASE_BASE_H
