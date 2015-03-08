#ifndef YUKI_BASE_BASE_H
#define YUKI_BASE_BASE_H

#include <utility>

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

// clz - count leading zero
#define YK_CLZ64(n)  \
    (!(( n ) & 0xffffffff00000000ull) ? \
        32 + YK_CLZ32(( n )) : YK_CLZ32((( n ) & 0xffffffff00000000ull) >> 32))

#define YK_CLZ32(n) __builtin_clz((uint32_t)( n ))

} // namespace base

} // namespace yukinodb

#endif // YUKI_BASE_BASE_H
