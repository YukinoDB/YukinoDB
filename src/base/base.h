#ifndef YUKI_BASE_BASE_H
#define YUKI_BASE_BASE_H

namespace yukino {

namespace base {

class DisableCopyAssign {
public:
    DisableCopyAssign() = default;

    DisableCopyAssign(const DisableCopyAssign &) = delete;

    void operator = (const DisableCopyAssign &) = delete;

}; // class DisableCopyAssign

// clz - count leading zero
#define YK_CLZ64(n)  \
    (!(( n ) & 0xffffffff00000000ull) ? \
        32 + YK_CLZ32(( n )) : YK_CLZ32((( n ) & 0xffffffff00000000ull) >> 32))

#define YK_CLZ32(n) __builtin_clz((uint32_t)( n ))

} // namespace base

} // namespace yukinodb

#endif // YUKI_BASE_BASE_H
