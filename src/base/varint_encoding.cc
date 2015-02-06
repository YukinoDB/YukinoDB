#include "base/varint_encoding.h"

namespace yukino {

namespace base {

#define TEST_OR_SET(c) \
    b = ((in & 0x7FU << (c*7)) >> (c*7))

#define FILL_AND_TEST_OR_SET(c) \
    out[i++] = (b | 0x80); \
    TEST_OR_SET(c)

/*static*/ size_t Varint32::Encode(void *buf, uint32_t in) {
    size_t i = 0;
    uint8_t b;
    auto out = static_cast<uint8_t *>(buf);

    if ((b = ((in & 0xFU << 28) >> 28)) != 0) // 29~32
        goto bit_28_k5;
    if ((TEST_OR_SET(3)) != 0) // 22~28
        goto bit_21_k6;
    if ((TEST_OR_SET(2)) != 0) // 15~21
        goto bit_14_k7;
    if ((TEST_OR_SET(1)) != 0) // 8~14
        goto bit_07_k8;
    TEST_OR_SET(0);
    out[i++] = b;
    return i;
bit_28_k5:
    FILL_AND_TEST_OR_SET(3);
bit_21_k6:
    FILL_AND_TEST_OR_SET(2);
bit_14_k7:
    FILL_AND_TEST_OR_SET(1);
bit_07_k8:
    FILL_AND_TEST_OR_SET(0);
    out[i++] = b;
    return i;
}
#undef FILL_AND_TEST_OR_SET
#undef TEST_OR_SET

#define TEST_OR_SET(c) \
    b = ((in & 0x7fULL << (c*7)) >> (c*7))

#define FILL_AND_TEST_OR_SET(c) \
    out[i++] = (b | 0x80); \
    TEST_OR_SET(c)

/*static*/ size_t Varint64::Encode(void *buf, uint64_t in) {
    size_t i = 0;
    uint8_t b;
    auto out = static_cast<uint8_t *>(buf);
    //       [+---+---+---+---]
    if (in & 0xFFFE000000000000ULL) {
        if ((b = ((in & 0x1ULL << 63) >> 63)) != 0) // 64
            goto bit_63_k0;
        if ((TEST_OR_SET(8)) != 0) // 58~63
            goto bit_56_k1;
        if ((TEST_OR_SET(7)) != 0) // 50~57
            goto bit_49_k2;
    }
    //      [-+---+---+---]
    if (in & 0x1FFFFF0000000ULL) {
        if ((TEST_OR_SET(6)) != 0) // 43~49
            goto bit_42_k3;
        if ((TEST_OR_SET(5)) != 0) // 36~42
            goto bit_35_k4;
        if ((TEST_OR_SET(4)) != 0) // 29~35
            goto bit_28_k5;
    }
    //       [---+---]
    if (in & 0xFFFFF80ULL) {
        if ((TEST_OR_SET(3)) != 0) // 22~28
            goto bit_21_k6;
        if ((TEST_OR_SET(2)) != 0) // 15~21
            goto bit_14_k7;
        if ((TEST_OR_SET(1)) != 0) // 8~14
            goto bit_07_k8;
    }
    TEST_OR_SET(0);
    out[i++] = b;
    return i;
bit_63_k0:
    FILL_AND_TEST_OR_SET(8);
bit_56_k1:
    FILL_AND_TEST_OR_SET(7);
bit_49_k2:
    FILL_AND_TEST_OR_SET(6);
bit_42_k3:
    FILL_AND_TEST_OR_SET(5);
bit_35_k4:
    FILL_AND_TEST_OR_SET(4);
bit_28_k5:
    FILL_AND_TEST_OR_SET(3);
bit_21_k6:
    FILL_AND_TEST_OR_SET(2);
bit_14_k7:
    FILL_AND_TEST_OR_SET(1);
bit_07_k8:
    FILL_AND_TEST_OR_SET(0);
    out[i++] = b;
    return i;
}
#undef FILL_AND_TEST_OR_SET
#undef TEST_OR_SET
    
/*static*/ uint64_t Varint64::Decode(const void *buf, size_t *len) {
    size_t i = 0;
    uint8_t b;
    uint64_t out = 0;
    auto in = static_cast<const uint8_t *>(buf);
    while ((b = in[i++]) >= 0x80) {
        out |= (b & 0x7f);
        out <<= 7;
    }
    out |= b; // last 7bit
    *len = i;
    return out;
}


} // namespace base

} // namespace yukino