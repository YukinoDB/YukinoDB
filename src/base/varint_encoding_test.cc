// The YukinoDB Unit Test Suite
//
//  varint_test.cc
//
//  Created by Niko Bellic.
//
//

#include "base/varint_encoding.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace base {

TEST(VarintTest, Sanity) {
    char buf[Varint64::kMaxLen];

    EXPECT_EQ(1, Varint32::Encode(buf, 0));
    EXPECT_EQ(0, buf[0]);
    EXPECT_EQ(1, Varint32::Sizeof(0));

    EXPECT_EQ(1, Varint32::Encode(buf, 1));
    EXPECT_EQ(1, buf[0]);

    EXPECT_EQ(1, Varint64::Encode(buf, 0));
    EXPECT_EQ(0, buf[0]);
    EXPECT_EQ(1, Varint64::Sizeof(0));

    EXPECT_EQ(1, Varint64::Encode(buf, 1));
    EXPECT_EQ(1, buf[0]);
}

class Varint32Test : public testing::TestWithParam<uint32_t> {
};

INSTANTIATE_TEST_CASE_P(Encoding, Varint32Test,
                        ::testing::Values(1, 1 << 7, 1<<14, 1<<21, 1<<28));

TEST_P(Varint32Test, Encoding) {
    char buf[Varint32::kMaxLen];

    size_t encode_len =Varint32::Encode(buf, GetParam());
    EXPECT_LE(1, encode_len);
    EXPECT_EQ(Varint32::Sizeof(GetParam()), encode_len);

    size_t len = 0;
    EXPECT_EQ(GetParam(), Varint32::Decode(buf, &len));
    EXPECT_EQ(encode_len, len);
}

class Varint64Test : public ::testing::TestWithParam<uint64_t> {
};

INSTANTIATE_TEST_CASE_P(Encoding, Varint64Test,
                        ::testing::Values(1, 1 << 7, 1<<14, 1<<21, 1<<28,
                                          1L<<35, 1L<<42, 1L<<49, 1L<<58));

TEST_P(Varint64Test, Encoding) {
    char buf[Varint64::kMaxLen];

    size_t encode_len =Varint64::Encode(buf, GetParam());
    EXPECT_LE(1, encode_len);
    EXPECT_EQ(Varint64::Sizeof(GetParam()), encode_len);

    size_t len = 0;
    EXPECT_EQ(GetParam(), Varint64::Decode(buf, &len));
    EXPECT_EQ(encode_len, len);
}

} // namespace base

} // namespace yukino
