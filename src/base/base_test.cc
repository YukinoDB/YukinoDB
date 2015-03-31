#include "base/mem_io.h"
#include "base/base.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace base {

TEST(BaseTest, Sanity) {
    ASSERT_EQ(0, 0) << "should ok!";
}

TEST(BaseTest, FindFirstZero) {
    EXPECT_EQ(0, Bits::FindFirstZero32(0));
    EXPECT_EQ(1, Bits::FindFirstZero32(1));
    EXPECT_EQ(2, Bits::FindFirstZero32(3));

    EXPECT_EQ(32, Bits::FindFirstZero32(0xffffffffu));
    EXPECT_EQ(4, Bits::FindFirstZero32(0xffffffefu));
    EXPECT_EQ(12, Bits::FindFirstZero32(0xffffefffu));
    EXPECT_EQ(24, Bits::FindFirstZero32(0xfeffffffu));
}

TEST(BaseTest, StringIO) {
    StringIO io;

    io.WriteVarint32(199, nullptr);
    io.WriteVarint32(0xffffffff, nullptr);
    io.WriteString("aaaa", nullptr);

    io.Seek(0);

    uint32_t dummy = 0;
    ASSERT_TRUE(io.ReadVarint32(&dummy, nullptr).ok());
    EXPECT_EQ(199, dummy);

    ASSERT_TRUE(io.ReadVarint32(&dummy, nullptr).ok());
    EXPECT_EQ(0xffffffff, dummy);

    std::string buf;
    ASSERT_TRUE(io.ReadString(&buf).ok());
    EXPECT_EQ("aaaa", buf);
}

TEST(BaseTest, StringIOVarint) {
    StringIO io;

    io.WriteFixed32(1);
    io.WriteFixed32(2);
    io.WriteVarint32(512, nullptr);
    io.Seek(0);

    uint32_t dummy = 0;
    io.ReadFixed32(&dummy);
    EXPECT_EQ(1, dummy);
    io.ReadFixed32(&dummy);
    EXPECT_EQ(2, dummy);
    io.ReadVarint32(&dummy, nullptr);
    EXPECT_EQ(512, dummy);
}

} // namespace base

} // namespace yukino


