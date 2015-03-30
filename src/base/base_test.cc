#include "base/base.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace base {

TEST(BaseTest, Sanity) {
    ASSERT_EQ(0, 0) << "should ok!";
}

TEST(BaseTest, FindFirstZero) {
    EXPECT_EQ(0, FindFirstZero32(0));
    EXPECT_EQ(1, FindFirstZero32(1));
    EXPECT_EQ(2, FindFirstZero32(3));

    EXPECT_EQ(32, FindFirstZero32(0xffffffffu));
    EXPECT_EQ(4, FindFirstZero32(0xffffffefu));
    EXPECT_EQ(12, FindFirstZero32(0xffffefffu));
    EXPECT_EQ(24, FindFirstZero32(0xfeffffffu));
}

} // namespace base

} // namespace yukino


