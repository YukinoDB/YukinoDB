#include "base/base.h"
#include "gtest/gtest.h"
#include <stdio.h>

TEST(BasicTest, Sanity) {
    ASSERT_EQ(0, 0) << "should ok!";
}