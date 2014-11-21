#include <stdio.h>
#include "gtest/gtest.h"

TEST(BasicTest, Sanity) {
    ASSERT_EQ(0, 0) << "should ok!";
}