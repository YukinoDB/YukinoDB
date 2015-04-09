// The YukinoDB Unit Test Suite
//
//  format_test.cc
//
//  Created by Niko Bellic.
//
//
#include "balance/format.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace balance {

class FormatTest : public ::testing::Test {
public:
    FormatTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }
};

TEST_F(FormatTest, InternalKey) {
    std::unique_ptr<const char[]> k(InternalKey::Pack("aaa", 1, kFlagValue, "bbb"));
    ASSERT_NE(nullptr, k);

    auto parsed = InternalKey::Parse(k.get());
    EXPECT_EQ("aaa", parsed.user_key.ToString());
    EXPECT_EQ("bbb", parsed.value.ToString());
    EXPECT_EQ(1, parsed.tx_id);
    auto flag = kFlagValue;
    EXPECT_EQ(flag, parsed.flag);
}

} // namespace balance

} // namespace yukino
