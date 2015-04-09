// The YukinoDB Unit Test Suite
//
//  format_test.cc
//
//  Created by Niko Bellic.
//
//
#include "balance/format.h"
#include "util/area-inl.h"
#include "util/area.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace balance {

class FormatTest : public ::testing::Test {
public:
    FormatTest ()
        : area_(4 * base::kKB) {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
        area_.Purge();
    }

    util::Area area_;
};

TEST_F(FormatTest, InternalKey) {
    auto k = InternalKey::Pack("aaa", 1, kFlagValue, "bbb", &area_);
    ASSERT_NE(nullptr, k);

    auto parsed = InternalKey::Parse(k);
    EXPECT_EQ("aaa", parsed.user_key.ToString());
    EXPECT_EQ("bbb", parsed.value.ToString());
    EXPECT_EQ(1, parsed.tx_id);
    auto flag = kFlagValue;
    EXPECT_EQ(flag, parsed.flag);
}

} // namespace balance

} // namespace yukino
