// The YukinoDB Unit Test Suite
//
//  bloom_filter_test.cc
//
//  Created by Niko Bellic.
//
//
#include "util/bloom_filter.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace util {

TEST(BloomFilterTest, Sanity) {
    Bitmap<> bits(128);

    for (auto i = 0; i < bits.num_bits(); ++i) {
        EXPECT_FALSE(bits.test(i));
        bits.set(i);
        EXPECT_TRUE(bits.test(i));
        bits.unset(i);
        EXPECT_FALSE(bits.test(i));
    }
}

TEST(BloomFilterTest, Filter) {
    static const char *kDummies[] = {
        "",
        "a",
        "b",
        "aaaaa",
        "cc",
    };

    BloomFilter<> filter(1024);

    for (auto dummy : kDummies) {
        filter.Offer(dummy);
    }
    for (auto dummy : kDummies) {
        EXPECT_TRUE(filter.Test(dummy));
    }

    EXPECT_FALSE(filter.Test("dd"));
    EXPECT_FALSE(filter.Test("ab"));
}

} // namespace util

} // namespace yukino
