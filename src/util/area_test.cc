// The YukinoDB Unit Test Suite
//
//  area_test.cc
//
//  Created by Niko Bellic.
//
//
#include "util/area-inl.h"
#include "util/area.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace util {

class AreaTest : public ::testing::Test {
public:
    AreaTest ()
        : area_(kPageSize) {
    }

    virtual void SetUp() override {
        EXPECT_EQ(0, area_.ApproximateMemoryUsage());
    }

    virtual void TearDown() override {
        area_.Purge();
    }

    static const size_t kPageSize = 4 * base::kKB;
    static const int kBench = 1000000;

    Area area_;
};

TEST_F(AreaTest, Sanity) {
    EXPECT_EQ(0, area_.ApproximateMemoryUsage());
    
    auto p = area_.Allocate(1);
    EXPECT_EQ(area_.page_size(), area_.ApproximateMemoryUsage());

    area_.Free(p);
    EXPECT_EQ(0, area_.ApproximateMemoryUsage());

    EXPECT_EQ(nullptr, area_.Allocate(0));
    area_.Free(nullptr);
}

TEST_F(AreaTest, MutilPageAllocation) {
    std::vector<const void*> collected;
    auto k = area_.page_size() / area_.segment_chunk_size(1);
    for (auto i = 0; i < k; ++i) {
        auto p = area_.Allocate(1);
        EXPECT_NE(nullptr, p);
        collected.push_back(p);
    }
    EXPECT_EQ(area_.page_size() * 2, area_.ApproximateMemoryUsage());

    for (auto p : collected) {
        area_.Free(p);
    }
    EXPECT_EQ(0, area_.ApproximateMemoryUsage());
}

TEST_F(AreaTest, LargePageAllocation) {
    auto large_size = area_.segment_chunk_size(Area::kNumSegments - 1) * 2;

    auto p = area_.Allocate(large_size);
    EXPECT_NE(nullptr, p);

    EXPECT_LE(large_size, area_.ApproximateMemoryUsage());

    area_.Free(p);
    EXPECT_EQ(0, area_.ApproximateMemoryUsage());
}

// 303 ms
// 160 ms
TEST_F(AreaTest, AreaBenchmark) {
    size_t slots[Area::kNumSegments] = {
        area_.segment_chunk_size(Area::kNumSegments - 1) * 2,
        area_.segment_chunk_size(1),
        area_.segment_chunk_size(2),
        area_.segment_chunk_size(3),
        area_.segment_chunk_size(4),
        area_.segment_chunk_size(5),
    };

    for (auto size : slots) {
        area_.Allocate(size);
    }

    for (auto i = 0; i < kBench; ++i) {
        auto p = area_.Allocate(slots[i % Area::kNumSegments]);
        area_.Free(p);
    }
}

} // namespace util

} // namespace yukino
