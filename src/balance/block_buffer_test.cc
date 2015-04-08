// The YukinoDB Unit Test Suite
//
//  block_buffer_test.cc
//
//  Created by Niko Bellic.
//
//
#include "balance/block_buffer-inl.h"
#include "balance/block_buffer.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace balance {

TEST(BlockBufferTest, Sanity) {
    BlockBuffer buf(32);

    std::string dummy(32 - Block::kHeaderSize, '1');
    auto addr = buf.Append(dummy);
    EXPECT_EQ(0, addr);

    dummy.assign(32 - Block::kHeaderSize, '2');
    addr = buf.Append(dummy);
    EXPECT_EQ(33, addr);
}

TEST(BlockBufferTest, BlockAlign) {
    BlockBuffer buf(32);

    std::string dummy(32 - Block::kHeaderSize - base::Varint32::kMaxLen, '1');
    EXPECT_EQ(0, buf.Append(dummy));

    dummy.assign(16, '2');
    EXPECT_EQ(32, buf.Append(dummy));
}

TEST(BlockBufferTest, Reading) {
    BlockBuffer buf(32);

    std::string dummy_1(32 - Block::kHeaderSize, '1');
    auto addr_1 = buf.Append(dummy_1);
    EXPECT_EQ(0, addr_1);

    std::string dummy_2(32 - Block::kHeaderSize, '2');
    auto addr_2 = buf.Append(dummy_2);
    EXPECT_EQ(33, addr_2);

    EXPECT_EQ(dummy_2, buf.Read(addr_2).ToString());
    EXPECT_EQ(dummy_1, buf.Read(addr_1).ToString());
}

} // namespace balance

} // namespace yukino
