// The YukinoDB Unit Test Suite
//
//  crc32_test.cc
//
//  Created by Niko Bellic.
//
//
#include "base/crc32.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace base {

TEST(CRC32Test, Sanity) {
    CRC32 crc32(0);

    ASSERT_EQ(0U, crc32.digest());
}

TEST(CRC32Test, Bytes) {
    CRC32 crc32(0);

    crc32.Update("abcd", 4);
    crc32.Update("efgh", 4);

    auto old = crc32.digest();
    crc32.Reset();

    crc32.Update("abcdefgh", 8);
    ASSERT_EQ(old, crc32.digest());
}

} // namespace base

} // namespace yukino
