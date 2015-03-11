// The YukinoDB Unit Test Suite
//
//  log_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/log.h"
#include "base/mem_io.h"
#include "base/io.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace lsm {

class LogTest : public ::testing::Test {
public:
    LogTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }

    static const size_t kBlockSize = 32;

};

TEST_F(LogTest, Sanity) {

    base::StringWriter buf;

    Log::Writer log(&buf, kBlockSize);

    log.Append("aaaa");
    log.Append("bbbb");

    Log::Reader rd(buf.buf().data(), buf.buf().size(), true, kBlockSize);

    base::Slice slice;
    EXPECT_TRUE(rd.Read(&slice));
    EXPECT_EQ("aaaa", slice.ToString());

    EXPECT_TRUE(rd.Read(&slice));
    EXPECT_EQ("bbbb", slice.ToString());

    EXPECT_FALSE(rd.Read(&slice));

}

} // namespace lsm

} // namespace yukino
