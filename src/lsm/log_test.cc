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
        writer_ = new base::StringWriter();
    }

    virtual void TearDown() override {
        delete writer_;
    }

    const std::string &buf() const { return writer_->buf(); }

    static const size_t kBlockSize = 32;

    base::StringWriter *writer_;
    std::string scratch_;
};

TEST_F(LogTest, Sanity) {

    Log::Writer log(writer_, kBlockSize);

    log.Append("aaaa");
    log.Append("bbbb");

    Log::Reader rd(buf().data(), buf().size(), true, kBlockSize);

    base::Slice slice;
    EXPECT_TRUE(rd.Read(&slice, &scratch_));
    EXPECT_TRUE(rd.status().ok());
    EXPECT_EQ("aaaa", slice.ToString());

    EXPECT_TRUE(rd.Read(&slice, &scratch_));
    EXPECT_TRUE(rd.status().ok());
    EXPECT_EQ("bbbb", slice.ToString());

    EXPECT_FALSE(rd.Read(&slice, &scratch_));
}

TEST_F(LogTest, LargeRecord) {
    std::string record1(kBlockSize, '0');
    std::string record2(kBlockSize, '1');

    Log::Writer log(writer_, kBlockSize);

    log.Append(record1);
    log.Append(record2);

    Log::Reader rd(buf().data(), buf().size(), true, kBlockSize);

    base::Slice slice;
    EXPECT_TRUE(rd.Read(&slice, &scratch_));
    EXPECT_TRUE(rd.status().ok());
    EXPECT_EQ(record1, slice.ToString());

    EXPECT_TRUE(rd.Read(&slice, &scratch_));
    EXPECT_TRUE(rd.status().ok());
    EXPECT_EQ(record2, slice.ToString());

    EXPECT_FALSE(rd.Read(&slice, &scratch_));
}

TEST_F(LogTest, BlockFilling) {
    std::string record(kBlockSize / 2, 'a');

    Log::Writer log(writer_, kBlockSize);
    auto i = 5;
    while (i--) {
        log.Append(record);
    }

    Log::Reader rd(buf().data(), buf().size(), true, kBlockSize);
    base::Slice slice;
    i = 5;
    while (i--) {
        EXPECT_TRUE(rd.Read(&slice, &scratch_));
        EXPECT_TRUE(rd.status().ok());
        EXPECT_EQ(record, slice.ToString());
    }

    EXPECT_FALSE(rd.Read(&slice, &scratch_));
}

} // namespace lsm

} // namespace yukino
