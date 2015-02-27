// The YukinoDB Unit Test Suite
//
//  memory_table_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/memory_table.h"
#include "lsm/builtin.h"
#include "yukino/iterator.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace lsm {

class MemoryTableTest : public ::testing::Test {
public:
    MemoryTableTest () {
    }

    virtual void SetUp() override {
        std::unique_ptr<MemoryTable> table(
                   new MemoryTable(InternalKeyComparator(BytewiseCompartor())));
        table_ = std::move(table);
    }

    virtual void TearDown() override {
        table_.release();
    }

    std::unique_ptr<MemoryTable> table_;
};

TEST_F(MemoryTableTest, Sanity) {
    table_->Put("aaa", "1", 1, kFlagValue);
    table_->Put("aaa", "2", 2, kFlagDeletion);
    table_->Put("aaa", "3", 3, kFlagValue);

    std::string value;
    auto rs = table_->Get("aaa", 9, &value);

    EXPECT_TRUE(rs.ok());
    EXPECT_EQ("3", value);

    rs = table_->Get("aaa", 1, &value);
    EXPECT_TRUE(rs.ok());
    EXPECT_EQ("1", value);

    rs = table_->Get("aaa", 2, &value);
    EXPECT_FALSE(rs.ok());
    EXPECT_TRUE(rs.IsNotFound());
}

TEST_F(MemoryTableTest, Sequence) {
    table_->Put("aaa", "1", 1, kFlagValue);
    table_->Put("aaa", "2", 2, kFlagValue);
    table_->Put("aaa", "3", 3, kFlagValue);

    std::unique_ptr<Iterator> iter(table_->NewIterator());

    iter->SeekToFirst();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("3", iter->value().ToString());

    iter->Next();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("2", iter->value().ToString());

    iter->Next();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("1", iter->value().ToString());
}

} // namespace lsm

} // namespace yukino
