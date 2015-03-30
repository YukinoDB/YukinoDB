// The YukinoDB Unit Test Suite
//
//  table_test.cc
//
//  Created by Niko Bellic.
//
//
#include "balance/table-inl.h"
#include "balance/table.h"
#include "balance/format.h"
#include "base/mem_io.h"
#include "yukino/iterator.h"
#include "yukino/comparator.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace balance {

class BtreeTableTest : public ::testing::Test {
public:
    BtreeTableTest () {
    }

    virtual void SetUp() override {
        InternalKeyComparator comparator(BytewiseCompartor());
        table_ = new Table(comparator);
    }

    virtual void TearDown() override {
        if (table_.get()) {
            table_->Release();
        }
        io_.Reset();
    }

    static const uint32_t kPageSize = 512;

    base::Handle<Table> table_;
    base::StringIO io_;
};

TEST_F(BtreeTableTest, Sanity) {

    auto rs = table_->Create(kPageSize, Config::kBtreeFileVersion, 3, &io_);
    ASSERT_TRUE(rs.ok());

    std::string dummy;
    EXPECT_FALSE(table_->Put("aaa", 0, kFlagValue, "1", &dummy));
    EXPECT_FALSE(table_->Put("bbb", 1, kFlagValue, "2", &dummy));

    table_->Flush(true);
    EXPECT_EQ(kPageSize * 2, io_.buf().size());

    EXPECT_TRUE(table_->Get("aaa", 3, &dummy));
    EXPECT_EQ("1", dummy);

    EXPECT_TRUE(table_->Get("bbb", 3, &dummy));
    EXPECT_EQ("2", dummy);
}

TEST_F(BtreeTableTest, Iterator) {
    auto rs = table_->Create(kPageSize, Config::kBtreeFileVersion, 3, &io_);
    ASSERT_TRUE(rs.ok());

    std::string dummy;
    EXPECT_FALSE(table_->Put("a", 0, kFlagValue, "1", &dummy));
    EXPECT_FALSE(table_->Put("aa", 1, kFlagValue, "2", &dummy));
    EXPECT_FALSE(table_->Put("aaa", 2, kFlagValue, "3", &dummy));

    std::unique_ptr<Iterator> iter(table_->CreateIterator());
    iter->SeekToFirst();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("1", iter->value());

    iter->Next();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("2", iter->value());

    iter->Next();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("3", iter->value());
}

TEST_F(BtreeTableTest, Reopen) {
    auto rs = table_->Create(kPageSize, Config::kBtreeFileVersion, 3, &io_);
    ASSERT_TRUE(rs.ok());

    std::string dummy;
    EXPECT_FALSE(table_->Put("a", 0, kFlagValue, "1", &dummy));
    EXPECT_FALSE(table_->Put("aa", 1, kFlagValue, "2", &dummy));
    EXPECT_FALSE(table_->Put("aaa", 2, kFlagValue, "3", &dummy));

    InternalKeyComparator comparator(BytewiseCompartor());
    table_ = new Table(comparator);

    rs = table_->Open(&io_, io_.buf().size());
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    std::unique_ptr<Iterator> iter(table_->CreateIterator());
    iter->SeekToFirst();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("1", iter->value());

    iter->Next();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("2", iter->value());

    iter->Next();
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ("3", iter->value());
}

} // namespace balance

} // namespace yukino
