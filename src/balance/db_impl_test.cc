// The YukinoDB Unit Test Suite
//
//  db_impl_test.cc
//
//  Created by Niko Bellic.
//
//
#include "balance/db_impl.h"
#include "yukino/options.h"
#include "yukino/env.h"
#include "yukino/write_batch.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace balance {

class BalanceDBImplTest : public ::testing::Test {
public:
    BalanceDBImplTest () {
    }

    virtual void SetUp() override {
        options_.create_if_missing = true;

        db_ = new DBImpl(options_, kDBName);
        auto rs = db_->Open();
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    virtual void TearDown() override {
        delete db_;
        db_ = nullptr;

        Env::Default()->DeleteFile(kDBName, true);
    }

    DBImpl *db_ = nullptr;
    Options options_;
    static constexpr const char *kDBName = "test";
};

TEST_F(BalanceDBImplTest, Sanity) {
    auto rs = db_->Put(WriteOptions(), "aaa", "1");
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    rs = db_->Put(WriteOptions(), "aab", "2");
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    rs = db_->Put(WriteOptions(), "aac", "3");
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    std::string dummy;
    rs = db_->Get(ReadOptions(), "aaa", &dummy);
    ASSERT_TRUE(rs.ok()) << rs.ToString();
    ASSERT_EQ("1", dummy);

    rs = db_->Get(ReadOptions(), "aab", &dummy);
    ASSERT_TRUE(rs.ok()) << rs.ToString();
    ASSERT_EQ("2", dummy);

    rs = db_->Get(ReadOptions(), "aac", &dummy);
    ASSERT_TRUE(rs.ok()) << rs.ToString();
    ASSERT_EQ("3", dummy);
}

TEST_F(BalanceDBImplTest, Checkpoint) {
    static const char *keys[] = {
        "a",
        "aa",
        "aaa",
        "aaaa",
        "aaaaa",
    };

    for (auto key : keys) {
        auto rs = db_->Put(WriteOptions(), key, "1");
    }

    db_->ScheduleCheckpoint();
    db_->TEST_WaitForCheckpoint();
}

TEST_F(BalanceDBImplTest, Recover) {
    static const char *keys[] = {
        "b",
        "bb",
        "bbb",
        "bbbb",
        "bbbbb",
    };

    for (auto key : keys) {
        auto rs = db_->Put(WriteOptions(), key, "0");
    }

    delete db_;

    options_.create_if_missing = false;

    db_ = new DBImpl(options_, kDBName);
    auto rs = db_->Open();
    ASSERT_TRUE(rs.ok()) << rs.ToString();
}

} // namespace balance

} // namespace yukino
