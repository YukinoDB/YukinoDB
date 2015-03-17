// The YukinoDB Unit Test Suite
//
//  db_impl_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/db_impl.h"
#include "yukino/env.h"
#include "yukino/options.h"
#include "yukino/write_batch.h"
#include "yukino/read_options.h"
#include "yukino/write_options.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace lsm {

class DBImplTest : public ::testing::Test {
public:
    DBImplTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
        Env::Default()->DeleteFile(kName, true);
    }

    

    constexpr static const auto kName = "demo";
};

TEST_F(DBImplTest, Sanity) {
    Options options;

    options.create_if_missing = true;

    DBImpl db(options, kName);
    auto rs = db.Open(options);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    rs = db.Put(WriteOptions(), "aaa", "1");
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    rs = db.Put(WriteOptions(), "bbb", "2");
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    std::string buf;
    rs = db.Get(ReadOptions(), "bbb", &buf);
    ASSERT_TRUE(rs.ok()) << rs.ToString();
    ASSERT_EQ("2", buf);
}

TEST_F(DBImplTest, UniqueDBLock) {
    Options options;

    options.create_if_missing = true;

    DBImpl db(options, kName);
    auto rs = db.Open(options);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    DBImpl db_miss(options, kName);
    rs = db_miss.Open(options);
    ASSERT_FALSE(rs.ok());
}

TEST_F(DBImplTest, Snapshot) {
    Options options;

    options.create_if_missing = true;

    DBImpl db(options, kName);
    auto rs = db.Open(options);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    db.Put(WriteOptions(), "aaa", "1");
    auto s1 = db.GetSnapshot();
    db.Put(WriteOptions(), "aaa", "2");
    auto s2 = db.GetSnapshot();
    db.Put(WriteOptions(), "aaa", "3");

    std::string value;
    ReadOptions read_options;

    read_options.snapshot = s1;
    db.Get(read_options, "aaa", &value);
    EXPECT_EQ("1", value);

    read_options.snapshot = s2;
    db.Get(read_options, "aaa", &value);
    EXPECT_EQ("2", value);

    read_options.snapshot = nullptr;
    db.Get(read_options, "aaa", &value);
    EXPECT_EQ("3", value);
}

TEST_F(DBImplTest, Recovery) {
    Options options;

    options.create_if_missing = true;

    {
        DBImpl db(options, kName);
        auto rs = db.Open(options);
        ASSERT_TRUE(rs.ok()) << rs.ToString();

        WriteBatch batch;
        batch.Put("aaa", "1");
        batch.Put("bbb", "2");
        batch.Put("ccc", "3");

        rs = db.Write(WriteOptions(), &batch);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    {
        DBImpl db(options, kName);
        auto rs = db.Open(options);
        ASSERT_TRUE(rs.ok()) << rs.ToString();

        std::string value;
        rs = db.Get(ReadOptions(), "bbb", &value);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
        EXPECT_EQ("2", value);
    }
}

TEST_F(DBImplTest, Level0Dump) {
    Options options;

    options.create_if_missing = true;
    options.write_buffer_size = 128;

    DBImpl db(options, kName);
    auto rs = db.Open(options);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    auto defer = base::Defer([&options](){
        options.env->DeleteFile(kName, true);
    });

    std::string value(64, '1');
    WriteBatch batch;
    batch.Put("aaa", value);
    batch.Put("bbb", value);

    rs = db.Write(WriteOptions(), &batch);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    rs = db.Put(WriteOptions(), "ccc", "3");
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    db.TEST_WaitForBackground();

    std::string found;
    rs = db.Get(ReadOptions(), "bbb", &found);
    ASSERT_TRUE(rs.ok()) << rs.ToString();
    EXPECT_EQ(value, found);

    rs = db.Get(ReadOptions(), "ccc", &found);
    ASSERT_TRUE(rs.ok()) << rs.ToString();
    EXPECT_EQ("3", found);
}

TEST_F(DBImplTest, DumpThenRecovery) {
    Options options;

    options.create_if_missing = true;
    options.write_buffer_size = 128;

    std::string value(64, '2');
    {
        std::unique_ptr<DBImpl> db(new DBImpl(options, kName));
        auto rs = db->Open(options);
        ASSERT_TRUE(rs.ok()) << rs.ToString();

        WriteBatch batch;
        batch.Put("aaa", value);
        batch.Put("bbb", value);

        rs = db->Write(WriteOptions(), &batch);
        ASSERT_TRUE(rs.ok()) << rs.ToString();

        rs = db->Put(WriteOptions(), "ccc", "3");
        ASSERT_TRUE(rs.ok()) << rs.ToString();
        db->TEST_WaitForBackground();
    }

    {
        std::unique_ptr<DBImpl> db(new DBImpl(options, kName));
        auto rs = db->Open(options);
        ASSERT_TRUE(rs.ok()) << rs.ToString();

        std::string found;
        rs = db->Get(ReadOptions(), "bbb", &found);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
        EXPECT_EQ(value, found);

        rs = db->Get(ReadOptions(), "ccc", &found);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
        EXPECT_EQ("3", found);
    }
}

TEST_F(DBImplTest, DISABLED_LargeWriteForDumping) {
    Options options;

    options.create_if_missing = true;

    std::string value(128, 'f');
    DBImpl db(options, kName);
    auto rs = db.Open(options);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    for (auto i = 0; i < base::kMB; i++) {
        rs = db.Put(WriteOptions(), "aaaa", value);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }
    db.TEST_DumpVersions();
}

} // namespace lsm

} // namespace yukino
