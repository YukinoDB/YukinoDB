// The YukinoDB Unit Test Suite
//
//  table_builder_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/table_builder.h"
#include "lsm/table.h"
#include "lsm/chunk.h"
#include "yukino/comparator.h"
#include "base/mem_io.h"
#include "base/io.h"
#include "gtest/gtest.h"
#include <stdio.h>
#include <memory>

namespace yukino {

namespace lsm {

class TableBuilderTest : public ::testing::Test {
public:
    TableBuilderTest () {
    }

    virtual void SetUp() override {
        TableOptions options;

        options.block_size = TableBuilderTest::kBlockSize;
        options.restart_interval = TableBuilderTest::kRestartInterval;

        writer_ = std::move(std::unique_ptr<base::StringWriter>(
            new base::StringWriter()));
        builder_ = std::move(std::unique_ptr<TableBuilder>(
            new TableBuilder{options, writer_.get()}));
    }

    virtual void TearDown() override {
        builder_.release();
        writer_.release();
    }

    void PrintBytes(const std::string &bytes) {
        for (auto c : bytes) {
            printf("0x%02x, ", static_cast<uint8_t>(c));
        }
        printf("\n");
    }

    std::unique_ptr<TableBuilder> builder_;
    std::unique_ptr<base::StringWriter> writer_;

    static const uint32_t kBlockSize = 64;
    static const int kRestartInterval = 3;
};

TEST_F(TableBuilderTest, Sanity) {
    auto key = {
        Chunk::CreateKey("a"),
        Chunk::CreateKey("aa"),
    };

    for (const auto &chunk : key) {
        auto rs = builder_->Append(chunk);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    auto rs = builder_->Finalize();
    ASSERT_TRUE(rs.ok());

    auto mmap = base::MappedMemory::Attach(writer_->mutable_buf());
    Table table(BytewiseCompartor(), &mmap);

    rs = table.Init();
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    Table::Iterator iter(&table);

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());

    iter.Seek("aa");
    EXPECT_TRUE(iter.Valid());

    iter.Seek("a");
    EXPECT_TRUE(iter.Valid());
}

TEST_F(TableBuilderTest, SequenceAppend) {
    auto key = {
        Chunk::CreateKey("a"),
        Chunk::CreateKey("aa"),
        Chunk::CreateKey("aaa"),
        Chunk::CreateKey("aaaa"),
        Chunk::CreateKey("aaaaa"),
        Chunk::CreateKey("aaaaaa"),
        Chunk::CreateKey("aaaaaaa"),
        Chunk::CreateKey("aaaaaaaa"),
        Chunk::CreateKey("aaaaaaaaa"),
    };

    for (const auto &chunk : key) {
        auto rs = builder_->Append(chunk);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    auto rs = builder_->Finalize();
    ASSERT_TRUE(rs.ok());

    auto mmap = base::MappedMemory::Attach(writer_->mutable_buf());
    Table table(BytewiseCompartor(), &mmap);

    rs = table.Init();
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    Table::Iterator iter(&table);
    iter.SeekToFirst();

    for (const auto &chunk : key) {
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(chunk.key_slice().ToString(), iter.key().ToString());
        iter.Next();
    }
}

TEST_F(TableBuilderTest, IteratorReserve) {
    const Chunk key[] = {
        Chunk::CreateKeyValue("a", "1"),
        Chunk::CreateKeyValue("aa", "2"),
        Chunk::CreateKeyValue("aaa", "3"),
    };

    for (const auto &chunk : key) {
        auto rs = builder_->Append(chunk);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    auto rs = builder_->Finalize();
    ASSERT_TRUE(rs.ok());

    auto mmap = base::MappedMemory::Attach(writer_->mutable_buf());
    Table table(BytewiseCompartor(), &mmap);

    rs = table.Init();
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    Table::Iterator iter(&table);
    iter.SeekToLast();
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("aaa", iter.key());
    EXPECT_EQ("3", iter.value());

    iter.Prev();
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("aa", iter.key());
    EXPECT_EQ("2", iter.value());

    iter.Prev();
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("a", iter.key());
    EXPECT_EQ("1", iter.value());

    iter.Prev();
    EXPECT_FALSE(iter.Valid());

    iter.SeekToFirst();
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("a", iter.key());
    EXPECT_EQ("1", iter.value());

    iter.Prev();
    EXPECT_FALSE(iter.Valid());
}

TEST_F(TableBuilderTest, LargeBlock) {
    std::string blob_1block(kBlockSize, 'a');
    std::string blob_2block(kBlockSize * 2, 'b');

    auto key = {
        Chunk::CreateKeyValue("1", blob_1block),
        Chunk::CreateKeyValue("2", blob_1block),
        Chunk::CreateKeyValue("3", blob_2block),
    };

    for (const auto &chunk : key) {
        auto rs = builder_->Append(chunk);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    auto rs = builder_->Finalize();
    ASSERT_TRUE(rs.ok());

    auto mmap = base::MappedMemory::Attach(writer_->mutable_buf());
    Table table(BytewiseCompartor(), &mmap);

    rs = table.Init();
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    Table::Iterator iter(&table);

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());

    iter.Seek("1");
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("1", iter.key());
    EXPECT_EQ(blob_1block, iter.value().ToString());

    iter.Seek("3");
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("3", iter.key());
    EXPECT_EQ(blob_2block, iter.value().ToString());

    iter.Seek("2");
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ("2", iter.key());
    EXPECT_EQ(blob_1block, iter.value());
}

int FindLessOrEqual(int *a, int n, int k) {

    int left = 0, right = n - 1, middle = 0;
    while (left <= right) {
        middle = (left + right) / 2;
        if (k < a[middle]) {
            right = middle - 1;
        } else if (k > a[middle]) {
            left = middle + 1;
        } else {
            return middle;
        }
    }

    for (auto i = middle; i < n; ++i) {
        if (k <= a[i]) {
            return i;
        }
    }
    return -1;
}

int SlowFindLessOrEqual(int *a, int n, int k) {
    for (auto i = 0; i < n; ++i) {
        if (k <= a[i]) {
            return i;
        }
    }
    return -1;
}

TEST_F(TableBuilderTest, BinarySearchDemo) {
    int a[] = {2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32};

    for (int i = 0; i < 33; ++i) {
        auto fast = FindLessOrEqual(a, sizeof(a)/sizeof(a[0]), i);
        auto slow = SlowFindLessOrEqual(a, sizeof(a)/sizeof(a[0]), i);
        EXPECT_EQ(fast, slow) << i;
    }
}

} // namespace lsm
} // namespace yukino
