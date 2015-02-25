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

} // namespace lsm
} // namespace yukino
