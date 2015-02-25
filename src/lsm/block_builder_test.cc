// The YukinoDB Unit Test Suite
//
//  block_builder_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/block.h"
#include "lsm/table.h"
#include "lsm/chunk.h"
#include "yukino/comparator.h"
#include "base/mem_io.h"
#include "base/varint_encoding.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace lsm {

class BlockBuilderTest : public ::testing::Test {
public:
    BlockBuilderTest () {
    }

    virtual void SetUp() override {
        buf_ = new base::StringWriter();
        builder_ = new BlockBuilder(buf_, kBlockSize, kRestartInterval);
    }

    virtual void TearDown() override {
        delete builder_;
        delete buf_;
    }

    void PrintBuf() {
        for (auto c : buf_->buf()) {
            printf("0x%02x, ", static_cast<uint8_t>(c));
        }
        printf("\n");
    }

    BlockBuilder *builder_;
    base::StringWriter *buf_;

    static const size_t kBlockSize = 512;
    static const size_t kRestartInterval = 3;
};

TEST_F(BlockBuilderTest, Sanity) {

    builder_->Append(Chunk::CreateKey("aaa"));

    BlockHandle handle(0);
    builder_->Finalize(0, &handle);

    //ASSERT_EQ(36, handle.size());
    ASSERT_EQ(buf_->buf().size(), handle.size());

    const char *buf = buf_->buf().data();
    size_t len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(3, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, strncmp("aaa", buf, 3));
}

TEST_F(BlockBuilderTest, PrefixWriting) {
    Chunk key[] = { Chunk::CreateKey("a"), Chunk::CreateKey("ab") };
    builder_->Append(key[0]);
    builder_->Append(key[1]);

    const char *buf = buf_->buf().data();
    size_t len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(1, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, strncmp("a", buf, 1));
    buf += 1;

    EXPECT_EQ(1, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(1, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, strncmp("b", buf, 1));
}

TEST_F(BlockBuilderTest, PrefixBreakWriting1) {
    Chunk key[] = { Chunk::CreateKey("a"), Chunk::CreateKey("b") };
    builder_->Append(key[0]);
    builder_->Append(key[1]);

    const char *buf = buf_->buf().data();
    size_t len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(1, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, strncmp("a", buf, 1));
    buf += 1;

    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(1, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, base::Varint32::Decode(buf, &len));
    buf += len;
    EXPECT_EQ(0, strncmp("b", buf, 1));
}

TEST_F(BlockBuilderTest, PrefixBreakWriting2) {
    Chunk key[] = {
        Chunk::CreateKey("aaa"),
        Chunk::CreateKey("aab"),
        Chunk::CreateKey("aac"),
    };

    for (const auto &chunk : key) {
        builder_->Append(chunk);
    }

    std::string expected{
        0x00, 0x03, 0x00, 0x61, 0x61, 0x61, 0x02, 0x01, 0x00, 0x62, 0x02, 0x01,
        0x00, 0x63,
    };
    EXPECT_EQ(expected, buf_->buf());
}

TEST_F(BlockBuilderTest, PrefixBreakWriting3) {
    Chunk key[] = {
        Chunk::CreateKey("aaa"),
        Chunk::CreateKey("aab"),
        Chunk::CreateKey("aac"),
        Chunk::CreateKey("aacd"),
    };

    for (const auto &chunk : key) {
        builder_->Append(chunk);
    }

    std::string expected{
        0x00, 0x03, 0x00, 0x61, 0x61, 0x61, 0x02, 0x01, 0x00, 0x62, 0x02, 0x01,
        0x00, 0x63, 0x00, 0x04, 0x00, 0x61, 0x61, 0x63, 0x64,
    };
    EXPECT_EQ(expected, buf_->buf());
}

TEST_F(BlockBuilderTest, PrefixBreakWriting4) {
    Chunk key[] = {
        Chunk::CreateKey("aaa"),
        Chunk::CreateKey("aa"),
    };

    for (const auto &chunk : key) {
        builder_->Append(chunk);
    }

    std::string expected{
        0x00, 0x03, 0x00, 0x61, 0x61, 0x61, 0x02, 0x00, 0x00,
    };
    EXPECT_EQ(expected, buf_->buf());
}

TEST_F(BlockBuilderTest, CalcChunkSize) {
    Chunk key[] = {
        Chunk::CreateKey("a"),
        Chunk::CreateKey("aa"),
        Chunk::CreateKey("ab"),
        Chunk::CreateKey("acd"),
    };

    auto size = builder_->CalcChunkSize(key[0]);
    EXPECT_EQ(8, size);
    builder_->Append(key[0]);

    size = builder_->CalcChunkSize(key[1]);
    EXPECT_EQ(4, size);
    builder_->Append(key[1]);

    size = builder_->CalcChunkSize(key[2]);
    EXPECT_EQ(4, size);
    builder_->Append(key[2]);

    size = builder_->CalcChunkSize(key[3]);
    EXPECT_EQ(10, size);
    builder_->Append(key[3]);
}

TEST_F(BlockBuilderTest, BlockIterating) {
    Chunk key[] = {
        Chunk::CreateKeyValue("a", "1"),
        Chunk::CreateKeyValue("aa", "2"),
        Chunk::CreateKeyValue("c", "3"),
        Chunk::CreateKeyValue("d", "4"),
    };

    for (const auto &chunk : key) {
        builder_->Append(chunk);
    }
    BlockHandle handle(0);
    builder_->Finalize(kTypeData, &handle);

    std::unique_ptr<Comparator> comparator(CreateBytewiseComparator());
    BlockIterator iter(comparator.get(), buf_->buf().data(), buf_->buf().size());
    iter.SeekToFirst();
    EXPECT_EQ("a", iter.key().ToString());
    EXPECT_EQ("1", iter.value().ToString());
    iter.Next();
    EXPECT_EQ("aa", iter.key().ToString());
    EXPECT_EQ("2", iter.value().ToString());
    iter.Next();
    EXPECT_EQ("c", iter.key().ToString());
    EXPECT_EQ("3", iter.value().ToString());
    iter.Next();
    EXPECT_EQ("d", iter.key().ToString());
    EXPECT_EQ("4", iter.value().ToString());
}

TEST_F(BlockBuilderTest, BlockUnlimited) {
    builder_->SetUnlimited(true);
    ASSERT_TRUE(builder_->unlimited());

    auto key = Chunk::CreateKey("aa");
    auto i = 512;
    while (i--) {
        EXPECT_TRUE(builder_->Append(key).ok());
    }
    BlockHandle handle(0);
    EXPECT_TRUE(builder_->Finalize(0, &handle).ok());
    EXPECT_EQ(2571ULL, handle.size());

    EXPECT_LT(0, handle.NumberOfBlocks(kBlockSize));
}

TEST_F(BlockBuilderTest, BlockLimited) {
    ASSERT_FALSE(builder_->unlimited());

    auto key = Chunk::CreateKey("aa");
    auto i = 512;
    while (i--) {
        if (builder_->CanAppend(key)) {
            builder_->Append(key);
        } else {
            break;
        }
    }

    BlockHandle handle(0);
    EXPECT_TRUE(builder_->Finalize(0, &handle).ok());
    EXPECT_EQ(504ULL, handle.size());

    EXPECT_EQ(1, handle.NumberOfBlocks(kBlockSize));
}

TEST_F(BlockBuilderTest, BlockSeeking) {
    Chunk key[] = {
        Chunk::CreateKeyValue("a", "1"),
        Chunk::CreateKeyValue("b", "2"),
        Chunk::CreateKeyValue("c", "3"),
        Chunk::CreateKeyValue("d", "4"),
    };

    for (const auto &chunk : key) {
        builder_->Append(chunk);
    }
    BlockHandle handle(0);
    builder_->Finalize(kTypeData, &handle);

    std::unique_ptr<Comparator> comparator(CreateBytewiseComparator());
    BlockIterator iter(comparator.get(), buf_->buf().data(), buf_->buf().size());

    iter.Seek("a");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("a", iter.key().ToString());
    EXPECT_EQ("1", iter.value().ToString());

    iter.Seek("b");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("b", iter.key().ToString());
    EXPECT_EQ("2", iter.value().ToString());

    iter.Seek("c");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("c", iter.key().ToString());
    EXPECT_EQ("3", iter.value().ToString());

    iter.Seek("d");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("d", iter.key().ToString());
    EXPECT_EQ("4", iter.value().ToString());

}

TEST_F(BlockBuilderTest, BlockPrefixSeeking) {
    Chunk key[] = {
        Chunk::CreateKeyValue("a", "1"),
        Chunk::CreateKeyValue("ab", "2"),
        Chunk::CreateKeyValue("abc", "3"),
        Chunk::CreateKeyValue("abcd", "4"),
    };

    for (const auto &chunk : key) {
        builder_->Append(chunk);
    }
    BlockHandle handle(0);
    builder_->Finalize(kTypeData, &handle);

    std::unique_ptr<Comparator> comparator(CreateBytewiseComparator());
    BlockIterator iter(comparator.get(), buf_->buf().data(), buf_->buf().size());

    iter.Seek("a");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("a", iter.key().ToString());
    EXPECT_EQ("1", iter.value().ToString());

    iter.Seek("ab");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("ab", iter.key().ToString());
    EXPECT_EQ("2", iter.value().ToString());

    iter.Seek("abc");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("abc", iter.key().ToString());
    EXPECT_EQ("3", iter.value().ToString());

    iter.Seek("abcd");
    EXPECT_EQ(true, iter.Valid());
    EXPECT_EQ("abcd", iter.key().ToString());
    EXPECT_EQ("4", iter.value().ToString());
}

} // namespace lsm

} // namespace yukino
