// The YukinoDB Unit Test Suite
//
//  block_builder_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/block.h"
#include "lsm/chunk.h"
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

} // namespace lsm

} // namespace yukino
