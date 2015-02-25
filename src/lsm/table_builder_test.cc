// The YukinoDB Unit Test Suite
//
//  table_builder_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/table_builder.h"
#include "lsm/chunk.h"
#include "base/mem_io.h"
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
        ASSERT_TRUE(rs.ok());
    }

    auto rs = builder_->Finalize();
    ASSERT_TRUE(rs.ok());

    //PrintBytes(writer_->buf());
}

} // namespace lsm
} // namespace yukino
