// The YukinoDB Unit Test Suite
//
//  compactor_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/compactor.h"
#include "lsm/table_builder.h"
#include "lsm/table.h"
#include "lsm/chunk.h"
#include "yukino/comparator.h"
#include "base/mem_io.h"
#include "base/io.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace lsm {

class CompactorTest : public ::testing::Test {
public:
    CompactorTest () {
    }

    virtual void SetUp() override {
        // TODO:
    }

    virtual void TearDown() override {
        // TODO:
    }

    std::string Build(std::initializer_list<Chunk> init) {
        base::StringWriter writer;

        TableOptions options;
        options.block_size = 64;
        options.restart_interval = 3;

        TableBuilder builder(options, &writer);
        
        for (const auto &chunk : init) {
            builder.Append(chunk);
        }
        builder.Finalize();

        return std::move(*writer.mutable_buf());
    }
};

TEST_F(CompactorTest, Sanity) {
    static const char *values[] = { "1", "2", "3", "4", "5", "6"};

    auto t1 = Build({
        InternalKey::CreateKey("a", values[0], 0, kFlagValue),
        InternalKey::CreateKey("aaa",  values[2], 2, kFlagValue),
        InternalKey::CreateKey("aaaaa",  values[4], 4, kFlagValue),
    });

    auto t2 = Build({
        InternalKey::CreateKey("aa",  values[1], 1, kFlagValue),
        InternalKey::CreateKey("aaaa",  values[3], 3, kFlagValue),
        InternalKey::CreateKey("aaaaaa",  values[5], 5, kFlagValue),
    });

    auto mm1 = base::MappedMemory::Attach(&t1);
    auto mm2 = base::MappedMemory::Attach(&t2);

    Table tt1(BytewiseCompartor(), &mm1);
    Table tt2(BytewiseCompartor(), &mm2);

    ASSERT_TRUE(tt1.Init().ok());
    ASSERT_TRUE(tt2.Init().ok());

    Iterator *iters[] = {
        new Table::Iterator(&tt1),
        new Table::Iterator(&tt2),
    };

    Compactor compactor((InternalKeyComparator(BytewiseCompartor())));

    base::StringWriter writer;

    TableOptions options;
    options.block_size = 64;
    options.restart_interval = 3;

    TableBuilder builder(options, &writer);

    ASSERT_TRUE(compactor.Compact(iters, 2, &builder).ok());

    auto mm = base::MappedMemory::Attach(writer.mutable_buf());
    Table tt(BytewiseCompartor(), &mm);
    auto rs = tt.Init();
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    Table::Iterator iter(&tt);
    iter.SeekToFirst();
    for (auto value : values) {
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(value, iter.value().ToString());
        iter.Next();
    }
}

} // namespace lsm

} // namespace yukino
