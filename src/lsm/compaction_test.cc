// The YukinoDB Unit Test Suite
//
//  compaction_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/compaction.h"
#include "lsm/table_builder.h"
#include "lsm/table_cache.h"
#include "lsm/table.h"
#include "lsm/chunk.h"
#include "yukino/options.h"
#include "yukino/comparator.h"
#include "base/mem_io.h"
#include "base/io.h"
#include "gtest/gtest.h"
#include <stdio.h>
#include <vector>

namespace yukino {

namespace lsm {

class CompactionTest : public ::testing::Test {
public:
    CompactionTest ()
        : internal_comparator_(BytewiseCompartor())
        , cache_(kDBName, Options()) {
        options_.block_size = 64;
        options_.restart_interval = 3;
    }

    virtual void SetUp() override {
        compaction_ = new Compaction(kDBName, internal_comparator_, &cache_);
    }

    virtual void TearDown() override {
        delete compaction_;
        compaction_ = nullptr;
    }

    std::string Build(std::initializer_list<Chunk> init) {
        base::StringWriter writer;
        TableBuilder builder(options_, &writer);

        for (const auto &chunk : init) {
            builder.Append(chunk);
        }
        builder.Finalize();

        return std::move(*writer.mutable_buf());
    }

    std::string Compact() {
        base::StringWriter writer;
        TableBuilder builder(options_, &writer);

        if (compaction_->Compact(&builder).ok()) {
            return writer.buf();
        } else {
            return "";
        }
    }

    constexpr static const auto kDBName = "demo";

    InternalKeyComparator internal_comparator_;
    TableOptions options_;
    Compaction *compaction_;
    TableCache cache_;
};

TEST_F(CompactionTest, Sanity) {
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

    Table tt1(&internal_comparator_, &mm1);
    Table tt2(&internal_comparator_, &mm2);

    ASSERT_TRUE(tt1.Init().ok());
    ASSERT_TRUE(tt2.Init().ok());

    compaction_->AddOriginIterator(new Table::Iterator(&tt1));
    compaction_->AddOriginIterator(new Table::Iterator(&tt2));
    auto tn = Compact();

    auto mm = base::MappedMemory::Attach(&tn);
    Table tt(&internal_comparator_, &mm);
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

TEST_F(CompactionTest, Deletion) {
    static const char *values[] = { "1", "2", "3", "4", "5", "6"};

    auto t1 = Build({
        InternalKey::CreateKey("a", values[0], 0, kFlagValue),
        InternalKey::CreateKey("aaa",  values[2], 2, kFlagDeletion),
        InternalKey::CreateKey("aaaaa",  values[4], 4, kFlagValue),
    });

    auto t2 = Build({
        InternalKey::CreateKey("aa",  values[1], 1, kFlagValue),
        InternalKey::CreateKey("aaaa",  values[3], 3, kFlagDeletion),
        InternalKey::CreateKey("aaaaaa",  values[5], 5, kFlagValue),
    });

    auto mm1 = base::MappedMemory::Attach(&t1);
    auto mm2 = base::MappedMemory::Attach(&t2);

    Table tt1(&internal_comparator_, &mm1);
    Table tt2(&internal_comparator_, &mm2);

    ASSERT_TRUE(tt1.Init().ok());
    ASSERT_TRUE(tt2.Init().ok());

    compaction_->AddOriginIterator(new Table::Iterator(&tt1));
    compaction_->AddOriginIterator(new Table::Iterator(&tt2));
    auto tn = Compact();

    auto mm = base::MappedMemory::Attach(&tn);
    Table tt(&internal_comparator_, &mm);
    auto rs = tt.Init();
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    Table::Iterator iter(&tt);

    auto key = InternalKey::CreateKey("aaa", 2);
    auto greater_key = InternalKey::CreateKey("aaaaa", 4);
    iter.Seek(key.key_slice());
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ(greater_key.key_slice(), iter.key());

    key = InternalKey::CreateKey("aaaa", 3);
    iter.Seek(key.key_slice());
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ(greater_key.key_slice(), iter.key());

    key = InternalKey::CreateKey("a", 0);
    iter.Seek(InternalKey::CreateKey("a", 99).key_slice());
    EXPECT_TRUE(iter.Valid());
    EXPECT_EQ(key.key_slice(), iter.key());
}

} // namespace lsm

} // namespace yukino
