// The YukinoDB Unit Test Suite
//
//  version_set_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/version.h"
#include "lsm/table_cache.h"
#include "yukino/options.h"
#include "yukino/env.h"
#include "base/base.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace lsm {

class VersionTest : public ::testing::Test {
public:
    VersionTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }
};

TEST_F(VersionTest, Sanity) {
    VersionPatch patch("test");

    EXPECT_FALSE(patch.has_field(VersionPatch::kLastVersion));
    EXPECT_FALSE(patch.has_field(VersionPatch::kNextFileNumber));
    EXPECT_FALSE(patch.has_field(VersionPatch::kRedoLogNumber));

    patch.set_last_version(1);
    EXPECT_TRUE(patch.has_field(VersionPatch::kLastVersion));
    EXPECT_EQ(1, patch.last_version());

    patch.set_redo_log_number(1);
    EXPECT_TRUE(patch.has_field(VersionPatch::kRedoLogNumber));
    EXPECT_EQ(1, patch.redo_log_number());
}

TEST_F(VersionTest, VersionPatchDeletionFiles) {
    VersionPatch patch("test");

    EXPECT_FALSE(patch.has_field(VersionPatch::kDeletion));
    patch.DeleteFile(1, 2);
    patch.DeleteFile(1, 3);
    EXPECT_TRUE(patch.has_field(VersionPatch::kDeletion));

    auto found = patch.deletion().find(std::make_pair(1, 2));
    EXPECT_TRUE(found != patch.deletion().end());

    found = patch.deletion().find(std::make_pair(1, 3));
    EXPECT_TRUE(found != patch.deletion().end());
}

TEST_F(VersionTest, VersionPatchCreationFiles) {
    VersionPatch patch("test");

    EXPECT_FALSE(patch.has_field(VersionPatch::kCreation));
    patch.CreateFile(1, 2, "aaaa", "cccc", 1024, 0);
    patch.CreateFile(2, 4, "dddd", "ffff", 1024, 1);
    EXPECT_TRUE(patch.has_field(VersionPatch::kCreation));

    EXPECT_EQ(1, patch.creation()[0].first);
    auto metadata = patch.creation()[0].second.get();
    EXPECT_EQ(2, metadata->number);
    EXPECT_EQ("aaaa", metadata->smallest_key.key_slice());
    EXPECT_EQ("cccc", metadata->largest_key.key_slice());
    EXPECT_EQ(1024, metadata->size);
    EXPECT_EQ(0, metadata->ctime);

    metadata = patch.creation()[1].second.get();
    EXPECT_EQ(4, metadata->number);
    EXPECT_EQ("dddd", metadata->smallest_key.key_slice());
    EXPECT_EQ("ffff", metadata->largest_key.key_slice());
    EXPECT_EQ(1024, metadata->size);
    EXPECT_EQ(1, metadata->ctime);
}

TEST_F(VersionTest, VersionPatchCodec) {
    VersionPatch patch("test");

    patch.set_last_version(1);
    patch.set_next_file_number(3);
    patch.set_prev_log_number(0);
    //patch.set_redo_log_number(2);
    patch.DeleteFile(1, 1);
    patch.CreateFile(1, 9, "aaaa", "bbbb", 4096, 99);

    std::string buf;
    patch.Encode(&buf);

    VersionPatch other("");
    other.Decode(buf);

    EXPECT_EQ(patch.last_version(), other.last_version());
    EXPECT_EQ(patch.next_file_number(), other.next_file_number());
    EXPECT_EQ(patch.prev_log_number(), other.prev_log_number());
    EXPECT_FALSE(patch.has_field(VersionPatch::kRedoLogNumber));

    auto found = patch.deletion().find(std::make_pair(1, 1));
    EXPECT_TRUE(found != patch.deletion().end());

    EXPECT_EQ(1, patch.creation()[0].first);

    auto metadata = patch.creation()[0].second.get();
    EXPECT_EQ(9, metadata->number);
    EXPECT_EQ("aaaa", metadata->smallest_key.key_slice());
    EXPECT_EQ("bbbb", metadata->largest_key.key_slice());
    EXPECT_EQ(4096, metadata->size);
    EXPECT_EQ(99, metadata->ctime);
}

TEST_F(VersionTest, VersionSet) {
    auto kDBName = "demo";
    Options opt;
    TableCache cache(kDBName, opt);
    VersionSet versions(kDBName, opt, &cache);

    EXPECT_EQ(0, versions.last_version());
    EXPECT_NE(nullptr, versions.current());
    versions.AdvanceVersion(9);

    VersionPatch patch("test");

    patch.CreateFile(0, 9, "aaaa", "eeee", 19, 8);
    patch.CreateFile(1, 10, "ffff", "hhhh", 19, 9);

    opt.env->CreateDir(kDBName);
    auto defer = base::Defer([&opt, &kDBName]() {
        opt.env->DeleteFile(kDBName, true);
    });

    auto rs = versions.Apply(&patch, nullptr);
    EXPECT_TRUE(rs.ok()) << rs.ToString();
    EXPECT_EQ(9, versions.last_version());
    EXPECT_EQ(versions.last_version(), patch.last_version());

    auto metadata = versions.current()->file(0)[0].get();
    EXPECT_EQ(9, metadata->number);
    EXPECT_EQ("aaaa", metadata->smallest_key.key_slice());
    EXPECT_EQ("eeee", metadata->largest_key.key_slice());
    EXPECT_EQ(19, metadata->size);
    EXPECT_EQ(8, metadata->ctime);

    metadata = versions.current()->file(1)[0].get();
    EXPECT_EQ(10, metadata->number);
    EXPECT_EQ("ffff", metadata->smallest_key.key_slice());
    EXPECT_EQ("hhhh", metadata->largest_key.key_slice());
    EXPECT_EQ(19, metadata->size);
    EXPECT_EQ(9, metadata->ctime);
}

} // namespace lsm

} // namespace yukino
