// The YukinoDB Unit Test Suite
//
//  shared_ttree_test.cc
//
//  Created by Niko Bellic.
//
//
#include "util/shared_ttree-inl.h"
#include "util/shared_ttree.h"
#include "base/io-inl.h"
#include "base/io.h"
#include "yukino/comparator.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace util {

class SharedTTreeTest : public ::testing::Test {
public:
    SharedTTreeTest ()
        : tree_(BytewiseCompartor(), kPageSize) {
    }

    virtual void SetUp() override {
        ::memset(buf_, 0, kBufSize);
        mmap_ = new base::MappedMemory(":memory:", buf_, kBufSize);

        auto rs = tree_.Init(mmap_);
        ASSERT_TRUE(rs.ok()) << rs.ToString();
    }

    virtual void TearDown() override {
        delete mmap_;
    }

    void BatchPut(std::initializer_list<std::string> keys, SharedTTree *tree) {
        base::Status rs;
        for (auto key : keys) {
            ASSERT_FALSE(tree->Put(key, nullptr, &rs));
            EXPECT_TRUE(rs.ok()) << rs.ToString();
        }
    }

    enum { kPageSize = 512, kBufSize  = kPageSize * 4, };

    char *buf_ = new char[kBufSize];
    base::MappedMemory *mmap_ = nullptr;
    SharedTTree tree_;
};

TEST_F(SharedTTreeTest, Sanity) {
    SharedTTree::Delegate page(tree_.TEST_Root(), &tree_);

    EXPECT_EQ(490, page.capacity());
    EXPECT_EQ(nullptr, page.parent());
    EXPECT_EQ(nullptr, page.lchild());
    EXPECT_EQ(nullptr, page.rchild());
}

TEST_F(SharedTTreeTest, AllocateNode) {
    SharedTTree::Node *node;
    bool ok;
    std::tie(node, ok) = tree_.AllocateNode();
    ASSERT_NE(nullptr, node);
    EXPECT_TRUE(ok);

    auto offset = reinterpret_cast<char*>(node) -
                  reinterpret_cast<char*>(tree_.TEST_Root());
    EXPECT_EQ(kPageSize, offset);

    for (auto i = 0; i < 2; ++i) {
        std::tie(node, ok) = tree_.AllocateNode();
        ASSERT_NE(nullptr, node);
        EXPECT_TRUE(ok);
    }

    std::tie(node, ok) = tree_.AllocateNode();
    ASSERT_EQ(nullptr, node);
    EXPECT_FALSE(ok);
}

TEST_F(SharedTTreeTest, NodePutting) {
    SharedTTree::Delegate page(tree_.TEST_Root(), &tree_);

    page.Put("0ab", nullptr);
    page.Put("1cd", nullptr);
    page.Put("2ef", nullptr);

    EXPECT_EQ("0ab", page.key(0).ToString());
    EXPECT_EQ("1cd", page.key(1).ToString());
    EXPECT_EQ("2ef", page.key(2).ToString());
}

TEST_F(SharedTTreeTest, NodeDeletion) {
    SharedTTree::Delegate page(tree_.TEST_Root(), &tree_);

    page.Put("0", nullptr);
    page.Put("1", nullptr);
    page.Put("2", nullptr);
    EXPECT_EQ(3, page.num_entries());

    page.DeleteAt(0);
    EXPECT_EQ("1", page.key(0).ToString());
    EXPECT_EQ("2", page.key(1).ToString());
    EXPECT_EQ(2, page.num_entries());

    page.DeleteAt(1);
    EXPECT_EQ("1", page.key(0).ToString());
    EXPECT_EQ(1, page.num_entries());

    page.DeleteAt(0);
    EXPECT_EQ(0, page.num_entries());
}

TEST_F(SharedTTreeTest, NodeReplace) {
    SharedTTree::Delegate page(tree_.TEST_Root(), &tree_);

    page.Put("01", nullptr);
    page.Put("123", nullptr);
    page.Put("2", nullptr);
    EXPECT_EQ(3, page.num_entries());

    EXPECT_EQ("01",  page.key(0).ToString());
    EXPECT_EQ("123", page.key(1).ToString());
    EXPECT_EQ("2",   page.key(2).ToString());

    page.ReplaceAt(1, "12345");
    EXPECT_EQ("01",  page.key(0).ToString());
    EXPECT_EQ("12345", page.key(1).ToString());
    EXPECT_EQ("2",   page.key(2).ToString());

    page.ReplaceAt(1, "12");
    EXPECT_EQ("01",  page.key(0).ToString());
    EXPECT_EQ("12",  page.key(1).ToString());
    EXPECT_EQ("2",   page.key(2).ToString());

    page.ReplaceAt(1, "1A");
    EXPECT_EQ("01",  page.key(0).ToString());
    EXPECT_EQ("1A",  page.key(1).ToString());
    EXPECT_EQ("2",   page.key(2).ToString());
}

TEST_F(SharedTTreeTest, RootPutting) {
    base::Status rs;
    ASSERT_FALSE(tree_.Put("a", nullptr, &rs));
    ASSERT_FALSE(tree_.Put("aa", nullptr, &rs));
    ASSERT_FALSE(tree_.Put("aaa", nullptr, &rs));

    SharedTTree::Delegate page(tree_.TEST_Root(), &tree_);
    EXPECT_EQ("a", page.key(0).ToString());
    EXPECT_EQ("aa", page.key(1).ToString());
    EXPECT_EQ("aaa", page.key(2).ToString());

    std::string buf;
    tree_.TEST_DumpTree(tree_.TEST_Root(), &buf);
    DLOG(INFO) << buf;
}

TEST_F(SharedTTreeTest, TreePutting) {
    tree_.set_limit_count(4);

    BatchPut({"a", "b", "c", "ddd", "eee", "ffff", "ggg"}, &tree_);

    base::Slice rv;
    std::string scratch;
    EXPECT_TRUE(tree_.Get("a", &rv, &scratch));
    EXPECT_TRUE(tree_.Get("b", &rv, &scratch));
    EXPECT_TRUE(tree_.Get("eee", &rv, &scratch));
    EXPECT_TRUE(tree_.Get("ggg", &rv, &scratch));
    EXPECT_FALSE(tree_.Get("1", &rv, &scratch));
}

TEST_F(SharedTTreeTest, TreeMutilNodePutting) {
    tree_.set_limit_count(4);

    BatchPut({"1", "3", "5", "7", "9"}, &tree_);

    std::string buf;
    base::Status rs;
    tree_.Put("2", &buf, &rs);

    tree_.TEST_DumpTree(tree_.TEST_Root(), &buf);

    DLOG(INFO) << "\n" << buf;
}

} // namespace util

} // namespace yukino
