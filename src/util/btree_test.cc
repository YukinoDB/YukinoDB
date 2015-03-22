// The YukinoDB Unit Test Suite
//
//  btree_test.cc
//
//  Created by Niko Bellic.
//
//
#include "util/btree.h"
#include "gtest/gtest.h"
#include <stdio.h>
#include <functional>

namespace yukino {

namespace util {

class BTreeTest : public ::testing::Test {
public:
    typedef BTree<int, std::function<int(int, int)>> IntTree;

    std::function<int (int, int)> int_comparator = [](int a, int b) {
        return a - b;
    };

    BTreeTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }

    void BatchPut(std::initializer_list<int> keys, IntTree *tree) {
        int old = 0;
        for (auto key : keys) {
            tree->Put(key, &old);
        }
    }
};

TEST_F(BTreeTest, Sanity) {
    IntTree::Page page(0, 3);

    page.Put(3, nullptr, int_comparator);
    page.Put(1, nullptr, int_comparator);
    page.Put(2, nullptr, int_comparator);

    EXPECT_EQ(1, page.entries[0].key);
    EXPECT_EQ(2, page.entries[1].key);
    EXPECT_EQ(3, page.entries[2].key);
}

TEST_F(BTreeTest, PageFrontMovement) {
    IntTree::Page page(0, 4), dest(1, 3);

    page.Put(3, nullptr, int_comparator);
    page.Put(1, nullptr, int_comparator);
    page.Put(2, nullptr, int_comparator);
    page.Put(4, nullptr, int_comparator);

    auto entry = page.MoveTo(2, &dest, int_comparator);
    EXPECT_EQ(2, entry->key);
    EXPECT_EQ(2, page.size());
    EXPECT_EQ(2, dest.size());

    EXPECT_EQ(1, dest.entries[0].key);
    EXPECT_EQ(2, dest.entries[1].key);
    EXPECT_EQ(3, page.entries[0].key);
    EXPECT_EQ(4, page.entries[1].key);
}

TEST_F(BTreeTest, PageBackMovement) {
    IntTree::Page page(0, 4), dest(1, 3);

    page.Put(3, nullptr, int_comparator);
    page.Put(1, nullptr, int_comparator);
    page.Put(2, nullptr, int_comparator);
    page.Put(4, nullptr, int_comparator);

    auto entry = page.MoveTo(-2, &dest, int_comparator);
    EXPECT_EQ(4, entry->key);
    EXPECT_EQ(2, page.size());
    EXPECT_EQ(2, dest.size());

    EXPECT_EQ(1, page.entries[0].key);
    EXPECT_EQ(2, page.entries[1].key);
    EXPECT_EQ(3, dest.entries[0].key);
    EXPECT_EQ(4, dest.entries[1].key);
}

TEST_F(BTreeTest, TreeSplitLeafPut) {
    IntTree tree(3, int_comparator);

    int old = 0;
    // [1]
    ASSERT_FALSE(tree.Put(1, &old));
    // [1][5]
    ASSERT_FALSE(tree.Put(5, &old));
    //       [3]
    // [1][3]   [5]
    ASSERT_FALSE(tree.Put(3, &old));
    //       [3]
    // [1][3]   [4][5]
    ASSERT_FALSE(tree.Put(4, &old));

    auto page = tree.TEST_GetRoot();
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(3, page->key(0));

    page = page->entries[0].link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(1, page->key(0));
    EXPECT_EQ(3, page->key(1));

    page = tree.TEST_GetRoot();
    page = page->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(4, page->key(0));
    EXPECT_EQ(5, page->key(1));
}

TEST_F(BTreeTest, TreeCoverPut) {
    IntTree tree(3, int_comparator);

    int old = 0;
    ASSERT_FALSE(tree.Put(1, &old));
    ASSERT_FALSE(tree.Put(5, &old));

    ASSERT_TRUE(tree.Put(1, &old));
    EXPECT_EQ(1, old);

    ASSERT_TRUE(tree.Put(5, &old));
    EXPECT_EQ(5, old);
    
    ASSERT_FALSE(tree.Put(4, &old));
}

TEST_F(BTreeTest, TreeSplitLeafPut2) {
    IntTree tree(3, int_comparator);

    int old = 0;
    BatchPut({1, 5, 3, 4}, &tree);
    //          [3]
    // [1][2][3]   [4][5]
    ASSERT_FALSE(tree.Put(2, &old));
    //         [2][3]
    // [0][1][2] [3] [4][5]
    ASSERT_FALSE(tree.Put(0, &old));

    auto page = tree.TEST_GetRoot();
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(2, page->key(0));
    EXPECT_EQ(3, page->key(1));

    page = page->child(0);
    ASSERT_EQ(3, page->size());
    EXPECT_EQ(0, page->key(0));
    EXPECT_EQ(1, page->key(1));
    EXPECT_EQ(2, page->key(2));

    page = tree.TEST_GetRoot()->child(1);
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(3, page->key(0));

    page= tree.TEST_GetRoot()->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(4, page->key(0));
    EXPECT_EQ(5, page->key(1));
}

} // namespace util

} // namespace yukino
