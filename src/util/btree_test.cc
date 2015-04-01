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
#include <vector>
#include <random>

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

    static std::string ToString(const IntTree::Page &page) {
        std::string buf("[");

        for (const auto &entry : page.entries) {
            buf.append(base::Strings::Sprintf("%d, ", entry.key));
        }
        buf.append("]");
        return buf;
    }

    bool CheckPageParent(const IntTree &tree) {
        return tree.Travel(tree.TEST_GetRoot(), [this](IntTree::Page *page) {
            auto rv = true;
            if (!page->is_leaf()) {
                if (page->link) {
                    if (page->link->parent.page != page) {
                        DLOG(INFO) << "link hit is leaf: " << page->link->is_leaf();
                        DLOG(INFO) << "page: " << this->ToString(*page);
                        DLOG(INFO) << "link: " << this->ToString(*page->link);
                        return false;
                    }
                }

                for (const auto &entry : page->entries) {
                    if (entry.link->parent.page != page) {
                        DLOG(INFO) << "entry hit";
                        return false;
                    }
                }
            }
            return rv;
        });
    }

    static void ShuffleArray(std::vector<int> *arr) {
        std::uniform_int_distribution<size_t> distribution(0, arr->size());
        std::mt19937 engine;
        auto rand = std::bind(distribution, engine);

        auto i = static_cast<int>(arr->size());
        while ( --i ) {
            auto j = rand() % (i + 1);
            auto temp = (*arr)[i];
            (*arr)[i] = (*arr)[j];
            (*arr)[j] = temp;
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

    page = tree.TEST_GetRoot()->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(4, page->key(0));
    EXPECT_EQ(5, page->key(1));
}

TEST_F(BTreeTest, TreeSplitNonLeafPut) {
    IntTree tree(3, int_comparator);

    //int old = 0;
    //         [2][3]
    // [0][1][2] [3] [4][5]
    BatchPut({1, 5, 3, 4, 2, 0}, &tree);
    int old = 0;
    //          [3]
    // [1][2][3]   [4][5][6]
    ASSERT_FALSE(tree.Put(6, &old));
    //         [2][3][5]
    // [0][1][2] [3] [4][5] [6][7]
    ASSERT_FALSE(tree.Put(7, &old));
    //         [2][3][5]
    // [0][1][2] [3] [4][5] [6][7][8]
    ASSERT_FALSE(tree.Put(8, &old));
    //              [3]
    //         [2]         [5][7]
    // [0][1][2] [3] [4][5] [6][7] [8][9]
    ASSERT_FALSE(tree.Put(9, &old));

    auto page = tree.TEST_GetRoot();
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(3, page->key(0));

    page = page->child(0);
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(2, page->key(0));

    page = tree.TEST_GetRoot()->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(5, page->key(0));
    EXPECT_EQ(7, page->key(1));
}

TEST_F(BTreeTest, LargeInsert) {
    IntTree tree(128, int_comparator);

    auto old = 0;
    auto i = 10000;
    while (i--) {
        ASSERT_FALSE(tree.Put(i, &old));
    }
}

TEST_F(BTreeTest, FindLessThan) {
    IntTree tree(3, int_comparator);

    BatchPut({0, 1, 2, 3, 4, 5}, &tree);

    auto rv = tree.FindLessThan(0);
    EXPECT_EQ(nullptr, std::get<0>(rv));
    EXPECT_EQ(-1, std::get<1>(rv));

    rv = tree.FindLessThan(1);
    EXPECT_EQ(0, std::get<0>(rv)->key(std::get<1>(rv)));

    rv = tree.FindLessThan(2);
    EXPECT_EQ(1, std::get<0>(rv)->key(std::get<1>(rv)));

    rv = tree.FindLessThan(6);
    EXPECT_EQ(5, std::get<0>(rv)->key(std::get<1>(rv)));
}

TEST_F(BTreeTest, IteratorNext) {
    IntTree tree(3, int_comparator);

    // [0][1][2]
    //-----------
    //      [1]
    // [0][1] [2][3]
    //--------------
    //        [1][3]
    // [0][1] [2][3] [4][5]
    //---------------------
    //        [1][3][5]
    // [0][1] [2][3] [4][5] [6][11]
    //-----------------------------
    //              [3]
    //       [1]            [5][11]
    // [0][1] [2][3] [4][5] [6][11] [13][17]
    auto numbers = {0, 1, 2, 3, 4, 5, 6, 11, 13, 17};
    BatchPut(numbers, &tree);

    IntTree::Iterator iter(&tree);
    iter.SeekToFirst();

    for (auto i : numbers) {
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(i, iter.key());
        iter.Next();
    }
    ASSERT_FALSE(iter.Valid());

    for (auto i : numbers) {
        iter.Seek(i);
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(i, iter.key());
    }

    iter.Seek(-1);
    ASSERT_TRUE(iter.Valid());
    EXPECT_EQ(0, iter.key());

    iter.Seek(18);
    ASSERT_FALSE(iter.Valid());
}

TEST_F(BTreeTest, IteratorSeek) {
    IntTree tree(3, int_comparator);

    // [0][1][2]
    //-----------
    //      [1]
    // [0][1] [2][3]
    //--------------
    //        [1][3]
    // [0][1] [2][3] [4][5]
    //---------------------
    //        [1][3][5]
    // [0][1] [2][3] [4][5] [6][11]
    //-----------------------------
    //              [3]
    //       [1]            [5][11]
    // [0][1] [2][3] [4][5] [6][11] [13][17]
    auto numbers = {0, 1, 2, 3, 4, 5, 6, 11, 13, 17};
    BatchPut(numbers, &tree);

    IntTree::Iterator iter(&tree);

    std::vector<int> rnumber;
    for (auto i : numbers) {
        rnumber.insert(rnumber.begin(), i);
    }

    iter.SeekToLast();
    for (auto i : rnumber) {
        ASSERT_TRUE(iter.Valid());
        EXPECT_EQ(i, iter.key());
        iter.Prev();
    }
    ASSERT_FALSE(iter.Valid());
}

TEST_F(BTreeTest, DeleteFrontLeaf) {
    IntTree tree(3, int_comparator);

    //---------------------------------------
    //              [3]
    //       [1]            [5][11]
    // [0][1] [2][3] [4][5] [6][11] [13][17]
    //---------------------------------------
    BatchPut({0, 1, 2, 3, 4, 5, 6, 11, 13, 17}, &tree);

    int old = 0;
    ASSERT_TRUE(tree.Delete(0, &old));
    ASSERT_TRUE(tree.Delete(1, &old));

    //---------------------------------------
    //         [3][5][11]
    // [2][3] [4][5] [6][11] [13][17]
    //---------------------------------------

    auto page = tree.TEST_GetRoot();
    ASSERT_EQ(3, page->size());
    EXPECT_EQ(3, page->key(0));
    EXPECT_EQ(5, page->key(1));
    EXPECT_EQ(11, page->key(2));

    page = tree.TEST_GetRoot()->child(0);
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(2, page->key(0));
    EXPECT_EQ(3, page->key(1));

    page = tree.TEST_GetRoot()->child(1);
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(4, page->key(0));
    EXPECT_EQ(5, page->key(1));

    page = tree.TEST_GetRoot()->child(2);
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(6, page->key(0));
    EXPECT_EQ(11, page->key(1));

    page = tree.TEST_GetRoot()->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(13, page->key(0));
    EXPECT_EQ(17, page->key(1));
}

TEST_F(BTreeTest, DeleteMiddleLeaf) {
    IntTree tree(3, int_comparator);

    //---------------------------------------
    //              [3]
    //       [1]            [5][11]
    // [0][1] [2][3] [4][5] [6][11] [13][17]
    //---------------------------------------
    BatchPut({0, 1, 2, 3, 4, 5, 6, 11, 13, 17}, &tree);

    int old = 0;
    ASSERT_TRUE(tree.Delete(4, &old));
    ASSERT_TRUE(tree.Delete(5, &old));

    //---------------------------------------
    //              [3]
    //       [1]            [11]
    // [0][1] [2][3]  [6][11] [13][17]
    //---------------------------------------
    auto page = tree.TEST_GetRoot();
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(3, page->key(0));

    page = tree.TEST_GetRoot()->child(0);
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(1, page->key(0));

    page = tree.TEST_GetRoot()->link;
    ASSERT_EQ(1, page->size());
    EXPECT_EQ(11, page->key(0));

    page = tree.TEST_GetRoot()->child(0)->child(0);
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(0, page->key(0));
    EXPECT_EQ(1, page->key(1));

    page = tree.TEST_GetRoot()->child(0)->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(2, page->key(0));
    EXPECT_EQ(3, page->key(1));

    page = tree.TEST_GetRoot()->link->child(0);
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(6, page->key(0));
    EXPECT_EQ(11, page->key(1));

    page = tree.TEST_GetRoot()->link->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(13, page->key(0));
    EXPECT_EQ(17, page->key(1));

    page = tree.TEST_FirstPage();
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(0, page->key(0));
    EXPECT_EQ(1, page->key(1));

    page = page->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(2, page->key(0));
    EXPECT_EQ(3, page->key(1));

    page = page->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(6, page->key(0));
    EXPECT_EQ(11, page->key(1));

    page = page->link;
    ASSERT_EQ(2, page->size());
    EXPECT_EQ(13, page->key(0));
    EXPECT_EQ(17, page->key(1));
}

TEST_F(BTreeTest, LargePuting) {
    IntTree tree(127, int_comparator);

    static const auto k = 100000;

    int dummy = 0;
    for (auto i = 0; i < k; ++i) {
        ASSERT_FALSE(tree.Put(i, &dummy)) << i;
    }

    IntTree::Iterator iter(&tree);
    auto i = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        ASSERT_EQ(i, iter.key());
        ++i;
    }

    for (auto i = 0; i < k; ++i) {
        iter.Seek(i);
        ASSERT_EQ(i, iter.key());
    }
}

TEST_F(BTreeTest, FuzzyPutting) {
    IntTree tree(127, int_comparator);

    static const auto k = 100000;
    std::vector<int> arr(k);
    for (auto i = 0; i < k; ++i) {
        arr[i] = i;
    }
    ShuffleArray(&arr);

    int dummy = 0;
    for (auto i : arr) {
        ASSERT_FALSE(tree.Put(i, &dummy)) << i;
    }

    IntTree::Iterator iter(&tree);
    for (auto i = 0; i < k; ++i) {
        iter.Seek(i);
        ASSERT_TRUE(iter.Valid()) << i;
        ASSERT_EQ(i, iter.key());
    }
}

} // namespace util

} // namespace yukino
