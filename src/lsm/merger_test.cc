// The YukinoDB Unit Test Suite
//
//  merger_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/merger.h"
#include "yukino/comparator.h"
#include "base/status.h"
#include "base/slice.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include <stdio.h>
#include <vector>

namespace yukino {

namespace lsm {

namespace {

class IteratorMock : public Iterator {
public:
    IteratorMock(const std::initializer_list<const char *> init) {
        for (const auto &data : init) {
            data_.push_back(data);
        }
    }

    virtual ~IteratorMock() override {}

    virtual bool Valid() const override { return i_ >= 0 && i_ < data_.size(); }
    virtual void SeekToFirst() override { i_ = 0; }
    virtual void SeekToLast() override { i_ = data_.size() - 1; }
    virtual void Seek(const base::Slice& target) override {
        int64_t i;
        for (i = 0; i < data_.size(); ++i) {
            if (target.compare(data_[i]) == 0) {
                break;
            }
        }
        i_ = i;
    }
    virtual void Next() override { i_++; }
    virtual void Prev() override { i_--; }
    virtual base::Slice key() const override {
        DCHECK(Valid());
        return data_[i_];
    }
    virtual base::Slice value() const override {
        DCHECK(Valid());
        return "";
    }
    virtual base::Status status() const override { return base::Status::OK(); }

private:
    std::vector<std::string> data_;
    int64_t i_;
};

} // namespace

class MergerTest : public ::testing::Test {
public:
    MergerTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }

};

TEST_F(MergerTest, Sanity) {
    auto iter1 = new IteratorMock {
        "a",
        "aaa",
        "aaaaa",
    };

    auto iter2 = new IteratorMock {
        "aa",
        "aaaa",
        "aaaaaa",
    };

    Iterator *iters[] = { iter1, iter2 };

    std::unique_ptr<Iterator> merger(CreateMergingIterator(BytewiseCompartor(),
                                                           iters, 2));
    merger->SeekToFirst();
    EXPECT_TRUE(merger->Valid());
    EXPECT_EQ("a", merger->key());

    merger->Next();
    EXPECT_TRUE(merger->Valid());
    EXPECT_EQ("aa", merger->key());

    auto i = 4;
    while (i--) {
        merger->Next();
        EXPECT_TRUE(merger->Valid());
    }
    EXPECT_EQ("aaaaaa", merger->key());

    merger->Next();
    EXPECT_FALSE(merger->Valid());
}

TEST_F(MergerTest, MergingSeek) {
    auto iter1 = new IteratorMock {
        "b",
        "bb",
        "bbb",
    };

    auto iter2 = new IteratorMock {
        "bbbb",
        "bbbbb",
        "bbbbbb",
    };

    Iterator *iters[] = { iter2, iter1 };
    std::unique_ptr<Iterator> merger(CreateMergingIterator(BytewiseCompartor(),
                                                           iters, 2));
    merger->Seek("b");
    EXPECT_TRUE(merger->Valid());
    EXPECT_EQ("b", merger->key());

    merger->Seek("bbbbbb");
    EXPECT_TRUE(merger->Valid());
    EXPECT_EQ("bbbbbb", merger->key().ToString());

    merger->Seek("a");
    EXPECT_FALSE(merger->Valid());
}

TEST_F(MergerTest, MergeOne) {

}

} // namespace lsm

} // namespace yukino
