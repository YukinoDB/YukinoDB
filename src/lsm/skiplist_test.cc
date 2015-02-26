// The YukinoDB Unit Test Suite
//
//  skiplist_test.cc
//
//  Created by Niko Bellic.
//
//
#include "lsm/skiplist.h"
#include "gtest/gtest.h"
#include <stdio.h>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>

namespace yukino {

namespace lsm {

class SkipListTest : public ::testing::Test {
public:
    typedef SkipList<int, std::function<int (int, int)>> IntSkipList;

    SkipListTest () {
    }

    virtual void SetUp() override {
    }

    virtual void TearDown() override {
    }

    void Fill(int k, IntSkipList *list) {
        while (k--) {
            list->Put(k);
        }
    }
};

TEST_F(SkipListTest, Sanity) {

    IntSkipList list([](int a, int b) { return a - b; });

    static const auto k = 100;
    Fill(k, &list);

    auto i = k;
    while (i--) {
        EXPECT_TRUE(list.Contains(i));
    }
}

TEST_F(SkipListTest, Sequence) {
    IntSkipList list([](int a, int b) { return a - b; });

    static const auto k = 100;
    Fill(k, &list);

    IntSkipList::Iterator iter(&list);
    auto i = 0;
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        EXPECT_EQ(i++, iter.key());
    }
}

TEST_F(SkipListTest, Seek) {
    IntSkipList list([](int a, int b) { return a - b; });

    static const auto k = 100;
    Fill(k, &list);

    IntSkipList::Iterator iter(&list);
    auto i = k;
    while (i--) {
        iter.Seek(i);
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(i, iter.key());
    }
}

TEST_F(SkipListTest, ThreadingPut) {
    IntSkipList list([](int a, int b) { return a - b; });
    std::condition_variable cv;
    std::mutex m;

    auto Putter = [&] (SkipListTest::IntSkipList *list, int start, int end) {
        std::unique_lock<std::mutex> lock(m);
        cv.wait(lock);

        DLOG(INFO) << "start: " << start << " end: " << end;
        for (auto i = start; i < end; ++i) {
            list->Put(i);
        }
    };

    std::thread threads[] = {
        std::thread(Putter, &list, 0,     10000),
        std::thread(Putter, &list, 10000, 20000),
        std::thread(Putter, &list, 20000, 30000),
        std::thread(Putter, &list, 30000, 40000),
    };


    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    cv.notify_all();

    for (auto &thread : threads) {
        thread.join();
    }

    IntSkipList::Iterator iter(&list);
    auto i = 40000;
    while (i--) {
        iter.Seek(i);
        EXPECT_TRUE(iter.Valid());
        EXPECT_EQ(i, iter.key());
    }
}

} // namespace lsm

} // namespace yukino
