// The YukinoDB Unit Test Suite
//
//  ref_counted_test.cc
//
//  Created by Niko Bellic.
//
//
#include "base/ref_counted.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include <stdio.h>

namespace yukino {

namespace base {

namespace {

class TestStub : public base::ReferenceCounted<TestStub> {
public:
    explicit TestStub(int n) : n_(n) {
    }

    ~TestStub() {
    }

    int n() const { return n_; }

private:
    int n_;
};

} // namespace

TEST(ReferenceCountedTest, Sanity) {
    base::Handle<TestStub> h1(new TestStub(0));
    EXPECT_EQ(1, h1->ref_count());

    base::Handle<TestStub> h2;
    h2 = h1;
    EXPECT_EQ(2, h1->ref_count());
    EXPECT_EQ(2, h2->ref_count());
}

TEST(ReferenceCountedTest, MoveReference) {
    base::Handle<const TestStub> h1(new TestStub(1));
    EXPECT_EQ(1, h1->ref_count());

    base::Handle<const TestStub> h2(std::move(h1));
    EXPECT_EQ(1, h2->ref_count());
    EXPECT_EQ(nullptr, h1.get());

    base::Handle<const TestStub> h3;
    h3 = std::move(h2);
    EXPECT_EQ(1, h3->ref_count());
    EXPECT_EQ(nullptr, h2.get());
}

} // namespace base

} // namespace yukino
