// The YukinoDB Unit Test Suite
//
//  table_test.cc
//
//  Created by Niko Bellic.
//
//
#include "balance/table-inl.h"
#include "balance/table.h"
#include "balance/format.h"
#include "yukino/comparator.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

namespace balance {

class BtreeTableTest : public ::testing::Test {
public:
    BtreeTableTest () {
    }

    virtual void SetUp() override {
        InternalKeyComparator comparator(BytewiseCompartor());
        table_ = new Table(comparator);
    }

    virtual void TearDown() override {
        delete table_;
    }

    Table *table_ = nullptr;
};

TEST_F(BtreeTableTest, Sanity) {

}

} // namespace balance

} // namespace yukino
