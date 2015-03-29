#ifndef YUKINO_BALANCE_TABLE_INL_H_
#define YUKINO_BALANCE_TABLE_INL_H_

#include "balance/table.h"

namespace yukino {

namespace balance {

#define CHECK_OK(expr) rs = (expr); if (!rs.ok()) return rs

inline uint64_t Table::Addr2Index(uint64_t addr) {
    DCHECK_LT(addr, file_size_);
    DCHECK_EQ(0, addr % page_size_);
    return (addr / page_size_) - 1;
}

inline bool Table::TestUsed(uint64_t addr) {
    auto i = Addr2Index(addr);
    DCHECK_LT((i + 31) / 32, bitmap_.size());
    return bitmap_[(i + 31) / 32] & (1 << (i % 32));
}

inline void Table::SetUsed(uint64_t addr) {
    auto i = Addr2Index(addr);
    DCHECK_LT((i + 31) / 32, bitmap_.size());
    bitmap_[(i + 31) / 32] |= (1 << (i % 32));
}

inline void Table::ClearUsed(uint64_t addr) {
    auto i = Addr2Index(addr);
    DCHECK_LT((i + 31) / 32, bitmap_.size());
    bitmap_[(i + 31) / 32] &= ~(1 << (i % 32));
}

inline Table::PageTy *Table::AllocatePage(int num_entries) {
    auto page_id = next_page_id_++;

    auto page = new PageTy(page_id, num_entries);
    id_map_[page_id] = 0;
    return page;
}

inline void Table::FreePage(const PageTy *page) {
    if (page) {
        FreeRoomForPage(page->id);
        id_map_[page->id] = 0;
    }
    delete page;
}

inline int Table::Comparator::operator()(const char *a, const char *b) {
    auto j = InternalKey::Parse(a);
    auto k = InternalKey::Parse(b);
    return comparator.Compare(j.key(), k.key());
}

namespace {

inline uint64_t NowMicroseconds() {
    using namespace std::chrono;

    auto now = high_resolution_clock::now();
    return duration_cast<microseconds>(now.time_since_epoch()).count();
}

inline int FindFirstZero(uint32_t bits) {
    uint32_t test = 1;
    for (auto i = 0; i < 32; ++i) {
        if ((bits & test) == 0) {
            return i;
        }
        test <<= 1;
    }
    return -1;
}
    
} // namespace

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_TABLE_INL_H_