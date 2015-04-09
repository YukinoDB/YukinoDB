#ifndef YUKINO_BALANCE_TABLE_INL_H_
#define YUKINO_BALANCE_TABLE_INL_H_

#include "balance/table.h"

namespace yukino {

namespace balance {

#if defined(CHECK_OK)
#   undef CHECK_OK
#else
#   define CHECK_OK(expr) rs = (expr); if (!rs.ok()) return rs
#endif

inline uint64_t Table::Addr2Index(uint64_t addr) {
    DCHECK_LT(addr, file_size_);
    DCHECK_EQ(0, addr % page_size_);
    return (addr / page_size_) - 1;
}

inline bool Table::TestUsed(uint64_t addr) {
    auto i = Addr2Index(addr);
    return bitmap_.test(static_cast<int>(i));
}

inline void Table::SetUsed(uint64_t addr) {
    auto i = Addr2Index(addr);
    bitmap_.set(static_cast<int>(i));
}

inline void Table::ClearUsed(uint64_t addr) {
    auto i = Addr2Index(addr);
    return bitmap_.unset(static_cast<int>(i));
}

inline void Table::ClearPage(const Page *page) const {
    for (const auto &entry : page->entries) {
        delete[] entry.key;
    }
}

inline Table::Page *Table::GetPage(uint64_t id, bool cached) {
    Page *page = nullptr;
    auto rs = CachedGet(id, &page, cached);
    return page;
}

inline const char *Table::DuplicateKey(const char *key) {
    auto parsed = InternalKey::Parse(key);

    // Only kept key.
    return InternalKey::Pack(parsed.key(), "");
}

inline int Table::Comparator::operator()(const char *a, const char *b) const {
    auto j = InternalKey::Parse(a);
    auto k = InternalKey::Parse(b);
    return comparator.Compare(j.key(), k.key());
}

inline float Table::ApproximateLargeRatio() const {
    auto num_pages = static_cast<float>(id_map_.size());
    float num_blocks = 0;

    for (auto bits : bitmap_.bits()) {
        num_blocks += base::Bits::CountOne32(bits);
    }
    DCHECK_NE(0, num_pages);
    return num_blocks / num_pages;
}

inline float Table::ApproximateUsageRatio() const {
    auto num_blocks = static_cast<float>(file_size_ / page_size_ - 1);
    float num_used_blocks = 0;

    for (auto bits : bitmap_.bits()) {
        num_used_blocks += base::Bits::CountOne32(bits);
    }
    DCHECK_NE(0, num_used_blocks);
    return num_used_blocks / num_blocks;
}

namespace {

inline uint64_t NowMicroseconds() {
    using namespace std::chrono;

    auto now = high_resolution_clock::now();
    return duration_cast<microseconds>(now.time_since_epoch()).count();
}
    
} // namespace

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_TABLE_INL_H_