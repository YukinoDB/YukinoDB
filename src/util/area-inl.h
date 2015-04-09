#ifndef YUKINO_UTIL_AREA_INL_H_
#define YUKINO_UTIL_AREA_INL_H_

#include "util/area.h"
#include "glog/logging.h"

namespace yukino {

namespace util {

inline size_t Area::ApproximateMemoryUsage() const {
    size_t count = 0;
    for (auto page = segments_[0].next; page != &segments_[0];
         page = page->next) {
        count += page->freed;
    }

    for (auto i = 1; i < kNumSegments; ++i) {
        for (auto page = segments_[i].next; page != &segments_[i];
             page = page->next) {
            count += page_size_;
        }
    }

    return count;
}

inline size_t Area::segment_chunk_size(int i) const {
    DCHECK_GT(i, 0);
    DCHECK_LT(i, kNumSegments);

    return (1U << segments_[i].shift);
}

inline Area::PageHead *Area::GetSegment(size_t size) {
    if (is_large(size)) {
        return &segments_[0];
    }
    int shift;
    for (shift = 1; (1 << (kPageBeginShift + shift)) < size; shift++)
        ;
    return &segments_[shift];
}

inline void Area::InsertHead(PageHead *h, PageHead *x) {
    (x)->next = (h)->next;
    (x)->next->prev = x;
    (x)->prev = h;
    (h)->next = x;
}

inline void Area::Remove(PageHead *x) {
    (x)->next->prev = (x)->prev;
    (x)->prev->next = (x)->next;
}

inline void Area::Init(PageHead *x, int32_t shift) {
    x->next  = x;
    x->prev  = x;
    x->shift = shift;
    x->freed = static_cast<int32_t>(page_payload_capacity(x));
}

inline Area::PageHead *Area::CreatePage(int32_t shift) {
    auto page = static_cast<PageHead *>(::valloc(page_size_));
    Init(page, shift);

    auto p = reinterpret_cast<uint8_t *>(page + 1);
    page->free = static_cast<void *>(p);

    const auto chunk_size = (1 << shift);
    const auto end = p + (page_size_ - sizeof(*page));
    for (auto i = p + chunk_size; i < end; i += chunk_size) {
        set_next(p, i);
        p = i;
    }
    set_next(end - chunk_size, nullptr);
    return page;
}

inline void Area::DebugFill(void *p, size_t n, int byte) {
#if defined(DEBUG)
    ::memset(p, byte, n);
#endif
}

} // namespace util

} // namespace yukino

inline void *operator new (size_t size, yukino::util::Area *area) {
    return area->Allocate(size);
}

inline void operator delete (void *chunk, yukino::util::Area *area) {
    return area->Free(chunk);
}
        
#endif // YUKINO_UTIL_AREA_INL_H_