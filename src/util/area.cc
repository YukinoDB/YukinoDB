#include "util/area-inl.h"
#include "util/area.h"
#include "glog/logging.h"

namespace yukino {

namespace util {

Area::Area(size_t page_size)
    : page_size_(page_size)
    , page_shift_(63 - base::Bits::CountLeadingZeros64(page_size))
    , page_mask_(~((1ull << page_shift_) - 1)) {

    DCHECK_EQ(page_size_, 1ull << page_shift_);

    for (auto i = 0; i < kNumSegments; ++i) {
        Init(&segments_[i], kPageBeginShift + i);
    }
}

Area::~Area() {
    Purge();
}

void *Area::Allocate(size_t size) {
    void *rv = nullptr;
    if (is_large(size)) {
        size += sizeof(PageHead);
        auto page = static_cast<PageHead*>(valloc(size));
        if (!page) {
            return nullptr;
        }
        Init(page, kLargePageType);
        page->freed = static_cast<int32_t>(size);
        InsertHead(&segments_[0], page);

        rv = static_cast<void *>(page + 1);
    } else if (size > 0) {
        auto segment = GetSegment(size);

        if (Empty(segment) || segment->next->freed < size) {
            auto page = CreatePage(segment->shift);
            InsertHead(segment, page);
        }

        auto page = segment->next;
        rv = page->free;
        page->free   = next(page->free);
        page->freed -= (1 << page->shift);
    }

    DebugFill(rv, size, kInitByte);
    return rv;
}

void Area::Free(const void *p) {
    if (!p) { // Ignore null pointer.
        return;
    }

    auto chunk = const_cast<void *>(p);
    auto page = reinterpret_cast<PageHead *>(reinterpret_cast<uintptr_t>(chunk) &
                                             page_mask_);

    auto chunk_size = (1 << page->shift);
    auto segment = GetSegment(chunk_size);
    DebugFill(chunk, chunk_size, kFreedByte);
    if (page->shift == kLargePageType) {
        Remove(page);
        ::free(page);
        return;
    }

    set_next(chunk, page->free);
    page->free   = chunk;
    page->freed += chunk_size;

    if (page->freed == page_payload_capacity(page)) {
        Remove(page);
        ::free(page);
    } else if (page->freed > segment->next->freed) {
        Remove(page);
        InsertHead(segment, page);
    }
}

void Area::Purge() {
    for (auto i = 0; i < kNumSegments; ++i) {
        while (!Empty(&segments_[i])) {
            auto purge = segments_[i].next;
            Remove(purge);
            ::free(purge);
        }
    }
}

} // namespace util

} // namespace yukino