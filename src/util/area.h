#ifndef YUKINO_UTIL_AREA_H_
#define YUKINO_UTIL_AREA_H_

#include "base/base.h"
#include <stdlib.h>

namespace yukino {

namespace util {

/**
 * The fast small memory block allocator
 *
 */
class Area : public base::DisableCopyAssign {
public:
    Area(size_t page_size);
    ~Area();

    void *Allocate(size_t size);
    void Free(const void *chunk);
    void Purge();

    inline void Verify() { Free(Allocate(1)); }
    inline size_t ApproximateMemoryUsage() const;

    size_t page_size() const { return page_size_; }
    inline size_t segment_chunk_size(int i) const;

    static const int kNumSegments    = 6;
    static const int kPageBeginShift = 4; // 16
    static const int kLargePageType  = kPageBeginShift;

    static const int kInitByte      = 0xcc;
    static const int kFreedByte     = 0xfe;
private:
    struct PageHead {
        PageHead *next; // For linked list.
        PageHead *prev; // For linked list.
        int32_t  shift; // The type of page: shift of chunk size.
        int32_t  freed; // Freed size.
        void     *free; // Freed chunk.
    };

    inline PageHead *CreatePage(int32_t type);

    bool is_large(size_t size) const {
        return size > segment_chunk_size(kNumSegments - 1);
    }

    inline size_t page_payload_capacity(const PageHead *page) const {
        return ((page_size_ - sizeof(*page)) >> page->shift) << page->shift;
    }

    static void *next(void *p) { return *static_cast<void **>(p); }
    static void set_next(void *p, void *n) { *static_cast<void **>(p) = n; }

    inline PageHead *GetSegment(size_t size);

    inline void InsertHead(PageHead *h, PageHead *x);
    inline void Remove(PageHead *x);
    inline void Init(PageHead *x, int32_t shift);
    inline bool Empty(PageHead *h) { return h->next == h && h->prev == h; }

    static inline void DebugFill(void *p, size_t n, int byte);

    const size_t page_size_;
    const int page_shift_;
    const uintptr_t page_mask_;

    // 0 - large page
    // 1 - 32   : 128
    // 2 - 64   : 64
    // 3 - 128  : 32
    // 4 - 256  : 16
    // 5 - 512  : 8
    PageHead segments_[kNumSegments];
}; // class Area

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_AREA_H_