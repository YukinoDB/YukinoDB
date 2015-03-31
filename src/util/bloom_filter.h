#ifndef YUKINO_UTIL_BLOOM_FILTER_H_
#define YUKINO_UTIL_BLOOM_FILTER_H_

#include "util/hashs.h"
#include "base/slice.h"
#include "base/base.h"
#include "glog/logging.h"
#include <stdint.h>
#include <vector>

namespace yukino {

namespace util {

template<class T = uint32_t>
class Bitmap : public base::DisableCopyAssign {
public:
    typedef T Bucket;

    static_assert(sizeof(Bucket) >= 2, "T is too small");

    Bitmap(int num_bits)
        : num_bits_(num_bits)
        , bitmap_(capacity(num_bits), Bucket(0)) {
        DCHECK_GT(num_bits_, 0);
    }

    static inline size_t capacity(int num_bits) {
        return (num_bits + kBitWide - 1) / kBitWide;
    }

    bool test(int i) const {
        DCHECK_LT(i, num_bits_);
        return bitmap_[i / kBitWide] & (1U << (i % kBitWide));
    }

    void set(int i) {
        DCHECK_LT(i, num_bits_);
        bitmap_[i / kBitWide] |= (1U << (i % kBitWide));
    }

    void unset(int i) {
        DCHECK_LT(i, num_bits_);
        bitmap_[i / kBitWide] &= ~(1U << (i % kBitWide));
    }

    const std::vector<Bucket> &bits() const { return bitmap_; }

    int num_bits() const { return num_bits_; }

    static constexpr const int kBitWide = sizeof(Bucket) * 8;

private:
    const int num_bits_;
    std::vector<Bucket> bitmap_;
};

struct DefaultBloomFilterPolicy {
    enum Hold : int {
        kNumHashs = 5,
    };

    template<class T>
    bool Apply(const char *data, size_t len, T apply) const {
        if (!apply(StringHash::JS(data, len))) {
            return false;
        }
        if (!apply(StringHash::BKDR(data, len))) {
            return false;
        }
        if (!apply(StringHash::ELF(data, len))) {
            return false;
        }
        if (!apply(StringHash::AP(data, len))) {
            return false;
        }
        if (!apply(StringHash::RS(data, len))) {
            return false;
        }
        return true;
    }
};

/**
 * The simple bloom-filter.
 *
 * filter.Offer(buf, len);
 * filter.Test(buf, len);
 */
template<class Policy = DefaultBloomFilterPolicy>
class BloomFilter : public base::DisableCopyAssign {
public:
    BloomFilter(int num_bits, Policy policy = Policy())
        : bitmap_(num_bits)
        , policy_(policy) {
    }

    void Offer(const base::Slice &buf) { Offer(buf.data(), buf.size()); }

    void Offer(const char *data, size_t size) {
        DCHECK(size > 0 || data != nullptr);
        policy_.Apply(data, size, [this](uint32_t i) {
            bitmap_.set(i % bitmap_.num_bits());
            return true;
        });
    }

    bool Test(const base::Slice &buf) const {
        return Test(buf.data(), buf.size());
    }

    bool Test(const char *data, size_t size) const {
        DCHECK(size > 0 || data != nullptr);
        return policy_.Apply(data, size, [this](int i) {
            return this->bitmap_.test(i % bitmap_.num_bits());
        });
    }

    int64_t ApproximateCounting() const {
        int64_t counter = 0;
        for (auto bucket : bitmap_.bits()) {
            counter += base::Bits::CountOne32(bucket);
        }
        return counter / Policy::kNumHashs;
    }

private:
    Bitmap<> bitmap_;
    Policy policy_;

}; // class BloomFilter

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_BLOOM_FILTER_H_