#include "yukino/comparator.h"
#include "base/slice.h"
#include "glog/logging.h"
#include <string.h>
#include <mutex>

namespace yukino {

namespace {

std::once_flag create_bytewise_comparator_once;

Comparator *bytewise_comparator;

}

Comparator::~Comparator() {
}

class BytewiseComparator : public Comparator {
public:
    virtual int Compare(const base::Slice& a, const base::Slice& b) const override {
        return a.compare(b);
    }

    virtual const char *Name() const override {
        return "yukino.BitwiseComparator";
    }

    virtual void FindShortestSeparator(std::string* start,
                                       const base::Slice& limit) const override {
        // Find length of common prefix
        size_t min_length = std::min(start->size(), limit.size());
        size_t diff_index = 0;
        while ((diff_index < min_length) &&
               ((*start)[diff_index] == limit[diff_index])) {
            diff_index++;
        }

        if (diff_index >= min_length) {
            // Do not shorten if one string is a prefix of the other
        } else {
            uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
            if (diff_byte < static_cast<uint8_t>(0xff) &&
                diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
                (*start)[diff_index]++;
                start->resize(diff_index + 1);
                assert(Compare(*start, limit) < 0);
            }
        }
    }

    virtual void FindShortSuccessor(std::string* key) const override {
        // Find first character that can be incremented
        size_t n = key->size();
        for (size_t i = 0; i < n; i++) {
            const uint8_t byte = (*key)[i];
            if (byte != static_cast<uint8_t>(0xff)) {
                (*key)[i] = byte + 1;
                key->resize(i+1);
                return;
            }
        }
        // *key is a run of 0xffs.  Leave it alone.
    }
};


Comparator *CreateBytewiseComparator() {

    return new BytewiseComparator();
}

Comparator *BytewiseCompartor() {
    std::call_once(create_bytewise_comparator_once, [] () {
        DCHECK(NULL == bytewise_comparator);

        bytewise_comparator = CreateBytewiseComparator();
    });

    return DCHECK_NOTNULL(bytewise_comparator);
}

} // namespace yukino