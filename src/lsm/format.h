#ifndef YUKINO_LSM_FORMAT_H_
#define YUKINO_LSM_FORMAT_H_

#include "yukino/comparator.h"
#include "glog/logging.h"
#include <stdint.h>

namespace yukino {

namespace lsm {

struct Tag {

    static const auto kTagSize = sizeof(uint64_t);

    Tag(uint64_t v, uint8_t f)
        : version(v)
        , flag(f) {
    }

    uint64_t version;

    // kFlagValue
    // kFlagDeletion
    uint8_t  flag;

    uint64_t Encode() {
        DCHECK_LT(version, 1ULL << 57);
        return (version << 8) | flag;
    }

    static Tag Decode(uint64_t tag) {
        return Tag(tag >> 8, tag & 0xffULL);
    }
};

class InternalKeyComparator : public Comparator {
public:
    InternalKeyComparator(Comparator *delegated);

    virtual ~InternalKeyComparator() override;

    virtual int Compare(const base::Slice& a, const base::Slice& b) const override;

    virtual const char* Name() const override;

    virtual void FindShortestSeparator(std::string* start,
                                       const base::Slice& limit) const override;

    virtual void FindShortSuccessor(std::string* key) const override;

    Comparator *delegated() const { return delegated_; }

private:
    Comparator *delegated_;
};

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_FORMAT_H_