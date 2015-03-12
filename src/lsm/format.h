#ifndef YUKINO_LSM_FORMAT_H_
#define YUKINO_LSM_FORMAT_H_

#include "yukino/comparator.h"
#include "base/base.h"
#include "glog/logging.h"
#include <stdint.h>

namespace yukino {

namespace lsm {

struct Tag {

    typedef uint64_t EncodedType;

    static const auto kTagSize = sizeof(EncodedType);

    Tag(uint64_t v, uint8_t f)
        : version(v)
        , flag(f) {
    }

    uint64_t version;

    // kFlagValue
    // kFlagDeletion
    uint8_t  flag;

    EncodedType Encode() {
        DCHECK_LT(version, 1ULL << 57);
        return (version << 8) | flag;
    }

    static Tag Decode(EncodedType tag) {
        return Tag(tag >> 8, tag & 0xffULL);
    }
};

class InternalKeyComparator : public Comparator {
public:
    explicit InternalKeyComparator(const Comparator *delegated);
    InternalKeyComparator() : delegated_(nullptr) {}
    InternalKeyComparator(const InternalKeyComparator &other)
        : delegated_(other.delegated_) {}

    virtual ~InternalKeyComparator() override;

    virtual int Compare(const base::Slice& a, const base::Slice& b) const override;

    virtual const char* Name() const override;

    virtual void FindShortestSeparator(std::string* start,
                                       const base::Slice& limit) const override;

    virtual void FindShortSuccessor(std::string* key) const override;

    const Comparator *delegated() const { return delegated_; }

private:
    const Comparator *delegated_;
};

inline std::string LogFileName(const std::string &db_name, uint64_t number) {
    return base::Strings::Sprintf("%s/%llu.log", db_name.c_str(), number);
}

inline std::string TableFileName(const std::string &db_name, uint64_t number) {
    return base::Strings::Sprintf("%s/%llu.sst", db_name.c_str(), number);
}

// MANIFEST
inline std::string ManifestFileName(const std::string &db_name, uint64_t number) {
    return base::Strings::Sprintf("%s/MANIFEST-%llu", db_name.c_str(), number);
}

// CURRENT
inline std::string CurrentFileName(const std::string &db_name) {
    return base::Strings::Sprintf("%s/CURRENT", db_name.c_str());
}

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_FORMAT_H_