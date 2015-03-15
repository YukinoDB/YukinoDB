#include "lsm/format.h"
#include "base/io.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

InternalKeyComparator::InternalKeyComparator(const Comparator *delegated)
    : delegated_(DCHECK_NOTNULL(delegated)) {

}

InternalKeyComparator::~InternalKeyComparator() {

}

int InternalKeyComparator::Compare(const base::Slice& a, const base::Slice& b) const {
    base::BufferedReader ra(a.data(), a.size());
    base::BufferedReader rb(b.data(), b.size());

    auto rv = delegated_->Compare(ra.Read(a.size() - Tag::kTagSize),
                                  rb.Read(b.size() - Tag::kTagSize));
    if (rv != 0) {
        return rv;
    }

    auto tag_a = Tag::Decode(ra.ReadFixed64());
    auto tag_b = Tag::Decode(rb.ReadFixed64());

    DCHECK_EQ(0, ra.active());
    DCHECK_EQ(0, rb.active());
    return static_cast<int>(tag_b.version - tag_a.version);
}

const char *InternalKeyComparator::Name() const {
    return "yukino.lsm.InternalKeyComparator";
}

void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const base::Slice& limit) const {
    return delegated_->FindShortestSeparator(start, limit);
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
    return delegated_->FindShortSuccessor(key);
}

/*static*/ std::tuple<Files::Kind, uint64_t>
Files::ParseName(const std::string &name) {
    auto rv = std::make_tuple(kUnknown, -1);

    if (name == kLockName) {
        rv = std::make_tuple(kLock, -1);
    } else if (name == kCurrentName) {
        rv = std::make_tuple(kCurrent, -1);
    } else if (name.find(kManifestPrefix) == 0) {
        // MANIFEST-1
        auto buf = name.substr(kManifestPrefixLength);
        if (IsNumber(buf)) {
            rv = std::make_tuple(kManifest, ::atoll(buf.c_str()));
        }
    } else if (name.rfind(kLogPostfix) == (name.length() - kLogPostfixLength)) {
        // 1.log len(5) postfix(4)
        auto buf = name.substr(0, name.length() - kLogPostfixLength);
        if (IsNumber(buf)) {
            rv = std::make_tuple(kLog, ::atoll(buf.c_str()));
        }
    } else if (name.rfind(kTablePostfix) ==
               (name.length() - kTablePostfixLength)) {
        // 1.sst len(5) postfix(4)
        auto buf = name.substr(0, name.length() - kTablePostfixLength);
        if (IsNumber(buf)) {
            rv = std::make_tuple(kTable, ::atoll(buf.c_str()));
        }
    }

    return rv;
}

} // namespace lsm

} // namespace yukino