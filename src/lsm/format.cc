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

} // namespace lsm

} // namespace yukino