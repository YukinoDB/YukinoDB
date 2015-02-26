#ifndef YUKINO_API_COMPARATOR_H_
#define YUKINO_API_COMPARATOR_H_

#include "base/base.h"
#include <string>

namespace yukino {

namespace base {

class Slice;

} // namespace base

class Comparator {
public:
    virtual ~Comparator();

    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    virtual int Compare(const base::Slice& a, const base::Slice& b) const = 0;

    // The name of the comparator.  Used to check for comparator
    // mismatches (i.e., a DB created with one comparator is
    // accessed using a different comparator.
    //
    // The client of this package should switch to a new name whenever
    // the comparator implementation changes in a way that will cause
    // the relative ordering of any two keys to change.
    //
    // Names starting with "yukino." are reserved and should not be used
    // by any clients of this package.
    virtual const char* Name() const = 0;

    // Advanced functions: these are used to reduce the space requirements
    // for internal data structures like index blocks.

    // If *start < limit, changes *start to a short string in [start,limit).
    // Simple comparator implementations may return with *start unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    virtual void FindShortestSeparator(std::string* start,
                                       const base::Slice& limit) const = 0;

    // Changes *key to a short string >= *key.
    // Simple comparator implementations may return with *key unchanged,
    // i.e., an implementation of this method that does nothing is correct.
    virtual void FindShortSuccessor(std::string* key) const = 0;

};

Comparator *CreateBytewiseComparator();

Comparator *BytewiseCompartor();

} // namespace yukino

#endif // YUKINO_API_COMPARATOR_H_
