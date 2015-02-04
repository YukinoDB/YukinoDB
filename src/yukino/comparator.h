#ifndef YUKINO_API_COMPARATOR_H_
#define YUKINO_API_COMPARATOR_H_

namespace yukino {

class Comparator {
public:
    virtual ~Comparator();

    // Three-way comparison.  Returns value:
    //   < 0 iff "a" < "b",
    //   == 0 iff "a" == "b",
    //   > 0 iff "a" > "b"
    virtual int Compare(const Slice& a, const Slice& b) const = 0;

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
};

}

#endif // YUKINO_API_COMPARATOR_H_
