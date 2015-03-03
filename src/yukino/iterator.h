#ifndef YUKINO_API_ITERATOR_H_
#define YUKINO_API_ITERATOR_H_

#include "base/base.h"

namespace yukino {

namespace base {

class Slice;
class Status;

} // namespace base

class Iterator : public base::DisableCopyAssign {
public:
    enum Direction {
        kForward,
        kReserve,
    };

    Iterator();
    virtual ~Iterator();

    // An iterator is either positioned at a key/value pair, or
    // not valid.  This method returns true iff the iterator is valid.
    virtual bool Valid() const = 0;

    // Position at the first key in the source.  The iterator is Valid()
    // after this call iff the source is not empty.
    virtual void SeekToFirst() = 0;

    // Position at the last key in the source.  The iterator is
    // Valid() after this call iff the source is not empty.
    virtual void SeekToLast() = 0;

    // Position at the first key in the source that is at or past target.
    // The iterator is Valid() after this call iff the source contains
    // an entry that comes at or past target.
    virtual void Seek(const base::Slice& target) = 0;

    // Moves to the next entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the last entry in the source.
    // REQUIRES: Valid()
    virtual void Next() = 0;

    // Moves to the previous entry in the source.  After this call, Valid() is
    // true iff the iterator was not positioned at the first entry in source.
    // REQUIRES: Valid()
    virtual void Prev() = 0;

    // Return the key for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    virtual base::Slice key() const = 0;

    // Return the value for the current entry.  The underlying storage for
    // the returned slice is valid only until the next modification of
    // the iterator.
    // REQUIRES: Valid()
    virtual base::Slice value() const = 0;

    // If an error has occurred, return it.  Else return an ok status.
    virtual base::Status status() const = 0;

    // Clients are allowed to register function/arg1/arg2 triples that
    // will be invoked when this iterator is destroyed.
    //
    // Note that unlike all of the preceding methods, this method is
    // not abstract and therefore clients should not override it.
    typedef void (*CleanupFunction)(void* arg1, void* arg2);
    void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);
    
private:
    struct Cleanup {
        CleanupFunction function;
        void* arg1;
        void* arg2;
        Cleanup* next;
    };
    Cleanup *cleanup_;

};

Iterator *EmptyIterator();

Iterator *CreateErrorIterator(const base::Status &err);

} // namespace yukino

#endif // YUKINO_API_ITERATOR_H_