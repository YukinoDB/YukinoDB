#ifndef YUKINO_API_DB_H_
#define YUKINO_API_DB_H_

#include "base/status.h"
#include "base/base.h"

namespace yukino {

namespace base {

class Slice;

} // namespace base

class Iterator;
class ReadOptions;
class WriteOptions;
class WriteBatch;
class Options;
class Snapshot;

class DB : public base::DisableCopyAssign {
public:
    // Open the database with the specified "name".
    // Stores a pointer to a heap-allocated database in *dbptr and returns
    // OK on success.
    // Stores NULL in *dbptr and returns a non-OK status on error.
    // Caller should delete *dbptr when it is no longer needed.
    static base::Status Open(const Options& options,
                             const std::string& name,
                             DB** dbptr);

    DB() { }
    virtual ~DB();

    // Set the database entry for "key" to "value".  Returns OK on success,
    // and a non-OK status on error.
    // Note: consider setting options.sync = true.
    virtual base::Status Put(const WriteOptions& options,
                             const base::Slice& key,
                             const base::Slice& value) = 0;

    // Remove the database entry (if any) for "key".  Returns OK on
    // success, and a non-OK status on error.  It is not an error if "key"
    // did not exist in the database.
    // Note: consider setting options.sync = true.
    virtual base::Status Delete(const WriteOptions& options,
                                const base::Slice& key) = 0;

    // Apply the specified updates to the database.
    // Returns OK on success, non-OK on failure.
    // Note: consider setting options.sync = true.
    virtual base::Status Write(const WriteOptions& options,
                               WriteBatch* updates) = 0;

    // If the database contains an entry for "key" store the
    // corresponding value in *value and return OK.
    //
    // If there is no entry for "key" leave *value unchanged and return
    // a status for which Status::IsNotFound() returns true.
    //
    // May return some other Status on an error.
    virtual base::Status Get(const ReadOptions& options,
                             const base::Slice& key, std::string* value) = 0;

    // Return a heap-allocated iterator over the contents of the database.
    // The result of NewIterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    //
    // Caller should delete the iterator when it is no longer needed.
    // The returned iterator should be deleted before this db is deleted.
    virtual Iterator* NewIterator(const ReadOptions& options) = 0;

    // Return a handle to the current DB state.  Iterators created with
    // this handle will all observe a stable snapshot of the current DB
    // state.  The caller must call ReleaseSnapshot(result) when the
    // snapshot is no longer needed.
    virtual const Snapshot* GetSnapshot() = 0;

    // Release a previously acquired snapshot.  The caller must not
    // use "snapshot" after this call.
    virtual void ReleaseSnapshot(const Snapshot* snapshot) = 0;
};

class Snapshot : public base::DisableCopyAssign {
public:
    Snapshot() {}
    virtual ~Snapshot();
};

class Transaction : public base::DisableCopyAssign {
public:
    Transaction() {}
    virtual ~Transaction();
};

} //namespace yukino

#endif // YUKINO_API_DB_H_
