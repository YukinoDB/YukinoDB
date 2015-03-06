#ifndef YUKINO_API_WRITE_BATCH_H_
#define YUKINO_API_WRITE_BATCH_H_

#include "base/io.h"
#include "base/status.h"

namespace yukino  {

namespace base {

class Slice;

} // namespace base

class WriteBatch {
public:
    WriteBatch();
    ~WriteBatch();

    // Store the mapping "key->value" in the database.
    void Put(const base::Slice& key, const base::Slice& value);

    // If the database contains a mapping for "key", erase it.  Else do nothing.
    void Delete(const base::Slice& key);

    // Clear all updates buffered in this batch.
    void Clear();

    // Support for iterating over the contents of a batch.
    class Handler {
    public:
        virtual ~Handler();
        virtual void Put(const base::Slice& key, const base::Slice& value) = 0;
        virtual void Delete(const base::Slice& key) = 0;
    };
    base::Status Iterate(Handler* handler) const;

private:
    base::BufferedWriter redo_;
    // Intentionally copyable
};

} // namespace yukino

#endif // YUKINO_API_WRITE_BATCH_H_
