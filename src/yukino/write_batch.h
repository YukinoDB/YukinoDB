#ifndef YUKINO_API_WRITE_BATCH_H_
#define YUKINO_API_WRITE_BATCH_H_

#include "base/io-inl.h"
#include "base/io.h"
#include "base/status.h"
#include "base/slice.h"

namespace yukino  {

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

    base::Status Iterate(Handler* handler) const {
        return Iterate(redo_.buf(), redo_.len(), handler);
    }

    static base::Status Iterate(const void *buf, size_t len, Handler *handler);

    base::Slice buf() const {
        return base::Slice(redo_.buf(), redo_.len());
    }

private:
    base::BufferedWriter redo_;
    // Intentionally copyable
};

} // namespace yukino

#endif // YUKINO_API_WRITE_BATCH_H_
