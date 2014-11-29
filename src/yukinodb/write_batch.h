#ifndef YUKINODB_API_WRITE_BATCH_H_
#define YUKINODB_API_WRITE_BATCH_H_

namespace yukinodb  {

class Slice;

class WriteBatch {
public:
    WriteBatch();
    ~WriteBatch();

    // Store the mapping "key->value" in the database.
    void Put(const Slice& key, const Slice& value);

    // If the database contains a mapping for "key", erase it.  Else do nothing.
    void Delete(const Slice& key);

    // Clear all updates buffered in this batch.
    void Clear();
    
};

} // namespace yukinodb

#endif // YUKINODB_API_WRITE_BATCH_H_