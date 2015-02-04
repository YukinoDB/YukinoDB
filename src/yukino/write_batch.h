#ifndef YUKINO_API_WRITE_BATCH_H_
#define YUKINO_API_WRITE_BATCH_H_

namespace yukino  {

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

} // namespace yukino

#endif // YUKINO_API_WRITE_BATCH_H_
