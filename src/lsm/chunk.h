#ifndef YUKINO_LSM_CHUNK_H_
#define YUKINO_LSM_CHUNK_H_

#include "base/slice.h"
#include "base/base.h"
#include <memory>

namespace yukino {

namespace lsm {

/**
 * The internal key-value pair, they are all byte array.
 *
 */
class Chunk : public base::DisableCopyAssign {
public:
    Chunk(char *packed_data, uint64_t size, uint32_t key_size);
    Chunk(char *key, uint32_t key_size);
    Chunk(Chunk &&);
    ~Chunk();

    base::Slice key_slice() const { return base::Slice(key(), key_size()); }

    base::Slice value_slice() const {
        return base::Slice(value(), value_size());
    }

    uint64_t size() const { return size_; }
    uint32_t key_size() const { return key_size_; }
    uint64_t value_size() const { return size_ - key_size_; }

    const char *key() const { return packed_data_.get(); }
    const char *value() const { return packed_data_.get() + key_size(); }

    char *mutable_key() { return packed_data_.get(); }
    char *mutable_value() { return packed_data_.get() + key_size(); }

    static Chunk CreateKey(const base::Slice &key);
//    static Chunk &&CreateKeyValue(const base::Slice &key,
//                                  const base::Slice &value);
private:
    uint64_t size_;
    uint32_t key_size_;

    std::unique_ptr<char[]> packed_data_;
};

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_CHUNK_H_