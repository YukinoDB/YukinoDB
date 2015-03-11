#ifndef YUKINO_LSM_CHUNK_H_
#define YUKINO_LSM_CHUNK_H_

#include "lsm/format.h"
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

    void operator = (Chunk &&other);

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
    static Chunk CreateKeyValue(const base::Slice &key,
                                const base::Slice &value);
private:
    uint64_t size_;
    uint32_t key_size_;

    std::unique_ptr<char[]> packed_data_;
};

/**
 * The internal key.
 *
 * +----------+
 * | user-key |
 * +----------+
 * |   tag    | 8 bytes
 * +----------+
 * |  value   |
 * +----------+
 */
class InternalKey : public Chunk {
public:
    explicit InternalKey() : InternalKey(nullptr, 0, 0) {}

    base::Slice user_key_slice() const {
        return base::Slice(user_key(), user_key_size());
    }

    uint32_t user_key_size() const { return key_size() - Tag::kTagSize; }

    const char *user_key() const { return key(); }

    char *mutable_user_key() { return mutable_key(); }

    Tag tag() const;

    static InternalKey CreateKey(const base::Slice &key,
                                 const base::Slice &value,
                                 uint64_t version,
                                 uint8_t flag);

    static InternalKey CreateKey(const base::Slice &key);

    static InternalKey CreateKey(const base::Slice &key, const base::Slice &value);

    static InternalKey CreateKey(const base::Slice &key, uint64_t version);

private:
    InternalKey(char *packed_data, uint64_t size, uint32_t user_key_size);
    InternalKey(char *key, uint32_t user_key_size);
};

static_assert(sizeof(InternalKey) == sizeof(Chunk), "Fixed subclass size.");

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_CHUNK_H_