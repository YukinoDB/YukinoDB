#include "lsm/chunk.h"
#include "lsm/builtin.h"
#include "base/io.h"
#include "base/varint_encoding.h"
#include "glog/logging.h"
#include <string.h>

namespace yukino {

namespace lsm {

Chunk::Chunk(char *packed_data, uint64_t size, uint32_t key_size)
    : packed_data_(packed_data)
    , size_(size)
    , key_size_(key_size) {
}

Chunk::Chunk(char *key, uint32_t key_size)
    : packed_data_(key)
    , size_(key_size)
    , key_size_(key_size) {
}

Chunk::Chunk(Chunk &&other)
    : size_(other.size_)
    , key_size_(other.key_size_)
    , packed_data_(other.packed_data_.release()) {
}

Chunk::~Chunk() {
}

void Chunk::operator = (Chunk &&other) {
    size_        = other.size_;
    key_size_    = other.key_size_;
    packed_data_ = std::move(other.packed_data_);
}

/*static*/ Chunk Chunk::CreateKey(const base::Slice &key) {
    char *dup = new char[key.size()];

    ::memcpy(DCHECK_NOTNULL(dup), key.data(), key.size());
    return std::move(Chunk(dup, static_cast<uint32_t>(key.size())));
}

/*static*/ Chunk Chunk::CreateKeyValue(const base::Slice &key,
                                       const base::Slice &value) {
    char *dup = new char[key.size() + value.size()];

    ::memcpy(DCHECK_NOTNULL(dup), key.data(), key.size());
    ::memcpy(dup + key.size(), value.data(), value.size());

    return std::move(Chunk(dup, key.size() + value.size(),
                           static_cast<uint32_t>(key.size())));
}

Tag InternalKey::tag() const {
    auto user_key = user_key_slice();
    auto base = user_key.data() + user_key.size();

    return Tag::Decode(*reinterpret_cast<const uint64_t *>(base));
}

/*static*/ InternalKey InternalKey::CreateKey(const base::Slice &key,
                                              const base::Slice &value,
                                              uint64_t version,
                                              uint8_t flag) {
    DCHECK(flag == kFlagDeletion || flag == kFlagValue);

    auto key_size = key.size() + Tag::kTagSize;
    auto size = key_size + value.size();

    base::BufferedWriter buf(size);
    buf.Write(key.data(), key.size(), nullptr);
    buf.WriteFixed64(Tag(version, flag).Encode());
    buf.Write(value.data(), value.size(), nullptr);

    DCHECK_EQ(size, buf.len());
    return InternalKey(buf.Drop(), size, static_cast<uint32_t>(key.size()));
}

/*static*/ InternalKey InternalKey::CreateKey(const base::Slice &key,
                                              uint64_t version) {
    auto size = key.size() + Tag::kTagSize;

    base::BufferedWriter buf(size);
    buf.Write(key.data(), key.size(), nullptr);
    buf.WriteFixed64(Tag(version, 0).Encode());

    DCHECK_EQ(size, buf.len());
    return InternalKey(buf.Drop(), static_cast<uint32_t>(key.size()));
}

/*static*/ InternalKey InternalKey::CreateKey(const base::Slice &key) {
    auto packed = new char[key.size()];
    DCHECK_NOTNULL(packed);

    ::memcpy(packed, key.data(), key.size());
    return InternalKey(packed,
                       static_cast<uint32_t>(key.size() - Tag::kTagSize));
}

/*static*/ InternalKey InternalKey::CreateKey(const base::Slice &key,
                                              const base::Slice &value) {
    auto size = key.size() + value.size();

    base::BufferedWriter buf(size);
    buf.Write(key.data(), key.size(), nullptr);
    buf.Write(value.data(), key.size(), nullptr);

    DCHECK_EQ(size, buf.len());
    return InternalKey(buf.Drop(),
                       static_cast<uint32_t>(key.size() - Tag::kTagSize));
}

InternalKey::InternalKey(char *packed_data, uint64_t size, uint32_t user_key_size)
    : Chunk(packed_data, size, user_key_size + Tag::kTagSize) {
}

InternalKey::InternalKey(char *key, uint32_t user_key_size)
    : Chunk(key, user_key_size + Tag::kTagSize) {
}

} // namespace lsm
    
} // namespace yukino