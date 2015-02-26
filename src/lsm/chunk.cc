#include "lsm/chunk.h"
#include "lsm/builtin.h"
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

base::Slice InternalKey::user_key_slice() const {
    size_t len = 0;

    auto user_key_size = base::Varint32::Decode(key(), &len);
    DCHECK_LE(len, base::Varint32::kMaxLen);

    auto user_key = key() + len;
    return base::Slice(user_key, user_key_size);
}

uint32_t InternalKey::user_key_size() const {
    size_t len = 0;

    auto rv = base::Varint32::Decode(key(), &len);
    DCHECK_LE(len, base::Varint32::kMaxLen);
    return rv;
}

const char *InternalKey::user_key() const {
    size_t len = 0;

    base::Varint32::Decode(key(), &len);
    DCHECK_LE(len, base::Varint32::kMaxLen);
    return key() + len;
}

char *InternalKey::mutable_user_key() {
    size_t len = 0;

    base::Varint32::Decode(key(), &len);
    DCHECK_LE(len, base::Varint32::kMaxLen);
    return mutable_key() + len;
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

    Tag tag(version, flag);
    auto seq = tag.Encode();

    auto key_size = base::Varint32::Sizeof(static_cast<uint32_t>(key.size()));
    key_size += sizeof(seq) + key.size();
    auto size = key_size + value.size();

    auto packed = new char[size];
    DCHECK_NOTNULL(packed);

    size_t len = 0;
    len += base::Varint32::Encode(packed, static_cast<uint32_t>(key.size()));
    ::memcpy(packed + len, key.data(), key.size());
    len += key.size();

    ::memcpy(packed + len, &seq, sizeof(seq));
    len += sizeof(seq);
    ::memcpy(packed + len, value.data(), value.size());
    len += value.size();

    DCHECK_EQ(size, len);
    return InternalKey(packed, size, static_cast<uint32_t>(key_size));
}

/*static*/ InternalKey InternalKey::CreateKey(const base::Slice &key) {
    auto size = base::Varint32::Sizeof(static_cast<uint32_t>(key.size()));
    size += key.size();

    auto packed = new char[size];
    DCHECK_NOTNULL(packed);

    size_t len = 0;
    len += base::Varint32::Encode(packed, static_cast<uint32_t>(key.size()));
    ::memcpy(packed + len, key.data(), key.size());
    len += key.size();

    DCHECK_EQ(size, len);
    return InternalKey(packed, static_cast<uint32_t>(size));
}

InternalKey::InternalKey(char *packed_data, uint64_t size, uint32_t key_size)
    : Chunk(packed_data, size, key_size) {
}

InternalKey::InternalKey(char *key, uint32_t key_size)
    : Chunk(key, key_size) {
}

} // namespace lsm
    
} // namespace yukino