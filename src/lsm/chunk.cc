#include "lsm/chunk.h"
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

} // namespace lsm
    
} // namespace yukino