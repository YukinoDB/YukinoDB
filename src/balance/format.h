#ifndef YUKINO_BALANCE_FORMAT_H_
#define YUKINO_BALANCE_FORMAT_H_

#include "base/slice.h"
#include "base/io-inl.h"
#include "base/io.h"
#include "yukino/comparator.h"

namespace yukino {

namespace balance {

struct Config final {
    static const size_t kBtreePageSize      = 4096;
    static const uint32_t kBtreeFileVersion = 0x00010001;
    static const uint32_t kBtreeFileMagic   = 0xa000000b;
    static const int kBtreeOrder       = 128;

    static const size_t kTxIdSize      = sizeof(uint64_t);

    static const uint8_t kPageTypeZero = 0;
    static const uint8_t kPageTypeFull = 1;
    static const uint8_t kPageLeafFlag = 0x80;

    // The B+tree page len is 2 byte wide.
    // So, the kMaxPageSize is max uint16_t.
    static const uint32_t kMaxPageSize = UINT16_MAX;

    Config() = delete;
    ~Config() = delete;
};

/**
 * The flags for key
 */
enum KeyFlag : uint8_t {
    kFlagValue    = 0,
    kFlagDeletion = 1,
    kFlagFind     = kFlagValue,
};

struct ParsedKey {
    base::Slice user_key;
    base::Slice value;

    // Transaction id
    uint64_t    tx_id = 0;

    // Flag: value or deletion
    uint8_t     flag  = kFlagValue;

    // The all key:
    base::Slice key() const {
        return base::Slice(user_key.data(), user_key.size() + sizeof(tx_id));
    }
};

/**
 * Internal Key-Value pair format:
 *
 * | size     | varint32
 * | key-size | varint32
 * | key      | varint-length
 * | tx_id    | 8 bytes
 * | value    | varint-lenght
 */
class InternalKey {
public:

    static ParsedKey Parse(const char *raw);

    static ParsedKey PartialParse(const char *raw, size_t len);

    static const char *Pack(const base::Slice &key, const base::Slice &value);

    static const char *Pack(const base::Slice &key) { return Pack(key, ""); }

    static const char *Pack(const base::Slice &key, uint64_t tx_id, uint8_t flag,
                            const base::Slice &value);
};


/**
 * Compare for internal key
 */
class InternalKeyComparator : public Comparator {
public:
    InternalKeyComparator(const Comparator *delegated)
        : delegated_(delegated) {
    }

    InternalKeyComparator() : InternalKeyComparator(nullptr) {}

    virtual ~InternalKeyComparator() override;

    virtual int Compare(const base::Slice& a,
                        const base::Slice& b) const override;

    virtual const char* Name() const override;

    virtual void FindShortestSeparator(std::string* start,
                                       const base::Slice& limit) const override;

    virtual void FindShortSuccessor(std::string* key) const override;

    const Comparator *delegated() const { return delegated_; }

private:
    const Comparator *delegated_;
};
    
} // namespace balance
    
} // namespace yukino


#endif // YUKINO_BALANCE_FORMAT_H_