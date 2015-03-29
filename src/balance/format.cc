#include "balance/format.h"
#include "base/io.h"
#include "base/varint_encoding.h"
#include "glog/logging.h"

namespace yukino {

namespace balance {

/*virtual*/ InternalKeyComparator::~InternalKeyComparator() {

}

/*virtual*/ int InternalKeyComparator::Compare(const base::Slice& a,
                                               const base::Slice& b) const {
    base::BufferedReader rda(a.data(), a.size()), rdb(b.data(), b.size());
    auto key_a = rda.Read(a.size() - Config::kTxIdSize);
    auto key_b = rdb.Read(b.size() - Config::kTxIdSize);

    auto rv = DCHECK_NOTNULL(delegated_)->Compare(key_a, key_b);
    if (rv == 0) {
        auto tx_id_a = rda.ReadFixed64();
        auto tx_id_b = rdb.ReadFixed64();

        if (tx_id_a > tx_id_b) {
            rv = -1;
        } else if (tx_id_a < tx_id_b) {
            rv = 1;
        } else {
            rv = 0;
        }
    }
    return rv;
}

/*virtual*/
const char* InternalKeyComparator::Name() const {
    return "yukino.balance.InternalKeyComparator";
}

/*virtual*/
void InternalKeyComparator::FindShortestSeparator(std::string* /*start*/,
                                                  const base::Slice& /*limit*/)
const {
}

/*virtual*/
void InternalKeyComparator::FindShortSuccessor(std::string* /*key*/) const {
}

/*static*/
const char *
InternalKey::Pack(const base::Slice &key, const base::Slice &value) {
    auto key_len = static_cast<uint32_t>(key.size());
    auto len = key_len + static_cast<uint32_t>(value.size());

    auto size = base::Varint32::Sizeof(len);
    size += base::Varint32::Sizeof(key_len);
    size += len;

    base::BufferedWriter w(size);
    w.WriteVarint32(len, nullptr);
    w.WriteVarint32(key_len, nullptr);
    w.Write(key.data(), key.size(), nullptr);
    w.Write(value.data(), value.size(), nullptr);

    DCHECK_EQ(size, w.active());
    return w.Drop();
}

/*static*/
const char *
InternalKey::Pack(const base::Slice &key, uint64_t tx_id, uint8_t flag,
                  const base::Slice &value) {
    auto key_len = static_cast<uint32_t>(key.size() + sizeof(tx_id));
    auto len = key_len + static_cast<uint32_t>(value.size());

    auto size = base::Varint32::Sizeof(len);
    size += base::Varint32::Sizeof(key_len);
    size += len;

    base::BufferedWriter w(size);
    w.WriteVarint32(len, nullptr);
    w.WriteVarint32(key_len, nullptr);
    w.Write(key.data(), key.size(), nullptr);
    w.WriteFixed64(tx_id << 8 | flag);
    w.Write(value.data(), value.size(), nullptr);

    DCHECK_EQ(size, w.active());
    return w.Drop();
}

} // namespace balance
    
} // namespace yukino