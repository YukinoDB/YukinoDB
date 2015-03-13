#include "lsm/chunk.h"
#include "lsm/memory_table.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "yukino/iterator.h"
#include "base/slice.h"
#include "base/status.h"
#include "base/varint_encoding.h"

namespace yukino {

namespace lsm {

int MemoryTable::KeyComparator::operator()(const InternalKey &a,
                                           const InternalKey &b) const {
    return comparator_.Compare(a.key_slice(), b.key_slice());
}

MemoryTable::MemoryTable(InternalKeyComparator comparator)
    : comparator_(comparator)
    , table_(KeyComparator(comparator)) {
}

void MemoryTable::Put(const base::Slice &key, const base::Slice &value,
                      uint64_t version,
                      uint8_t flag) {
    auto internal_key = InternalKey::CreateKey(key, value, version, flag);

    memory_usage_size_.fetch_add(internal_key.size(),
                                 std::memory_order_release);
    table_.Put(std::move(internal_key));
}

base::Status MemoryTable::Get(const base::Slice &key, uint64_t version,
                 std::string *value) {
    auto lookup_key = InternalKey::CreateKey(key, version);
    return Get(lookup_key, value);
}

base::Status MemoryTable::Get(const InternalKey &key, std::string *value) {

    Table::Iterator iter(&table_);
    iter.Seek(key);
    if (!iter.Valid() ||
        comparator_.delegated()->Compare(key.user_key_slice(),
                                         iter.key().user_key_slice()) != 0) {
        return base::Status::NotFound("MemoryTable::Get()");
    }

    auto tag = iter.key().tag();
    switch (tag.flag) {
        case kFlagValue:
            value->assign(iter.key().value_slice().ToString());
            break;

        case kFlagDeletion:
            return base::Status::NotFound("InternalKey deletion");

        default:
            DLOG(FATAL) << "noreached";
            break;
    }
    
    return base::Status::OK();
}

class MemoryTableIterator : public Iterator {
public:
    MemoryTableIterator(const MemoryTable::Table::Iterator &iter)
        : iter_(iter) {}
    virtual ~MemoryTableIterator() override {}
    virtual bool Valid() const override { return iter_.Valid(); }
    virtual void SeekToFirst() override { iter_.SeekToFirst(); }
    virtual void SeekToLast() override { iter_.SeekToLast(); }
    virtual void Seek(const base::Slice& target) override {
        return iter_.Seek(InternalKey::CreateKey(target));
    }
    virtual void Next() override { iter_.Next(); }
    virtual void Prev() override { iter_.Prev(); }
    virtual base::Slice key() const override {
        return iter_.key().key_slice();
    }
    virtual base::Slice value() const override {
        return iter_.key().value_slice();
    }
    virtual base::Status status() const override {
        return base::Status::OK();
    }

private:
    MemoryTable::Table::Iterator iter_;

};

Iterator *MemoryTable::NewIterator() {
    return new MemoryTableIterator(MemoryTable::Table::Iterator(&table_));
}

} // namespace lsm

} // namespace yukino