#include "lsm/chunk.h"
#include "lsm/memory_table.h"
#include "lsm/format.h"
#include "yukino/iterator.h"
#include "base/slice.h"
#include "base/status.h"
#include "base/varint_encoding.h"

namespace yukino {

namespace lsm {

int MemoryTable::KeyComparator::operator()(const InternalKey &a,
                                           const InternalKey &b) const {
    return comparator_->Compare(a.key_slice(), b.key_slice());
}

MemoryTable::MemoryTable(const InternalKeyComparator *comparator)
    : comparator_(DCHECK_NOTNULL(comparator))
    , table_(KeyComparator(comparator)) {
}

void MemoryTable::Put(const base::Slice &key, const base::Slice &value,
                      uint64_t version,
                      uint8_t flag) {
    auto internal_key = InternalKey::CreateKey(key, value, version, flag);
    table_.Put(std::move(internal_key));
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