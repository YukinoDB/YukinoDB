#ifndef YUKINO_LSM_DB_ITER_H_
#define YUKINO_LSM_DB_ITER_H_

#include "yukino/iterator.h"
#include "base/slice.h"
#include "base/status.h"
#include "glog/logging.h"
#include <string>
#include <memory>
#include <vector>

namespace yukino {

class Comparator;

namespace lsm {

class InternalKeyComparator;

class DBIterator : public Iterator {
public:
    DBIterator(const Comparator *comparator, Iterator *iter, uint64_t version);
    virtual ~DBIterator() override;

    virtual bool Valid() const override;
    virtual void SeekToFirst() override;
    virtual void SeekToLast() override;
    virtual void Seek(const base::Slice& target) override;
    virtual void Next() override;
    virtual void Prev() override;
    virtual base::Slice key() const override;
    virtual base::Slice value() const override;
    virtual base::Status status() const override;

    Iterator *delegated() const {
        return DCHECK_NOTNULL(delegated_.get());
    }

    void ClearSavedValue() {
        if (saved_value_.capacity() > 1048576) {
            std::string empty;
            swap(empty, saved_value_);
        } else {
            saved_value_.clear();
        }
    }

    static void SaveKey(const base::Slice &raw, std::string *key) {
        key->assign(raw.data(), raw.size());
    }

private:
    void FindNextUserEntry(bool skipping, std::string *skip);
    void FindPrevUserEntry();

    const Comparator *comparator_;
    std::unique_ptr<Iterator> delegated_;
    const uint64_t version_;

    base::Status status_;
    std::string saved_key_;
    std::string saved_value_;
    Direction direction_ = kForward;
    bool valid_ = false;
};

Iterator *CreateDBIterator(const InternalKeyComparator *comparator,
                           Iterator **children, size_t n,
                           uint64_t version);

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_DB_ITER_H_