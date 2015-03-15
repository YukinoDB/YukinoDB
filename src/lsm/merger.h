#ifndef YUKINO_LSM_MERGER_H_
#define YUKINO_LSM_MERGER_H_

#include "yukino/iterator.h"
#include <stddef.h>
#include <memory>

namespace yukino {

class Comparator;

namespace lsm {

class IteratorWarpper;

class MergingIterator : public Iterator {
public:
    MergingIterator(const Comparator *comparator, Iterator **children, size_t n);
    virtual ~MergingIterator() override;

    virtual bool Valid() const override;
    virtual void SeekToFirst() override;
    virtual void SeekToLast() override;
    virtual void Seek(const base::Slice& target) override;
    virtual void Next() override;
    virtual void Prev() override;
    virtual base::Slice key() const override;
    virtual base::Slice value() const override;
    virtual base::Status status() const override;

private:
    void FindSmallest();
    void FindLargest();

    std::unique_ptr<IteratorWarpper[]> children_;
    const size_t num_children_;

    IteratorWarpper *current_;
    const Comparator *comparator_;

    Direction direction_ = kForward;
};

Iterator *CreateMergingIterator(const Comparator *comparator,
                                Iterator **children, size_t n);

} // namespace lsm

} // namespace yukino


#endif // YUKINO_LSM_MERGER_H_