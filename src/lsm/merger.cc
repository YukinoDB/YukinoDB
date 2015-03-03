#include "base/slice.h"
#include "base/status.h"
#include "yukino/comparator.h"
#include "lsm/merger.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

class IteratorWarpper : public Iterator {
public:
    IteratorWarpper() {}

    void set_delegated(Iterator *delegated) {
        delegated_ = std::move(std::unique_ptr<Iterator>(delegated));
    }

    virtual bool Valid() const override { return valid_; }
    virtual void SeekToFirst() override {
        delegated_->SeekToFirst();
        Update();
    }
    virtual void SeekToLast() override {
        delegated_->SeekToLast();
        Update();
    }
    virtual void Seek(const base::Slice& target) override {
        delegated_->Seek(target);
        Update();
    }
    virtual void Next() override {
        delegated_->Next();
        Update();
    }
    virtual void Prev() override {
        delegated_->Prev();
        Update();
    }
    virtual base::Slice key() const override {
        DCHECK(Valid());
        return key_;
    }
    virtual base::Slice value() const override {
        return delegated_->value();
    }
    virtual base::Status status() const override {
        return delegated_->status();
    }

private:
    void Update() {
        valid_ = delegated_->Valid();
        if (valid_) {
            key_ = delegated_->key();
        }
    }

    std::unique_ptr<Iterator> delegated_;
    bool valid_ = false;

    base::Slice key_;

};

MergingIterator::MergingIterator(Comparator *comparator, Iterator **children,
                                 size_t n)
    : children_(new IteratorWarpper[n])
    , comparator_(comparator)
    , num_children_(n) {
    for (size_t i = 0; i < n; ++i) {
        children_[i].set_delegated(children[i]);
    }
}

MergingIterator::~MergingIterator() {
}

bool MergingIterator::Valid() const {
    return current_ != nullptr && DCHECK_NOTNULL(current_)->Valid();
}

void MergingIterator::SeekToFirst() {
    for (size_t i = 0; i < num_children_; ++i) {
        auto child = &children_[i];

        child->SeekToFirst();
    }

    FindSmallest();
    direction_ = kForward;
}

void MergingIterator::SeekToLast() {
    for (size_t i = 0; i < num_children_; ++i) {
        auto child = &children_[i];

        child->SeekToLast();
    }

    FindLargest();
    direction_ = kReserve;
}

void MergingIterator::Seek(const base::Slice& target) {
    for (size_t i = 0; i < num_children_; ++i) {
        auto child = &children_[i];

        child->Seek(target);
    }

    FindSmallest();
    direction_ = kForward;
}

void MergingIterator::Next() {
    DCHECK(Valid());

    if (direction_ != kForward) {
        for (int64_t i = 0; i < num_children_; ++i) {
            auto child = &children_[i];

            if (child != current_) {
                child->Seek(key());

                if (child->Valid() &&
                    comparator_->Compare(key(), child->key()) == 0) {
                    child->Next();
                }
            }
        }
        direction_ = kForward;
    }
    current_->Next();
    FindSmallest();
}

void MergingIterator::Prev() {
    DCHECK(Valid());

    if (direction_ != kReserve) {
        for (int64_t i = 0; i < num_children_; ++i) {
            auto child = &children_[i];

            if (child != current_) {
                child->Seek(key());

                if (child->Valid()) {
                    child->Prev();
                } else {
                    child->SeekToLast();
                }
            }
        }

        direction_ = kReserve;
    }
    current_->Prev();
    FindLargest();
}

base::Slice MergingIterator::key() const {
    DCHECK(Valid());
    return current_->key();
}

base::Slice MergingIterator::value() const {
    DCHECK(Valid());
    return current_->value();
}

base::Status MergingIterator::status() const {
    for (size_t i = 0; i < num_children_; ++i) {
        auto child = &children_[i];

        auto rs = child->status();
        if (!rs.ok()) {
            return rs;
        }
    }

    return base::Status::OK();
}

void MergingIterator::FindSmallest() {
    IteratorWarpper *smallest = nullptr;

    for (int64_t i = 0; i < num_children_; ++i) {
        auto child = &children_[i];

        if (child->Valid()) {
            if (smallest == nullptr) {
                smallest = child;
            } else if (comparator_->Compare(child->key(),
                                            smallest->key()) < 0) {
                smallest = child;
            }
        }
    }

    current_ = smallest;
}

void MergingIterator::FindLargest() {
    IteratorWarpper *largest = nullptr;

    for (int64_t i = num_children_ - 1; i >= 0; --i) {
        auto child = &children_[i];

        if (child->Valid()) {
            if (largest == nullptr) {
                largest = child;
            } else if (comparator_->Compare(child->key(),
                                            largest->key()) > 0) {
                largest = child;
            }
        }
    }

    current_ = largest;
}

Iterator *CreateMergingIterator(Comparator *comparator, Iterator **children,
                                size_t n) {
    switch (n) {
    case 0:
        return EmptyIterator();

    case 1:
        return DCHECK_NOTNULL(children[0]);

    default: {
        auto iter = new MergingIterator(comparator, children, n);
        return DCHECK_NOTNULL(iter);
    } break;
    }
}

} // namespace lsm
    
} // namespace yukino