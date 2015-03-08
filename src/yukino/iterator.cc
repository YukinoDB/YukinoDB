#include "base/status.h"
#include "yukino/iterator.h"
#include "glog/logging.h"
#include <string.h>
#include <mutex>

namespace yukino {

Iterator::Iterator()
    : cleanup_(nullptr) {
}

Iterator::~Iterator() {

    Cleanup *p = nullptr;
    while (cleanup_) {
        cleanup_->callback();
        p = cleanup_;
        cleanup_ = cleanup_->next;
        delete p;
    }
}

void Iterator::RegisterCleanup(const std::function<void()> &callback) {
    auto cleanup = new Cleanup {
        callback,
        cleanup_,
    };
    DCHECK_NOTNULL(cleanup);

    cleanup_ = cleanup;
}

namespace {

class ErrorIterator : public Iterator {
public:
    ErrorIterator(const base::Status &status) : status_(status) {}
    virtual ~ErrorIterator() override {}

    virtual bool Valid() const override { return false; }
    virtual void SeekToFirst() override {}
    virtual void SeekToLast() override {}
    virtual void Seek(const base::Slice& target) override {}
    virtual void Next() override {}
    virtual void Prev() override {}
    virtual base::Slice key() const override { DLOG(FATAL); return ""; }
    virtual base::Slice value() const override { DLOG(FATAL); return ""; }
    virtual base::Status status() const override { return status_; }

private:
    base::Status status_;
};

std::once_flag empty_iterator_once;

Iterator *empty_iterator;


} // namespace

Iterator *EmptyIterator() {
    std::call_once(empty_iterator_once, []() {
        empty_iterator = CreateErrorIterator(base::Status::OK());
    });

    return DCHECK_NOTNULL(empty_iterator);
}

Iterator *CreateErrorIterator(const base::Status &err) {
    auto iter = new ErrorIterator(err);
    return DCHECK_NOTNULL(iter);
}

} // namespace yukino