#ifndef YUKINO_BASE_REF_COUNTED_H_
#define YUKINO_BASE_REF_COUNTED_H_

#include "base/base.h"
#include <atomic>

namespace yukino {

namespace base {

class DefautlDeleter {
public:

    template<class T>
    static void Delete(T *p) { delete p; }
};

template <class T, class Deleter = DefautlDeleter>
class ReferenceCounted : public DisableCopyAssign {
public:

    int AddRef() const { return counter_++; }

    void Release() const {
        if (--counter_ == 0) {
            Deleter::Delete(static_cast<T*>(const_cast<ReferenceCounted*>(this)));
        }
    }

    int ref_count() const { return counter_; }

private:
    mutable int counter_ = 0;
};

template <class T, class Deleter = DefautlDeleter>
class ThreadSafeReferenceCounted : public DisableCopyAssign {
public:

    int AddRef() const {
        return std::atomic_fetch_add(&counter_, 1);
    }

    void Release() const {
        if (std::atomic_fetch_sub(&counter_, 1) == 1) {
            Deleter::Delete(static_cast<T*>(this));
        }
    }

    int ref_count() const { return counter_.load(); }

private:
    mutable std::atomic<int> counter_;
};

template <class T>
class Handle {
public:
    Handle() {}

    explicit Handle(T *naked) : naked_(naked) {
        if (naked_) naked_->AddRef();
    }

    explicit Handle(const Handle<T> &other) : naked_(other.naked_) {
        if (naked_) naked_->AddRef();
    }

    explicit Handle(Handle<T> &&other) : naked_(other.naked_) {
        other.naked_ = nullptr;
    }

    ~Handle() {
        if (naked_) naked_->Release();
    }

    Handle<T> &operator = (const Handle<T> &other) {
        Handle<T>().Swap(this);
        naked_ = other.get();
        if (naked_) naked_->AddRef();
        return *this;
    }

    Handle<T> &operator = (Handle<T> &&other) {
        Handle<T>().Swap(this);
        this->Swap(&other);
        return *this;
    }

    Handle<T> &Swap(Handle<T> *other) {
        auto tmp = other->get();
        other->naked_ = get();
        naked_ = tmp;
        return *this;
    }

    T *operator -> () const { return naked_; }

    T &operator * () const { return *naked_; }

    T *get() const { return naked_; }

private:
    T *naked_ = nullptr;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_REF_COUNTED_H_