#ifndef YUKINO_BASE_IO_H_
#define YUKINO_BASE_IO_H_

#include "base/base.h"
#include "base/status.h"
#include "base/slice.h"

namespace yukino {

namespace base {

/**
 * The sequence wirter.
 *
 */
class Writer : public DisableCopyAssign {
public:
    Writer() {}
    virtual ~Writer() {}

    Status Write(const Slice &buf, size_t *written) {
        return Write(buf.data(), buf.size(), written);
    }

    Status WriteVarint32(uint32_t, size_t *written);

    Status WriteVarint64(uint64_t, size_t *written);

    virtual Status Write(const void *data, size_t size, size_t *written) = 0;

    virtual Status Skip(size_t count) = 0;
};

template<class Checksum>
class VerifiedWriter : public Writer {
public:
    VerifiedWriter(Writer *delegated) : delegated_(delegated) {}
    virtual ~VerifiedWriter() {}

    virtual Status Write(const void *data, size_t size,
                         size_t *written) override {
        checker_.Update(data, size);
        return delegated_->Write(data, size, written);
    }

    virtual Status Skip(size_t count) override {
        return delegated_->Skip(count);
    }

    void Reset() {
        checker_.Reset();
    }

    typename Checksum::DigestTy digest() const {
        return checker_.digest();
    }

    Writer *delegated() const {
        return delegated_;
    }

private:
    Writer *delegated_;
    Checksum checker_;
};

} // namespace base

} // namespace yukino

#endif // YUKINO_BASE_IO_H_