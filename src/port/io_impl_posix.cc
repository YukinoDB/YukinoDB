#include "port/io_impl.h"
#include "base/io-inl.h"
#include "base/io.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <algorithm>

namespace yukino {

namespace port {

namespace {

class FileIOImpl : public base::FileIO {
public:
    static base::Status CreateFile(const char *file_name,
                                   const char *mod,
                                   FileIOImpl **writer) {
        FILE *file = fopen(file_name, mod);
        if (!file) {
            return base::Status::IOError(strerror(errno));
        }
        *writer = new FileIOImpl(file);

        return base::Status::OK();
    }

    virtual ~FileIOImpl() override {
        if (file_) {
            Close();
        }
    }

    virtual base::Status Write(const void *data, size_t size, size_t *written) override {
        auto rv = fwrite(data, 1, size, file_);
        auto err = ferror(file_);
        if (err) {
            return Error(err);
        }
        if (written) {
            *written = rv;
        }
        active_ += rv;
        return base::Status::OK();
    }

    virtual base::Status Skip(size_t count) override {
        auto avil = count;
        base::Status rs;
        while (avil) {
            auto fill_size = std::min(avil, static_cast<size_t>(kFillingSize));
            rs = Write(kFillingZero, fill_size, nullptr);
            if (!rs.ok()) {
                break;
            }
            avil -= fill_size;
        }
        return rs;
    }

// DO NOT use ftell, it's too slow
//    virtual size_t active() const override {
//        auto rv = ftell(file_);
//        DCHECK_GE(rv, 0);
//        return rv;
//    }

    virtual base::Status Read(void *buf, size_t size) override {
        auto rv = fread(buf, 1, size, file_);
        auto err = ferror(file_);
        if (err) {
            return Error(err);
        }
        active_ += rv;
        return base::Status::OK();
    }

    virtual int ReadByte() override {
        auto rv = fgetc(file_);
        if (rv != EOF) {
            active_ += 1;
        }
        return rv;
    }

    virtual base::Status Ignore(size_t count) {
        if (fseek(file_, count, SEEK_CUR) < 0) {
            return Error();
        }
        active_ += count;
        return base::Status::OK();
    }

    virtual base::Status Close() override {
        if (fclose(DCHECK_NOTNULL(file_)) < 0) {
            return Error();
        }
        file_ = nullptr;
        return base::Status::OK();
    }

    virtual base::Status Flush() override {
        if (fflush(file_) < 0) {
            return Error();
        }
        return base::Status::OK();
    }

    virtual base::Status Sync() override {
        auto rs = Flush();
        if (!rs.ok()) {
            return rs;
        }
        if (fsync(fileno(file_)) < 0) {
            return Error();
        }
        return base::Status::OK();
    }

    virtual base::Status Truncate(uint64_t offset) {
        if (ftruncate(fileno(file_), offset) < 0) {
            return Error();
        }
        return base::Status::OK();
    }

    virtual base::Status Seek(uint64_t offset) {
        if (fseek(file_, offset, SEEK_SET) < 0) {
            return Error();
        }
        active_ = offset;
        return base::Status::OK();
    }

private:
    FileIOImpl(FILE *file) : file_(DCHECK_NOTNULL(file)) {}

    base::Status Error() {
        return Error(errno);
    }

    base::Status Error(int err) {
        return base::Status::IOError(strerror(err));
    }
    
    FILE *file_ = nullptr;

    enum { kFillingSize = 128 };
    static const uint8_t kFillingZero[kFillingSize];
};

const uint8_t FileIOImpl::kFillingZero[kFillingSize] = {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
};

class MappedMemoryImpl : public base::MappedMemory {
public:

    virtual ~MappedMemoryImpl() override {
        if (fd_ >= 0) {
            auto rs = Close();
            if (!rs.ok()) {
                DLOG(ERROR) << rs.ToString();
            }
        }
    }

    virtual base::Status Close() override {
        auto rv = ::munmap(buf_, len_);
        if (rv < 0) {
            return Return(rv);
        }
        buf_ = nullptr; len_ = 0;

        rv = ::close(fd_);
        if (rv < 0) {
            return Return(rv);
        }
        fd_ = -1;
        return base::Status::OK();
    }

    virtual base::Status Sync(size_t offset, size_t len) override {
        DCHECK_LT(offset + len, size());
        return Return( ::msync(buf_ + offset, len, MS_SYNC));
    }

    static base::Status CreateMapping(const char *file_name,
                                      MappedMemoryImpl **impl) {
        auto fd = ::open(file_name, O_RDWR);
        if (fd < 0) {
            return Return(fd);
        }

        struct stat stub;
        auto rv = ::fstat(fd, &stub);
        if (rv < 0) {
            auto rs = Return(rv);
            close(fd);
            return rs;
        }

        auto buf = ::mmap(nullptr, stub.st_size, PROT_READ|PROT_WRITE,
                          MAP_FILE|MAP_PRIVATE, fd, 0);
        if (reinterpret_cast<intptr_t>(buf) < 0) {
            auto rs = Return(-1);
            close(fd);
            return rs;
        }

        *impl = new MappedMemoryImpl(file_name, fd, buf, stub.st_size);
        return base::Status::OK();
    }

private:
    MappedMemoryImpl(const std::string &file_name, int fd,
                     void *buf, size_t len)
        : base::MappedMemory(file_name, buf, len)
        , fd_(fd) {
    }

    static base::Status Return(int rv) {
        return rv < 0 ? base::Status::IOError(strerror(errno)) :
        base::Status::OK();
    }

    int fd_ = -1;
};

class FileLockImpl : public base::FileLock {
public:

    FileLockImpl(const std::string name, int fd, bool locked)
        : name_(name)
        , fd_(fd)
        , locked_(locked) {
        DCHECK_GE(fd_, 0);
    }

    virtual ~FileLockImpl() override {
        auto rv = ::close(fd_);
        DCHECK_EQ(0, rv);
        ::unlink(name_.c_str());
    }

    static base::Status CreateFileLock(const char *name, bool locked,
                                       FileLockImpl **rv) {
        auto flags = O_CREAT | O_TRUNC | O_EXCL | O_WRONLY;
        if (locked) {
            flags |= O_EXLOCK;
        }

        auto fd = ::open(name, flags);
        if (fd < 0) {
            return Error();
        }

        *rv = new FileLockImpl(name, fd, locked);
        return base::Status::OK();
    }

    virtual base::Status Lock() const override {
        DCHECK(!locked());
        if (flock(fd_, LOCK_EX) < 0) {
            return Error();
        } else {
            locked_ = true;
        }
        return base::Status::OK();
    }

    virtual base::Status Unlock() const override {
        DCHECK(locked());
        if (flock(fd_, LOCK_UN) < 0) {
            return Error();
        } else {
            locked_ = false;
        }
        return base::Status::OK();
    }

    virtual std::string name() const override { return name_; }

    virtual bool locked() const override { return locked_; }

    static base::Status Error() {
        return base::Status::IOError(strerror(errno));
    }

private:
    std::string name_;
    int fd_;
    mutable bool locked_;
};
    
} // namespace

base::Status CreateAppendFile(const char *file_name, base::AppendFile **file) {
    FileIOImpl *impl = nullptr;

    auto rs = FileIOImpl::CreateFile(file_name, "a", &impl);
    if (!rs.ok()) {
        return rs;
    }

    *DCHECK_NOTNULL(file) = impl;
    return rs;
}

base::Status CreateFileIO(const char *file_name, base::FileIO **file) {
    FileIOImpl *impl = nullptr;

    auto rs = FileIOImpl::CreateFile(file_name, "w", &impl);
    if (!rs.ok()) {
        return rs;
    }

    *DCHECK_NOTNULL(file) = impl;
    return rs;
}

base::Status CreateRandomAccessFile(const char *file_name,
                                    base::MappedMemory **file) {
    MappedMemoryImpl *impl = nullptr;

    auto rs = MappedMemoryImpl::CreateMapping(file_name, &impl);
    if (!rs.ok()) {
        return rs;
    }

    *DCHECK_NOTNULL(file) = impl;
    return rs;
}

base::Status CreateFileLock(const char *file_name, bool locked,
                            base::FileLock **file) {
    FileLockImpl *impl = nullptr;

    auto rs = FileLockImpl::CreateFileLock(file_name, locked, &impl);
    if (!rs.ok()) {
        return rs;
    }

    *DCHECK_NOTNULL(file) = impl;
    return rs;
}

} // namespace port

} // namespace yukino