#include "port/io_impl.h"
#include "base/io.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

namespace yukino {

namespace port {

namespace {

class AppendFileImpl : public base::AppendFile {
public:
    static base::Status CreateFile(const char *file_name,
                                   AppendFileImpl **writer) {
        FILE *file = fopen(file_name, "w");
        if (!file) {
            return base::Status::IOError(strerror(errno));
        }
        *writer = new AppendFileImpl(file);

        return base::Status::OK();
    }

    virtual ~AppendFileImpl() override {
        if (file_) {
            Close();
        }
    }

    virtual base::Status Write(const void *data, size_t size, size_t *written) override {
        auto rv = fwrite(data, 1, size, file_);
        auto err = ferror(file_);
        if (err) {
            return base::Status::IOError(strerror(err));
        }
        if (written) {
            *written = rv;
        }
        return base::Status::OK();
    }

    virtual base::Status Skip(size_t count) override {
        return Return(fseek(file_, count, SEEK_CUR));
    }

    virtual base::Status Close() {
        auto rv = fclose(DCHECK_NOTNULL(file_));
        file_ = nullptr;
        return Return(rv);
    }

    virtual base::Status Flush() {
        return Return(fflush(file_));
    }

    virtual base::Status Sync() {
        auto rs = Flush();
        if (!rs.ok()) {
            return rs;
        }
        return Return(fsync(fileno(file_)));
    }

private:
    AppendFileImpl(FILE *file) : file_(DCHECK_NOTNULL(file)) {}

    base::Status Return(int rv) {
        return rv < 0 ? base::Status::IOError(strerror(errno)) :
            base::Status::OK();
    }
    
    FILE *file_ = nullptr;
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
    
} // namespace

base::Status CreateAppendFile(const char *file_name, base::AppendFile **file) {
    AppendFileImpl *impl = nullptr;

    auto rs = AppendFileImpl::CreateFile(file_name, &impl);
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

} // namespace port

} // namespace yukino