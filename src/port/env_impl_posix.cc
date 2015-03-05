#include "port/env_impl.h"
#include "port/io_impl.h"
#include "glog/logging.h"
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>

namespace yukino {

namespace port {

EnvImpl::EnvImpl() {
}

EnvImpl::~EnvImpl() {
}

base::Status EnvImpl::CreateAppendFile(const std::string &fname,
                                       base::AppendFile **file) {
    return port::CreateAppendFile(fname.c_str(), file);
}

base::Status EnvImpl::CreateRandomAccessFile(const std::string &fname,
                                             base::MappedMemory **file) {
    return port::CreateRandomAccessFile(fname.c_str(), file);
}

bool EnvImpl::FileExists(const std::string& fname) {
    struct stat stub;

    auto rv = ::stat(fname.c_str(), &stub);
    if (rv < 0) {
        if (errno != ENOENT) {
            PLOG(ERROR) << "FileExists() false, cause: ";
        }
        return false;
    }

    return true;
}

base::Status EnvImpl::DeleteFile(const std::string& fname) {
    auto rv = ::unlink(fname.c_str());
    if (rv < 0) {
        return base::Status::IOError(strerror(errno));
    }

    return base::Status::OK();
}

} // namespace port
} // namespace yukino