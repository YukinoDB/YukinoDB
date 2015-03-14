#include "port/env_impl.h"
#include "port/io_impl.h"
#include "glog/logging.h"
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

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
            PLOG(ERROR) << "stat() fail, cause: ";
        }
        return false;
    }

    return true;
}

bool IsDir(const char *path) {
    struct stat buf;
    if (::stat(path, &buf) < 0) {
        PLOG(ERROR) << "stat() fail, cause: ";
        return false;
    }

    return buf.st_mode & S_IFDIR;
}

template<class Callback>
int ForeachDirectory(const char *root, const Callback &call, bool include) {
    if (!IsDir(root)) {
        return 0;
    }

    auto handle = ::opendir(root);
    if (!handle) {
        return -1;
    }

    struct dirent stub, *next = nullptr;

    auto rv = ::readdir_r(handle, &stub, &next);
    if (rv < 0) {
        return -1;
    }
    while (next == &stub) {
        if (!include && (strcmp(".", stub.d_name) == 0 ||
                         strcmp("..", stub.d_name) == 0)) {
            // do nothing
        } else {
            call(stub);
        }

        rv = ::readdir_r(handle, &stub, &next);
        if (rv < 0) {
            return -1;
        }
    }

    return 0;
}

base::Status EnvImpl::DeleteFile(const std::string& fname, bool deep) {
    if (deep) {
        auto rv = ForeachDirectory(fname.c_str(), [&](const dirent &ent) {
            std::string name(fname);
            name.append("/");
            name.append(ent.d_name);

            base::Status rs;
            if (ent.d_type & DT_DIR) {
                rs = DeleteFile(name, true);
            } else {
                rs = DeleteFile(name, false);
            }
        }, false);
        if (rv < 0) {
            return Error();
        }
    }

    auto rv = IsDir(fname.c_str()) ? ::rmdir(fname.c_str())
        : ::unlink(fname.c_str());
    if (rv < 0) {
        return Error();
    }

    return base::Status::OK();
}

base::Status EnvImpl::GetChildren(const std::string& dir,
                                  std::vector<std::string>* result) {

    result->clear();
    auto rv = ForeachDirectory(dir.c_str(), [&](const dirent &ent) {
        result->push_back(ent.d_name);
    }, false);

    if (rv < 0) {
        return Error();
    }
    return base::Status::OK();
}

base::Status EnvImpl::CreateDir(const std::string& dirname) {
    auto rv = ::mkdir(dirname.c_str(), 0766);
    if (rv < 0)
        return Error();
    else
        return base::Status::OK();
}

base::Status EnvImpl::GetFileSize(const std::string& fname,
                                  uint64_t* file_size) {
    struct stat rv;
    if (::stat(fname.c_str(), &rv) < 0) {
        return Error();
    }

    *file_size = static_cast<uint64_t>(rv.st_size);
    return base::Status::OK();
}

base::Status EnvImpl::RenameFile(const std::string& src,
                                 const std::string& target) {
    auto rv = ::rename(src.c_str(), target.c_str());
    if (rv < 0) {
        return Error();
    } else {
        return base::Status::OK();
    }
}

} // namespace port
} // namespace yukino