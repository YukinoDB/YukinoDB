#ifndef YUKINO_PORT_ENV_IMPL_H_
#define YUKINO_PORT_ENV_IMPL_H_

#include "yukino/env.h"
#include "base/status.h"
#include <errno.h>

namespace yukino {

namespace port {

class EnvImpl : public Env {
public:
    EnvImpl();
    virtual ~EnvImpl() override;

    virtual base::Status CreateAppendFile(const std::string &fname,
                                          base::AppendFile **file) override;
    virtual base::Status CreateFileIO(const std::string &fname,
                                      base::FileIO **file) override;
    virtual base::Status CreateRandomAccessFile(const std::string &fname,
                                                base::MappedMemory **file) override;

    virtual bool FileExists(const std::string& fname) override;
    virtual base::Status DeleteFile(const std::string& fname, bool deep) override;
    virtual base::Status GetChildren(const std::string& dir,
                                     std::vector<std::string>* result) override;
    virtual base::Status CreateDir(const std::string& dirname) override;
    virtual base::Status GetFileSize(const std::string& fname,
                                     uint64_t* file_size) override;
    virtual base::Status RenameFile(const std::string& src,
                                    const std::string& target) override;
    virtual base::Status LockFile(const std::string& fname,
                                  base::FileLock** lock) override;

private:
    base::Status Error() { return base::Status::IOError(strerror(errno)); }
};

} // namespace port

} // namespace yukino

#endif // YUKINO_PORT_ENV_IMPL_H_