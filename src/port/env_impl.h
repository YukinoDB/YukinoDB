#ifndef YUKINO_PORT_ENV_IMPL_H_
#define YUKINO_PORT_ENV_IMPL_H_

#include "yukino/env.h"

namespace yukino {

namespace port {

class EnvImpl : public Env {
public:
    EnvImpl();
    virtual ~EnvImpl() override;

    virtual base::Status CreateAppendFile(const std::string &fname,
                                          base::AppendFile **file) override;
    virtual base::Status CreateRandomAccessFile(const std::string &fname,
                                                base::MappedMemory **file) override;

    virtual bool FileExists(const std::string& fname) override;
    virtual base::Status DeleteFile(const std::string& fname) override;
};

} // namespace port

} // namespace yukino

#endif // YUKINO_PORT_ENV_IMPL_H_