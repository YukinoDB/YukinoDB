#include "yukino/env.h"
#include "port/env_impl.h"
#include "glog/logging.h"
#include <mutex>

namespace yukino {

Env::~Env() {
}

Env *Env::Default() {
    static std::once_flag create_default_env_once;
    static port::EnvImpl *env = nullptr;

    std::call_once(create_default_env_once, []() {
        env = new port::EnvImpl();
    });

    return DCHECK_NOTNULL(env);
}

} // namespace yukino