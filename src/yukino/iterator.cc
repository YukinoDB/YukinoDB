#include "yukino/iterator.h"
#include "glog/logging.h"
#include <string.h>

namespace yukino {

Iterator::Iterator()
    : cleanup_(nullptr) {
}

Iterator::~Iterator() {

    Cleanup *p = nullptr;
    while (cleanup_) {
        cleanup_->function(cleanup_->arg1, cleanup_->arg2);
        p = cleanup_;
        cleanup_ = cleanup_->next;
        delete p;
    }
}

void Iterator::RegisterCleanup(CleanupFunction function, void* arg1, void* arg2) {
    DCHECK_NOTNULL(function);

    auto cleanup = new Cleanup {
        function,
        arg1,
        arg2,
        cleanup_,
    };
    DCHECK_NOTNULL(cleanup);

    cleanup_ = cleanup;
}

} // namespace yukino