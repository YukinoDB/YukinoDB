#include "balance/redo_log.h"
#include "base/io-inl.h"
#include "base/io.h"

#if defined(CHECK_OK)
#   undef CHECK_OK
#else
#   define CHECK_OK(expr) rs = (expr); if (!rs.ok()) return rs
#endif

namespace yukino {

namespace balance {

base::Status Command::Encode(base::Writer *w) const {
    return w->WriteByte(code_);
}

base::Status BeginTransaction::Encode(base::Writer *w) const {
    base::Status rs;
    CHECK_OK(Command::Encode(w));
    CHECK_OK(w->WriteVarint64(tx_id_, nullptr));
    return rs;
}

} // namespace balance

} // namespace yukino