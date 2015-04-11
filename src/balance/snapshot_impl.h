#ifndef YUKINO_BALANCE_SNAPSHOT_IMPL_H_
#define YUKINO_BALANCE_SNAPSHOT_IMPL_H_

#include "yukino/db.h"
#include <stdint.h>

namespace yukino {

namespace balance {

class SnapshotImpl : public Snapshot {
public:
    SnapshotImpl(uint64_t tx_id) : next(this), prev(this), tx_id_(tx_id) {}
    virtual ~SnapshotImpl() {}

    SnapshotImpl *next;
    SnapshotImpl *prev;

    uint64_t tx_id() const { return tx_id_; }

private:
    uint64_t tx_id_;
};

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_SNAPSHOT_IMPL_H_