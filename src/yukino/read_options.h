#ifndef YUKINO_API_READ_OPTIONS_H_
#define YUKINO_API_READ_OPTIONS_H_

namespace yukino  {

class Snapshot;

struct ReadOptions {
    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    // Default: false
    bool verify_checksums;

    // Should the data read for this iteration be cached in memory?
    // Callers may wish to set this field to false for bulk scans.
    // Default: true
    bool fill_cache;

    // If "snapshot" is non-NULL, read as of the supplied snapshot
    // (which must belong to the DB that is being read and which must
    // not have been released).  If "snapshot" is NULL, use an implicit
    // snapshot of the state at the beginning of this read operation.
    // Default: NULL
    const Snapshot *snapshot;

    ReadOptions();
};

}

#endif // YUKINO_API_READ_OPTIONS_H_
