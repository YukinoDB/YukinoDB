#ifndef YUKINO_BALANCE_VERSION_SET_H_
#define YUKINO_BALANCE_VERSION_SET_H_

#include "balance/format.h"
#include "base/status.h"
#include "base/base.h"

namespace yukino {

class Env;
class Comparator;

namespace base {

class AppendFile;

} // namespace base

namespace util {

class LogWriter;

} // namespace util

namespace balance {

class VersionSet;
class VersionPatch;

class VersionSet : public base::DisableCopyAssign {
public:
    VersionSet(const std::string name, const Comparator *comparator, Env *env)
        : files_(name)
        , comparator_(comparator)
        , env_(env) {
    }

    base::Status Apply(VersionPatch *patch, std::mutex *mutex);

    base::Status Recover(uint64_t manifest_file_number);

    void AdvacneTxId(uint64_t add) { last_tx_id_ += add; }

    uint64_t NextTxId() { return last_tx_id_++; }

    uint64_t NextFileNumber() { return last_file_number_++; }

    uint64_t last_tx_id() const { return last_tx_id_; }

    uint64_t startup_tx_id() const { return startup_tx_id_; }

    uint64_t log_file_number() const { return log_file_number_; }

private:
    base::Status CreateManifest(uint64_t file_number);

    uint64_t startup_tx_id_ = 0;    // The startup transaction id.

    uint64_t last_tx_id_ = 0;       // The biggest transaction id.
    uint64_t last_file_number_ = 0; // The biggest file number.

    uint64_t prev_log_file_number_ = 0;
    uint64_t log_file_number_ = 0;

    std::unique_ptr<util::LogWriter> manifest_log_;
    std::unique_ptr<base::AppendFile> manifest_file_;
    uint64_t manifest_file_number_ = 0;

    const Files files_;
    Env * const env_;
    const Comparator * const comparator_;
};

class VersionPatch : public base::DisableCopyAssign {
public:

    std::string Encode() const;

    base::Status Decode(const base::Slice &buf);

    //--------------------------------------------------------------------------
    // Patch Setter
    //--------------------------------------------------------------------------
    void set_log_file_number(uint64_t number) { log_file_number_ = number; }

    void set_prev_log_file_number(uint64_t number) { prev_log_file_number_ = number; }

    void set_comparator(const std::string &name) { comparator_ = name; }

    //--------------------------------------------------------------------------
    // Snapshot Setter
    //--------------------------------------------------------------------------
    void set_last_tx_id(uint64_t id) { last_tx_id_ = id; }

    void set_last_file_number(uint64_t number) { last_file_number_ = number; }

    friend class VersionSet;
private:
    // patch variable
    std::string comparator_;
    uint64_t    log_file_number_ = 0;
    uint64_t    prev_log_file_number_ = 0;

    // snapshot variable
    uint64_t last_tx_id_ = 0;       // The biggest transaction id.
    uint64_t last_file_number_ = 0; // The biggest file number.
};

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_VERSION_SET_H_