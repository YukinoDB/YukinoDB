#ifndef YUKINO_BALANCE_REDO_LOG_H_
#define YUKINO_BALANCE_REDO_LOG_H_

#include "util/log.h"
#include <stdint.h>

namespace yukino {

namespace balance {

struct Log final {

    enum Command : uint8_t {
        kZero,
        kBeginTransaction,
        kAbortTransaction,
        kCommitTransaction,
        kPut,
        kStartCheckpoint,
        kEndCheckpoint,
    };

    Log() = delete;
    ~Log() = delete;
};

class Command : public base::DisableCopyAssign {
public:
    Command(Log::Command code) : code_(code) {}

    Log::Command code() const { return code_; }

    virtual base::Status Encode(base::Writer *w) const;

private:
    Log::Command code_;
};

class BeginTransaction : public Command {
public:
    BeginTransaction(uint64_t tx_id)
        : tx_id_(tx_id)
        , Command(Log::kBeginTransaction) {}

    uint64_t tx_id() const { return tx_id_; }

    virtual base::Status Encode(base::Writer *w) const override;

private:
    uint64_t tx_id_;
};


class LogWriter : public base::DisableCopyAssign {
public:
    LogWriter(base::Writer *writer, size_t block_size)
        : core_(writer, block_size) {}

    base::Status Apply(const Command &command) {
        buf_.Clear();
        command.Encode(&buf_);
        return core_.Append(buf());
    }

    base::Status Apply(const Command **commands, size_t n) {
        buf_.Clear();
        for (size_t i = 0; i < n; ++i) {
            commands[i]->Encode(&buf_);
        }
        return core_.Append(buf());
    }

private:
    base::Slice buf() const { return base::Slice(buf_.buf(), buf_.len()); }

    util::LogWriter core_;
    base::BufferedWriter buf_;
};

} // namespace balance

} // namespace yukino

#endif // YUKINO_BALANCE_REDO_LOG_H_