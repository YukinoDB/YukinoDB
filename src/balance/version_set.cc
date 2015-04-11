#include "balance/version_set.h"
#include "util/log.h"
#include "base/io.h"
#include "yukino/env.h"

#if defined(CHECK_OK)
#   undef CHECK_OK
#endif
#   define CHECK_OK(expr) rs = (expr); if (!rs.ok()) return rs

namespace yukino {

namespace balance {

base::Status VersionSet::Apply(VersionPatch *patch, std::mutex *mutex) {
    base::Status rs;

    if (!manifest_log_ && !manifest_file_) {
        CHECK_OK(CreateManifest(NextFileNumber()));
    }
    patch->set_last_file_number(last_file_number_);
    patch->set_last_tx_id(last_tx_id_);

    if (mutex) mutex->unlock();
    CHECK_OK(manifest_log_->Append(patch->Encode()));
    CHECK_OK(manifest_file_->Sync());
    if (mutex) mutex->lock();

    log_file_number_      = patch->log_file_number_;
    prev_log_file_number_ = patch->prev_log_file_number_;
    return rs;
}

base::Status VersionSet::Recover(uint64_t manifest_file_number) {
    base::Status rs;
    base::MappedMemory *rv = nullptr;
    CHECK_OK(env_->CreateRandomAccessFile(files_.ManifestFile(manifest_file_number), &rv));

    std::unique_ptr<base::MappedMemory> file(rv);
    util::Log::Reader reader(file->buf(), file->size(), true,
                             util::Log::kDefaultBlockSize);
    base::Slice record;
    std::string buf;

    VersionPatch patch;
    while (reader.Read(&record, &buf) && reader.status().ok()) {
        CHECK_OK(patch.Decode(record));

        if (!patch.comparator_.empty()) {
            if (patch.comparator_.compare(comparator_->Name()) != 0) {
                auto msg = base::Strings::Sprintf("differenct comparator. "
                                                  "unexpected %s, expected %s",
                                                  patch.comparator_.c_str(),
                                                  comparator_->Name());

                return base::Status::Corruption(msg);
            }
        }

        last_tx_id_           = patch.last_tx_id_;
        last_file_number_     = patch.last_file_number_;
        log_file_number_      = patch.log_file_number_;
        prev_log_file_number_ = patch.prev_log_file_number_;
    }
    startup_tx_id_ = last_tx_id_;

    return reader.status();
}

base::Status VersionSet::CreateManifest(uint64_t file_number) {
    base::Status rs;
    manifest_file_number_ = file_number;

    base::AppendFile *rv;
    CHECK_OK(env_->CreateAppendFile(files_.ManifestFile(file_number), &rv));
    manifest_file_ = base::make_unique_ptr(rv);
    manifest_log_  = base::make_unique_ptr(new util::LogWriter(
                                           manifest_file_.get(),
                                           util::Log::kDefaultBlockSize));

    auto buf = base::Strings::Sprintf("%" PRIu64 "\n", file_number);
    return base::WriteAll(files_.CurrentFile(), buf, nullptr);
}


std::string VersionPatch::Encode() const {
    base::BufferedWriter w;

    w.WriteString(comparator_, nullptr);
    w.WriteVarint64(log_file_number_, nullptr);
    w.WriteVarint64(prev_log_file_number_, nullptr);

    w.WriteVarint64(last_tx_id_, nullptr);
    w.WriteVarint64(last_file_number_, nullptr);

    return std::string(w.buf(), w.len());
}

base::Status VersionPatch::Decode(const base::Slice &buf) {
    base::Status rs;

    base::BufferedReader rd(buf.data(), buf.size());
    auto slice = rd.ReadString();
    comparator_.assign(slice.data(), slice.size());

    log_file_number_      = rd.ReadVarint64();
    prev_log_file_number_ = rd.ReadVarint64();
    last_tx_id_           = rd.ReadVarint64();
    last_file_number_     = rd.ReadVarint64();
    return rs;
}

} // namespace balance

} // namespace yukino