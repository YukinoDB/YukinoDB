#include "lsm/version.h"
#include "lsm/table_cache.h"
#include "lsm/merger.h"
#include "lsm/log.h"
#include "lsm/compaction.h"
#include "yukino/options.h"
#include "yukino/iterator.h"
#include "yukino/env.h"
#include "base/io.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

VersionSet::VersionSet(const std::string &db_name, const Options &options,
                       TableCache *table_cache)
    : db_name_(db_name)
    , env_(DCHECK_NOTNULL(options.env))
    , comparator_(InternalKeyComparator(DCHECK_NOTNULL(options.comparator)))
    , version_dummy_(this)
    , table_cache_(DCHECK_NOTNULL(table_cache)) {
    Append(new Version(this));
}

VersionSet::~VersionSet() {
    auto iter = version_dummy_.next();
    auto p = iter;
    while (iter != &version_dummy_) {
        iter = iter->next();
        p->Release();
        p = iter;
    }
}

Version::Version(VersionSet *owned)
: owned_(DCHECK_NOTNULL(owned)) {

}

Version::~Version() {

}

base::Status Version::Get(const ReadOptions &options, const InternalKey &key,
                 std::string *value) {

    std::vector<base::Handle<FileMetadata>> maybe_file;
    auto ucmp = owned_->comparator_.delegated();
    auto ukey = key.user_key_slice();

    for (const auto &metadata : file(0)) {
        if (ucmp->Compare(ukey, metadata->smallest_key.user_key_slice()) >= 0 ||
            ucmp->Compare(ukey, metadata->largest_key.user_key_slice()) <= 0) {
            maybe_file.push_back(metadata);
        }
    }
    // The newest file should be first.
    std::sort(maybe_file.begin(), maybe_file.end(),
              [](const base::Handle<FileMetadata> &a,
                 const base::Handle<FileMetadata> &b) {
                  return a->ctime > b->ctime;
              });

    for (auto i = 1; i < kMaxLevel; ++i) {
        if (file(i).empty()) {
            continue;
        }

        const auto &metadata = file(i).at(0);
        if (ucmp->Compare(ukey, metadata->smallest_key.user_key_slice()) >= 0 ||
            ucmp->Compare(ukey, metadata->largest_key.user_key_slice()) <= 0) {
            maybe_file.push_back(metadata);
        }
    }

    if (maybe_file.empty()) {
        return base::Status::NotFound("");
    }

    std::vector<Iterator*> iters;
    for (const auto &metadata : maybe_file) {
        auto iter = owned_->table_cache_->CreateIterator(options,
                                                         metadata->number,
                                                         metadata->size);
        if (!iter->status().ok()) {
            delete iter;
            return iter->status();
        }
        iters.push_back(iter);
    }

    std::unique_ptr<Iterator> merger(CreateMergingIterator(&owned_->comparator_,
                                                           &iters[0],
                                                           iters.size()));
    merger->Seek(key.key_slice());
    if (!merger->Valid()) {
        return base::Status::NotFound("");
    }

    base::BufferedReader rd(merger->key().data(), merger->key().size());
    auto found_user_key = rd.Read(merger->key().size() - Tag::kTagSize);
    auto tag = Tag::Decode(rd.ReadFixed64());

    if (ucmp->Compare(key.user_key(), found_user_key) != 0 ||
        tag.flag == kFlagDeletion) {
        return base::Status::NotFound("");
    }

    value->assign(std::move(merger->value().ToString()));
    return base::Status::OK();
}

void VersionPatch::CreateFile(int level, uint64_t file_number,
                              const base::Slice &smallest_key,
                              const base::Slice &largest_key,
                              size_t size, uint64_t ctime) {
    auto metadata = new FileMetadata(file_number);

    metadata->smallest_key = InternalKey::CreateKey(smallest_key);
    metadata->largest_key = InternalKey::CreateKey(largest_key);
    metadata->size = size;
    metadata->ctime = ctime;

    CreateFile(level, metadata);
}

base::Status VersionPatch::Encode(std::string *buf) const {
    base::BufferedWriter writer;

    writer.WriteByte(has_field(kComparator));
    if (has_field(kComparator)) {
        writer.WriteString(comparator_, nullptr);
    }

    writer.WriteByte(has_field(kLastVersion));
    if (has_field(kLastVersion)) {
        writer.WriteVarint64(last_version_, nullptr);
    }

    writer.WriteByte(has_field(kNextFileNumber));
    if (has_field(kNextFileNumber)) {
        writer.WriteVarint64(next_file_number_, nullptr);
    }

    writer.WriteByte(has_field(kRedoLogNumber));
    if (has_field(kRedoLogNumber)) {
        writer.WriteVarint64(redo_log_number_, nullptr);
    }

    writer.WriteByte(has_field(kPrevLogNumber));
    if (has_field(kPrevLogNumber)) {
        writer.WriteVarint64(prev_log_number_, nullptr);
    }

    writer.WriteByte(has_field(kCompactionPoint));
    if (has_field(kCompactionPoint)) {
        writer.WriteVarint32(compaction_level_, nullptr);
        writer.WriteString(compaction_key_.key_slice(), nullptr);
    }

    writer.WriteVarint32(static_cast<uint32_t>(deletion_.size()), nullptr);
    for (const auto &entry : deletion_) {
        writer.WriteVarint32(entry.first, nullptr);
        writer.WriteVarint64(entry.second, nullptr);
    }

    writer.WriteVarint32(static_cast<uint32_t>(creation_.size()), nullptr);
    for (const auto &entry : creation_) {
        writer.WriteVarint32(entry.first, nullptr);

        auto metadata = entry.second.get();
        writer.WriteVarint64(metadata->number, nullptr);
        writer.WriteString(metadata->smallest_key.key_slice(), nullptr);
        writer.WriteString(metadata->largest_key.key_slice(), nullptr);
        writer.WriteVarint64(metadata->size, nullptr);
        writer.WriteFixed64(metadata->ctime);
    }

    buf->assign(writer.buf(), writer.len());
    return base::Status::OK();
}

base::Status VersionPatch::Decode(const base::Slice &buf) {
    base::BufferedReader rd(buf.data(), buf.size());

    if (rd.ReadByte()) {
        set_field(kComparator);
        comparator_.assign(rd.ReadString().ToString());
    }

    if (rd.ReadByte()) {
        set_field(kLastVersion);
        last_version_ = rd.ReadVarint64();
    }

    if (rd.ReadByte()) {
        set_field(kNextFileNumber);
        next_file_number_ = rd.ReadVarint64();
    }

    if (rd.ReadByte()) {
        set_field(kRedoLogNumber);
        redo_log_number_ = rd.ReadVarint64();
    }

    if (rd.ReadByte()) {
        set_field(kPrevLogNumber);
        prev_log_number_ = rd.ReadVarint64();
    }

    if (rd.ReadByte()) {
        set_field(kCompactionPoint);
        compaction_level_ = static_cast<int>(rd.ReadVarint32());
        compaction_key_ = InternalKey::CreateKey(rd.ReadString());
    }

    auto i = rd.ReadVarint32();
    while (i--) {
        DeleteFile(static_cast<int>(rd.ReadVarint32()), rd.ReadVarint64());
    }

    i = rd.ReadVarint32();
    while (i--) {
        auto level = static_cast<int>(rd.ReadVarint32());
        set_field(kCreation);

        auto metadata = new FileMetadata(rd.ReadVarint32());
        metadata->smallest_key = InternalKey::CreateKey(rd.ReadString());
        metadata->largest_key  = InternalKey::CreateKey(rd.ReadString());
        metadata->size = rd.ReadVarint64();
        metadata->ctime = rd.ReadFixed64();

        creation_.emplace_back(level, base::Handle<FileMetadata>(metadata));
    }

    return base::Status::OK();
}

void VersionPatch::Reset() {
    ::memset(bits_, 0, kNum32Bits * sizeof(uint32_t));
    creation_.clear();
    deletion_.clear();
}

bool VersionSet::NeedsCompaction() const {
    if (current()->NumberLevelFiles(0) > kMaxNumberLevel0File) {
        return true;
    }

    if (current()->SizeLevelFiles(0) > kMaxSizeLevel0File) {
        return true;
    }

    for (auto i = 1; i < kMaxLevel; i++) {
        if (current()->SizeLevelFiles(i) > kMaxSizeLevel0File * (i + 1)) {
            return true;
        }
    }
    return false;
}

base::Status VersionSet::GetCompaction(VersionPatch *patch, Compaction **rv) {
    std::unique_ptr<Compaction> compaction(new Compaction(db_name_, comparator_,
                                                          table_cache_));

    compaction->set_target(GenerateFileNumber());

    if (current()->NumberLevelFiles(0) > kMaxNumberLevel0File) {
        auto num_should_compact = current()->NumberLevelFiles(0) / 2;

        std::vector<base::Handle<FileMetadata>> files(current()->file(0));
        std::sort(files.begin(), files.end(),
                  [](const base::Handle<FileMetadata> &a,
                     const base::Handle<FileMetadata> &b) {
                      return a->ctime > b->ctime;
                  });

        for (auto i = 0; i < num_should_compact; ++i) {

            auto rs = compaction->AddOriginFile(files[i]->number,
                                                files[i]->size);
            if (!rs.ok()) {
                return rs;
            }
            patch->DeleteFile(0, files[i]->number);
        }
    } else if (current()->SizeLevelFiles(0) > kMaxSizeLevel0File) {
        std::vector<base::Handle<FileMetadata>> files(current()->file(0));
        std::sort(files.begin(), files.end(),
                  [](const base::Handle<FileMetadata> &a,
                     const base::Handle<FileMetadata> &b) {
                      return a->size > b->size;
                  });

        auto rs = compaction->AddOriginFile(files[0]->number,
                                            files[0]->size);
        if (!rs.ok()) {
            return rs;
        }
        patch->DeleteFile(0, files[0]->number);
    } else {
        auto found = 0;
        for (auto i = 1; i < kMaxLevel; i++) {
            if (current()->SizeLevelFiles(i) > kMaxSizeLevel0File * (i + 1)) {
                found = i;
                break;
            }
        }

        DCHECK_GT(found, 0);
        DCHECK(!current()->file(found).empty());
        auto level = (found == (kMaxLevel - 1)) ? found : found + 1;
        for (const auto &file : current()->file(found)) {
            auto rs = compaction->AddOriginFile(file->number, file->size);
            if (!rs.ok()) {
                return rs;
            }
            patch->DeleteFile(level, file->number);
        }
    }

    *rv = compaction.release();
    return base::Status::OK();
}

base::Status VersionSet::AddIterators(const ReadOptions &options,
                                      std::vector<Iterator *> *rv) const {
    base::Status rs;

    for (auto i = 0; i < kMaxLevel; ++i) {
        auto files = current()->file(i);

        for (const auto &file : files) {
            std::unique_ptr<Iterator> iter(table_cache_->CreateIterator(options,
                                                                        file->number,
                                                                        file->size));
            rs = iter->status();
            if (!rs.ok()) {
                break;
            }
            rv->push_back(iter.release());
        }
    }

    return rs;
}

base::Status VersionSet::Recovery(uint64_t file_number,
                                  std::vector<uint64_t> *logs) {
    base::MappedMemory *rv = nullptr;
    auto rs = env_->CreateRandomAccessFile(ManifestFileName(db_name_,
                                                            file_number), &rv);
    if (!rs.ok()) {
        return rs;
    }
    std::unique_ptr<base::MappedMemory> file(rv);
    Log::Reader reader(file->buf(), file->size(), true, Log::kDefaultBlockSize);

    VersionPatch patch("");
    VersionSet::Builder builder(this, current());
    base::Slice record;
    std::string buf;
    while (reader.Read(&record, &buf) && reader.status().ok()) {
        patch.Reset();
        patch.Decode(record);
        if (patch.has_field(VersionPatch::kComparator)) {
            if (patch.comparator() != comparator_.delegated()->Name()) {
                return base::Status::Corruption("difference comparators");
            }
        }
        logs->push_back(patch.last_version());

        builder.Apply(patch);

        if (patch.has_field(VersionPatch::kRedoLogNumber)) {
            redo_log_number_ = patch.redo_log_number();
        }
        if (patch.has_field(VersionPatch::kPrevLogNumber)) {
            prev_log_number_ = patch.prev_log_number();
        }
        if (patch.has_field(VersionPatch::kNextFileNumber)) {
            next_file_number_ = patch.next_file_number();
        }
        if (patch.has_field(VersionPatch::kLastVersion)) {
            last_version_ = patch.last_version();
        }
    }

    Append(builder.Build());
    return reader.status();
}

base::Status VersionSet::Apply(VersionPatch *patch, std::mutex *mutex) {

    if (patch->has_field(VersionPatch::kRedoLogNumber)) {
        DCHECK_GE(patch->redo_log_number(), redo_log_number_);
        DCHECK_LT(patch->redo_log_number(), next_file_number_);
    } else {
        patch->set_redo_log_number(redo_log_number_);
    }

    if (!patch->has_field(VersionPatch::kPrevLogNumber)) {
        patch->set_prev_log_number(prev_log_number_);
    }

    patch->set_last_version(last_version_);
    patch->set_next_file_number(next_file_number_);

    if (log_.get() == nullptr) {
        auto rs = CreateManifestFile();
        if (!rs.ok()) {
            return rs;
        }
        patch->set_next_file_number(next_file_number_);
    }

    auto rs = WritePatch(*patch);
    if (!rs.ok()) {
        return rs;
    }

    std::string buf;
    rs = patch->Encode(&buf);
    if (!rs.ok()) {
        return rs;
    }

    Builder builder(this, current());
    builder.Apply(*patch);
    Append(builder.Build());

    redo_log_number_ = patch->redo_log_number();
    prev_log_number_ = patch->prev_log_number();

    return base::Status::OK();
}

base::Status VersionSet::CreateManifestFile() {
    DCHECK(log_file_.get() == nullptr);

    manifest_file_number_ = GenerateFileNumber();
    auto file_name = ManifestFileName(db_name_, manifest_file_number_);

    base::AppendFile *file = nullptr;
    auto rs = env_->CreateAppendFile(file_name, &file);
    if (!rs.ok()) {
        return rs;
    }
    log_file_ = std::unique_ptr<base::AppendFile>(file);
    log_ = std::unique_ptr<Log::Writer>(new Log::Writer(log_file_.get(),
                                                        Log::kDefaultBlockSize));

    return WriteSnapshot();
}

base::Status VersionSet::WriteSnapshot() {
    VersionPatch patch(comparator_.delegated()->Name());

    patch.set_last_version(last_version_);
    patch.set_next_file_number(next_file_number_);
    patch.set_prev_log_number(prev_log_number_);
    patch.set_redo_log_number(redo_log_number_);

    for (auto i = 0; i < kMaxLevel; i++) {
        auto level = current()->file(i);

        for (const auto &metadata : level) {
            patch.CreateFile(i, metadata.get());
        }
    }

    std::string current_manifest = base::Strings::Sprintf("%llu\n",
                                                          manifest_file_number_);
    auto rs = base::WriteAll(CurrentFileName(db_name_), current_manifest,
                             nullptr);
    if (!rs.ok()) {
        return rs;
    }

    return WritePatch(patch);
}

base::Status VersionSet::WritePatch(const VersionPatch &patch) {
    std::string buf;

    auto rs = patch.Encode(&buf);
    if (!rs.ok()) {
        return rs;
    }

    rs = log_->Append(buf);
    if (!rs.ok()) {
        return rs;
    }

    return log_file_->Sync();
}

VersionBuilder::VersionBuilder(VersionSet *versions, Version *current)
    : owns_(versions)
    , current_(current) {
    BySmallestKey cmp{versions->comparator_};
    for (auto i = 0; i < kMaxLevel; i++) {
        levels_[i].creation = std::set<base::Handle<FileMetadata>,
                                       BySmallestKey>(cmp);
    }
}

VersionBuilder::~VersionBuilder() {
}

void VersionBuilder::Apply(const VersionPatch &patch) {

    for (const auto &entry : patch.deletion()) {
        levels_[entry.first].deletion.insert(entry.second);
    }

    for (const auto &entry : patch.creation()) {
        levels_[entry.first].deletion.erase(entry.second->number);
        levels_[entry.first].creation.emplace(entry.second.get());
    }
}

Version *VersionBuilder::Build() {

    std::unique_ptr<Version> version(new Version(owns_));

    for (auto i = 0; i < kMaxLevel; i++) {

        auto level = current_->file(i);

        for (const auto &metadata : level) {

            if (levels_[i].deletion.find(metadata->number) !=
                levels_[i].deletion.end()) {
                version->mutable_file(i)->push_back(metadata);
            }
        }
        levels_[i].deletion.clear();

        for (const auto &metadata : levels_[i].creation) {
            version->mutable_file(i)->push_back(metadata);
        }
        levels_[i].creation.clear();
    }

    return version.release();
}

} // namespace lsm
    
} // namespace yukino