#include "lsm/version.h"
#include "lsm/table_cache.h"
#include "lsm/merger.h"
#include "yukino/options.h"
#include "yukino/iterator.h"
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

    std::vector<FileMetadata *> maybe_file;
    auto ucmp = owned_->comparator_.delegated();
    auto ukey = key.user_key_slice();

    for (auto metadata : file(0)) {
        if (ucmp->Compare(ukey, metadata->smallest_key.user_key_slice()) >= 0 ||
            ucmp->Compare(ukey, metadata->largest_key.user_key_slice()) <= 0) {
            maybe_file.push_back(metadata);
        }
    }

    for (auto i = 1; i < kMaxLevel; ++i) {
        if (file(i).empty()) {
            continue;
        }

        auto metadata = file(i).at(0);
        if (ucmp->Compare(ukey, metadata->smallest_key.user_key_slice()) >= 0 ||
            ucmp->Compare(ukey, metadata->largest_key.user_key_slice()) <= 0) {
            maybe_file.push_back(metadata);
        }
    }

    if (maybe_file.empty()) {
        return base::Status::NotFound("");
    }

    Iterator *merger = nullptr;
    std::vector<Iterator*> iters;
    auto defer = base::Defer([&iters, merger] () {
        std::for_each(iters.begin(), iters.end(), [merger](Iterator *iter) {
            if (iter != merger) {
                delete iter;
            }
        });
        delete merger;
    });
    for (auto metadata : maybe_file) {
        auto iter = owned_->table_cache_->CreateIterator(options,
                                                         metadata->number,
                                                         metadata->size);
        if (!iter->status().ok()) {
            return iter->status();
        }
    }

    merger = CreateMergingIterator(&owned_->comparator_, &iters[0],
                                   iters.size());
    merger->Seek(key.key());
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

} // namespace lsm
    
} // namespace yukino