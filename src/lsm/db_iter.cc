#include "lsm/db_iter.h"
#include "lsm/merger.h"
#include "lsm/format.h"
#include "lsm/builtin.h"
#include "lsm/chunk.h"
#include "yukino/comparator.h"
#include "base/io.h"
#include "glog/logging.h"

namespace yukino {

namespace lsm {

Iterator *CreateDBIterator(const InternalKeyComparator *comparator,
                           Iterator **children, size_t n,
                           uint64_t version) {

    std::unique_ptr<Iterator> merger(CreateMergingIterator(comparator, children,
                                                           n));
    if (!merger->status().ok()) {
        return CreateErrorIterator(merger->status());
    }

    return new DBIterator(comparator->delegated(), merger.release(), version);
}

DBIterator::DBIterator(const Comparator *comparator, Iterator *iter,
                       uint64_t version)
    : comparator_(comparator)
    , delegated_(iter)
    , version_(version) {
}

DBIterator::~DBIterator() {
}

bool DBIterator::Valid() const {
    return valid_;
}

void DBIterator::SeekToFirst() {
    direction_ = kForward;
    ClearSavedValue();
    delegated_->SeekToFirst();
    if (delegated_->Valid()) {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    } else {
        valid_ = false;
    }
}

void DBIterator::SeekToLast() {
    direction_ = kReserve;
    ClearSavedValue();
    delegated_->SeekToLast();
    FindPrevUserEntry();
}

void DBIterator::Seek(const base::Slice& target) {
    direction_ = kForward;
    ClearSavedValue();
    saved_key_.clear();

    auto key = InternalKey::CreateKey(target, "", version_, kFlagValueForSeek);
    delegated_->Seek(key.key_slice());
    if (delegated_->Valid()) {
        FindNextUserEntry(false, &saved_key_ /* temporary storage */);
    } else {
        valid_ = false;
    }
}

void DBIterator::Next() {
    DCHECK(Valid());

    if (direction_ == kReserve) {  // Switch directions?
        direction_ = kForward;
        // iter_ is pointing just before the entries for this->key(),
        // so advance into the range of entries for this->key() and then
        // use the normal skipping code below.
        if (!delegated_->Valid()) {
            delegated_->SeekToFirst();
        } else {
            delegated_->Next();
        }
        if (!delegated_->Valid()) {
            valid_ = false;
            saved_key_.clear();
            return;
        }
        // saved_key_ already contains the key to skip past.
    } else {
        // Store in saved_key_ the current key so we skip it below.
        SaveKey(InternalKey::ExtractUserKey(delegated_->key()), &saved_key_);
    }
    
    FindNextUserEntry(true, &saved_key_);
}

void DBIterator::Prev() {
    DCHECK(Valid());

    if (direction_ == kForward) {  // Switch directions?
        // iter_ is pointing at the current entry.  Scan backwards until
        // the key changes so we can use the normal reverse scanning code.
        DCHECK(delegated_->Valid());  // Otherwise valid_ would have been false
        SaveKey(InternalKey::ExtractUserKey(delegated_->key()), &saved_key_);
        while (true) {
            delegated_->Prev();
            if (!delegated_->Valid()) {
                valid_ = false;
                saved_key_.clear();
                ClearSavedValue();
                return;
            }
            if (comparator_->Compare(InternalKey::ExtractUserKey(delegated_->key()),
                                     saved_key_) < 0) {
                break;
            }
        }
        direction_ = kReserve;
    }
    
    FindPrevUserEntry();
}

base::Slice DBIterator::key() const {
    DCHECK(Valid());
    return direction_ == kForward ?
        InternalKey::ExtractUserKey(delegated_->key()) : saved_key_;
}

base::Slice DBIterator::value() const {
    DCHECK(Valid());
    return direction_ == kForward ? delegated_->value() : saved_value_;
}

base::Status DBIterator::status() const {
    return status_.ok() ? delegated_->status() : status_;
}

void DBIterator::FindNextUserEntry(bool skipping, std::string *skip) {
    // Loop until we hit an acceptable entry to yield
    DCHECK(delegated_->Valid());
    DCHECK(direction_ == kForward);
    do {
        base::BufferedReader rd(delegated_->key().data(),
                                delegated_->key().size());
        auto user_key = rd.Read(delegated_->key().size() - Tag::kTagSize);
        auto tag = Tag::Decode(rd.ReadFixed64());

        if (tag.version <= version_) {
            switch (tag.flag) {
            case kFlagDeletion:
                // Arrange to skip all upcoming entries for this key since
                // they are hidden by this deletion.
                skip->assign(user_key.data(), user_key.size());
                skipping = true;
                break;

            case kFlagValue:
                if (skipping &&
                    comparator_->Compare(user_key, *skip) <= 0) {
                    // Entry hidden
                } else {
                    valid_ = true;
                    saved_key_.clear();
                    return;
                }
                break;

            default:
                DCHECK(false) << "noreached";
                break;
            }
        }
        delegated_->Next();
    } while (delegated_->Valid());
    saved_key_.clear();
    valid_ = false;
}

void DBIterator::FindPrevUserEntry() {
    DCHECK(direction_ == kReserve);

    uint8_t value_type = kFlagDeletion;
    if (delegated_->Valid()) {
        do {
            base::BufferedReader rd(delegated_->key().data(),
                                    delegated_->key().size());
            auto user_key = rd.Read(delegated_->key().size() - Tag::kTagSize);
            auto tag = Tag::Decode(rd.ReadFixed64());

            if (tag.version <= version_) {
                if ((value_type != kFlagDeletion) &&
                    comparator_->Compare(user_key, saved_key_) < 0) {
                    // We encountered a non-deleted value in entries for previous keys,
                    break;
                }
                value_type = tag.flag;
                if (value_type == kFlagDeletion) {
                    saved_key_.clear();
                    ClearSavedValue();
                } else {
                    base::Slice raw_value = delegated_->value();
                    if (saved_value_.capacity() > raw_value.size() + 1048576) {
                        std::string empty;
                        swap(empty, saved_value_);
                    }
                    SaveKey(InternalKey::ExtractUserKey(delegated_->key()),
                            &saved_key_);
                    saved_value_.assign(raw_value.data(), raw_value.size());
                }
            }
            delegated_->Prev();
        } while (delegated_->Valid());
    }
    
    if (value_type == kFlagDeletion) {
        // End
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        direction_ = kForward;
    } else {
        valid_ = true;
    }
}

} // namespace lsm

} // namespace yukino
