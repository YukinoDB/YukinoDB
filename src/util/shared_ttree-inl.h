#ifndef YUKINO_UTIL_SHARED_TTREE_INL_H_
#define YUKINO_UTIL_SHARED_TTREE_INL_H_

#include "util/shared_ttree.h"
#include "base/varint_encoding.h"
#include "yukino/comparator.h"

namespace yukino {

namespace util {

class SharedTTree::Delegate {
public:
    inline Delegate(const void *page, const SharedTTree *owns,
                    bool initial = false);

    inline void ToUsed();

    void ToFree() { mutable_spce()->type = kPageFree; }

    inline bool InBounds(const base::Slice &key) const;

    inline base::Slice key(int i) const;

    base::Slice min_key() const { return key(0); }
    base::Slice max_key() const { return key(node_->num_entries - 1); }

    Node *node() const { return node_; }

    const Node *parent() const { return get(node_->parent); }
    const Node *lchild() const { return get(node_->lchild); }
    const Node *rchild() const { return get(node_->rchild); }

    void set_lchild(const Node *child) {
        node_->lchild = to_compressed_offset(child);
    }

    void set_rchild(const Node *child) {
        node_->rchild = to_compressed_offset(child);
    }

    void set_parent(const Node *child) {
        node_->parent = to_compressed_offset(child);
    }

    bool is_leaf() const { return node_->lchild == 0 && node_->rchild == 0; }

    inline bool Put(const base::Slice &target, std::string *old);
    inline int FindGreaterOrEqual(const base::Slice &target, bool *equal);

    inline void DeleteAt(int i);
    inline void ReplaceAt(int i, const base::Slice &target);
    inline void InsertAt(int i, const base::Slice &target);
    inline void Add(const base::Slice &target);

    inline Spec *mutable_spce();
    inline const Spec *spec() const;

    inline size_t capacity() const;

    static size_t used_space(const base::Slice &target) {
        return base::Varint64::Sizeof(target.size()) + target.size();
    }

    size_t num_entries() const { return node_->num_entries; }

    inline size_t GetOriginSize(int i) const;

    inline std::string ToString() const;

private:
    inline void WriteKey(char *base, const base::Slice &target);
    inline void Expand(size_t base, size_t backward);
    inline void Shrink(size_t base, size_t forward);

    size_t offset(uint32_t ptr) const {
        return static_cast<size_t>(ptr) << owns_->page_shift_;
    }

    inline uint32_t to_compressed_offset(const Node *child) const;
    inline const Node *get(uint32_t ptr) const;

    union {
        Node *node_;
        char *base_;
        const void *dummy_;
    };
    const SharedTTree *owns_;
};

SharedTTree::Delegate::Delegate(const void *page, const SharedTTree *owns,
                                bool initial)
    : dummy_(page)
    , owns_(DCHECK_NOTNULL(owns)) {
    if (initial) {
        ToUsed();
    }
}

inline void SharedTTree::Delegate::ToUsed() {
    ::memset(node_, 0, sizeof(*node_));
    mutable_spce()->top  = owns_->page_size_ - sizeof(Spec);
    mutable_spce()->type = kPageUsed;
}

inline bool SharedTTree::Delegate::InBounds(const base::Slice &key) const {
    return node_->num_entries > 1 &&
    owns_->comparator_->Compare(key, min_key()) >= 0 &&
    owns_->comparator_->Compare(key, max_key()) <= 0;
}

inline base::Slice SharedTTree::Delegate::key(int i) const {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, node_->num_entries);

    size_t len = 0;
    auto size = base::Varint32::Decode(base_ + node_->index[i], &len);
    return base::Slice(base_ + node_->index[i] + len, size);
}

inline uint32_t
SharedTTree::Delegate::to_compressed_offset(const Node *child) const {
    DCHECK(owns_->IsUsed(child));
    if (!child) {
        return 0;
    }
    auto p = reinterpret_cast<const uint8_t *>(child);
    DCHECK_GE(p, owns_->mmap_->buf(0));

    auto offset = static_cast<size_t>(p - owns_->mmap_->buf());
    DCHECK_EQ(0, offset % owns_->page_size_);

    return static_cast<uint32_t>(offset >> owns_->page_shift_);
}

inline const SharedTTree::Node *SharedTTree::Delegate::get(uint32_t ptr) const {
    const Node *rv = nullptr;
    if (ptr == 0) {
        rv = nullptr;
    } else {
        rv = reinterpret_cast<const Node *>(owns_->mmap_->buf(offset(ptr)));
    }
    DCHECK(owns_->IsUsed(rv));
    return rv;
}

inline bool SharedTTree::Delegate::Put(const base::Slice &target,
                                       std::string *old) {
    bool equal = false;
    auto i = FindGreaterOrEqual(target, &equal);
    if (i < 0) {
        Add(target);
        return false;
    }

    if (equal) {
        if (old) {
            auto old_val = key(i);
            old->assign(old_val.data(), old_val.size());
        }
        ReplaceAt(i, target);
    } else {
        InsertAt(i, target);
    }
    return equal;
}

inline int SharedTTree::Delegate::FindGreaterOrEqual(const base::Slice &target,
                                                     bool *equal) {
    *equal = true;

    int left = 0, right = node_->num_entries - 1, middle = 0;
    while (left <= right) {
        middle = (left + right) / 2;

        auto rv = owns_->comparator_->Compare(target, key(middle));
        if (rv < 0) {
            right = middle - 1;
        } else if (rv > 0) {
            left = middle + 1;
        } else {
            return middle;
        }
    }
    *equal = false;

    for (auto i = middle; i < node_->num_entries; i++) {
        if (owns_->comparator_->Compare(target, key(i)) < 0) {
            return i;
        }
    }
    return -1;
}

inline void SharedTTree::Delegate::DeleteAt(int i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, node_->num_entries);

    Shrink(node_->index[i], GetOriginSize(i));

    ::memmove(&node_->index[i], &node_->index[i+1],
              (node_->num_entries - i - 1) * sizeof(node_->index[0]));
    --node_->num_entries;
}

inline void SharedTTree::Delegate::ReplaceAt(int i, const base::Slice &target) {
    auto need_size = used_space(target);

    auto origin_size = GetOriginSize(i);
    if (origin_size > need_size) {
        Shrink(node_->index[i], origin_size - need_size);
    } else if (origin_size < need_size) {
        DCHECK_LE(need_size - origin_size, capacity());
        Expand(node_->index[i], need_size - origin_size);
    }
    WriteKey(base_ + node_->index[i], target);
}

inline void SharedTTree::Delegate::InsertAt(int i, const base::Slice &target) {
    auto data_size = used_space(target);
    auto need_size = sizeof(node_->index[0]) + data_size;
    DCHECK_LE(need_size, capacity());

    ::memmove(&node_->index[i + 1], &node_->index[i],
              sizeof(node_->index[0]));
    ++node_->num_entries;

    mutable_spce()->top -= data_size;
    node_->index[i] = spec()->top;

    WriteKey(base_ + node_->index[i], target);
}

inline void SharedTTree::Delegate::Add(const base::Slice &target) {
    auto data_size = used_space(target);
    auto need_size = sizeof(node_->index[0]) + data_size;
    DCHECK_LE(need_size, capacity());

    mutable_spce()->top -= data_size;
    node_->index[node_->num_entries++] = spec()->top;

    WriteKey(base_ + mutable_spce()->top, target);
}

inline void SharedTTree::Delegate::WriteKey(char *base,
                                            const base::Slice &target) {
    base += base::Varint64::Encode(base, target.size());
    ::memcpy(base, target.data(), target.size());
}

inline void SharedTTree::Delegate::Expand(size_t base, size_t backward) {
    auto top = base_ + mutable_spce()->top;
    DCHECK_GE(base, spec()->top);
    ::memmove(top - backward, top, base - mutable_spce()->top);
    mutable_spce()->top -= backward;

    for (auto i = 0; i < node_->num_entries; ++i) {
        if (node_->index[i] <= base) {
            node_->index[i] -= backward;
        }
    }
}

inline void SharedTTree::Delegate::Shrink(size_t base, size_t forward) {
    auto top = base_ + mutable_spce()->top;
    DCHECK_GE(base, spec()->top);
    ::memmove(top + forward, top, base - mutable_spce()->top);
    mutable_spce()->top += forward;

    for (auto i = 0; i < node_->num_entries; ++i) {
        if (node_->index[i] <= base) {
            node_->index[i] += forward;
        }
    }
}

inline size_t SharedTTree::Delegate::GetOriginSize(int i) const {
    size_t origin = 0;
    auto size = base::Varint32::Decode(base_ + node_->index[i], &origin);
    return origin + size;
}

inline std::string SharedTTree::Delegate::ToString() const {
    std::string buf("[");
    for (auto i = 0; i < num_entries(); ++i) {
        if (i > 0) {
            buf.append(", ");
        }
        buf.append(key(i).ToString());
    }
    buf.append("]");
    return buf;
}

inline size_t SharedTTree::Delegate::capacity() const {
    size_t limit = sizeof(*node_) - sizeof(node_->index[0]) +
    node_->num_entries * sizeof(node_->index[0]);
    DCHECK_GE(spec()->top, limit);
    return spec()->top - limit;
}

inline
SharedTTree::Spec *
SharedTTree::Delegate::mutable_spce() {
    return reinterpret_cast<Spec *>(base_ + owns_->page_size_ - sizeof(Spec));
}

inline
const SharedTTree::Spec *
SharedTTree::Delegate::spec() const {
    return reinterpret_cast<const Spec *>(base_ + owns_->page_size_ - sizeof(Spec));
}

//------------------------------------------------------------------------------
// class SharedTTree
//------------------------------------------------------------------------------

inline int SharedTTree::GetDepth(const Node *node) const {
    // TODO:
    return 0;
}

} // namespace util

} // namespace yukino
        
#endif // YUKINO_UTIL_SHARED_TTREE_INL_H_