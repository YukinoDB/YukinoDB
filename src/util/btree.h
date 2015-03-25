#ifndef YUKINO_UTIL_BTREE_H_
#define YUKINO_UTIL_BTREE_H_

#include "base/status.h"
#include "base/slice.h"
#include "base/base.h"
#include "glog/logging.h"
#include <vector>
#include <string>

namespace yukino {

namespace util {

/**
 * The B+tree implements.
 *
 *
 */
template <class Key, class Comparator>
class BTree : public base::DisableCopyAssign {
public:
    struct Page;
    struct Entry;

    class Iterator;

    enum NodeType {
        kLeaf,
        kNormal,
    };

    BTree(int order, Comparator comparator)
        : order_(order)
        , root_(AllocatePage(0, kLeaf))
        , comparator_(comparator) {
    }

    bool Put(const Key &key, Key *old);
    bool Delete(const Key &key, Key *old);

    std::tuple<Page*, int> FindLessThan(const Key &key) const;
    std::tuple<Page*, int> FindGreaterOrEqual(const Key &key) const;

    Page *TEST_GetRoot() const { return root_; }
private:
    Entry *Insert(const Key &key, Page *node, bool *is_new);
    inline Page *FindLeafPage(const Key &key, Page *node) const;
    int PageMaxSize(Page *node) const { return order_; }
    void SplitLeaf(Page *page);
    void SplitNonLeaf(Page *page);
    inline Page *AllocatePage(int num_entries, NodeType type);

    const int order_;
    /**
     * Non-root node entries: [m/2, m-1]
     * split node entries: m/2
     *
     */
    Page *root_;

    Comparator comparator_;
};

template<class Key, class Comparator>
struct BTree<Key, Comparator>::Page {
    struct {
        Page  *page = nullptr;  // parent's page
        Entry *entry = nullptr; // parent's entry
    } parent;

    /**
     * Non-Leaf Node: pointer to last child node
     *     Leaf Node: pointer to sibling node
     */
    Page *link = nullptr;

    int id; // page id for filesystem
    int dirty = 0; // is page dirty?
    std::vector<Entry> entries; // owned entries

    Page(int page_id, int cap_entries): id(page_id) {
        entries.reserve(cap_entries);
    }

    inline Entry *FindGreaterOrEqual(const Key &key, Comparator cmp,
                                     bool or_new,
                                     bool *is_new) {
        int i = 0;
        for (i = 0; i < entries.size(); ++i) {
            auto rv = cmp(key, entries[i].key);
            if (rv < 0) {
                if (or_new) {
                    entries.insert(entries.begin() + i, Entry{key, nullptr});
                }
                if (is_new) *is_new = true;
                return &entries[i];
            } else if (rv == 0) {
                if (is_new) *is_new = false;
                return &entries[i];
            }
        }
        if (or_new) {
            if (is_new) *is_new = true;
            entries.push_back(Entry{key, nullptr});
            return &entries[i];
        }
        return nullptr;
    }

    inline int FindLessThan(const Key &target, Comparator cmp) const {
        for (int i = static_cast<int>(size() - 1); i >= 0; i--) {
            if (cmp(key(i), target) < 0) {
                return i;
            }
        }
        return -1;
    }

    int FindGreaterOrEqual(const Key &target, Comparator cmp) const {
        int left = 0, right = static_cast<int>(size()) - 1, middle = 0;
        while (left <= right) {
            middle = (left + right) / 2;

            auto rv = cmp(target, key(middle));
            if (rv < 0) {
                right = middle - 1;
            } else if (rv > 0) {
                left = middle + 1;
            } else {
                return middle;
            }
        }

        for (auto i = middle; i < static_cast<int>(size()); i++) {
            if (cmp(target, key(i)) < 0) {
                return i;
            }
        }
        return -1;
    }

    inline Entry *Put(const Entry &entry, Comparator cmp) {
        auto found = FindGreaterOrEqual(entry.key, cmp, true, nullptr);
        *found = entry;
        return found;
    }

    inline Entry *Put(const Key &key, Page *link, Comparator cmp) {
        return Put(Entry{key, link}, cmp);
    }

    inline Page *GetChild(Entry *entry) {
        return !entry ? link : entry->link;
    }

    inline void SetLChild(Entry *entry, Page *child) {
        entry->link = child;
    }

    inline void SetRChild(Entry *entry, Page *child) {
        auto i = index_of(entry);
        if (i == entries.size() - 1) {
            link = child;
        } else {
            entries[i + 1].link = child;
        }
    }

    /**
     * Move owned entries to target node.
     *
     * @param num >0 front entry be moved, <0 back entry be moved.
     * @param to target node
     * @return Last entry be moved.
     */
    inline Entry *MoveTo(int num, Page *to, Comparator cmp) {
        DCHECK_NE(this, to);
        Entry *rv = nullptr;
        if (num > 0) {
            for (auto i = 0; i < num; ++i) {
                rv = to->Put(entries[i], cmp);
            }
            entries.erase(entries.begin(), entries.begin() + num);
        } else if (num < 0) {
            num = -num;
            for (auto i = size() - num; i < size(); ++i) {
                rv = to->Put(entries[i], cmp);
            }
            entries.erase(entries.end() - num, entries.end());
        }
        return rv;
    }

    inline size_t size() const { return entries.size(); }

    inline const Entry &back() const { return entries.back(); }

    inline const Key &key(int i) const { return entries[i].key; }

    inline Page *child(int i) const { return entries[i].link; }

    inline bool is_leaf() const {
        return entries.empty() || entries[0].link == nullptr;
    }

    inline int index_of(Entry *entry) {
        return static_cast<int>(entry - &entries[0]);
    }
};

template<class Key, class Comparator>
struct BTree<Key, Comparator>::Entry {
    Page *link = nullptr; // pointer to child node
    /**
     * Non-Leaf Node: store only key
     *     Leaf Node: store key-value pair
     */
    Key key;

    Entry(const Key &k, Page *p) : link(p), key(k) {}
};


template<class Key, class Comparator>
class BTree<Key, Comparator>::Iterator {
public:
    enum Direction {
        kForward,
        kReserve,
    };

    Iterator(BTree<Key, Comparator> *owns)
        : owns_(owns) {
    }

    void SeekToFirst() {
        page_ = GetFirstLeaf();
        DCHECK_GT(page_->size(), 0);
        local_ = 0;
    }

    void SeekToLast() {
        page_ = GetLastLeaf();
        DCHECK_GT(page_->size(), 0);
        local_ = static_cast<int>(page_->size()) - 1;
    }

    void Seek(const Key &key) {
        auto rv = owns_->FindGreaterOrEqual(key);
        page_  = std::get<0>(rv);
        local_ = std::get<1>(rv);
        direction_ = kForward;
    }

    // [0][1][2] [3][4][5]
    void Next() {
        DCHECK(Valid());

        if (local_ >= page_->size() - 1) {
            if (page_) {
                page_ = page_->link;
                local_ = 0;
            }
        } else {
            local_++;
        }
        direction_ = kForward;
    }

    // [0][1][2] [3][4][5]
    void Prev() {
        DCHECK(Valid());

        if (local_ - 1 < 0) {
            auto rv = owns_->FindLessThan(page_->key(0));
            page_ = std::get<0>(rv);
            local_ = std::get<1>(rv);
        } else {
            local_--;
        }
        direction_ = kReserve;
    }

    const Key &key() const {
        DCHECK(Valid());
        return page_->key(local_);
    }

    bool Valid() const {
        return page_ != nullptr;
    }

private:
    typename BTree<Key, Comparator>::Page *GetFirstLeaf() {
        auto page = owns_->root_;
        while (!page->is_leaf()) {
            page = page->child(0);
        }
        return page;
    }

    typename BTree<Key, Comparator>::Page *GetLastLeaf() {
        auto page = owns_->root_;
        while (!page->is_leaf()) {
            page = page->link;
        }
        return page;
    }

    BTree<Key, Comparator> *owns_;
    typename BTree<Key, Comparator>::Page *page_ = nullptr;
    int local_ = 0;
    Direction direction_ = kForward;
};

template<class Key, class Comparator>
bool BTree<Key, Comparator>::Put(const Key &key, Key *old) {
    bool is_new = false;
    auto entry = Insert(key, root_, &is_new);
    if (!is_new) {
        *old = entry->key;
        return true;
    }
    if (comparator_(entry->key, key) != 0) {
        entry->key = key;
    }
    return false;
}

template<class Key, class Comparator>
std::tuple<typename BTree<Key, Comparator>::Page*, int>
BTree<Key, Comparator>::FindLessThan(const Key &key) const {
    auto page = root_;
    while (!page->is_leaf()) {
        auto i = page->FindLessThan(key, comparator_);
        if (i == static_cast<int>(page->size()) - 1) {
            auto rv = comparator_(key, page->link->key(0));
            if (rv <= 0) {
                page = page->child(i);
            } else if (rv > 0) {
                page = page->link;
            }
        } else {
            page = page->child(i < 0 ? 0 : i);
        }
    }

    int i = -1;
    if (page) {
        i = page->FindLessThan(key, comparator_);
        if (i < 0) {
            page = nullptr;
        }
    }
    return std::make_tuple(page, i);
}

template<class Key, class Comparator>
std::tuple<typename BTree<Key, Comparator>::Page*, int>
BTree<Key, Comparator>::FindGreaterOrEqual(const Key &key) const {
    auto page = FindLeafPage(key, root_);
    auto i = page->FindGreaterOrEqual(key, comparator_);
    if (i < 0) {
        page = nullptr;
    }
    return std::make_tuple(page, i);
}

template<class Key, class Comparator>
typename BTree<Key, Comparator>::Entry *
BTree<Key, Comparator>::Insert(const Key &key, Page *node, bool *is_new) {
    auto page = FindLeafPage(key, node);
    if (page->size() + 1 > PageMaxSize(page)) {
        SplitLeaf(page);
        return Insert(key, root_, is_new);
    }
    return page->FindGreaterOrEqual(key, comparator_, true, is_new);
}

template<class Key, class Comparator>
void BTree<Key, Comparator>::SplitLeaf(Page *page) {
    DCHECK(page->is_leaf());
    //bool is_new = false;

    auto num_entries = static_cast<int>(page->entries.size()) / 2;
    auto sibling = AllocatePage(num_entries, kLeaf);

    if (page == root_) {
        root_ = AllocatePage(1, kNormal);
        page->parent.page = root_;
    }
    auto parent = page->parent.page;

    page->MoveTo(-num_entries, sibling, comparator_);
    sibling->link = page->link;
    page->link = sibling; // leaf link
    sibling->parent.page = parent;

    auto entry = parent->Put(page->back(), comparator_);
    parent->SetLChild(entry, page);
    parent->SetRChild(entry, sibling);
    if (parent->size() > PageMaxSize(parent)) {
        SplitNonLeaf(parent);
    }

    page->dirty++;
    parent->dirty++;
    sibling->dirty++;
}

template<class Key, class Comparator>
void BTree<Key, Comparator>::SplitNonLeaf(Page *page) {
    DCHECK(!page->is_leaf());

    auto num_entries = static_cast<int>(page->entries.size()) / 2;
    auto sibling = AllocatePage(num_entries, kNormal);

    if (page == root_) {
        root_ = AllocatePage(1, kNormal);
        page->parent.page = root_;
    }
    auto parent = page->parent.page;

    page->MoveTo(-num_entries, sibling, comparator_);
    sibling->parent.page = parent;
    sibling->link = page->link;

    page->link = page->back().link;
    auto entry = page->MoveTo(-1, parent, comparator_);
    parent->SetLChild(entry, page);
    parent->SetRChild(entry, sibling);

    if (parent->size() > PageMaxSize(parent)) {
        SplitNonLeaf(parent);
    }

    page->dirty++;
    parent->dirty++;
    sibling->dirty++;
}

template<class Key, class Comparator>
inline
typename BTree<Key, Comparator>::Page *
BTree<Key, Comparator>::FindLeafPage(const Key &key, Page *page) const {
    while (!page->is_leaf()) {
        auto i = page->FindGreaterOrEqual(key, comparator_);
        page = page->GetChild(i < 0 ? nullptr : &page->entries[i]);
    }
    return page;
}

template<class Key, class Comparator>
inline
typename BTree<Key, Comparator>::Page *
BTree<Key, Comparator>::AllocatePage(int num_entries, NodeType /*type*/) {
    auto page = new Page(0, num_entries);
    return page;
}

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_BTREE_H_