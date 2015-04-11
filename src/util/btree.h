#ifndef YUKINO_UTIL_BTREE_H_
#define YUKINO_UTIL_BTREE_H_

#include "base/status.h"
#include "base/slice.h"
#include "base/base.h"
#include "base/ref_counted.h"
#include "glog/logging.h"
#include <vector>
#include <string>

namespace yukino {

namespace util {

namespace detail {

template<class Key, class Comparator> struct Entry;
template<class Key, class Comparator> struct Page;

template<class Key, class Comparator>
struct Entry {
    typedef detail::Page<Key, Comparator> Page;

    uint64_t link = 0; // pointer to child node
    /**
     * Non-Leaf Node: store only key
     *     Leaf Node: store key-value pair
     */
    Key key;

    Entry(const Key &k, uintptr_t page) : link(page), key(k) {}
    
    Entry() {}
};

template<class Key, class Comparator>
struct Page : public base::ReferenceCounted<Page<Key, Comparator>> {
    typedef detail::Entry<Key, Comparator> Entry;

    uint64_t parent = 0;

    /**
     * Non-Leaf Node: pointer to last child node
     *     Leaf Node: pointer to sibling node
     */
    uint64_t link = 0;

    uint64_t id; // page id for filesystem
    int dirty = 1; // is page dirty?
    std::vector<Entry> entries; // owned entries

    Page(uint64_t page_id, int cap_entries): id(page_id) {
        entries.reserve(cap_entries);
    }

    Entry *FindOrInsert(const Key &target, Comparator cmp, bool *is_new) {
        auto i = FindGreaterOrEqual(target, cmp);
        if (i >= 0) {
            if (cmp(target, key(i)) == 0) {
                if (is_new) *is_new = false;
            } else {
                if (is_new) *is_new = true;
                entries.insert(entries.begin() + i, Entry{target, 0});
            }
            return &entries[i];
        }
        if (is_new) *is_new = true;
        entries.push_back(Entry {target, 0});
        return &entries.back();
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
        auto found = FindOrInsert(entry.key, cmp, nullptr);
        *found = entry;
        return found;
    }

    inline Entry *Put(const Key &key, uintptr_t link, Comparator cmp) {
        return Put(Entry{key, link}, cmp);
    }

    inline const Entry *Get(const Key &target, Comparator cmp) const {
        auto i = FindGreaterOrEqual(target, cmp);
        if (i >= 0) {
            if (cmp(target, key(i)) == 0) {
                return &entries[i];
            }
        }
        return nullptr;
    }

    inline void Delete(const Entry *entry) {
        DeleteAt(index_of(entry));
    }

    inline void DeleteAt(int i) {
        DCHECK_GE(i, 0);
        DCHECK_LT(i, size());

        if (i > 0) {
            SetRChild(&entries[i - 1], GetLChild(i));
        }
        if (i < size() - 1) {
            SetLChild(&entries[i + 1], GetRChild(i));
        }
        entries.erase(entries.begin() + i);
    }

    inline uint64_t GetChild(Entry *entry) {
        return !entry ? link : entry->link;
    }

    inline void SetLChild(Entry *entry, uint64_t child) {
        entry->link = child;
    }

    inline void SetRChild(Entry *entry, uint64_t child) {
        auto i = index_of(entry);
        if (i == entries.size() - 1) {
            link = child;
        } else {
            entries[i + 1].link = child;
        }
    }

    inline uint64_t GetLChild(int i) { return child(i); }

    inline uint64_t GetRChild(int i) {
        if (i == size() - 1) {
            return link;
        } else {
            return child(i + 1);
        }
    }

    inline size_t size() const { return entries.size(); }
    
    inline const Entry &back() const { return entries.back(); }
    
    inline const Key &key(int i) const { return entries[i].key; }
    
    inline uint64_t child(int i) const { return entries[i].link; }
    
    inline bool is_leaf() const {
        return entries.empty() || entries[0].link == 0;
    }

    inline bool is_root() const { return parent == 0; }
    
    inline int index_of(const Entry *entry) {
        return static_cast<int>(entry - &entries[0]);
    }
};


} // namespace detail

template<class Key, class Comparator>
class BTreeDefaultAllocator {
public:
    typedef detail::Page<Key, Comparator> Page;

    Page *Allocate(int num_entries) {
        auto rv = new detail::Page<Key, Comparator>(0, num_entries);
        rv->id = reinterpret_cast<uintptr_t>(rv);
        pages_.emplace_back(rv);
        return rv;
    }

    void Free(const Page *page) {
        for (auto iter = pages_.begin(); iter != pages_.end(); ++iter) {
            if (page == iter->get()) {
                pages_.erase(iter);
                break;
            }
        }
    }

    Key Duplicate(const Key &key) { return key; }

    Page *Get(uint64_t id, bool /*cached*/) const {
        return reinterpret_cast<Page *>(id);
    }

private:
    std::vector<base::Handle<Page>> pages_;
};

/**
 * The B+tree implements.
 */
template <class Key, class Comparator,
          class Allocator = BTreeDefaultAllocator<Key, Comparator>>
class BTree : public base::DisableCopyAssign {
public:
    typedef detail::Page<Key, Comparator> Page;
    typedef detail::Entry<Key, Comparator> Entry;

    class Iterator;

    enum NodeType {
        kLeaf,
        kNormal,
    };

    BTree(int order, Comparator comparator,
          Allocator allocator = BTreeDefaultAllocator<Key, Comparator>())
        : order_(order)
        , comparator_(comparator)
        , allocator_(allocator) {
        // Ensure the allocator initialized.
        root_ = AllocatePage(0, kLeaf);
    }

    /**
     * Put the key into b+tree
     *
     * @param key key for putting.
     * @param old old key if exists.
     * @return true - key already exists; false - new key.
     */
    inline bool Put(const Key &key, Key *old);

    /**
     * Delete the key from b+tree
     *
     * @param key key for deleting.
     * @param old old key if exists.
     * @return @return true - key exists; false - not exists.
     */
    inline bool Delete(const Key &key, Key *old);

    std::tuple<Page*, int> FindLessThan(const Key &key) const;
    std::tuple<Page*, int> FindGreaterOrEqual(const Key &key) const;

    /**
     * Travel for every pages
     */
    template<class T>
    bool Travel(Page *page, T callback) {
        if (!callback(page)) {
            return false;
        }
        if (!page->is_leaf()) {
            for (const auto &entry : page->entries) {
                if (!Travel(GetPage(entry.link), callback)) {
                    return false;
                }
            }
        }
        if (page->is_leaf()) {
            return true;
        } else {
            return Travel(GetPage(page->link), callback);
        }
    }

    inline Page *GetPage(uint64_t id, bool cached = true) const {
        return allocator_.Get(id, cached);
    }

    int order() const { return order_; }

    //--------------------------------------------------------------------------
    // Testing
    //--------------------------------------------------------------------------
    Page *TEST_GetRoot() const { return root_.get(); }
    void TEST_Attach(Page *root) { root_ = root; }
    inline Page *TEST_FirstPage() const;

private:
    Entry *Insert(const Key &key, Page *node, bool *is_new);
    Entry  Erase(const Key &key, Page *page, bool *is_exists);

    inline Page *FindLeafPage(const Key &key, Page *node) const;
    int PageMaxSize(Page *node) const { return order_; }

    void SplitLeaf(Page *page);
    void SplitNonLeaf(Page *page);

    void RemoveLeaf(const Key &hint, Page *page);
    void RemoveNonLeaf(const Entry &hint, Page *page);

    /**
     * Move owned entries to target node.
     *
     * @param num >0 front entry be moved, <0 back entry be moved.
     * @param to target node
     * @return Last entry be moved.
     */
    inline Entry *MoveTo(Page *from, int num, Page *to);

    inline Page *AllocatePage(int num_entries, NodeType type) {
        return allocator_.Allocate(num_entries);
    }

    inline void FreePage(Page *page) {
        allocator_.Free(page);
    }

    const int order_;

    /**
     * Non-root node entries: [m/2, m-1]
     * split node entries: m/2
     */
    base::Handle<Page> root_;

    Comparator comparator_;
    Allocator allocator_;
};

template<class Key, class Comparator, class Allocator>
class BTree<Key, Comparator, Allocator>::Iterator {
public:
    enum Direction {
        kForward,
        kReserve,
    };

    typedef typename BTree<Key, Comparator, Allocator>::Page Page;

    Iterator(BTree<Key, Comparator, Allocator> *owns, bool cached = false)
        : owns_(owns)
        , cached_(cached) {
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
                page_ = owns_->GetPage(page_->link, cached_);
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
            std::tie(page_, local_) = owns_->FindLessThan(page_->key(0));
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
        return !page_.is_null();
    }

private:
    typename BTree<Key, Comparator, Allocator>::Page *GetFirstLeaf() {
        auto page = owns_->root_.get();
        while (!page->is_leaf()) {
            page = owns_->GetPage(page->child(0));
        }
        return page;
    }

    typename BTree<Key, Comparator, Allocator>::Page *GetLastLeaf() {
        auto page = owns_->root_.get();
        while (!page->is_leaf()) {
            page = owns_->GetPage(page->link);
        }
        return page;
    }

    BTree<Key, Comparator, Allocator> *owns_;
    base::Handle<Page> page_;
    int local_ = 0;
    const bool cached_;
    Direction direction_ = kForward;
};

template<class Key, class Comparator, class Allocator>
inline bool BTree<Key, Comparator, Allocator>::Put(const Key &key, Key *old) {
    bool is_new = false;
    auto entry = Insert(key, root_.get(), &is_new);
    if (is_new) {
        return false;
    }

    *old = entry->key;
    entry->key = key;
    return true;
}

template<class Key, class Comparator, class Allocator>
inline bool BTree<Key, Comparator, Allocator>::Delete(const Key &key, Key *old) {
    bool is_exists = false;
    auto entry = Erase(key, root_.get(), &is_exists);
    if (is_exists) {
        *old = entry.key;
    }
    return is_exists;
}

template<class Key, class Comparator, class Allocator>
std::tuple<typename BTree<Key, Comparator, Allocator>::Page*, int>
BTree<Key, Comparator, Allocator>::FindLessThan(const Key &key) const {
    auto page = root_.get();
    while (!page->is_leaf()) {
        auto i = page->FindLessThan(key, comparator_);
        if (i == static_cast<int>(page->size()) - 1) {
            auto rv = comparator_(key, GetPage(page->link)->key(0));
            if (rv <= 0) {
                page = GetPage(page->child(i));
            } else if (rv > 0) {
                page = GetPage(page->link);
            }
        } else {
            page = GetPage(page->child(i < 0 ? 0 : i));
        }
    }

    int i = -1;
    if (page) {
        i = page->FindLessThan(key, comparator_);
        if (i < 0) {
            page = nullptr;
        }
    }
    return {page, i};
}

template<class Key, class Comparator, class Allocator>
std::tuple<typename BTree<Key, Comparator, Allocator>::Page*, int>
BTree<Key, Comparator, Allocator>::FindGreaterOrEqual(const Key &key) const {
    auto page = FindLeafPage(key, root_.get());
    auto i = page->FindGreaterOrEqual(key, comparator_);
    if (i < 0) {
        page = nullptr;
    }
    return {page, i};
}

template<class Key, class Comparator, class Allocator>
inline
typename BTree<Key, Comparator, Allocator>::Page *
BTree<Key, Comparator, Allocator>::TEST_FirstPage() const {
    auto page = root_.get();
    while (!page->is_leaf()) {
        page = GetPage(page->child(0));
    }
    return page;
}

template<class Key, class Comparator, class Allocator>
typename BTree<Key, Comparator, Allocator>::Entry *
BTree<Key, Comparator, Allocator>::Insert(const Key &key, Page *node,
                                          bool *is_new) {
    base::Handle<Page> page(FindLeafPage(key, node));
    if (page->size() + 1 > PageMaxSize(page.get())) {
        SplitLeaf(page.get());
        return Insert(key, root_.get(), is_new);
    }
    return page->FindOrInsert(key, comparator_, is_new);
}

template<class Key, class Comparator, class Allocator>
typename BTree<Key, Comparator, Allocator>::Entry
BTree<Key, Comparator, Allocator>::Erase(const Key &key, Page *node,
                                         bool *is_exists) {
    Entry rv {key, 0};

    base::Handle<Page> page(FindLeafPage(key, node));
    auto entry = page->Get(key, comparator_);
    if (entry) {
        rv = *entry;
        page->Delete(entry);
        if (page->size() == 0) {
            RemoveLeaf(rv.key, page.get());
        }
        *is_exists = true;
    } else {
        *is_exists = false;
    }
    return rv;
}

template<class Key, class Comparator, class Allocator>
void BTree<Key, Comparator, Allocator>::SplitLeaf(Page *page) {
    DCHECK(page->is_leaf());

    auto num_entries = static_cast<int>(page->entries.size()) / 2;
    base::Handle<Page> sibling(AllocatePage(num_entries, kLeaf));

    base::Handle<Page> parent;
    if (page == root_.get()) {
        root_ = AllocatePage(1, kNormal);
        page->parent = root_->id;
        parent = root_;
    } else {
        parent = GetPage(page->parent);
    }

    MoveTo(page, -num_entries, sibling.get());
    sibling->link = page->link;
    page->link = sibling->id; // leaf link
    sibling->parent = parent->id;

    auto entry = parent->Put(allocator_.Duplicate(page->back().key),
                             page->back().link,
                             comparator_);
    parent->SetLChild(entry, page->id);
    parent->SetRChild(entry, sibling->id);

    DCHECK_EQ(sibling->parent, parent->id);
    if (parent->size() > PageMaxSize(parent.get())) {
        SplitNonLeaf(parent.get());
    }

    page->dirty++;
    parent->dirty++;
    sibling->dirty++;
}

template<class Key, class Comparator, class Allocator>
void BTree<Key, Comparator, Allocator>::SplitNonLeaf(Page *page) {
    DCHECK(!page->is_leaf());

    auto num_entries = static_cast<int>(page->entries.size()) / 2;
    base::Handle<Page> sibling(AllocatePage(num_entries, kNormal));

    base::Handle<Page> parent;
    if (page == root_.get()) {
        root_ = AllocatePage(1, kNormal);
        page->parent = root_->id;
        parent = root_;
    } else {
        parent = GetPage(page->parent);
    }

    sibling->parent = parent->id;
    sibling->link = page->link;

    // NOTICE: Ensure sibling's children parent pointer.
    GetPage(sibling->link)->parent = sibling->id;
    GetPage(sibling->link)->dirty++;
    MoveTo(page, -num_entries, sibling.get());

    page->link = page->back().link;
    auto entry = MoveTo(page, -1, parent.get());
    parent->SetLChild(entry, page->id);
    parent->SetRChild(entry, sibling->id);

    // NOTICE: Ensure page's children parent pointer.
    GetPage(page->link)->parent = page->id;
    GetPage(page->link)->dirty++;

    DCHECK_EQ(sibling->parent, parent->id);
    if (parent->size() > PageMaxSize(parent.get())) {
        SplitNonLeaf(parent.get());
    }

    page->dirty++;
    parent->dirty++;
    sibling->dirty++;
}

//         [3][5]
// [1][2][3] [4][5] [6][7]
template<class Key, class Comparator, class Allocator>
void BTree<Key, Comparator, Allocator>::RemoveLeaf(const Key &hint, Page *page) {
    DCHECK(page->is_leaf());

    if (page == root_.get()) {
        return;
    }

    base::Handle<Page> prev(std::get<0>(FindLessThan(hint)));
    if (prev) {
        prev->link = page->link;
        prev->dirty++;
    }

    base::Handle<Page> parent(DCHECK_NOTNULL(GetPage(page->parent)));
    auto i = parent->FindGreaterOrEqual(hint, comparator_);
    if (i < 0) {
        i = static_cast<int>(parent->size()) - 1;
    }

    base::Handle<Page> link(GetPage(parent->GetLChild(i) == page->id
                                    ? parent->GetRChild(i)
                                    : parent->GetRChild(i)));
    Entry old{parent->key(i), link->id};

    if (i == parent->size() - 1) {
        parent->link = parent->child(i);
    }
    parent->DeleteAt(i);
    parent->dirty++;

    if (parent->size() == 0) {
        if (parent == root_) {
            root_ = link;
            root_->parent = 0;
            root_->dirty++;
            FreePage(parent.get());
        } else {
            RemoveNonLeaf(old, parent.get());
        }
    }

    FreePage(page);
}

// Remove first:
//
//                    [3][7]
//      [1]             [5]             [9]
//[0][1]   [2][3] [4][5]   [6][7] [8][9]   [a][b]
//-----------------------------------------------
//
//                   [7]
//       [3][5]              [9]
//[0][1] [4][5] [6][7] [8][9]   [a][b]
//-----------------------------------------------
//
// Remove last:
//
//                    [3][7]
//      [1]             [5]             [9]
//[0][1]   [2][3] [4][5]   [6][7] [8][9]   [a][b]
//-----------------------------------------------
//
//                [3]
//      [1]              [5][7]
//[0][1]   [2][3] [4][5] [6][7] [8][9]
//-----------------------------------------------
//
// Remove middle:
//
//                    [3][7]
//      [1]             [5]             [9]
//[0][1]   [2][3] [4][5]   [6][7] [8][9]   [a][b]
//-----------------------------------------------
//                    [3]
//      [1]                [7][9]
//[0][1]   [2][3]   [4][5] [8][9] [a][b]
//-----------------------------------------------
template<class Key, class Comparator, class Allocator>
void BTree<Key, Comparator, Allocator>::RemoveNonLeaf(const Entry &hint,
                                                      Page *page) {
    if (page == root_.get()) {
        return;
    }

    base::Handle<Page> parent(GetPage(page->parent));
    auto i = parent->FindGreaterOrEqual(hint.key, comparator_);
    if (i < 0) {
        i = static_cast<int>(parent->size()) - 1;
    }

    base::Handle<Page> sibling(GetPage(parent->GetLChild(i) == page->id
                                       ? parent->GetRChild(i)
                                       : parent->GetLChild(i)));

    auto put = sibling->Put(parent->key(i), 0, comparator_);
    if (sibling->index_of(put) == sibling->size() - 1) {
        sibling->SetLChild(put, sibling->link);
        sibling->SetRChild(put, hint.link);
    } else {
        sibling->Put(parent->key(i), hint.link, comparator_);
    }
    GetPage(hint.link)->parent = sibling->id;

    auto link = GetPage(parent->GetLChild(i) == page->id ? parent->GetRChild(i)
                        : parent->GetRChild(i));
    Entry old{parent->key(i), link->id};

    parent->DeleteAt(i);
    sibling->dirty++;
    parent->dirty++;

    if (parent->size() == 0) {
        if (parent == root_) {
            root_ = sibling;
            root_->dirty++;
            root_->parent = 0;
            FreePage(parent.get());
        } else {
            RemoveNonLeaf(old, parent.get());
        }
    }

    FreePage(page);
}

template<class Key, class Comparator, class Allocator>
inline
typename BTree<Key, Comparator, Allocator>::Page *
BTree<Key, Comparator, Allocator>::FindLeafPage(const Key &key,
                                                Page *page) const {
    while (!page->is_leaf()) {
        auto i = page->FindGreaterOrEqual(key, comparator_);
        page = GetPage(page->GetChild(i < 0 ? nullptr : &page->entries[i]));
        DCHECK_NOTNULL(page);
    }
    return page;
}

template<class Key, class Comparator, class Allocator>
inline
typename BTree<Key, Comparator, Allocator>::Entry *
BTree<Key, Comparator, Allocator>::MoveTo(Page *from, int num, Page *to) {
    DCHECK_NE(from, to);
    Entry *rv = nullptr;
    if (num > 0) {
        for (auto i = 0; i < num; ++i) {
            rv = to->Put(from->entries[i], comparator_);
            if (rv->link) {
                base::Handle<Page> page(GetPage(rv->link));
                page->parent = to->id;
                page->dirty++;
            }
        }
        from->entries.erase(from->entries.begin(), from->entries.begin() + num);
    } else if (num < 0) {
        num = -num;
        for (auto i = from->size() - num; i < from->size(); ++i) {
            rv = to->Put(from->entries[i], comparator_);
            if (rv->link) {
                base::Handle<Page> page(GetPage(rv->link));
                page->parent = to->id;
                page->dirty++;
            }
        }
        from->entries.erase(from->entries.end() - num, from->entries.end());
    }
    return rv;
}

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_BTREE_H_