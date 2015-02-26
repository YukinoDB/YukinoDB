#ifndef YUKINO_LSM_SKIPLIST_H_
#define YUKINO_LSM_SKIPLIST_H_

#include "glog/logging.h"
#include <stdint.h>
#include <random>
#include <atomic>

namespace yukino {

namespace lsm {

template <class Key, class Comparator>
class SkipList {
public:
    struct Node;

    class Iterator;

    SkipList(Comparator compare)
        : compare_(compare)
        , head_(NewNode(Key(), kMaxHeight))
        , max_height_(1) {

        for (auto i = 0; i < kMaxHeight; ++i) {
            head_->set_next(i, nullptr);
        }

        std::uniform_int_distribution<intptr_t> distribution(0, kBranching);
        std::mt19937 engine;
        rand_ = std::bind(distribution, engine);
    }

    void Put(Key &&key) {
        // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
        // here since Insert() is externally synchronized.
        Node* prev[kMaxHeight];
        Node* x = FindGreaterOrEqual(key, prev);

        // Our data structure does not allow duplicate insertion
        DCHECK(x == NULL || !Equal(key, x->key));

        int height = RandomHeight();
        if (height > max_height()) {
            for (int i = max_height(); i < height; i++) {
                prev[i] = head_;
            }
            //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (NULL), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since NULL sorts after all
            // keys.  In the latter case the reader will use the new node.
            max_height_.store(height, std::memory_order_relaxed);
        }

        x = NewNode(std::move(key), height);
        for (int i = 0; i < height; i++) {
            // NoBarrier_SetNext() suffices since we will add a barrier when
            // we publish a pointer to "x" in prev[i].
            x->nobarrier_set_next(i, prev[i]->nobarrier_next(i));
            prev[i]->set_next(i, x);
        }
    }

    bool Contains(const Key &key) const {
        Node* x = FindGreaterOrEqual(key, NULL);
        if (x != NULL && Equal(key, x->key)) {
            return true;
        } else {
            return false;
        }
    }

    static const intptr_t kMaxHeight = 12;
    static const intptr_t kBranching = 4;
private:

    Node *NewNode(Key &&key, uintptr_t height) {
        auto chunk = new char[sizeof(Node) +
                              sizeof(std::atomic<Node *>) * (height - 1)];
        return new (chunk) Node(std::move(key));
    }

    Node *FindGreaterOrEqual(const Key& key, Node** prev) const {
        auto x = head_;
        int level = max_height() - 1;
        while (true) {
            Node* next = x->next(level);
            if (KeyIsAfterNode(key, next)) {
                // Keep searching in this list
                x = next;
            } else {
                if (prev != NULL) prev[level] = x;
                if (level == 0) {
                    return next;
                } else {
                    // Switch to next list
                    level--;
                }
            }
        }
    }

    Node *FindLessThan(const Key &key) const {
        Node* x = head_;
        int level = max_height() - 1;
        while (true) {
            DCHECK(x == head_ || compare_(x->key, key) < 0);
            Node* next = x->next(level);
            if (next == NULL || compare_(next->key, key) >= 0) {
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    Node *FindLast() const {
        Node* x = head_;
        int level = max_height() - 1;
        while (true) {
            Node* next = x->next(level);
            if (next == NULL) {
                if (level == 0) {
                    return x;
                } else {
                    // Switch to next list
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    int RandomHeight() {
        int height = 1;

        while (height < kMaxHeight && rand_() == 0) {
            height++;
        }

        DCHECK_GT(height, 0);
        DCHECK_LE(height, kMaxHeight);

        return height;
    }

    bool Equal(const Key &a, const Key &b) const {
        return compare_(a, b) == 0;
    }

    int max_height() const {
        return static_cast<int>(max_height_.load(std::memory_order_relaxed));
    }

    bool KeyIsAfterNode(const Key& key, Node* n) const {
        // NULL n is considered infinite
        return (n != nullptr) && (compare_(n->key, key) < 0);
    }

    Comparator const compare_;
    Node *const head_;

    std::atomic<intptr_t> max_height_;
    std::function<intptr_t()> rand_;
};

template <class Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
public:
    explicit Node(Key &&k)
        : key(std::move(k)) {
        DCHECK(next_[0].is_lock_free());
    }

    Key key;

    Node *next(int n) {
        DCHECK_GE(n, 0);

        return next_[n].load(std::memory_order_acquire);
    }

    void set_next(int n, Node *x) {
        DCHECK_GE(n, 0);

        next_[n].store(x, std::memory_order_release);
    }

    Node *nobarrier_next(int n) {
        DCHECK_GE(n, 0);

        return next_[n].load(std::memory_order_relaxed);
    }

    void nobarrier_set_next(int n, Node *x) {
        DCHECK_GE(n, 0);

        next_[n].store(x, std::memory_order_relaxed);
    }

private:
    std::atomic<SkipList<Key, Comparator>::Node *> next_[1];
};


template <class Key, class Comparator>
class SkipList<Key, Comparator>::Iterator {
public:
    explicit Iterator(SkipList<Key, Comparator> *list)
        : list_(list)
        , node_(nullptr) {
    }

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const { return node_ != nullptr; }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const {
        DCHECK(Valid());
        return node_->key;
    }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() {
        node_ = node_->next(0);
    }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() {
        DCHECK(Valid());
        node_ = list_->FindLessThan(node_->key);
        if (node_ == list_->head_) {
            node_ = nullptr;
        }
    }

    // Advance to the first entry with a key >= target
    void Seek(const Key& target) {
        node_ = list_->FindGreaterOrEqual(target, nullptr);
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() {
        node_ = list_->head_->next(0);
    }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() {
        node_ = list_->FindLast();
        if (node_ == list_->head_) {
            node_ = NULL;
        }
    }

private:
    SkipList<Key, Comparator> *list_;
    Node *node_;
};

} // namespace lsm

} // namespace yukino

#endif // YUKINO_LSM_SKIPLIST_H_