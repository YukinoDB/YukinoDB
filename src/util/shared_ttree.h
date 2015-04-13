#ifndef YUKINO_UTIL_SHARED_TTREE_H_
#define YUKINO_UTIL_SHARED_TTREE_H_

#include "bloom_filter.h"
#include "base/status.h"
#include "base/slice.h"
#include "base/io.h"
#include "base/io-inl.h"
#include "base/base.h"
#include <atomic>
#include <tuple>

namespace yukino {

class Comparator;

namespace util {

class SharedTTree : public base::DisableCopyAssign {
public:
    SharedTTree(const Comparator *comparator, size_t page_size);

    base::Status Attach(base::MappedMemory *mmap);

    base::Status Init(base::MappedMemory *mmap);

    bool Put(const base::Slice &key, std::string *old, base::Status *rs);
    bool Get(const base::Slice &key, base::Slice *rv, std::string *scratch) const;
    bool Delete(const base::Slice &key, std::string *old);

    struct Node {
        //uint32_t crc32;
        uint32_t parent;
        uint32_t lchild;
        uint32_t rchild;
        std::atomic<uint32_t> mutex;
        int16_t  num_entries;
        uint16_t index[1];
    };

    enum NodeType : uint8_t {
        kPageFree = 0,
        kPageUsed = 1,
    };

    struct Spec {
        uint16_t top;
        uint8_t  type;
    };

    class Delegate;

    std::tuple<Node *, int> FindGreaterOrEqual(const base::Slice &key) const;
    std::tuple<Node *, bool> FindNode(const base::Slice &key) const;

    std::tuple<Node *, bool> AllocateNode();
    bool IsUsed(const Node *node) const;
    void FreeNode(const Node *node);

    inline int GetDepth(const Node *node) const;

    int32_t limit_count() const { return limit_count_; }
    void set_limit_count(int32_t n) { limit_count_ = n; }

    //--------------------------------------------------------------------------
    // For Testing
    //--------------------------------------------------------------------------
    Node *TEST_Root() const { return root_; }

    int TEST_DumpTree(const Node *subtree, std::string *buf, int indent = 0) const;

    static const size_t kMaxPageSize = UINT16_MAX;

private:

    const Comparator * const comparator_;
    const size_t page_size_;
    const int page_shift_;
    int32_t limit_count_ = INT32_MAX;

    Node *root_ = nullptr;
    base::MappedMemory *mmap_ = nullptr;
    util::Bitmap<> bitmap_;
};

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_SHARED_TTREE_H_