#include "util/shared_ttree-inl.h"
#include "util/shared_ttree.h"
#include "glog/logging.h"

namespace yukino {

namespace util {

SharedTTree::SharedTTree(const Comparator *comparator, size_t page_size)
    : comparator_(comparator)
    , page_size_(page_size)
    , page_shift_(63 - base::Bits::CountLeadingZeros64(page_size))
    , bitmap_(0) {

    DCHECK_EQ(page_size_, 1ull << page_shift_);
    DCHECK_LT(page_size, kMaxPageSize);
}

base::Status SharedTTree::Init(base::MappedMemory *mmap) {
    mmap_ = DCHECK_NOTNULL(mmap);

    if (mmap->size() % page_size_ != 0) {
        return base::Status::InvalidArgument("size not align to page_size");
    }

    if (mmap->size() < page_size_) {
        return base::Status::InvalidArgument("size less than page_size");
    }

    bitmap_.Resize(static_cast<int>(mmap_->size() / page_size_));

    Node *node = nullptr;
    bool ok;
    std::tie(node, ok) = AllocateNode();
    if (!ok) {
        return base::Status::Corruption("Not enough space.");
    }
    Delegate page(node, this, true);
    root_ = page.node();

    return base::Status::OK();
}

bool SharedTTree::Put(const base::Slice &key, std::string *old,
                      base::Status *rs) {
    Node *node = nullptr;
    bool in_bounds = false;
    std::tie(node, in_bounds) = FindNode(key);

    Delegate page(node, this);
    if (Delegate::used_space(key) <= page.capacity() &&
        page.node()->num_entries < limit_count_) {

        return page.Put(key, old);
    } else {
        if (in_bounds) {
            std::string min_key(page.min_key().data(), page.min_key().size());
            page.DeleteAt(0);
            auto rv = page.Put(key, old);
            Put(min_key, nullptr, rs);
            if (!rs->ok()) {
                return false;
            }
            return rv;
        }
    }

    bool ok;
    std::tie(node, ok) = AllocateNode();
    if (!ok) {
        *rs = base::Status::Corruption("Not enough space.");
        return false;
    }

    Delegate last(node, this, true);
    DCHECK(!page.InBounds(key));
    if (comparator_->Compare(key, page.min_key()) < 0) {
        page.set_lchild(last.node());
    } else {
        page.set_rchild(last.node());
    }
    last.Add(key);
    return false;
}

bool SharedTTree::Get(const base::Slice &key, base::Slice *rv,
                      std::string *scratch) const {
    Node *node = nullptr;
    int i = -1;

    bool equal = false;
    std::tie(node, i) = FindGreaterOrEqual(key);
    if (i >= 0) {

        Delegate page(node, this);
        i = page.FindGreaterOrEqual(key, &equal);
        if (equal) {
            *rv = page.key(i);
        }
    }

    return equal;
}

std::tuple<SharedTTree::Node *, int>
SharedTTree::FindGreaterOrEqual(const base::Slice &key) const {
    Delegate page(std::get<0>(FindNode(key)), this);
    bool equal = false;
    auto i = page.FindGreaterOrEqual(key, &equal);

    return { page.node(), i };
}

std::tuple<SharedTTree::Node *, bool> SharedTTree::AllocateNode() {
    auto index = 0;
    for (auto bits : bitmap_.bits()) {
        auto i = base::Bits::FindFirstZero32(bits);
        if (i >= 0 && i < 32) {
            index += i;
            break;
        }
        index += 32;
    }

    auto offset = static_cast<size_t>(index) << page_shift_;
    if (offset >= mmap_->size()) {
        return { nullptr, false };
    } else {
        bitmap_.set(index);
        return { reinterpret_cast<Node *>(mmap_->mutable_buf(offset)), true };
    }
}

bool SharedTTree::IsUsed(const Node *node) const {
    if (!node) {
        return true;
    }

    auto p = reinterpret_cast<const uint8_t *>(node);
    DCHECK_GE(p, mmap_->mutable_buf());
    DCHECK_LT(p, mmap_->mutable_buf(mmap_->size()));

    auto offset = static_cast<size_t>(p - mmap_->buf());
    DCHECK_EQ(0, offset % page_size_);

    return bitmap_.test(static_cast<int>(offset >> page_shift_));
}

void SharedTTree::FreeNode(const Node *node) {
    if (!node) {
        return;
    }

    auto p = reinterpret_cast<const uint8_t *>(node);
    DCHECK(!IsUsed(node));

    auto offset = static_cast<size_t>(p - mmap_->buf());
    DCHECK_EQ(0, offset % page_size_);

    Delegate page(node, this);
    page.ToFree();

    auto index = static_cast<int>(offset >> page_shift_);
    bitmap_.set(index);
}

std::tuple<SharedTTree::Node *, bool>
SharedTTree::FindNode(const base::Slice &key) const {
    Delegate page(root_, this);
    bool in_bounds = false;

    while (page.num_entries() > 0 && !(in_bounds = page.InBounds(key))) {

        if (comparator_->Compare(key, page.min_key()) < 0) {
            if (!page.lchild()) {
                break;
            }
            page = Delegate(page.lchild(), this);
        } else if (comparator_->Compare(key, page.max_key()) > 0) {
            if (!page.rchild()) {
                break;
            }
            page = Delegate(page.rchild(), this);
        }
    }

    return { DCHECK_NOTNULL(page.node()), in_bounds };
}

int SharedTTree::TEST_DumpTree(const Node *subtree, std::string *buf,
                               int indent) const {
    Delegate page(subtree, this);

    buf->append(indent * 2, ' ');
    buf->append(base::Strings::Sprintf("<%p P:%p L:%p, R:%p ",
                                       page.node(),
                                       page.parent(), page.lchild(),
                                       page.rchild()));
    buf->append(page.ToString());
    buf->append(">\n");

    auto rv = 0;
    if (page.lchild()) {
        buf->append("L:");
        rv += TEST_DumpTree(page.lchild(), buf, indent + 1);
    }
    if (page.rchild()) {
        buf->append("R:");
        rv += TEST_DumpTree(page.rchild(), buf, indent + 1);
    }

    return rv;
}

} // namespace util

} // namespace yukino