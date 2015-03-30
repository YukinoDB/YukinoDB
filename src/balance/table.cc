#include "balance/table-inl.h"
#include "balance/table.h"
#include "base/io.h"
#include "base/crc32.h"
#include "base/varint_encoding.h"
#include "yukino/iterator.h"
#include <map>

namespace yukino {

namespace balance {

Table::Table(InternalKeyComparator comparator)
    : comparator_(comparator) {
}

Table::~Table() {
    if (tree_.get()) {
        Flush(true);

        std::vector<const PageTy*> collected;
        tree_->Travel(tree_->TEST_GetRoot(), [&collected] (const PageTy *page) {
            collected.push_back(page);
            return true;
        });

        for (auto page : collected) {
            if (page->is_leaf()) {
                for (auto entry : page->entries) {
                    delete[] entry.key;
                }
            }
            delete page;
        }
    }
}

/*
 * File Header format:
 *
 * | magic        | 4 bytes
 * | version      | 4 bytes
 * | page-size    | varint32
 * | b+tree order | varint32
 * | ...          |
 */
base::Status Table::Create(uint32_t page_size, uint32_t version, int order,
                           base::FileIO *file) {
    page_size_ = page_size;
    version_   = version;
    file_      = DCHECK_NOTNULL(file);

    base::Status rs;

    CHECK_OK(InitFile(order));
    auto tree = new TreeTy(order, Comparator(comparator_), Allocator(this));
    tree_ = std::unique_ptr<TreeTy>(tree);

    return rs;
}

base::Status Table::Open(base::FileIO *file, size_t file_size) {
    file_size_ = file_size;
    file_      = DCHECK_NOTNULL(file);

    base::Status rs;

    CHECK_OK(file_->Seek(0));
    uint32_t magic = 0;
    CHECK_OK(file_->ReadFixed32(&magic));
    if (magic != Config::kBtreeFileMagic)  {
        return base::Status::IOError("Not a b+tree file.");
    }

    uint32_t version = 0;
    CHECK_OK(file_->ReadFixed32(&version));
    if (version < Config::kBtreeFileVersion) {
        return base::Status::IOError("B+tree file version is too old.");
    }

    CHECK_OK(file_->ReadVarint32(&page_size_, nullptr));
    uint32_t order = 0;
    CHECK_OK(file_->ReadVarint32(&order, nullptr));

    tree_ = std::unique_ptr<TreeTy>(new TreeTy(order, Comparator(comparator_),
                                               Allocator(this)));
    CHECK_OK(LoadTree());
    return rs;
}

bool Table::Put(const base::Slice &key, uint64_t tx_id, KeyFlag flag,
                const base::Slice &value,
                std::string *old_value) {

    auto packed = InternalKey::Pack(key, tx_id, flag, value);

    const char *old = nullptr;
    auto rv = tree_->Put(packed, &old);
    if (old) {
        auto parsed = InternalKey::Parse(old);
        old_value->assign(parsed.value.data(), parsed.value.size());
        delete[] old;
    }
    return rv;
}

bool Table::Get(const base::Slice &key, uint64_t tx_id, std::string *value) {
    TreeTy::Iterator iter(tree_.get());

    std::unique_ptr<const char[]> packed(InternalKey::Pack(key, tx_id,
                                                           kFlagFind, ""));
    iter.Seek(packed.get());
    if (!iter.Valid()) {
        return false;
    }

    auto parsed = InternalKey::Parse(iter.key());
    switch (parsed.flag) {
        case kFlagDeletion:
            return false;

        case kFlagValue:
            if (key.compare(parsed.user_key) != 0) {
                return false;
            }
            value->assign(parsed.value.data(), parsed.value.size());
            return true;

        default:
            DCHECK(false) << "Noreached:" << parsed.flag;
            return false;
    }
}

base::Status Table::Flush(bool sync) {
    base::Status rs;
    TreeTy::Travel(tree_->TEST_GetRoot(), [this, &rs](PageTy *page) {
        if (page->dirty) {
            rs = WritePage(page);
            if (rs.ok()) {
                page->dirty = 0;
            }
        }
        return rs.ok();
    });

    if (rs.ok() && sync) {
        file_->Sync();
    }
    return rs;
}

namespace {

class TableIterator : public Iterator {
public:
    TableIterator(Table::TreeTy *tree)
        : iter_(tree) {
    }

    virtual ~TableIterator() override {}
    virtual bool Valid() const override { return iter_.Valid(); }
    virtual void SeekToFirst() override { iter_.SeekToFirst(); }
    virtual void SeekToLast() override { iter_.SeekToLast(); }
    virtual void Seek(const base::Slice& target) override {
        std::unique_ptr<const char[]> packed(InternalKey::Pack(target, ""));
        iter_.Seek(packed.get());
    }
    virtual void Next() override { iter_.Next(); }
    virtual void Prev() override { iter_.Prev(); }
    virtual base::Slice key() const override {
        auto parsed = InternalKey::Parse(iter_.key());
        return parsed.key();
    }
    virtual base::Slice value() const override {
        auto parsed = InternalKey::Parse(iter_.key());
        return parsed.value;
    }
    virtual base::Status status() const override {
        return base::Status::OK();
    }

private:
    Table::TreeTy::Iterator iter_;
};

} // namespace

Iterator *Table::CreateIterator() const {
    auto iter = new TableIterator(tree_.get());

    AddRef();
    iter->RegisterCleanup([this]() {
        this->Release();
    });
    return iter;
}

/* Page format:
 * 
 * |         | crc32 | fixed32
 * | header  | len   | fixed32
 * |         | type  | 1 byte
 * |         | id    | 8 bytes
 * |         | parent| 8 bytes
 * |         | ts    | 8 bytes
 * +---------+-------+---------
 * | payload | link  | varint64
 * |         |entries| ...
 *
 */
base::Status Table::WritePage(const PageTy *page) {
    // Skip crc32, len and type.
    // We should write type in the last time.
    static const uint64_t kFirstSipped = sizeof(uint32_t) * 2 + 1;

    // Double writing for page
    base::Status rs;
    uint64_t addr = 0;

    CHECK_OK(MakeRoomForPage(page->id, &addr));
    // Ignore crc32, len ...
    CHECK_OK(file_->Seek(addr + kFirstSipped));

    base::VerifiedWriter<base::CRC32> w(file_);

    CHECK_OK(w.WriteFixed64(page->id));           // id
    CHECK_OK(w.WriteFixed64(page->parent.page ? page->parent.page->id : -1));
    CHECK_OK(w.WriteFixed64(NowMicroseconds()));  // ts

    // Record payload size:
    size_t active = file_->active();
    size_t size = 0;

    CHECK_OK(w.WriteVarint64(page->link ? page->link->id : -1, nullptr));
    if (page->is_leaf()) {
        for (const auto &entry : page->entries) {
            auto parsed = InternalKey::Parse(entry.key);

            CHECK_OK(w.WriteString(parsed.key(), nullptr));
            CHECK_OK(w.WriteString(parsed.value, nullptr));
        }
    } else {
        for (const auto &entry : page->entries) {
            auto parsed = InternalKey::Parse(entry.key);

            CHECK_OK(w.WriteVarint64(entry.link->id, &size));
            CHECK_OK(w.WriteString(parsed.key(), &size));
        }
    }
    size = file_->active() - active;

    CHECK_OK(file_->Seek(addr));
    CHECK_OK(file_->WriteFixed32(w.digest()));
    CHECK_OK(file_->WriteFixed32(static_cast<uint32_t>(size)));
    if (page->is_leaf()) {
        CHECK_OK(w.WriteByte(Config::kPageTypeFull | Config::kPageLeafFlag));
    } else {
        CHECK_OK(w.WriteByte(Config::kPageTypeFull));
    }

    CHECK_OK(FreeRoomForPage(page->id));
    id_map_[page->id] = addr;
    SetUsed(addr);
    return rs;
}

base::Status Table::MakeRoomForPage(uint64_t id, uint64_t *addr) {
    base::Status rs;

    uint64_t index = 0;
    for (auto bits : bitmap_) {
        auto i = base::FindFirstZero32(bits);
        if (i >= 0 && i < 32) {
            //bitmap_[(index + 31) / 32] |= (1 << i);
            index += i;
            break;
        }
        index += 32;
    }

    // The first page is header.
    if (index >= (file_size_ / page_size_) - 1) {

        // Allocate new file space.
        file_size_ += page_size_;
        CHECK_OK(file_->Truncate(file_size_));

        if (bitmap_.size() < ((index + 31) / 32) + 1) {
            bitmap_.push_back(0u);
        }
    }

    *DCHECK_NOTNULL(addr) = (index + 1) * page_size_;
    return rs;
}

base::Status Table::FreeRoomForPage(uint64_t id) {
    base::Status rs;
    auto addr = id_map_[id];
    if (addr == 0) {
        return rs;
    }

    DCHECK(TestUsed(addr));
    ClearUsed(addr);

    CHECK_OK(file_->Seek(addr));
    CHECK_OK(file_->WriteFixed32(0)); // crc32
    CHECK_OK(file_->WriteFixed32(0)); // len
    CHECK_OK(file_->WriteByte(Config::kPageTypeZero)); // type
    return rs;
}

base::Status Table::InitFile(int order) {
    base::Status rs;

    CHECK_OK(file_->Truncate(page_size_));
    CHECK_OK(file_->Seek(0));
    CHECK_OK(file_->WriteFixed32(Config::kBtreeFileMagic));
    CHECK_OK(file_->WriteFixed32(version_));
    CHECK_OK(file_->WriteVarint32(static_cast<uint32_t>(page_size_), nullptr));
    CHECK_OK(file_->WriteVarint32(order, nullptr));

    // ...
    file_size_ = page_size_;
    return rs;
}

base::Status Table::LoadTree() {
    auto num_dwords = (file_size_ / page_size_) - 1;
    bitmap_.resize((num_dwords + 31) / 32, 0u);

    base::Status rs;

    uint64_t root_id = -1;
    std::map<uint64_t, PageMetadata> metadatas;
    for (auto addr = page_size_; addr < file_size_; addr += page_size_) {
        ScanPage(&metadatas, addr);
    }
    for (const auto &entry : metadatas) {
        if (entry.second.parent == -1) {
            if (root_id != -1) {
                return base::Status::Corruption("Double root pages!");
            }
            root_id = entry.first;
        }

        if (entry.first > next_page_id_) {
            next_page_id_ = entry.first;
        }
    }
    if (root_id == -1) {
        return base::Status::Corruption("No any root page!");
    }

    PageTy *root = nullptr;
    CHECK_OK(ReadTreePage(&metadatas, root_id, &root));
    tree_->TEST_Attach(root);

    // Release repeated memory.
    TreeTy::Iterator iter(tree_.get());
    tree_->Travel(tree_->TEST_GetRoot(), [this, &iter] (PageTy *page) {
        if (!page->is_leaf()) {
            for (auto &entry : page->entries) {
                iter.Seek(entry.key);
                DCHECK(iter.Valid());

                delete[] entry.key;
                entry.key = iter.key();
            }
        }
        return true;
    });

    next_page_id_++;
    return rs;
}

base::Status Table::ScanPage(std::map<uint64_t, PageMetadata> *metadatas,
                             uint64_t addr) {
    base::Status rs;
    CHECK_OK(file_->Seek(addr + sizeof(uint32_t) * 2));

    auto type = file_->ReadByte();
    if (type == EOF) {
        return base::Status::IOError("Unexpected EOF.");
    }

    if (type == Config::kPageTypeZero) {
        // unused page
        return rs;
    }

    uint64_t id = 0;
    PageMetadata meta;

    meta.addr = addr;
    CHECK_OK(file_->ReadFixed64(&id));
    CHECK_OK(file_->ReadFixed64(&meta.parent));
    CHECK_OK(file_->ReadFixed64(&meta.ts));

    auto found = metadatas->find(id);
    if (found == metadatas->end()) {
        metadatas->emplace(id, meta);
    } else {
        if (meta.ts > found->second.ts) {
            metadatas->emplace(id, meta);
            ClearUsed(found->second.addr);
        }
    }

    SetUsed(addr);
    return rs;
}

base::Status
Table::ReadTreePage(std::map<uint64_t, PageMetadata> *metadatas,
                    uint64_t id, PageTy **rv) {
    base::Status rs;

    if (id == -1) {
        *rv = nullptr;
        return rs;
    }

    auto meta = &metadatas->find(id)->second;
    if (meta->page) {
        *rv = meta->page;
        return rs;
    }

    CHECK_OK(file_->Seek(meta->addr));

    uint32_t checksum = 0;
    CHECK_OK(file_->ReadFixed32(&checksum));

    uint32_t len = 0;
    CHECK_OK(file_->ReadFixed32(&len));
    auto byte = file_->ReadByte();
    if (byte == EOF) {
        return base::Status::IOError("Unexpected EOF.");
    }

    uint64_t dummy = 0;
    base::VerifiedReader<base::CRC32> rd(file_);
    CHECK_OK(rd.ReadFixed64(&dummy)); // Ignore id
    uint64_t parent_id = 0;
    CHECK_OK(rd.ReadFixed64(&parent_id));
    CHECK_OK(rd.ReadFixed64(&dummy)); // Ignore ts

    auto size = file_->active() + len;

    //==========================================================================
    // Payload:
    //==========================================================================
    uint64_t link_id = 0;
    CHECK_OK(rd.ReadVarint64(&link_id, nullptr));

    meta->page = new PageTy(id, 16);

    std::string key, value;
    std::vector<uint64_t> children_id;
    if (static_cast<uint8_t>(byte) & Config::kPageLeafFlag) {
        while (file_->active() < size) {
            CHECK_OK(rd.ReadString(&key));
            CHECK_OK(rd.ReadString(&value));

            auto k = InternalKey::Pack(key, value);
            meta->page->entries.push_back(EntryTy{k, nullptr});
        }
    } else {
        while (file_->active() < size) {
            uint64_t child_id = 0;
            CHECK_OK(rd.ReadVarint64(&child_id, nullptr));
            DCHECK_NE(id, child_id);
            children_id.push_back(child_id);

            CHECK_OK(rd.ReadString(&key));

            EntryTy entry;
            entry.key = InternalKey::Pack(key);
            meta->page->entries.push_back(entry);
        }
        DCHECK_EQ(children_id.size(), meta->page->size());
    }

    if (checksum != rd.digest()) {
        return base::Status::IOError("CRC32 verify fail.");
    }

    //==========================================================================
    // Links:
    //==========================================================================
    if ((static_cast<uint8_t>(byte) & Config::kPageLeafFlag) == 0) {
        for (auto i = 0; i < children_id.size(); ++i) {
            CHECK_OK(ReadTreePage(metadatas, children_id[i],
                                  &meta->page->entries[i].link));
        }
    }
    CHECK_OK(ReadTreePage(metadatas, parent_id, &meta->page->parent.page));
    CHECK_OK(ReadTreePage(metadatas, link_id, &meta->page->link));

    *rv = meta->page;
    return rs;
}

} // namespace balance
    
} // namespace yukino