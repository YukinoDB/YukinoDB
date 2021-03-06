#include "balance/table-inl.h"
#include "balance/table.h"
#include "util/linked_queue.h"
#include "base/io-inl.h"
#include "base/io.h"
#include "base/crc32.h"
#include "base/varint_encoding.h"
#include "yukino/iterator.h"
#include <map>

namespace yukino {

namespace balance {

namespace {

inline size_t ApproximatePageSize(const Table::Page *page) {
    size_t count = sizeof(*page);

    for (const auto &entry : page->entries) {
        count += sizeof(Table::Entry);

        size_t len = 0;
        count += base::Varint32::Decode(entry.key, &len);
        count += len;
    }
    return count;
}
    
} // namespace

/* Physical Layout:
 * 
 * Block:
 * +---------+-------+
 * |         | crc32 | 4 bytes
 * |         +-------+
 * | header  | len   | 2 bytes
 * |         +-------+
 * |         | type  | 1 bytes
 * |         +-------+
 * |         | next  | 4 bytes | 0 or null
 * +---------+-------+
 * | payload | data  | len bytes
 * +---------+-------+
 *
 * Page Layout:
 * |         | type  | 1 byte
 * |  header | id    | 8 bytes
 * |         | parent| 8 bytes
 * |         | ts    | 8 bytes
 * +---------+-------+---------
 * |         | link  | varint64
 * | payload | num   | varint32
 * |         |entries| ...
 *
 */

struct PhysicalBlock {

    enum Type : uint8_t {
        // Zero is reserved for preallocated files
        kZeroType = 0,

        kFullType = 1,

        // For fragments
        kFirstType  = 2,
        kMiddleType = 3,
        kLastType   = 4
    };

    // 11
    static const size_t kHeaderSize
        = sizeof(uint32_t)  // crc32
        + sizeof(uint16_t)  // len
        + 1                 // type
        + sizeof(uint32_t); // next

    static const uint64_t kTypeOffset
        = sizeof(uint32_t)   // crc32
        + sizeof(uint16_t);  // len

    static const char kZeroHeader[kHeaderSize];
};

const char PhysicalBlock::kZeroHeader[PhysicalBlock::kHeaderSize] = {0};

Table::Table(InternalKeyComparator comparator, size_t max_cache_size)
    : comparator_(comparator)
    , bitmap_(0)
    , cache_dummy_(nullptr)
    , cache_purge_(nullptr)
    , max_cache_size_(max_cache_size) {
}

Table::~Table() {
    if (tree_.get()) {
        Flush(true);

        auto purge = cache_dummy_.next;
        while (purge != &cache_dummy_) {
            auto tmp = purge;
            purge = purge->next;
            ClearPage(tmp->page.get());
            delete tmp;
        }

        purge = cache_purge_.next;
        while (purge != &cache_purge_) {
            auto tmp = purge;
            purge = purge->next;
            ClearPage(tmp->page.get());
            delete tmp;
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
    auto tree = new Tree(order, Comparator(comparator_), Allocator(this));
    tree_ = std::unique_ptr<Tree>(tree);

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

    tree_ = std::unique_ptr<Tree>(new Tree(order, Comparator(comparator_),
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
        if (old_value) {
            auto parsed = InternalKey::Parse(old);
            old_value->assign(parsed.value.data(), parsed.value.size());
        }
        delete[] old;
    }
    return rv;
}

bool Table::Get(const base::Slice &key, uint64_t tx_id, std::string *value) {
    Tree::Iterator iter(tree_.get());
    {
        auto packed = InternalKey::Pack(key, tx_id, kFlagFind, "");
        iter.Seek(packed);
        delete[] packed;
    }

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

bool Table::Purge(const base::Slice &key, uint64_t tx_id, std::string *value) {

    auto packed = InternalKey::Pack(key, tx_id, kFlagFind, "");

    const char *old = nullptr;
    auto rv = tree_->Delete(packed, &old);
    delete[] packed;

    if (rv) {
        if (value) {
            auto parsed = InternalKey::Parse(old);
            value->assign(parsed.value.data(), parsed.value.size());
        }
        delete[] old;
    }
    return rv;
}

base::Status Table::Flush(bool sync) {
    base::Status rs;

    for (auto entry = cache_dummy_.next; entry != &cache_dummy_;
         entry = entry->next) {
        if (entry->page->dirty > 0 && entry->page->size() > 0) {
            rs = WritePage(entry->page.get());
            if (rs.ok()) {
                entry->page->dirty = 0;
            } else {
                return rs;
            }
        }
    }

    if (rs.ok() && sync) {
        file_->Sync();
    }
    return rs;
}

namespace {

class TableIterator : public Iterator {
public:
    TableIterator(Table::Tree *tree)
        : iter_(DCHECK_NOTNULL(tree)) {
    }

    virtual ~TableIterator() override {}
    virtual bool Valid() const override { return iter_.Valid(); }
    virtual void SeekToFirst() override { iter_.SeekToFirst(); }
    virtual void SeekToLast() override { iter_.SeekToLast(); }
    virtual void Seek(const base::Slice& target) override {
        auto packed = InternalKey::Pack(target, "");
        iter_.Seek(packed);
        delete[] packed;
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
    Table::Tree::Iterator iter_;
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

base::Status Table::WritePage(const Page *page) {
    // Double writing for page
    base::Status rs;

    base::BufferedWriter w;
    // type
    if (page->is_leaf()) {
        CHECK_OK(w.WriteByte(Config::kPageTypeFull | Config::kPageLeafFlag));
    } else {
        CHECK_OK(w.WriteByte(Config::kPageTypeFull));
    }
    CHECK_OK(w.WriteFixed64(page->id));           // id
    // parent
    CHECK_OK(w.WriteFixed64(page->parent ? page->parent : -1));
    CHECK_OK(w.WriteFixed64(NowMicroseconds()));  // ts

    // link and entries
    CHECK_OK(w.WriteVarint64(page->link ? page->link : -1, nullptr));
    CHECK_OK(w.WriteVarint32(static_cast<uint32_t>(page->size()), nullptr));
    if (page->is_leaf()) {
        for (const auto &entry : page->entries) {
            auto parsed = InternalKey::Parse(entry.key);

            CHECK_OK(w.WriteString(parsed.key(), nullptr));
            CHECK_OK(w.WriteString(parsed.value, nullptr));
        }
    } else {
        for (const auto &entry : page->entries) {
            auto parsed = InternalKey::Parse(entry.key);

            CHECK_OK(w.WriteVarint64(entry.link, nullptr));
            CHECK_OK(w.WriteString(parsed.key(), nullptr));
        }
    }

    uint64_t addr = 0;
    CHECK_OK(WriteChunk(w.buf(), w.len(), &addr));
    id_map_[page->id] = addr;

    PageMetadata meta;
    meta.addr   = addr;
    meta.parent = page->parent;
    meta.ts     = NowMicroseconds();
    metadata_.emplace(page->id, meta);
    return rs;
}

base::Status Table::WriteChunk(const char *buf, size_t len, uint64_t *addr) {
    base::Status rs;

    DCHECK_LT(PhysicalBlock::kHeaderSize, page_size_);
    const auto block_payload_size = page_size_ - PhysicalBlock::kHeaderSize;
    const auto num_blocks = (len + block_payload_size - 1) / block_payload_size;
    std::vector<uint64_t> blocks(num_blocks);
    for (auto i = 0; i < num_blocks; ++i) {
        CHECK_OK(MakeRoomForPage(&blocks[i]));
        SetUsed(blocks[i]);
    }
    blocks.push_back(0);

    auto left = len;
    size_t offset = 0;
    auto type = PhysicalBlock::kZeroType;
    for (auto i = 0; i < num_blocks; ++i) {

        if (left > block_payload_size) {
            len = block_payload_size;
        } else {
            len = left;
        }

        if (num_blocks == 1) {
            type = PhysicalBlock::kFullType;
        } else if (num_blocks > 1 && i == 0) {
            type = PhysicalBlock::kFirstType;
        } else if (num_blocks > 1 && i == num_blocks - 1) {
            type = PhysicalBlock::kLastType;
        } else {
            type = PhysicalBlock::kMiddleType;
        }

        DCHECK_LT(len, Config::kMaxPageSize);
        CHECK_OK(WriteBlock(buf + offset, len, type, blocks[i], blocks[i + 1]));
        left   -= len;
        offset += len;
    }
    DCHECK_EQ(0, left);

    *addr = blocks[0];
    return rs;
}

base::Status Table::WriteBlock(const char *buf, uint16_t len, uint8_t type,
                               uint64_t addr, uint64_t next) {
    DCHECK_EQ(0, next % page_size_);
    auto np = static_cast<uint32_t>(next) / page_size_;

    base::Status rs;
    base::CRC32 crc;

    crc.Update(&len, sizeof(len));
    crc.Update(&type, sizeof(type));
    crc.Update(&np, sizeof(np));

    crc.Update(buf, len);

    CHECK_OK(file_->Seek(addr));
    CHECK_OK(file_->WriteFixed32(crc.digest())); // crc32
    CHECK_OK(file_->WriteFixed16(len));          // len
    CHECK_OK(file_->WriteByte(type));            // type
    CHECK_OK(file_->WriteFixed32(np));           // next

    CHECK_OK(file_->Write(buf, len, nullptr));   // payload data
    return rs;
}

base::Status Table::ReadChunk(uint64_t addr, std::string *buf) {
    base::Status rs;

    uint8_t type = 0;
    buf->clear();
    do {
        CHECK_OK(file_->Seek(addr));

        uint32_t checksum = 0;
        CHECK_OK(file_->ReadFixed32(&checksum));

        base::VerifiedReader<base::CRC32> rd(file_);
        uint16_t len = 0;
        CHECK_OK(rd.ReadFixed16(&len));

        CHECK_OK(rd.Read(&type, 1));
        uint32_t np = 0;
        CHECK_OK(rd.ReadFixed32(&np));
        addr = np * page_size_; DCHECK_LT(addr, file_size_);

        auto begin = buf->size();
        buf->resize(buf->size() + len);
        CHECK_OK(rd.Read(&(*buf)[begin], len));

        if (checksum != rd.digest()) {
            rs = base::Status::IOError("CRC32 verify fail!");
            break;
        }
    } while (type == PhysicalBlock::kFirstType ||
             type == PhysicalBlock::kMiddleType);
    return rs;
}

base::Status Table::MakeRoomForPage(uint64_t *addr) {
    base::Status rs;

    int index = 0;
    for (auto bits : bitmap_.bits()) {
        auto i = base::Bits::FindFirstZero32(bits);
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

        if (bitmap_.num_buckets() < ((index + 31) / 32) + 1) {
            bitmap_.AddBucket(0u);
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

    static_assert(sizeof(PhysicalBlock::Type) == 1,
                  "PhysicalBlock::Type too big");

    std::vector<uint64_t> will_free;
    PhysicalBlock::Type type = PhysicalBlock::kZeroType;
    do {
        will_free.push_back(addr);

        CHECK_OK(file_->Seek(addr + PhysicalBlock::kTypeOffset));
        CHECK_OK(file_->Read(&type, sizeof(type)));

        uint32_t np = 0;
        CHECK_OK(file_->ReadFixed32(&np));
        if (np != 0) {
            addr = np * page_size_;
        }
    } while (type == PhysicalBlock::kFirstType ||
             type == PhysicalBlock::kMiddleType);

    for (auto addr : will_free) {
        CHECK_OK(file_->Seek(addr));
        CHECK_OK(file_->Write(PhysicalBlock::kZeroHeader,
                              PhysicalBlock::kHeaderSize, nullptr));
        DCHECK(TestUsed(addr));
        ClearUsed(addr);
    }
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
    auto num_pages = (file_size_ / page_size_) - 1;
    bitmap_.Resize(static_cast<int>(num_pages));

    base::Status rs;

    uint64_t root_id = -1;
    for (auto addr = page_size_; addr < file_size_; addr += page_size_) {
        ScanPage(addr);
    }
    for (const auto &entry : metadata_) {
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

    // Clear cache first, has a unused root page.
    cache_map_.clear();
    auto purge = cache_dummy_.next;
    util::Dll::Remove(purge);
    delete purge;


    Page *root = nullptr;
    CHECK_OK(CachedGet(root_id, &root, true));
    tree_->TEST_Attach(root);

    next_page_id_++;
    return rs;
}

base::Status Table::ScanPage(uint64_t addr) {
    base::Status rs;
    CHECK_OK(file_->Seek(addr + PhysicalBlock::kTypeOffset));

    PhysicalBlock::Type type = PhysicalBlock::kZeroType;
    CHECK_OK(file_->Read(&type, sizeof(type)));
    if (type == PhysicalBlock::kZeroType ||
        type == PhysicalBlock::kLastType ||
        type == PhysicalBlock::kMiddleType) {
        // unused or last or middle page, ignore it.
        return rs;
    }

    // Go to payload:
    CHECK_OK(file_->Seek(addr + PhysicalBlock::kHeaderSize));

    uint64_t id = 0;
    uint8_t  page_type = 0;
    PageMetadata meta;

    meta.addr = addr;
    CHECK_OK(file_->Read(&page_type, sizeof(page_type)));
    DCHECK_NE(Config::kPageTypeZero, page_type);
    CHECK_OK(file_->ReadFixed64(&id));
    CHECK_OK(file_->ReadFixed64(&meta.parent));
    CHECK_OK(file_->ReadFixed64(&meta.ts));

    auto found = metadata_.find(id);
    if (found == metadata_.end()) {
        metadata_.emplace(id, meta);
    } else {
        if (meta.ts > found->second.ts) {
            metadata_.emplace(id, meta);
            ClearUsed(found->second.addr);
        }
    }

    SetUsed(addr);
    return rs;
}

base::Status
Table::ReadPage(uint64_t id, Page **rv) {
    base::Status rs;

    if (id == -1) {
        *rv = nullptr;
        return rs;
    }

    auto found = metadata_.find(id);
    DCHECK(metadata_.end() != found);
    const auto &meta = found->second;

    std::string buf;
    CHECK_OK(ReadChunk(meta.addr, &buf));

    base::BufferedReader rd(buf.data(), buf.size());

    uint8_t  type      = rd.ReadByte(); // type;
    uint64_t dummy     = rd.ReadFixed64(); // Ignore id
    uint64_t parent_id = rd.ReadFixed64(); // parent
    dummy = rd.ReadFixed64(); //

    //==========================================================================
    // Payload:
    //==========================================================================
    uint64_t link_id     = rd.ReadVarint64();
    uint32_t num_entries = rd.ReadVarint32();

    auto page = new Page(id, 16);
    page->dirty = 0; // Readed page not dirty.
    if (type & Config::kPageLeafFlag) {
        for (auto i = 0; i < num_entries; ++i) {
            auto key = rd.ReadString();
            auto value = rd.ReadString();

            auto k = InternalKey::Pack(key, value);
            page->entries.push_back(Entry{k, 0});
        }
    } else {
        for (auto i = 0; i < num_entries; ++i) {
            Entry entry;

            entry.link = rd.ReadVarint64();
            entry.key  = InternalKey::Pack(rd.ReadString());
            page->entries.push_back(entry);
        }
    }

    //==========================================================================
    // Links:
    //==========================================================================
    page->parent = (parent_id == -1 ? 0 : parent_id);
    page->link   = (link_id   == -1 ? 0 : link_id);

    *rv = page;
    return rs;
}

base::Status Table::CachedActivity(Page *page, bool cached) {
    base::Status rs;

    auto entry = new CacheEntry(DCHECK_NOTNULL(page));
    if (cached) {
        cache_map_.emplace(page->id, entry);
        util::Dll::InsertHead(&cache_dummy_, entry);
    } else {
        util::Dll::InsertTail(&cache_purge_, entry);
    }

    cache_size_ += ApproximatePageSize(page);
    if (util::Dll::Count(&cache_dummy_) > Config::kHoldCachedPage &&
        cache_size_ > max_cache_size_) {

        auto oldest = cache_dummy_.prev;
        if (oldest->page && oldest->page->dirty > 0 &&
            oldest->page->size() > 0) {
            CHECK_OK(WritePage(oldest->page.get()));
        }

        util::Dll::Remove(oldest);
        util::Dll::InsertTail(&cache_purge_, oldest);

        cache_size_ -= ApproximatePageSize(oldest->page.get());
    }

    return CachedPurge();
}

base::Status Table::CachedPurge() {
    base::Status rs;

    for (auto purge = cache_purge_.next; purge != &cache_purge_;
         purge = purge->next) {

        if (purge->page->ref_count() == 1) {
            cache_map_.erase(purge->page->id);

            auto next = purge->next;
            util::Dll::Remove(purge);

            if (purge->page->dirty > 0) {
                CHECK_OK(WritePage(purge->page.get()));
            }
            ClearPage(purge->page.get());
            delete purge;

            purge = next;
        }
    }
    return rs;
}

} // namespace balance
    
} // namespace yukino