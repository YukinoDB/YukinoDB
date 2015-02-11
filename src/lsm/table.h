#ifndef YUKINO_LSM_TABLE_H_
#define YUKINO_LSM_TABLE_H_

#include "lsm/block.h"
#include "base/status.h"
#include "base/base.h"
#include "yukino/iterator.h"
#include <vector>

namespace yukino {

class Comparator;

namespace base {

class MappedMemory;
class BufferedReader;

} // namespace base

namespace lsm {

class TableIterator;
class BlockIterator;

class Table : public base::DisableCopyAssign {
public:
    Table(const Comparator *comparator, base::MappedMemory *mmap);
    virtual ~Table();

    base::Status Init();

    BlockHandle ReadHandle(base::BufferedReader *reader);

    bool VerifyBlock(const BlockHandle &handle, char *type);

    uint32_t file_version() const { return file_version_; }
    int restart_interval() const { return restart_interval_; }
    uint32_t block_size() const { return block_size_; }

    typedef TableIterator Iterator;

    friend class TableIterator;
    friend class ChunkIterator;
private:
    struct IndexEntry {
        std::string key;
        BlockHandle handle;
    };

    base::MappedMemory *mmap_;
    const Comparator *comparator_;
    std::vector<IndexEntry> index_;

    uint32_t file_version_ = 0;
    int restart_interval_ = 0;
    uint32_t block_size_ = 0;
};

class TableIterator : public Iterator {
public:
    TableIterator(const Table *table);
    virtual ~TableIterator();

    virtual bool Valid() const override;

    virtual void SeekToFirst() override;

    virtual void SeekToLast() override;

    virtual void Seek(const base::Slice& target) override;

    virtual void Next() override;

    virtual void Prev() override;

    virtual base::Slice key() const override;

    virtual base::Slice value() const override;

    virtual base::Status status() const override;
};


} // namespace lsm

} // namespace yukino


#endif // YUKINO_LSM_TABLE_H_