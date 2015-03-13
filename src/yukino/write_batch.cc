#include "yukino/write_batch.h"
#include "base/io.h"
#include "glog/logging.h"

namespace yukino {

namespace {

static const char kTypeValue = 0;
static const char kTypeDeletion = 1;

} // namespace

WriteBatch::Handler::~Handler() {
}

WriteBatch::WriteBatch() {
}

WriteBatch::~WriteBatch() {
}

void WriteBatch::Put(const base::Slice& key, const base::Slice& value) {
    auto rs = redo_.Write(kTypeValue);
    DCHECK(rs.ok());

    rs = redo_.WriteVarint32(static_cast<uint32_t>(key.size()), nullptr);
    DCHECK(rs.ok());
    rs = redo_.Write(key.data(), key.size(), nullptr);
    DCHECK(rs.ok());

    rs = redo_.WriteVarint64(value.size(), nullptr);
    DCHECK(rs.ok());
    rs = redo_.Write(value.data(), value.size(), nullptr);
    DCHECK(rs.ok());
}

void WriteBatch::Delete(const base::Slice& key) {
    auto rs = redo_.Write(kTypeDeletion);
    DCHECK(rs.ok());

    rs = redo_.WriteVarint32(static_cast<uint32_t>(key.size()), nullptr);
    DCHECK(rs.ok());
    rs = redo_.Write(key.data(), key.size(), nullptr);
    DCHECK(rs.ok());
}

void WriteBatch::Clear() {
    redo_.Clear();
}

/*static*/ base::Status WriteBatch::Iterate(const void *buf, size_t len,
                                            WriteBatch::Handler *handler) {
    if (len == 0) {
        return base::Status::OK();
    }

    base::BufferedReader reader(buf, len);
    while (reader.active() > 0) {
        auto type = reader.Read();
        size_t size = reader.ReadVarint32();
        auto key  = reader.Read(size);

        switch (type) {
            case kTypeValue: {
                size = reader.ReadVarint64();
                handler->Put(key, reader.Read(size));
            } break;

            case kTypeDeletion: {
                handler->Delete(key);
            } break;

            default:
                DLOG(FATAL) << "noreached!";
                break;
        }
    }
    
    return base::Status::OK();
}

} // namespace yukino