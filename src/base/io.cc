#include "base/io-inl.h"
#include "base/io.h"
#include "port/io_impl.h"
#include "base/varint_encoding.h"
#include <stdio.h>

namespace yukino {

namespace base {

Writer::Writer() {
}

Writer::~Writer() {
}

Status Writer::WriteString(const Slice &str, size_t *written) {
    size_t len = 0;
    auto rs = WriteVarint32(static_cast<uint32_t>(str.size()), &len);
    if (!rs.ok()) {
        return rs;
    }
    if (written) {
        *written += len;
    }
    rs = Write(str, &len);
    if (!rs.ok()) {
        return rs;
    }
    if (written) {
        *written += len;
    }
    return rs;
}

Status Writer::WriteVarint32(uint32_t value, size_t *written) {
    char buf[Varint32::kMaxLen];

    auto len = Varint32::Encode(buf, value);
    return Write(buf, len, written);
}

Status Writer::WriteVarint64(uint64_t value, size_t *written) {
    char buf[Varint64::kMaxLen];

    auto len = Varint64::Encode(buf, value);
    return Write(buf, len, written);
}

Reader::Reader() {
}

Reader::~Reader() {
}

Status Reader::ReadVarint32(uint32_t *value, size_t *read) {
    size_t count = 0;
    int byte = 0;
    *value = 0;
    while ((byte = ReadByte()) >= 0x80 && count++ < Varint32::kMaxLen) {
        (*value) |= (byte & 0x7f);
        (*value) <<= 7;
    }
    if (byte == EOF) {
        return Status::Corruption("Unexpected EOF");
    }
    if (count > Varint32::kMaxLen) {
        return Status::IOError("Varint32 decoding too large");
    }
    (*value) |= byte;
    if (read) *read = count;
    return Status::OK();
}

Status Reader::ReadVarint64(uint64_t *value, size_t *read) {
    size_t count = 0;
    int byte = 0;
    *value = 0;
    while ((byte = ReadByte()) >= 0x80 && count++ < Varint64::kMaxLen) {
        (*value) |= (byte & 0x7f);
        (*value) <<= 7;
    }
    if (byte == EOF) {
        return Status::Corruption("Unexpected EOF");
    }
    if (count > Varint64::kMaxLen) {
        return Status::IOError("Varint64 decoding too large");
    }
    (*value) |= byte;
    if (read) *read = count;
    return Status::OK();
}

uint32_t BufferedReader::ReadVarint32() {
    size_t read = 0;
    auto rv = Varint32::Decode(buf_, &read);
    DCHECK_GE(active_, read);
    Advance(read);
    return rv;
}

uint64_t BufferedReader::ReadVarint64() {
    size_t read = 0;
    auto rv = Varint64::Decode(buf_, &read);
    DCHECK_GE(active_, read);
    Advance(read);
    return rv;
}

MappedMemory::MappedMemory(const std::string &file_name, void *buf, size_t len)
    : file_name_(file_name)
    , buf_(static_cast<uint8_t*>(buf))
    , len_(len) {
}

MappedMemory::MappedMemory(MappedMemory &&other)
    : file_name_(std::move(other.file_name_))
    , buf_(other.buf_)
    , len_(other.len_) {

    other.buf_ = nullptr;
    other.len_ = 0;
}

MappedMemory::~MappedMemory() {
}

base::Status MappedMemory::Close() {
    return base::Status::OK();
}

base::Status MappedMemory::Sync(size_t, size_t) {
    return base::Status::OK();
}

Status BufferedWriter::Write(const void *data, size_t size, size_t *written) {
    if (!Advance(size)) {
        return Status::Corruption("not enough memory.");
    }

    ::memcpy(tail(), data, size);
    len_ += size;

    if (written)
        *written = size;
    return Status::OK();
}

Status BufferedWriter::Skip(size_t count) {
    if (!Advance(count)) {
        return Status::Corruption("not enough memory.");
    }

    len_ += count;
    return Status::OK();
}

BufferedWriter::~BufferedWriter() {
    if (ownership_) delete[] buf_;
}

bool BufferedWriter::Reserve(size_t size) {
    if (size < len())
        return false;

    auto buf = new char[size];
    if (!buf)
        return false;

    cap_ = size;
    delete[] buf_;
    buf_ = buf;

    return true;
}

bool BufferedWriter::Advance(size_t add) {

    if (len() + add > cap()) {
        if (!ownership_) {
            return false;
        }

        auto res = cap();
        while (res < len() + add) {
            res = res * 2 + 128;
        }

        auto buf = new char[res];
        if (!buf)
            return false;

        ::memcpy(buf, buf_, len_);
        cap_ = res;
        delete[] buf_;
        buf_ = buf;
    }
    return true;
}

AppendFile::~AppendFile() {
}

FileLock::~FileLock() {
}

base::Status WriteAll(const std::string &file_name, const base::Slice &buf,
                      size_t *written) {
    AppendFile *file = nullptr;
    auto rs = port::CreateAppendFile(file_name.c_str(), &file);
    auto defer = base::Defer([file]() {
        delete file;
    });
    if (!rs.ok()) {
        return rs;
    }

    rs = file->Write(buf, written);
    if (!rs.ok()) {
        return rs;
    }

    return file->Sync();
}

base::Status ReadAll(const std::string &file_name, std::string *buf) {
    FILE *file = ::fopen(file_name.c_str(), "r");
    if (!file) {
        return base::Status::IOError(file_name + " not found.");
    }
    auto defer = base::Defer([file]() {
        fclose(file);
    });

    if (fseek(file, 0, SEEK_END) < 0) {
        return base::Status::IOError("fseek() fail.");
    }
    auto file_size = ftell(file);
    if (file_size < 0) {
        return base::Status::IOError("ftell() fail.");
    }
    rewind(file);
    buf->resize(file_size);

    auto rv = fread(&buf->at(0), 1, file_size, file);
    if (rv != file_size) {
        return base::Status::IOError("fread() fail.");
    }

    return base::Status::OK();
}

} // namespace base
    
} // namespace yukino