// The YukinoDB Unit Test Suite
//
//  env_impl_test.cc
//
//  Created by Niko Bellic.
//
//
#include "yukino/env.h"
#include "base/io.h"
#include "gtest/gtest.h"
#include <stdio.h>

namespace yukino {

TEST(EnvImplTest, Sanity) {
    auto env = Env::Default();
    ASSERT_NE(nullptr, env);
}

class ScopedDeletion {
public:
    ScopedDeletion(const std::string &fname)
        : fname_(fname) {
        EnsureFileExist(fname);
    }

    ~ScopedDeletion() {
        EnsureDeleteFile();
    }

    void EnsureFileExist(const std::string &fname) {
        ASSERT_TRUE(Env::Default()->FileExists(fname));
    }

    void EnsureDeleteFile() {
        ASSERT_TRUE(Env::Default()->DeleteFile(fname_).ok());
    }

private:
    std::string fname_;
};

TEST(EnvImplTest, FileReadWrite) {
    base::AppendFile *afile = nullptr;

    static const auto file_name = "env_test.tmp";
    auto rs = Env::Default()->CreateAppendFile(file_name, &afile);
    ASSERT_TRUE(rs.ok());
    ScopedDeletion scoped(file_name);

    std::unique_ptr<base::AppendFile> writer(afile);

    rs = writer->WriteFixed32(199);
    EXPECT_TRUE(rs.ok());

    rs = writer->Sync();
    EXPECT_TRUE(rs.ok());

    rs = writer->WriteFixed32(201);
    EXPECT_TRUE(rs.ok());

    rs = writer->Sync();
    EXPECT_TRUE(rs.ok());

    rs = writer->Close();
    EXPECT_TRUE(rs.ok());

    base::MappedMemory *rfile = nullptr;
    rs = Env::Default()->CreateRandomAccessFile(file_name, &rfile);
    ASSERT_TRUE(rs.ok()) << rs.ToString();

    std::unique_ptr<base::MappedMemory> reader(rfile);

    base::BufferedReader buf(reader->buf(), reader->size());
    EXPECT_EQ(199, buf.ReadFixed32());
    EXPECT_EQ(201, buf.ReadFixed32());
    reader->Close();
}

} // namespace yukino
