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

TEST(EnvImplTest, FileReadWrite) {
    base::AppendFile *afile = nullptr;

    static const auto file_name = "env_test.tmp";
    auto rs = Env::Default()->CreateAppendFile(file_name, &afile);
    ASSERT_TRUE(rs.ok());
    auto defer = base::Defer([&]() {
        rs = Env::Default()->DeleteFile(file_name, false);
        EXPECT_TRUE(rs.ok()) << rs.ToString();
    });

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

TEST(EnvImplTest, Directory) {
    static const auto root = "env_test_root";

    auto rs = Env::Default()->CreateDir(root);
    ASSERT_TRUE(rs.ok());
    auto defer = base::Defer([&]() {
        rs = Env::Default()->DeleteFile(root, true);
        EXPECT_TRUE(rs.ok()) << rs.ToString();
    });

    std::string name(root);
    name.append("/");
    name.append("1");

    rs = Env::Default()->CreateDir(name);
    ASSERT_TRUE(rs.ok());

    name.assign(root);
    name.append("/");
    name.append("2");

    rs = Env::Default()->CreateDir(name);
    ASSERT_TRUE(rs.ok());

    std::vector<std::string> children;
    rs = Env::Default()->GetChildren(root, &children);
    ASSERT_TRUE(rs.ok());

    EXPECT_EQ(2, children.size());
    EXPECT_EQ("1", children[0]);
    EXPECT_EQ("2", children[1]);

}

} // namespace yukino
