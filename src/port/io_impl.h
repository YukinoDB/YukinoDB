#ifndef YUKINO_PORT_IO_IMPL_H_
#define YUKINO_PORT_IO_IMPL_H_

#include "base/status.h"

namespace yukino {

namespace base {

class AppendFile;
class FileIO;
class MappedMemory;
class FileLock;

} // namespace base

namespace port {

base::Status CreateAppendFile(const char *file_name, base::AppendFile **file);

base::Status CreateFileIO(const char *file_name, base::FileIO **file);

base::Status CreateRandomAccessFile(const char *file_name,
                                    base::MappedMemory **file);

base::Status CreateFileLock(const char *file_name, bool locked,
                            base::FileLock **file);

} // namespace port

} // namespace yukino

#endif // YUKINO_PORT_IO_IMPL_H_