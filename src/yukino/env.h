#ifndef YUKINO_API_ENV_H_
#define YUKINO_API_ENV_H_

#include "base/status.h"
#include "base/base.h"
#include <string>

namespace yukino {

namespace base {

class Writer;
class AppendFile;
class MappedMemory;

} // namespace base

class Env : public base::DisableCopyAssign {
public:
    Env() { }
    virtual ~Env();

    // Return a default environment suitable for the current operating
    // system.  Sophisticated users may wish to provide their own Env
    // implementation instead of relying on this default environment.
    //
    // The result of Default() belongs to leveldb and must never be deleted.
    static Env* Default();

    // Create an object that writes to a new file with the specified
    // name.  Deletes any existing file with the same name and creates a
    // new file.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.
    //
    // The returned file will only be accessed by one thread at a time.
    virtual base::Status CreateAppendFile(const std::string &fname,
                                          base::AppendFile **file) = 0;

    // Create a brand new random access read-only file with the
    // specified name.  On success, stores a pointer to the new file in
    // *result and returns OK.  On failure stores NULL in *result and
    // returns non-OK.  If the file does not exist, returns a non-OK
    // status.
    //
    // The returned file may be concurrently accessed by multiple threads.
    virtual base::Status CreateRandomAccessFile(const std::string &fname,
                                                base::MappedMemory **file) = 0;

    // Returns true iff the named file exists.
    virtual bool FileExists(const std::string& fname) = 0;

    // Delete the named file.
    virtual base::Status DeleteFile(const std::string& fname) = 0;

}; // class Env

} // namespace yukino

#endif // YUKINO_API_ENV_H_
