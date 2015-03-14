#ifndef YUKINO_API_ENV_H_
#define YUKINO_API_ENV_H_

#include "base/status.h"
#include "base/base.h"
#include <string>
#include <vector>

namespace yukino {

namespace base {

class Writer;
class AppendFile;
class MappedMemory;
class FileLock;

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

    // Delete the named file or directory.
    virtual base::Status DeleteFile(const std::string& fname, bool deep) = 0;

    // Store in *result the names of the children of the specified directory.
    // The names are relative to "dir".
    // Original contents of *results are dropped.
    virtual base::Status GetChildren(const std::string& dir,
                                     std::vector<std::string>* result) = 0;

    // Create the specified directory.
    virtual base::Status CreateDir(const std::string& dirname) = 0;

    // Store the size of fname in *file_size.
    virtual base::Status GetFileSize(const std::string& fname,
                                     uint64_t* file_size) = 0;

    // Rename file src to target.
    virtual base::Status RenameFile(const std::string& src,
                                    const std::string& target) = 0;

    // Lock the specified file.  Used to prevent concurrent access to
    // the same db by multiple processes.  On failure, stores NULL in
    // *lock and returns non-OK.
    //
    // On success, stores a pointer to the object that represents the
    // acquired lock in *lock and returns OK.  The caller should call
    // UnlockFile(*lock) to release the lock.  If the process exits,
    // the lock will be automatically released.
    //
    // If somebody else already holds the lock, finishes immediately
    // with a failure.  I.e., this call does not wait for existing locks
    // to go away.
    //
    // May create the named file if it does not already exist.
    virtual base::Status LockFile(const std::string& fname,
                                  base::FileLock** lock) = 0;

}; // class Env

} // namespace yukino

#endif // YUKINO_API_ENV_H_
