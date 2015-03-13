#ifndef YUKINO_API_OPTION_H_
#define YUKINO_API_OPTION_H_

#include <stddef.h>

namespace yukino {

class Env;
class Comparator;

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
    // -------------------
    // Parameters that affect behavior

    // The storage engine name
    const char *engine_name;

    // Comparator used to define the order of keys in the table.
    // Default: a comparator that uses lexicographic byte-wise ordering
    //
    // REQUIRES: The client must ensure that the comparator supplied
    // here has the same name and orders keys *exactly* the same as the
    // comparator provided to previous open calls on the same DB.
    const Comparator* comparator;

    // If true, the database will be created if it is missing.
    // Default: false
    bool create_if_missing;

    // If true, an error is raised if the database already exists.
    // Default: false
    bool error_if_exists;

    // Use the specified object to interact with the environment,
    // e.g. to read/write files, schedule background work, etc.
    // Default: Env::Default()
    Env* env;

    // -------------------
    // Parameters that affect performance

    // Amount of data to build up in memory (backed by an unsorted log
    // on disk) before converting to a sorted on-disk file.
    //
    // Larger values increase performance, especially during bulk loads.
    // Up to two write buffers may be held in memory at the same time,
    // so you may wish to adjust this parameter to control memory usage.
    // Also, a larger write buffer will result in a longer recovery time
    // the next time the database is opened.
    //
    // Default: 4MB
    size_t write_buffer_size;
    
    // Create an Options object with default values for all fields.
    Options();
};


} // namespace yukino

#endif // YUKINO_API_OPTION_H_
