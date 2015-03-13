#ifndef YUKINO_API_READ_OPTIONS_H_
#define YUKINO_API_READ_OPTIONS_H_

namespace yukino  {

class Snapshot;

struct ReadOptions {
    // TODO:

    Snapshot *snapshot;

    ReadOptions();
};

}

#endif // YUKINO_API_READ_OPTIONS_H_
