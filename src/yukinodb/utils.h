#ifndef YUKI_BASE_BASE_H
#define YUKI_BASE_BASE_H

namespace yukinodb {

namespace base {

class DisableCopyAssign {
public:
    DisableCopyAssign(const DisableCopyAssign &) = delete;

    void operator = (const DisableCopyAssign &) = delete;

}; // class DisableCopyAssign

} // namespace base

} // namespace yukinodb

#endif // YUKI_BASE_BASE_H
