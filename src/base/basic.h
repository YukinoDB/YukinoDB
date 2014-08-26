#ifndef YUKI_BASE_BASE_H
#define YUKI_BASE_BASE_H

namespace yuki {

class DisableCopyAssign {
public:
    DisableCopyAssign(const DisableCopyAssign &) = delete;

    operator = (const DisableCopyAssign &) = delete;
};

} // namespace yuki

#endif // YUKI_BASE_BASE_H
