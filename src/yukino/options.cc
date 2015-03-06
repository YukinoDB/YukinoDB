#include "yukino/options.h"
#include "yukino/comparator.h"
#include "yukino/env.h"

namespace yukino {

Options::Options()
    : engine_name(nullptr)
    , comparator(BytewiseCompartor())
    , create_if_missing(false)
    , error_if_exists(false)
    , env(Env::Default()) {
}

} // namespace yukino
