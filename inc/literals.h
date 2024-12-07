#pragma once

#include <ns.h>
#include <cstdint>

namespace KVSTORE_NS::literals
    {
        constexpr std::size_t operator""_KiB(unsigned long long x) { return 1024ULL * x; }

        constexpr std::size_t operator""_MiB(unsigned long long x) { return 1024_KiB * x; }

        constexpr std::size_t operator""_GiB(unsigned long long x) { return 1024_MiB * x; }
} // namespace PROJ_NS::literals
