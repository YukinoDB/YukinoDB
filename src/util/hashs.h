#ifndef YUKINO_UTIL_HASHS_H_
#define YUKINO_UTIL_HASHS_H_

#include <stdint.h>
#include <stddef.h>

namespace yukino {

namespace util {

struct StringHash final {

    static inline uint32_t SDBM(const char *data, size_t len) {
        uint32_t hash = 0;
        while(len--) {
            // equivalent to:
            hash = 65599 * hash + (*data++);
            hash = (*data++) + (hash << 6) + (hash << 16) - hash;
        }
        return (hash & 0x7FFFFFFF);
    }


    static inline uint32_t RS(const char *data, size_t len) {
        uint32_t b = 378551;
        uint32_t a = 63689;
        uint32_t hash = 0;
        while (len--) {
            hash = hash * a + (*data++); a *= b;
        }
        return (hash & 0x7FFFFFFF);
    }

    // Js Hash
    static inline uint32_t JS(const char *data, size_t len) {
        uint32_t hash = 1315423911;
        while (len--) {
            hash ^= ((hash << 5) + (*data++) + (hash >> 2));
        }
        return (hash & 0x7FFFFFFF);
    }

    // P. J. Weinberger Hash
    static inline uint32_t PJW(const char *data, size_t len) {
        uint32_t BitsInUnignedInt = (uint32_t)(sizeof(unsigned int) * 8);
        uint32_t ThreeQuarters = (uint32_t)((BitsInUnignedInt * 3) / 4);
        uint32_t OneEighth = (uint32_t)(BitsInUnignedInt / 8);
        uint32_t HighBits = (uint32_t)(0xFFFFFFFF) << (BitsInUnignedInt - OneEighth);
        uint32_t hash = 0;
        uint32_t test = 0;
        while (len--) {
            hash = (hash << OneEighth) + (*data++);
            if ((test = hash & HighBits) != 0) {
                hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
            }
        }
        return (hash & 0x7FFFFFFF);
    }

    // ELF Hash
    static inline uint32_t ELF(const char *data, size_t len) {
        uint32_t hash = 0;
        uint32_t x = 0;
        while (len--) {
            hash = (hash << 4) + (*data++);
            if ((x = hash & 0xF0000000L) != 0) {
                hash ^= (x >> 24);
                hash &= ~x;
            }
        }
        return (hash & 0x7FFFFFFF);
    }

    // BKDR Hash
    static inline uint32_t BKDR(const char *data, size_t len) {
        uint32_t seed = 131; // 31 131 1313 13131 131313 etc..
        uint32_t hash = 0;
        while (len--) {
            hash = hash * seed + (*data++);
        }
        return (hash & 0x7FFFFFFF);
    }

    // DJB Hash
    static inline uint32_t DJB(const char *data, size_t len) {
        uint32_t hash = 5381;
        while (len--) {
            hash += (hash << 5) + (*data++);
        }
        return (hash & 0x7FFFFFFF);
    }

    // AP Hash
    static inline uint32_t AP(const char *data, size_t len) {
        uint32_t hash = 0;
        for (auto i = 0U; i < len; ++i) {
            if ((i & 1) == 0) {
                hash ^= ((hash << 7) ^ (*data++) ^ (hash >> 3));
            } else {
                hash ^= (~((hash << 11) ^ (*data++) ^ (hash >> 5)));
            }
        }
        return (hash & 0x7FFFFFFF);
    }

    StringHash() = delete;
    ~StringHash() = delete;

}; // struct StringHash

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_HASHS_H_