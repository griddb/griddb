#ifndef MSGPACK_UTILS_H_
#define MSGPACK_UTILS_H_

template const uint32_t& std::max<uint32_t>(const uint32_t&, const uint32_t&);

#ifdef _WIN32
#define NOCRYPT
#endif

#include "msgpack.hpp"

#endif
