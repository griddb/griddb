#ifndef UUID_UTILS_H_
#define UUID_UTILS_H_

#include "util/type.h"

const int32_t UUID_BYTE_SIZE = 16;
const int32_t UUID_STRING_SIZE = 37;
typedef uint8_t UUIDValue[UUID_BYTE_SIZE];

struct UUIDUtils {
	static void generate(UUIDValue &out);
	static int parse(const char8_t *uuidString, UUIDValue &out);
	static void unparse(const UUIDValue &in, char8_t *uuidString);
};

#endif
