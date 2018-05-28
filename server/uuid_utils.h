#ifndef UUID_UTILS_H_
#define UUID_UTILS_H_

#include "util/type.h"

typedef uint8_t UUIDValue[16];

struct UUIDUtils {
	static void generate(UUIDValue &out);
	static int parse(const char *uuidString, UUIDValue &out);
	static void unparse(UUIDValue &out, char *uuidString);
};

#endif
