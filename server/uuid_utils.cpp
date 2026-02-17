#include "uuid_utils.h"
#include "uuid/uuid.h"

void UUIDUtils::generate(UUIDValue &out) {
	uuid_generate(out);
}

int UUIDUtils::parse(const char8_t *uuidString, UUIDValue &out) {
	return uuid_parse(uuidString, out);
}

void UUIDUtils::unparse(const UUIDValue &in, char8_t *uuidString) {
	uuid_unparse(in, uuidString);
}
