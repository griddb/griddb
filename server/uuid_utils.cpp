#include "uuid_utils.h"
#include "uuid/uuid.h"

void UUIDUtils::generate(UUIDValue &out) {
	uuid_generate(out);
}

int UUIDUtils::parse(const char *uuidString, UUIDValue &out) {
	return uuid_parse(uuidString, out);
}

void UUIDUtils::unparse(UUIDValue &in, char *uuidString ) {
	uuid_unparse(in, uuidString);
}