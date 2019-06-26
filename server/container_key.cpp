/*
	Copyright (c) 2017 TOSHIBA Digital Solutions Corporation

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as
	published by the Free Software Foundation, either version 3 of the
	License, or (at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include "container_key.h"
#include "value_processor.h"
#include "value_operator.h"


const char8_t* FullContainerKey::symbol = "_-./=";
const char8_t* FullContainerKey::forbiddenPattern[] = { NULL };

const DatabaseId FullContainerKey::MAX_ENCODABLE_DBID = static_cast<DatabaseId>(MAX_NODE_AFFINITY_NUMBER);

const size_t FullContainerKey::MAX_NODE_AFFINITY_NUMBER_DIGITS = 19;
const size_t FullContainerKey::MAX_LARGE_CONTAINERID_DIGITS = 19;
const size_t FullContainerKey::MAX_SYSTEM_PART_ID_DIGITS = 19;

const uint8_t FullContainerKey::DBID_EXISTS					= 0x01 << 0;
const uint8_t FullContainerKey::LARGE_CONTAINERID_EXISTS	= 0x01 << 1;
const uint8_t FullContainerKey::NODE_AFFINITY_NUM			= 0x01 << 2;
const uint8_t FullContainerKey::NODE_AFFINITY_STR			= 0x01 << 3;
const uint8_t FullContainerKey::NODE_AFFINITY_EXISTS		= FullContainerKey::NODE_AFFINITY_NUM | FullContainerKey::NODE_AFFINITY_STR;
const uint8_t FullContainerKey::SYSTEM_PART_ID_NUM			= 0x01 << 4;
const uint8_t FullContainerKey::SYSTEM_PART_ID_STR			= 0x01 << 5;
const uint8_t FullContainerKey::SYSTEM_PART_ID_EXISTS		= FullContainerKey::SYSTEM_PART_ID_NUM | FullContainerKey::SYSTEM_PART_ID_STR;

const uint64_t FullContainerKey::DEFAULT_UPPER_CASE_BIT_LENGTH = 1024;

KeyConstraint::KeyConstraint() :
	maxTotalLength_(0),
	systemPartAllowed_(false),
	largeContainerIdAllowed_(false) {
}

KeyConstraint::KeyConstraint(uint32_t maxTotalLength,
							 bool systemPartAllowed,
							 bool largeContainerIdAllowed) :
	maxTotalLength_(maxTotalLength),
	systemPartAllowed_(systemPartAllowed),
	largeContainerIdAllowed_(largeContainerIdAllowed) {
}

KeyConstraint KeyConstraint::getUserKeyConstraint(uint32_t maxTotalLength) {
	return KeyConstraint(maxTotalLength, false, false);
}

KeyConstraint KeyConstraint::getSystemKeyConstraint(uint32_t maxTotalLength) {
	return KeyConstraint(maxTotalLength, true, true);
}

KeyConstraint KeyConstraint::getNoLimitKeyConstraint() {
	return KeyConstraint(std::numeric_limits<uint32_t>::max(), true, true);
}


FullContainerKeyComponents::FullContainerKeyComponents() {
	clear();
}

void FullContainerKeyComponents::clear() {
	dbId_ = UNDEF_DBID;
	baseName_ = NULL;
	baseNameSize_ = 0;
	affinityString_ = NULL;
	affinityStringSize_ = 0;
	affinityNumber_ = UNDEF_NODE_AFFINITY_NUMBER;
	largeContainerId_ = UNDEF_LARGE_CONTAINERID;
	systemPart_ = NULL;
	systemPartSize_ = 0;
	systemPartId_ = UNDEF_SYSTEM_PART_ID;
}

FullContainerKeyComponents::FullContainerKeyComponents(const FullContainerKeyComponents &another) :
	dbId_(another.dbId_),
	baseName_(another.baseName_), baseNameSize_(another.baseNameSize_),
	affinityString_(another.affinityString_), affinityStringSize_(another.affinityStringSize_),
	affinityNumber_(another.affinityNumber_),
	largeContainerId_(another.largeContainerId_),
	systemPart_(another.systemPart_), systemPartSize_(another.systemPartSize_),
	systemPartId_(another.systemPartId_)
{
}

FullContainerKeyComponents&
FullContainerKeyComponents::operator=(const FullContainerKeyComponents &another) {
	if (this == &another) {
		return *this;
	}
	dbId_ = another.dbId_;
	baseName_ = another.baseName_;
	baseNameSize_ = another.baseNameSize_;
	affinityString_ = another.affinityString_;
	affinityStringSize_ = another.affinityStringSize_;
	affinityNumber_ = another.affinityNumber_;
	largeContainerId_ = another.largeContainerId_;
	systemPart_ = another.systemPart_;
	systemPartSize_ = another.systemPartSize_;
	systemPartId_ = another.systemPartId_;
	return *this;
}

uint32_t FullContainerKeyComponents::getStringSize() const {
	return (baseNameSize_ + affinityStringSize_ + systemPartSize_);
}

bool FullContainerKeyComponents::equals(const FullContainerKeyComponents &another) const {

	if (dbId_ != another.dbId_) {
		return false;
	}

	if (baseNameSize_ != another.baseNameSize_) {
		return false;
	}
	else if (strncmp(baseName_, another.baseName_, baseNameSize_) != 0) {
		return false;
	}

	if (largeContainerId_ != another.largeContainerId_) {
		return false;
	}

	if (affinityNumber_ == another.affinityNumber_) {
		if (affinityStringSize_ != another.affinityStringSize_) {
			return false;
		}
		else if (strncmp(affinityString_, another.affinityString_, affinityStringSize_) != 0) {
			return false;
		}
	}
	else {
		return false;
	}

	if (systemPartId_ == another.systemPartId_) {
		if (systemPartSize_ != another.systemPartSize_) {
			return false;
		}
		else if (strncmp(systemPart_, another.systemPart_, systemPartSize_) != 0) {
			return false;
		}
	}
	else {
		return false;
	}

	return true;
}

void FullContainerKeyComponents::dump(const char8_t *msg) const {
	std::cerr << msg << ":(";

	std::cerr << dbId_ << ",";
	{
		std::string str;
		if (baseNameSize_ > 0) {
			str.append(baseName_, baseNameSize_);
		}
		std::cerr << str << ",";
	}
	if (systemPartId_ != UNDEF_SYSTEM_PART_ID) {
		std::cerr << systemPartId_ << ",";
	} else if (systemPartSize_ > 0) {
		std::string str(systemPart_, systemPartSize_);
		std::cerr << str << ",";
	}
	std::cerr << largeContainerId_ << ",";
	if (affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
		std::cerr << affinityNumber_;
	} else if (affinityStringSize_ > 0) {
		std::string str(affinityString_, affinityStringSize_);
		std::cerr << str << ",";
	}

	std::cerr << ")" << std::endl;;
}



FullContainerKey::FullContainerKey(const KeyConstraint &constraint) :
	constraint_(constraint), body_(NULL), size_(0) {
}

FullContainerKey::FullContainerKey(const KeyConstraint &constraint,
								   const void *data) :
	constraint_(constraint), body_(NULL), size_(0) {
	try {
		if (data == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Container/Table name is empty");
		}

		ContainerKeyInStream in(util::ArrayInStream(data, sizeof(uint32_t)));
		if (ValueProcessor::varSizeIs1Byte(static_cast<const uint8_t*>(data)[0])
			|| ValueProcessor::varSizeIs4Byte(static_cast<const uint8_t*>(data)[0])) {
			size_ = static_cast<size_t>(decodeVarInt(in));
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Unexpected container/table name size");
		}

		body_ = static_cast<const uint8_t*>(data) + in.base().position();

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "constructing container/table name"));
	}
}

FullContainerKey::FullContainerKey(util::StackAllocator &alloc,
								   const KeyConstraint &constraint,
								   const void *body,
								   size_t size) :
	constraint_(constraint), body_(static_cast<const uint8_t*>(body)), size_(size) {
	try {
		FullContainerKeyComponents components;
		BitArray upperCaseBit(DEFAULT_UPPER_CASE_BIT_LENGTH);

		deserialize(alloc, components, upperCaseBit, false);
		validate(components);

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "constructing container/table name with size"));
	}
}
	
FullContainerKey::FullContainerKey(util::StackAllocator &alloc,
								   const KeyConstraint &constraint,
								   DatabaseId dbId, const char8_t *str, uint32_t length) :
	constraint_(constraint), body_(NULL), size_(0) {
	try {
		FullContainerKeyComponents components;
		BitArray upperCaseBit(DEFAULT_UPPER_CASE_BIT_LENGTH);

		parseAndValidate(dbId, str, length, components, upperCaseBit);
		serialize(alloc, components, upperCaseBit);

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "constructing container/table name with string"));
	}
}

FullContainerKey::FullContainerKey(util::StackAllocator &alloc,
								   const KeyConstraint &constraint,
								   const FullContainerKeyComponents &components) :
	constraint_(constraint), body_(NULL), size_(0) {
	try {
		validate(components);

		BitArray upperCaseBit(components.getStringSize());
		setUpperCaseBit(components, upperCaseBit);

		serialize(alloc, components, upperCaseBit);

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "constructing container/table name with components"));
	}
}

FullContainerKey::FullContainerKey(const FullContainerKey &another) :
	constraint_(another.constraint_), body_(another.body_), size_(another.size_) {
}

FullContainerKey::~FullContainerKey() {
}

FullContainerKey& FullContainerKey::operator=(const FullContainerKey &another) {
	if (this == &another) {
		return *this;
	}
	constraint_ = another.constraint_;
	body_ = another.body_;
	size_ = another.size_;
	return *this;
}

FullContainerKeyComponents FullContainerKey::getComponents(util::StackAllocator &alloc,
														   bool unNormalized) const {
	FullContainerKeyComponents components;
	BitArray upperCaseBit(DEFAULT_UPPER_CASE_BIT_LENGTH);

	deserialize(alloc, components, upperCaseBit, unNormalized);

	return components;
}

void FullContainerKey::toString(util::StackAllocator &alloc, util::String &str) const {
	try {
		FullContainerKeyComponents components;
		BitArray upperCaseBit(DEFAULT_UPPER_CASE_BIT_LENGTH);

		deserialize(alloc, components, upperCaseBit, true);

		str.clear();
		if (components.baseNameSize_ > 0) {
			str.append(components.baseName_, components.baseNameSize_);
		}

		if (components.systemPartId_ != UNDEF_SYSTEM_PART_ID) {
			util::NormalOStringStream oss;
			oss << components.systemPartId_;
			str.append("#");
			str.append(oss.str().c_str());
		}
		else if (components.systemPart_ != NULL && components.systemPartSize_ > 0) {
			str.append("#");
			str.append(components.systemPart_, components.systemPartSize_);
		}

		if (components.largeContainerId_ != UNDEF_LARGE_CONTAINERID) {
			util::NormalOStringStream oss;
			oss << components.largeContainerId_;
			str.append("@");
			str.append(oss.str().c_str());
		}

		if (components.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
			util::NormalOStringStream oss;
			oss << components.affinityNumber_;
			str.append("@");
			str.append(oss.str().c_str());
		}
		else if (components.affinityString_ != NULL && components.affinityStringSize_ > 0) {
			str.append("@");
			str.append(components.affinityString_, components.affinityStringSize_);
		}

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "formatting container/table name"));
	}
}

void FullContainerKey::toBinary(const void *&body, size_t &size) const {
	body = body_;
	size = size_;
}

bool FullContainerKey::isEmpty() const {
	return (body_ == NULL);
}

int32_t FullContainerKey::compareTo(TransactionContext &txn, const uint8_t *key1, const uint8_t *key2, bool caseSensitive) {
	try {
		const uint8_t *addr1 = key1;
		const uint8_t *addr2 = key2;

		uint32_t totalStrLen = 0;

		const uint8_t flag1 = addr1[0];
		addr1 += sizeof(flag1);

		const uint8_t flag2 = addr2[0];
		addr2 += sizeof(flag2);

		uint8_t isDbId1 = flag1 & DBID_EXISTS;
		uint8_t isDbId2 = flag2 & DBID_EXISTS;
		if (isDbId1 != isDbId2 || isDbId1 > 0) {
			DatabaseId dbId1, dbId2;
			if (isDbId1 > 0) {
				dbId1 = *reinterpret_cast<const DatabaseId*>(addr1);
				addr1 += sizeof(dbId1);
			} else {
				dbId1 = GS_PUBLIC_DB_ID;
			}
			if (isDbId2 > 0) {
				dbId2 = *reinterpret_cast<const DatabaseId*>(addr2);
				addr2 += sizeof(dbId2);
			} else {
				dbId2 = GS_PUBLIC_DB_ID;
			}
			if (dbId1 != dbId2) {
				return (dbId1 < dbId2) ? -1 : 1;
			}
		}

		uint32_t baseLen1 = ValueProcessor::decodeVarSize(addr1);
		uint32_t baseLen2 = ValueProcessor::decodeVarSize(addr2);
		addr1 += ValueProcessor::getEncodedVarSize(baseLen1);
		addr2 += ValueProcessor::getEncodedVarSize(baseLen2);

		int32_t retBase = compareStringString(txn, addr1, baseLen1, addr2, baseLen2);
		if (retBase != 0) {
			return retBase;
		}
		addr1 += baseLen1;
		addr2 += baseLen2;
		totalStrLen += baseLen1;

		uint8_t largeFlag1 = flag1 & LARGE_CONTAINERID_EXISTS;
		uint8_t largeFlag2 = flag2 & LARGE_CONTAINERID_EXISTS;
		if (largeFlag1 != largeFlag2) {
			return (largeFlag1 < largeFlag2) ? -1 : 1;
		}
		if (largeFlag1 > 0) {
			uint64_t largeContainerId1 = ValueProcessor::decodeVarSize64(addr1);
			addr1 += ValueProcessor::getEncodedVarSize(largeContainerId1);

			uint64_t largeContainerId2 = ValueProcessor::decodeVarSize64(addr2);
			addr2 += ValueProcessor::getEncodedVarSize(largeContainerId2);
			if (largeContainerId1 != largeContainerId2) {
				return (largeContainerId1 < largeContainerId2) ? -1 : 1;
			}
		}

		uint8_t affinityFlag1 = flag1 & NODE_AFFINITY_EXISTS;
		uint8_t affinityFlag2 = flag2 & NODE_AFFINITY_EXISTS;
		if (affinityFlag1 != affinityFlag2) {
			return (affinityFlag1 < affinityFlag2) ? -1 : 1;
		}
		if (flag1 & NODE_AFFINITY_NUM) {
			uint64_t affinityNum1 = ValueProcessor::decodeVarSize64(addr1);
			addr1 += ValueProcessor::getEncodedVarSize(affinityNum1);

			uint64_t affinityNum2 = ValueProcessor::decodeVarSize64(addr2);
			addr2 += ValueProcessor::getEncodedVarSize(affinityNum2);

			if (affinityNum1 != affinityNum2) {
				return (affinityNum1 < affinityNum2) ? -1 : 1;
			}
		} 
		else if (flag1 & NODE_AFFINITY_STR) {
			uint32_t affinityLen1 = ValueProcessor::decodeVarSize(addr1);
			uint32_t affinityLen2 = ValueProcessor::decodeVarSize(addr2);
			addr1 += ValueProcessor::getEncodedVarSize(affinityLen1);
			addr2 += ValueProcessor::getEncodedVarSize(affinityLen2);

			int32_t retBase = compareStringString(txn, addr1, affinityLen1, addr2, affinityLen2);
			if (retBase != 0) {
				return retBase;
			}
			addr1 += affinityLen1;
			addr2 += affinityLen2;
			totalStrLen += affinityLen1;
		}

		uint8_t systemFlag1 = flag1 & SYSTEM_PART_ID_EXISTS;
		uint8_t systemFlag2 = flag2 & SYSTEM_PART_ID_EXISTS;
		if (systemFlag1 != systemFlag2) {
			return (systemFlag1 < systemFlag2) ? -1 : 1;
		}
		if (flag1 & SYSTEM_PART_ID_NUM) {
			uint64_t systemId1 = ValueProcessor::decodeVarSize64(addr1);
			addr1 += ValueProcessor::getEncodedVarSize(systemId1);

			uint64_t systemId2 = ValueProcessor::decodeVarSize64(addr2);
			addr2 += ValueProcessor::getEncodedVarSize(systemId2);

			if (systemId1 != systemId2) {
				return (systemId1 < systemId2) ? -1 : 1;
			}
		} else if (flag1 & SYSTEM_PART_ID_STR) {
			uint32_t systemLen1 = ValueProcessor::decodeVarSize(addr1);
			uint32_t systemLen2 = ValueProcessor::decodeVarSize(addr2);
			addr1 += ValueProcessor::getEncodedVarSize(systemLen1);
			addr2 += ValueProcessor::getEncodedVarSize(systemLen2);

			int32_t retBase = compareStringString(txn, addr1, systemLen1, addr2, systemLen2);
			if (retBase != 0) {
				return retBase;
			}
			addr1 += systemLen1;
			addr2 += systemLen2;
			totalStrLen += systemLen1;
		}

		if (caseSensitive) {
			const uint8_t BITE_POS_FILTER = 0x3;
			const uint8_t BIT_POS_FILTER = 0x7;
			uint32_t bitLen = totalStrLen >> BITE_POS_FILTER;
			if ((totalStrLen & BIT_POS_FILTER) != 0) {
				bitLen++;
			}
			return memcmp(addr1, addr2, bitLen);
		}
		return 0;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "comparing container/table name"));
	}
}

int32_t FullContainerKey::compareTo(util::StackAllocator &alloc,
									const FullContainerKey &key, bool caseSensitive) const {
	try {
		if (isEmpty()) {
			return (key.isEmpty() ? 0 : 1);
		}
		else if (key.isEmpty()) {
			return -1;
		}

		const FullContainerKeyComponents components1 = getComponents(alloc, caseSensitive);
		BitArray upperCaseBit1(components1.getStringSize());
		setUpperCaseBit(components1, upperCaseBit1);

		const FullContainerKeyComponents components2 = key.getComponents(alloc, caseSensitive);
		BitArray upperCaseBit2(components2.getStringSize());
		setUpperCaseBit(components2, upperCaseBit2);

		if (components1.dbId_ > components2.dbId_) {
			return 1;
		}
		else if (components1.dbId_ < components2.dbId_) {
			return -1;
		}


		uint64_t startPos1 = 0;
		uint64_t startPos2= 0;

		int32_t result = compareNormalizedString(
			components1.baseName_, components1.baseNameSize_,
			upperCaseBit1, startPos1,
			components2.baseName_, components2.baseNameSize_,
			upperCaseBit2, startPos2,
			caseSensitive);
		startPos1 += components1.baseNameSize_;
		startPos2 += components2.baseNameSize_;

		if (result != 0) {
			return result;
		}
		else if (components1.largeContainerId_ != UNDEF_LARGE_CONTAINERID) {
			if (components2.largeContainerId_ != UNDEF_LARGE_CONTAINERID) {
				result = static_cast<int32_t>(components1.largeContainerId_ - components2.largeContainerId_);
			}
			else {
				result = 1;
			}
		}

		if (result != 0) {
			return result;
		}
		else if (components1.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
			if (components2.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
				result = static_cast<int32_t>(components1.affinityNumber_ - components2.affinityNumber_);
			}
			else {
				result = 1;
			}
		}
		else if (components1.affinityStringSize_ > 0) {
			if (components2.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
				return -1;
			}
			else if (components2.affinityStringSize_ > 0) {
				result = compareNormalizedString(
					components1.affinityString_, components1.affinityStringSize_,
					upperCaseBit1, startPos1,
					components2.affinityString_, components2.affinityStringSize_,
					upperCaseBit2, startPos2,
					caseSensitive);
				startPos1 += components1.affinityStringSize_;
				startPos2 += components2.affinityStringSize_;
			}
			else {
				result = 1;
			}
		}
		else {
			if (components2.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER
				|| components2.affinityStringSize_ > 0) {
				result = -1;
			}
			else {
				result = 0;
			}
		}

		if (result != 0) {
			return result;
		}
		else if (components1.systemPartId_ != UNDEF_SYSTEM_PART_ID) {
			if (components2.systemPartId_ != UNDEF_SYSTEM_PART_ID) {
				result = static_cast<int32_t>(components1.systemPartId_ - components2.systemPartId_);
			}
			else {
				result = 1;
			}
		}
		else if (components1.systemPartSize_ > 0 ) {
			if (components2.systemPartId_ != UNDEF_SYSTEM_PART_ID) {
				result = -1;
			}
			else if (components2.systemPartSize_ > 0) {
				result = compareNormalizedString(
					components1.systemPart_, components1.systemPartSize_,
					upperCaseBit1, startPos1,
					components2.systemPart_, components2.systemPartSize_,
					upperCaseBit2, startPos2,
					caseSensitive);
				startPos1 += components1.systemPartSize_;
				startPos2 += components2.systemPartSize_;
			}
			else {
				result = 1;
			}
		}
		else {
			if (components2.systemPartId_ != UNDEF_NODE_AFFINITY_NUMBER
				|| components2.systemPartSize_ > 0) {
				result = -1;
			}
			else {
				result = 0;
			}
		}

		return result;

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "comparing container/table name"));
	}
}

const char8_t* FullContainerKey::getExtendedSymbol() {
	assert(symbol[0] == '_');
	return &symbol[1];
}

void FullContainerKey::parseAndValidate(DatabaseId dbId, const char8_t *str, uint32_t length,
										FullContainerKeyComponents &components,
										BitArray &upperCaseBit) const {
	try {
		size_t len = length;
		if (len <= 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"Size of container/table name is zero");
		}
		if (len > constraint_.maxTotalLength_) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"Size of container/table name exceeds maximum size");
		}

		size_t partCounter = 0;
		size_t sharpCounter = 0;
		size_t atmarkCounter = 0;
		const char8_t *part[4] = { NULL, NULL, NULL, NULL };
		uint32_t partLen[4] = { 0, 0, 0, 0 };

		part[partCounter] = str;

		for (size_t i = 0; i < len; i++) {
			if (str[i] == '#') {
				sharpCounter++;
				if (sharpCounter > 1) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"System part must be exactly one");
				}
				else if (atmarkCounter > 0) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"System part can not be described after node affinity");
				}

				partCounter++;
				if (i + 1 <= len) {
					part[partCounter] = str + (i+1);
				}

			}
			else if (str[i] == '@') {
				atmarkCounter++;
				if (atmarkCounter > 2) {
					GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
						"Node affinity or largeId can not be described twice or more");
				}

				partCounter++;
				if (i + 1 <= len) {
					part[partCounter] = str + (i+1);
				}

			}
			else {
				partLen[partCounter]++;
			}
		}

		assert(partCounter == sharpCounter + atmarkCounter);


		components.clear();

		components.dbId_ = dbId;
		validateDbId(components.dbId_);

		components.baseName_ = part[0];
		components.baseNameSize_ = partLen[0];
		const bool hasSystemPart = (sharpCounter == 1);
		const bool hasLargeContainerId = (atmarkCounter > 1);

		if (!constraint_.systemPartAllowed_ && hasSystemPart) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"System part is not allowed");
		}
		if (!constraint_.largeContainerIdAllowed_ && hasLargeContainerId) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"LargeId is not allowed");
		}

		validateBaseContainerName(
			components.baseName_, components.baseNameSize_, hasSystemPart);

		if (sharpCounter == 0) {
			if (atmarkCounter == 1) {
				validateAndSetNodeAffinity(part[1], partLen[1], components);
			}
			else if (atmarkCounter == 2) {
				validateAndSetLargeContainerId(part[1], partLen[1], components);
				validateAndSetNodeAffinity(part[2], partLen[2], components);
			}
		}
		else {
			validateAndSetSystemPart(part[1], partLen[1], components);
			if (atmarkCounter == 1) {
				validateAndSetNodeAffinity(part[2], partLen[2], components);
			}
			else if (atmarkCounter == 2) {
				validateAndSetLargeContainerId(part[2], partLen[2], components);
				validateAndSetNodeAffinity(part[3], partLen[3], components);
			}
		}

		setUpperCaseBit(components, upperCaseBit);

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "parsing and validating container/table name"));
	}
}

void FullContainerKey::validate(const FullContainerKeyComponents &components) const {
	try {
		uint64_t totalLength = 0;

		validateDbId(components.dbId_);

		const bool hasSystemPart =
			(components.systemPartId_ != UNDEF_SYSTEM_PART_ID || components.systemPartSize_ > 0);
		const bool hasLargeContainerId = (components.largeContainerId_ != UNDEF_LARGE_CONTAINERID);

		if (!constraint_.systemPartAllowed_ && hasSystemPart) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"System part is not allowed");
		}
		if (!constraint_.largeContainerIdAllowed_ && hasLargeContainerId) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"LargeId is not allowed");
		}

		validateBaseContainerName(
			components.baseName_, components.baseNameSize_, hasSystemPart);
		totalLength += components.baseNameSize_;

		if (components.largeContainerId_ != UNDEF_LARGE_CONTAINERID) {
			validateLargeContainerId(components.largeContainerId_);
			util::NormalOStringStream oss;
			oss << components.largeContainerId_;
			totalLength += oss.str().length();
			totalLength++;
		}
		if (components.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
			validateAffinityNumber(components.affinityNumber_);
			util::NormalOStringStream oss;
			oss << components.affinityNumber_;
			totalLength += oss.str().length();
			totalLength++;
		}
		else if (components.affinityStringSize_ > 0) {
			validateExtendedName(components.affinityString_, components.affinityStringSize_, "node affinity");
			totalLength += components.affinityStringSize_;
			totalLength++;
		}

		if (components.systemPartId_ != UNDEF_SYSTEM_PART_ID) {
			validateSystemPartId(components.systemPartId_);
			util::NormalOStringStream oss;
			oss << components.systemPartId_;
			totalLength += oss.str().length();
			totalLength++;
		}
		else if (components.systemPartSize_ > 0) {
			validateExtendedName(components.systemPart_, components.systemPartSize_, "system part");
			totalLength += components.systemPartSize_;
			totalLength++;
		}

		if (totalLength <= 0) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"Size of container/table name is zero");
		}
		if (totalLength > constraint_.maxTotalLength_) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"Size of container/table name exceeds maximum size");
		}

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "validating container/table name"));
	}
}

void FullContainerKey::setUpperCaseBit(const FullContainerKeyComponents &components,
									   BitArray &upperCaseBit) const {
	upperCaseBit.clear();

	const char8_t *strList[3] = { components.baseName_, components.affinityString_, components.systemPart_ };
	uint32_t lengthList[3] = { components.baseNameSize_, components.affinityStringSize_, components.systemPartSize_ };

	for (size_t i = 0; i < 3; i++) {
		for (size_t j = 0; j < lengthList[i]; j++) {
			upperCaseBit.append(isupper(strList[i][j]) != 0);
		}
	}
}

void FullContainerKey::serialize(util::StackAllocator &alloc,
								 const FullContainerKeyComponents &components,
								 const BitArray &upperCaseBit) {
	try {
		util::XArray<char8_t> buf(alloc);
		buf.resize(static_cast<size_t>(components.getStringSize()), '\0');

		util::XArray<uint8_t> binary(alloc);
		util::XArrayOutStream<> arrayOut(binary);
		ContainerKeyOutStream out(arrayOut);

		const size_t flagPos = out.base().position();

		uint8_t flag = 0;
		out << flag;

		if (components.dbId_ != GS_PUBLIC_DB_ID) {
			flag |= DBID_EXISTS;
			out << components.dbId_;
		}

		{
			ValueProcessor::convertLowerCase(
				components.baseName_, components.baseNameSize_, buf.data());
			encodeString(out, buf.data(), components.baseNameSize_);
		}

		if (components.largeContainerId_ != UNDEF_LARGE_CONTAINERID) {
			flag |= LARGE_CONTAINERID_EXISTS;
			encodeVarLong(out, components.largeContainerId_);
		}

		if (components.affinityNumber_ != UNDEF_NODE_AFFINITY_NUMBER) {
			flag |= NODE_AFFINITY_NUM;
			encodeVarLong(out, components.affinityNumber_);
		}
		else if (components.affinityStringSize_ > 0) {
			flag |= NODE_AFFINITY_STR;
			ValueProcessor::convertLowerCase(
				components.affinityString_, components.affinityStringSize_, buf.data());
			encodeString(out, buf.data(), components.affinityStringSize_);
		}

		if (components.systemPartId_ != UNDEF_SYSTEM_PART_ID) {
			flag |= SYSTEM_PART_ID_NUM;
			encodeVarLong(out, components.systemPartId_);
		}
		else if (components.systemPartSize_ > 0) {
			flag |= SYSTEM_PART_ID_STR;
			ValueProcessor::convertLowerCase(
				components.systemPart_, components.systemPartSize_, buf.data());
			encodeString(out, buf.data(), components.systemPartSize_);
		}

		out.writeAll(upperCaseBit.data(), upperCaseBit.byteLength());

		const size_t lastPos = out.base().position();
		out.base().position(flagPos);
		out << flag;
		out.base().position(lastPos);

		if (binary.size() >= static_cast<size_t>(UINT32_MAX)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"Size of serialized container/table name exceeds maximum size");
		}

		body_ = binary.data();
		size_ = binary.size();


	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "serializing container/table name"));
	}
}

void FullContainerKey::deserialize(util::StackAllocator &alloc,
								   FullContainerKeyComponents &components,
								   BitArray &upperCaseBit,
								   bool unNormalized) const {
	try {
		if (isEmpty()) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Container/table name is empty");
		}

		components.clear();
		upperCaseBit.clear();

		ContainerKeyInStream in(util::ArrayInStream(body_, size_));

		const char8_t *normalizedStr[3] = { NULL, NULL, NULL };

		uint8_t flag;
		in >> flag;

		if (flag & DBID_EXISTS) {
			in >> components.dbId_;
		}
		else {
			components.dbId_ = GS_PUBLIC_DB_ID;
		}

		components.baseNameSize_ = decodeVarInt(in);
		normalizedStr[0] = reinterpret_cast<const char8_t*>(body_ + in.base().position());
		in.base().position(in.base().position() + components.baseNameSize_);

		if (flag & LARGE_CONTAINERID_EXISTS) {
			components.largeContainerId_ = decodeVarLong(in);
		}

		if (flag & NODE_AFFINITY_NUM) {
			components.affinityNumber_ = decodeVarLong(in);
		}
		else if (flag & NODE_AFFINITY_STR) {
			components.affinityStringSize_ = decodeVarInt(in);
			normalizedStr[1] = reinterpret_cast<const char8_t*>(body_ + in.base().position());
			in.base().position(in.base().position() + components.affinityStringSize_);
		}

		if (flag & SYSTEM_PART_ID_NUM) {
			components.systemPartId_ = decodeVarLong(in);
		}
		else if (flag & SYSTEM_PART_ID_STR) {
			components.systemPartSize_ = decodeVarInt(in);
			normalizedStr[2] = reinterpret_cast<const char8_t*>(body_ + in.base().position());
			in.base().position(in.base().position() + components.systemPartSize_);
		}

		const uint64_t strLength = components.baseNameSize_
			+ components.affinityStringSize_
			+ components.systemPartSize_;

		if (in.base().remaining() != strLengthToBitLength(strLength)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Size of container/table name is invalid");
		}
		else {
			upperCaseBit.reserve(strLength);
			upperCaseBit.putAll(body_ + in.base().position(), strLength);
			in.base().position(in.base().position() + in.base().remaining());
		}

		if (in.base().position() != size_) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Size of container/table name is invalid");
		}


		if (unNormalized) {
			util::XArray<char8_t> buf(alloc);
			buf.resize(static_cast<size_t>(strLength), '\0');
			uint64_t startPos = 0;

			createOriginalString(normalizedStr[0], components.baseNameSize_,
				buf.data()+startPos, upperCaseBit, startPos);
			components.baseName_ = buf.data()+startPos;
			startPos += components.baseNameSize_;

			if (components.affinityStringSize_ > 0) {
				createOriginalString(normalizedStr[1], components.affinityStringSize_,
					buf.data()+startPos, upperCaseBit, startPos);
				components.affinityString_ = buf.data()+startPos;
				startPos += components.affinityStringSize_;
			}

			if (components.systemPartSize_ > 0) {
				createOriginalString(normalizedStr[2], components.systemPartSize_,
					buf.data()+startPos, upperCaseBit, startPos);
				components.systemPart_ = buf.data()+startPos;
				startPos += components.systemPartSize_;
			}
		}
		else {
			components.baseName_ = normalizedStr[0];
			if (components.affinityStringSize_ > 0) {
				components.affinityString_ = normalizedStr[1];
			}
			if (components.systemPartSize_ > 0) {
				components.systemPart_ = normalizedStr[2];
			}
		}

	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MESSAGE_ON(
				e, "deserializing container/table name"));
	}
}

void FullContainerKey::clear() {
	body_ = NULL;
	size_ = 0;
}

uint64_t FullContainerKey::strLengthToBitLength(uint64_t strLength) const {
	return (strLength / (sizeof(uint8_t) * CHAR_BIT)
			+ (strLength % (sizeof(uint8_t) * CHAR_BIT) > 0 ? 1 : 0));
}

uint32_t FullContainerKey::getEncodedVarSize32(uint32_t val) const {
	return ValueProcessor::getEncodedVarSize(val);
}

uint32_t FullContainerKey::getEncodedVarSize64(uint64_t val) const {
	return ValueProcessor::getEncodedVarSize(val);
}

void FullContainerKey::encodeVarInt(ContainerKeyOutStream &out,
									uint32_t val) const {
	if (val > VAR_SIZE_4BYTE_THRESHOLD) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Too large value to encode : " << val);
	}
	switch (getEncodedVarSize32(val)) {
		case 1:
			out << ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(val));
			break;
		case 4:
			out << ValueProcessor::encode4ByteVarSize(val);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Value is not encodable : " << val);
	}
}

void FullContainerKey::encodeVarLong(ContainerKeyOutStream &out,
									 uint64_t val) const {
	if (val > VAR_SIZE_8BYTE_THRESHOLD) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Too large value to encode : " << val);
	}
	switch (getEncodedVarSize64(val)) {
		case 1:
			out << ValueProcessor::encode1ByteVarSize(static_cast<uint8_t>(val));
			break;
		case 4:
			out << ValueProcessor::encode4ByteVarSize(static_cast<uint32_t>(val));
			break;
		case 8:
			out << ValueProcessor::encode8ByteVarSize(val);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Value is not encodable : " << val);
	}
}

void FullContainerKey::encodeString(ContainerKeyOutStream &out,
									const char8_t *str, uint32_t len) const {
	encodeVarInt(out, len);
	out.writeAll(str, len);
}

void FullContainerKey::encodeBinary(ContainerKeyOutStream &out,
								 const uint8_t *data, uint32_t len) const {
	encodeVarInt(out, len);
	out.writeAll(data, len);
}

uint32_t FullContainerKey::decodeVarInt(ContainerKeyInStream &in) const {
	return ValueProcessor::getVarSize(in);
}

uint64_t FullContainerKey::decodeVarLong(ContainerKeyInStream &in) const {
	uint64_t currentPos = in.base().position();
	uint8_t byteData;
	in >> byteData;
	if (ValueProcessor::varSizeIs1Byte(byteData)) {
		return ValueProcessor::decode1ByteVarSize(byteData);
	} else if (ValueProcessor::varSizeIs4Byte(byteData)) {
		in.base().position(static_cast<size_t>(currentPos));
		uint32_t rawData;
		in >> rawData;
		return ValueProcessor::decode4ByteVarSize(rawData);
	} else if (ValueProcessor::varSizeIs8Byte(byteData)) {
		in.base().position(static_cast<size_t>(currentPos));
		uint64_t rawData;
		in >> rawData;
		return ValueProcessor::decode8ByteVarSize(rawData);
	} else {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Size of encoded value is invalid");
	}
}

bool FullContainerKey::isEncodableVarInt(uint32_t val) const {
	return (val < VAR_SIZE_4BYTE_THRESHOLD);
}

bool FullContainerKey::isEncodableVarLong(uint64_t val) const {
	return (val < VAR_SIZE_8BYTE_THRESHOLD);
}

bool FullContainerKey::isSymbol(char8_t ch) {
	bool match = false;
	for (size_t i = 0; i < strlen(symbol) && !match; i++) {
		match = (ch == symbol[i]);
	}
	return match;
}

void FullContainerKey::createOriginalString(char8_t const *src, uint32_t size, char8_t *dest,
											const BitArray &upperCaseBit, uint64_t startPos) const {
	for (uint32_t i = 0; i < size; i++) {
		const char8_t c = *(src + i);
		if (upperCaseBit.get(startPos + i)) {
			*(dest + i) = c - 32;
		}
		else {
			*(dest + i) = c;
		}
	}
}

void FullContainerKey::validateDbId(DatabaseId dbId) const {
	if (dbId < 0 || dbId > MAX_DBID) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Invalid database id : " << dbId);
	}
}

void FullContainerKey::validateBaseContainerName(const char8_t *baseName,
												 uint32_t baseNameLength,
												 bool systemPartExists) const {
	const char8_t *str = baseName;
	const uint32_t len = baseNameLength;
	if ((str == NULL || len <= 0) && !systemPartExists) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of base name is zero");
	}
	else if (len > constraint_.maxTotalLength_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of base name exceeds maximum size : " << str);
	}

	for (size_t i = 0; forbiddenPattern[i] != NULL; i++) {
		if (compareOriginalString(baseName, baseNameLength,
				forbiddenPattern[i], static_cast<uint32_t>(strlen(forbiddenPattern[i])),
				false) == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
				"Base name contains forbidden patterns : " << forbiddenPattern[i]);
		}
	}
	
	for (uint32_t i = 0; i < len; i++) {
		const unsigned char ch = static_cast<unsigned char>(str[i]);
		if (!isalpha(ch) && !isdigit(ch) && !isSymbol(ch)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				"Base name contains forbidden characters : " << ch);
		}
	}
}

bool FullContainerKey::validateExtendedName(const char8_t *str, uint32_t len,
											const char8_t *partName) const {
	bool allNum = true;

	if (str == NULL || len <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << partName << " string is zero : " << str);
	}
	if (len > constraint_.maxTotalLength_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << partName << " string exceeds maximum size : " << str);
	}

	for (uint32_t i = 0; i < len; i++) {
		const unsigned char ch = static_cast<unsigned char>(str[i]);
		if (isdigit(ch)) {
			allNum &= true;
		}
		else if (isalpha(ch) || isSymbol(ch)) {
			allNum &= false;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				partName << "contains forbidden characters : " << ch);
		}
	}

	if (allNum) {
		if (len > 1 && str[0] == '0') {
			allNum = false;
		}
		else if (len > MAX_NODE_AFFINITY_NUMBER_DIGITS) {
			allNum = false;
		}
	}

	return allNum;
}

void FullContainerKey::validateNumeric(const char8_t *str, uint32_t len,
									   const char8_t *partName) const {
	if (str == NULL || len <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << partName << " string is zero");
	}
	if (len > constraint_.maxTotalLength_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << partName << " string exceeds maximum size");
	}

	for (uint32_t i = 0; i < len; i++) {
		const unsigned char ch = static_cast<unsigned char>(str[i]);
		if (!isdigit(ch)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				partName << "contains forbidden characters : " << ch);
		}
	}

	if (len > 1 && str[0] == '0') {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			partName << " is not numeric");
	}
	else if (len > MAX_LARGE_CONTAINERID_DIGITS) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Number of digits of " << partName << " exceeds maximum size");
	}
}

void FullContainerKey::validateAffinityNumber(NodeAffinityNumber affinityNumber) const {
	if (!isEncodableVarLong(affinityNumber)) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Invalid node affinity : " << affinityNumber);
	}
}

void FullContainerKey::validateLargeContainerId(LargeContainerId largeContainerId) const {
	if (!isEncodableVarLong(largeContainerId)) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Invalid largeId : " << largeContainerId);
	}
}

void FullContainerKey::validateSystemPartId(SystemPartId systemPartId) const {
	if (!isEncodableVarLong(systemPartId)) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_CONTAINER_NAME_INVALID,
			"Invalid system internal id : " << systemPartId);
	}
}

void FullContainerKey::validateAndSetNodeAffinity(const char8_t *str, uint32_t len,
												  FullContainerKeyComponents &components) const {
	if (validateExtendedName(str, len, "node affinity")) {
		const NodeAffinityNumber affinityNumber = getNodeAffinityNumber(str, len);
		if (isEncodableVarLong(affinityNumber)) {
			components.affinityNumber_ = affinityNumber;
		}
		else {
			components.affinityString_ = str;
			components.affinityStringSize_ = len;
		}
	}
	else {
		components.affinityString_ = str;
		components.affinityStringSize_ = len;
	}
}

void FullContainerKey::validateAndSetLargeContainerId(const char8_t *str, uint32_t len,
													  FullContainerKeyComponents &components) const {
	validateNumeric(str, len, "largeId");
	components.largeContainerId_ = getLargeContainerId(str, len);
	validateLargeContainerId(components.largeContainerId_);
}

void FullContainerKey::validateAndSetSystemPart(const char8_t *str, uint32_t len,
												FullContainerKeyComponents &components) const {
	if (validateExtendedName(str, len, "system part")) {
		const SystemPartId systemPartId = getSystemPartId(str, len);
		if (isEncodableVarLong(systemPartId)) {
			components.systemPartId_ = systemPartId;
		}
		else {
			components.systemPart_ = str;
			components.systemPartSize_ = len;
		}
	}
	else {
		components.systemPart_ = str;
		components.systemPartSize_ = len;
	}
}

NodeAffinityNumber FullContainerKey::getNodeAffinityNumber(const char8_t *affinityString,
												   uint32_t affinityStringLength) const {
	if (affinityString == NULL || affinityStringLength == 0) {
		return UNDEF_NODE_AFFINITY_NUMBER;
	}
	else {
		char8_t tmp[MAX_NODE_AFFINITY_NUMBER_DIGITS + 1];
		memcpy(tmp, affinityString, affinityStringLength*sizeof(char8_t));
		tmp[affinityStringLength] = '\0';
		util::NormalIStringStream iss(tmp);
		NodeAffinityNumber affinityNumber;
		iss >> affinityNumber;
		return affinityNumber;
	}
}

ContainerId FullContainerKey::getLargeContainerId(const char8_t *largeContainerIdString,
												  uint32_t largeContainerIdStringLength) const {
	if (largeContainerIdString == NULL || largeContainerIdStringLength == 0) {
		return UNDEF_LARGE_CONTAINERID;
	}
	else {
		char8_t tmp[MAX_LARGE_CONTAINERID_DIGITS + 1];
		memcpy(tmp, largeContainerIdString, largeContainerIdStringLength*sizeof(char8_t));
		tmp[largeContainerIdStringLength] = '\0';
		util::NormalIStringStream iss(tmp);
		ContainerId largeContainerId;
		iss >> largeContainerId;
		return largeContainerId;
	}
}

SystemPartId FullContainerKey::getSystemPartId(const char8_t *systemPart,
											   uint32_t systemPartLength) const {
	if (systemPart == NULL || systemPartLength == 0) {
		return UNDEF_SYSTEM_PART_ID;
	}
	else {
		char8_t tmp[MAX_SYSTEM_PART_ID_DIGITS + 1];
		memcpy(tmp, systemPart, systemPartLength*sizeof(char8_t));
		tmp[systemPartLength] = '\0';
		util::NormalIStringStream iss(tmp);
		SystemPartId systemPartId;
		iss >> systemPartId;
		return systemPartId;
	}
}

int32_t FullContainerKey::compareOriginalString(
		const char8_t *str1, uint32_t str1Length,
		const char8_t *str2, uint32_t str2Length,
		bool caseSensitive) const {

	const size_t len = std::min(str1Length, str2Length);

	int32_t result = 0;

	if (caseSensitive) {
		result = strncmp(str1, str2, len);
	}
	else {
		for (size_t i = 0; i < len && result == 0; i++) {
			int ch1 = static_cast<int>(str1[i]);
			int ch2 = static_cast<int>(str2[i]);

			result = ch1 - ch2;

			if (result != 0) {
				if (!caseSensitive && isalpha(ch1) && isalpha(ch2)) {
					ch1 = tolower(ch1);
					ch2 = tolower(ch2);
					result = ch1 - ch2;
				}
			}
		}
	}

	if (result == 0) {
		result = str1Length - str2Length;
	}

	return result;
}

int32_t FullContainerKey::compareNormalizedString(
		const char8_t *str1, uint32_t str1Length, const BitArray &upperCaseBit1, uint64_t startPos1,
		const char8_t *str2, uint32_t str2Length, const BitArray &upperCaseBit2, uint64_t startPos2,
		bool caseSensitive) const {

	const size_t len = std::min(str1Length, str2Length);

	int32_t result = strncmp(str1, str2, len);

	if (caseSensitive) {
		for (uint64_t i = 0; i < len && result == 0; i++) {
			const bool bit1 = upperCaseBit1.get(startPos1+i);
			const bool bit2 = upperCaseBit2.get(startPos2+i);
			if (bit1 != bit2) {
				result = (bit1 ? -1 : 1);
			}
		}
	}

	if (result == 0) {
		result = str1Length - str2Length;
	}

	return result;
}


EmptyAllowedKey::EmptyAllowedKey(const KeyConstraint &constraint) :
	FullContainerKey(constraint) {
}

void EmptyAllowedKey::validate(
	const KeyConstraint &constraint,
	const char8_t *str, uint32_t length,
	const char8_t *keyName) {
	if (str == NULL) {
		return;
	}

	if (length > constraint.maxTotalLength_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << keyName << " exceeds maximum size : " << str);
	}

	for (uint32_t i = 0; i < length; i++) {
		const unsigned char ch = static_cast<unsigned char>(str[i]);
		if (!isalpha(ch) && !isdigit(ch) && !isSymbol(ch)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				keyName << " contains forbidden characters : " << ch);
		}
	}
}


NoEmptyKey::NoEmptyKey(const KeyConstraint &constraint) :
	FullContainerKey(constraint) {
}

void NoEmptyKey::validate(
	const KeyConstraint &constraint,
	const char8_t *str, uint32_t length,
	const char8_t *keyName) {
	if (length <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << keyName << " is zero");
	}
	if (length > constraint.maxTotalLength_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << keyName << " exceeds maximum size : " << str);
	}

	for (uint32_t i = 0; i < length; i++) {
		const unsigned char ch = static_cast<unsigned char>(str[i]);
		if (!isalpha(ch) && !isdigit(ch) && !isSymbol(ch)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				keyName << " contains forbidden characters : " << ch);
		}
	}
}


AlphaOrDigitKey::AlphaOrDigitKey(uint32_t maxLength) :
	FullContainerKey(KeyConstraint::getUserKeyConstraint(maxLength)) {
}

void AlphaOrDigitKey::validate(
	const char8_t *str, uint32_t length, uint32_t maxLength,
	const char8_t *keyName) {
	if (length <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << keyName << " is zero");
	}
	if (length > maxLength) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
			"Size of " << keyName << " exceeds maximum size : " << str);
	}

	for (uint32_t i = 0; i < length; i++) {
		const unsigned char ch = static_cast<unsigned char>(str[i]);
		if (!isalpha(ch) && !isdigit(ch)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_LIMITS_EXCEEDED,
				keyName << " contains forbidden characters : " << ch);
		}
	}
}
