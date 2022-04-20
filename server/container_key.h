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
#ifndef FULL_CONTAINER_KEY_H_
#define FULL_CONTAINER_KEY_H_

#include "data_type.h"
#include "data_store_common.h"
#include "bit_array.h"
#include "util/allocator.h"

class TransactionContext;

typedef uint64_t NodeAffinityNumber;
const NodeAffinityNumber MAX_NODE_AFFINITY_NUMBER = VAR_SIZE_8BYTE_THRESHOLD-1;
const NodeAffinityNumber UNDEF_NODE_AFFINITY_NUMBER = UINT64_MAX;

typedef NodeAffinityNumber LargeContainerId;
const LargeContainerId MAX_LARGE_CONTAINERID = MAX_NODE_AFFINITY_NUMBER;
const LargeContainerId UNDEF_LARGE_CONTAINERID = UNDEF_NODE_AFFINITY_NUMBER;

typedef NodeAffinityNumber SystemPartId;
const SystemPartId MAX_SYSTEM_PART_ID = MAX_NODE_AFFINITY_NUMBER;
const SystemPartId UNDEF_SYSTEM_PART_ID = UNDEF_NODE_AFFINITY_NUMBER;

struct KeyConstraint {
	uint32_t maxTotalLength_;
	bool systemPartAllowed_;
	bool largeContainerIdAllowed_;

	KeyConstraint();
	KeyConstraint(
		uint32_t maxTotalLength,
		bool systemPartAllowed,
		bool largeContainerIdAllowed);

	static KeyConstraint getUserKeyConstraint(uint32_t maxTotalLength);
	static KeyConstraint getSystemKeyConstraint(uint32_t maxTotalLength);
	static KeyConstraint getNoLimitKeyConstraint();
};

struct FullContainerKeyComponents {
	DatabaseId dbId_;

	const char8_t *baseName_;
	uint32_t baseNameSize_;

	const char8_t *affinityString_;
	uint32_t affinityStringSize_;
	NodeAffinityNumber affinityNumber_;

	LargeContainerId largeContainerId_;

	const char8_t *systemPart_;
	uint32_t systemPartSize_;
	SystemPartId systemPartId_;

	FullContainerKeyComponents();
	FullContainerKeyComponents(const FullContainerKeyComponents &another);
	FullContainerKeyComponents& operator=(const FullContainerKeyComponents &another);

	uint32_t getStringSize() const;

	void clear();

	bool equals(const FullContainerKeyComponents &another) const;

	void dump(const char8_t *msg) const;
private:
	bool equalAffinity(const FullContainerKeyComponents& another) const;
	bool equalSystemPart(const FullContainerKeyComponents& another) const;
};

class FullContainerKey {
public:
	FullContainerKey(const KeyConstraint &constraint);

	FullContainerKey(const KeyConstraint &constraint,
		const void *data);

	FullContainerKey(util::StackAllocator &alloc,
		const KeyConstraint &constraint,
		const void *body, size_t size);
	
	FullContainerKey(util::StackAllocator &alloc,
		const KeyConstraint &constraint,
		DatabaseId dbId, const char8_t *str, uint32_t length);
	
	FullContainerKey(util::StackAllocator &alloc,
		const KeyConstraint &constraint,
		const FullContainerKeyComponents &components);
	
	FullContainerKey(const FullContainerKey &another);

	virtual ~FullContainerKey();

	FullContainerKey& operator=(const FullContainerKey &another);

	FullContainerKeyComponents getComponents(util::StackAllocator &alloc,
		bool unNormalized = true) const;

	void toString(util::StackAllocator &alloc, util::String &str) const;
	
	void toBinary(const void *&body, size_t &size) const;

	bool isEmpty() const;

	int32_t compareTo(util::StackAllocator &alloc,
		const FullContainerKey &key, bool caseSensitive) const;

	static int32_t compareTo(TransactionContext &txn,
		const uint8_t *key1, const uint8_t *key2, bool caseSensitive);

	static const char8_t* getExtendedSymbol();

protected:
	typedef util::ByteStream< util::XArrayOutStream<> > ContainerKeyOutStream;
	typedef util::ByteStream<util::ArrayInStream> ContainerKeyInStream;

	KeyConstraint constraint_;

	const uint8_t *body_;
	size_t size_;

	static const char8_t *symbol;
	static const char8_t *extendedSymbol;
	static const char8_t *forbiddenPattern[];

	static const DatabaseId MAX_ENCODABLE_DBID;

	static const size_t MAX_NODE_AFFINITY_NUMBER_DIGITS;
	static const size_t MAX_LARGE_CONTAINERID_DIGITS;
	static const size_t MAX_SYSTEM_PART_ID_DIGITS;

	static const uint8_t DBID_EXISTS;
	static const uint8_t LARGE_CONTAINERID_EXISTS;
	static const uint8_t NODE_AFFINITY_STR;
	static const uint8_t NODE_AFFINITY_NUM;
	static const uint8_t NODE_AFFINITY_EXISTS;
	static const uint8_t SYSTEM_PART_ID_STR;
	static const uint8_t SYSTEM_PART_ID_NUM;
	static const uint8_t SYSTEM_PART_ID_EXISTS;

	static const uint64_t DEFAULT_UPPER_CASE_BIT_LENGTH;


	void parseAndValidate(
		DatabaseId dbId, const char8_t *str, uint32_t length,
		FullContainerKeyComponents &components, ContainerKeyBitArray &upperCaseBit) const;

	void validate(const FullContainerKeyComponents &components) const;

	void setUpperCaseBit(const FullContainerKeyComponents &components,
		ContainerKeyBitArray &upperCaseBit) const;

	void serialize(util::StackAllocator &alloc,
		const FullContainerKeyComponents &components, const ContainerKeyBitArray &upperCaseBit);

	void deserialize(util::StackAllocator &alloc,
		FullContainerKeyComponents &components, ContainerKeyBitArray &upperCaseBit,
		bool unNormalized) const;

	void clear();


	uint64_t strLengthToBitLength(uint64_t strLength) const;

	uint32_t getEncodedVarSize32(uint32_t val) const;
	uint32_t getEncodedVarSize64(uint64_t val) const;

	void encodeVarInt(ContainerKeyOutStream &out, uint32_t val) const;
	void encodeVarLong(ContainerKeyOutStream &out, uint64_t val) const;
	void encodeString(ContainerKeyOutStream &out, const char8_t *str, uint32_t len) const;
	void encodeBinary(ContainerKeyOutStream &out, const uint8_t *data, uint32_t len) const;

	uint32_t decodeVarInt(ContainerKeyInStream &in) const;
	uint64_t decodeVarLong(ContainerKeyInStream &in) const;

	bool isEncodableVarInt(uint32_t val) const;
	bool isEncodableVarLong(uint64_t val) const;


	static bool isSymbol(char8_t ch);

	void createOriginalString(
		char8_t const *src, uint32_t size, char8_t *dest,
		const ContainerKeyBitArray &upperCaseBit, uint64_t startPos) const;

	void validateDbId(DatabaseId dbId) const;
	void validateBaseContainerName(
		const char8_t *baseName, uint32_t baseNameLength, bool systemPartExists) const;
	bool validateExtendedName(
		const char8_t *str, uint32_t len, const char8_t *partName = "part") const;
	void validateNumeric(
		const char8_t *str, uint32_t len, const char8_t *partName = "part") const;
	void validateAffinityNumber(NodeAffinityNumber affinityNumber) const;
	void validateLargeContainerId(LargeContainerId largeContainerId) const;
	void validateSystemPartId(SystemPartId systemPartId) const;

	void validateAndSetNodeAffinity(
		const char8_t *str, uint32_t len, FullContainerKeyComponents &component) const;
	void validateAndSetLargeContainerId(
		const char8_t *str, uint32_t len, FullContainerKeyComponents &component) const;
	void validateAndSetSystemPart(
		const char8_t *str, uint32_t len, FullContainerKeyComponents &component) const;

	NodeAffinityNumber getNodeAffinityNumber(
		const char8_t *affinityString, uint32_t affinityStringLength) const;
	LargeContainerId getLargeContainerId(
		const char8_t *largeContainerIdString, uint32_t largeContainerIdStringLength) const;
	SystemPartId getSystemPartId(
		const char8_t *systemPart, uint32_t systemPartLength) const;

	int32_t compareOriginalString(
		const char8_t *str1, uint32_t str1Length,
		const char8_t *str2, uint32_t str2Length,
		bool caseSensitive) const;
	int32_t compareNormalizedString(
		const char8_t *str1, uint32_t str1Length, const ContainerKeyBitArray &upperCaseBit1, uint64_t startPos1,
		const char8_t *str2, uint32_t str2Length, const ContainerKeyBitArray &upperCaseBit2, uint64_t startPos2,
		bool caseSensitive) const;

	static void dumpComponent(const FullContainerKeyComponents &component, const char8_t *msg);
};


class EmptyAllowedKey : private FullContainerKey {
public:
	static void validate(const KeyConstraint &constraint,
		const char8_t *str, uint32_t length,
		const char8_t *keyName);

protected:
	EmptyAllowedKey(const KeyConstraint &constraint);
};


class NoEmptyKey : private FullContainerKey {
public:
	static void validate(const KeyConstraint &constraint,
		const char8_t *str, uint32_t length,
		const char8_t *keyName);

protected:
	NoEmptyKey(const KeyConstraint &constraint);
};


class AlphaOrDigitKey : private FullContainerKey {
public:
	static void validate(const char8_t *str, uint32_t length, uint32_t maxLength,
		const char8_t *keyName);

private:
	AlphaOrDigitKey(uint32_t maxLength);
};

#endif
