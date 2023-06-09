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
/*!
	@file
	@brief Definition of tuple list
*/

#ifndef SQL_TUPLE_H_
#define SQL_TUPLE_H_

#include "data_type.h"
#include "sql_tuple_var.h"
#include "sql_temp_store.h"
#include "sql_common.h"







class LocalTempStore;
class ResourceGroup;
class TupleList {
public:
	class ReadableTuple;
	class WritableTuple;
	class Reader;
	class Writer;
	class WriterHandler;
	class BlockReader;
	class BlockWriter;
	class TupleColumnTypeCoder;

	friend class ReadableTuple;
	friend class WritableTuple;
	friend class Reader;
	friend class Writer;
	friend class BlockReader;
	friend class BlockWriter;
	friend class TupleValue;


	enum TupleColumnTypeBits {
		TYPE_BITS_NORMAL_SIZE = 3,
		TYPE_BITS_SUB_TYPE = 8,
		TYPE_BITS_VAR = 12,
		TYPE_BITS_ARRAY = 13,
		TYPE_BITS_NULLABLE = 14,
		TYPE_BITS_RESERVED = 15
	};

	enum TupleColumnTypeMask {
		TYPE_MASK_FIXED_SIZE = ((1 << TYPE_BITS_SUB_TYPE) - 1),
		TYPE_MASK_SUB = ((1 << (TYPE_BITS_VAR - TYPE_BITS_SUB_TYPE)) - 1) <<
				TYPE_BITS_SUB_TYPE,
		TYPE_MASK_ARRAY = 1 << TYPE_BITS_ARRAY,
		TYPE_MASK_VAR = 1 << TYPE_BITS_VAR,
		TYPE_MASK_ARRAY_VAR = TYPE_MASK_ARRAY | TYPE_MASK_VAR,
		TYPE_MASK_NULLABLE = 1 << TYPE_BITS_NULLABLE,
		TYPE_MASK_TIMESTAMP_BASE = (3 << TYPE_BITS_SUB_TYPE),
		TYPE_MASK_TIMESTAMP_EXT =
				TYPE_MASK_TIMESTAMP_BASE | (4 << TYPE_BITS_SUB_TYPE)
	};

	enum TupleColumnTypeTag {
		TYPE_NULL = (0 << TYPE_BITS_SUB_TYPE) | 0,
		TYPE_BYTE = (1 << TYPE_BITS_SUB_TYPE) | 1,
		TYPE_SHORT = (1 << TYPE_BITS_SUB_TYPE) | 2,
		TYPE_INTEGER = (1 << TYPE_BITS_SUB_TYPE) | 4,
		TYPE_LONG = (1 << TYPE_BITS_SUB_TYPE) | 8,
		TYPE_FLOAT = (2 << TYPE_BITS_SUB_TYPE) | 4,
		TYPE_DOUBLE = (2 << TYPE_BITS_SUB_TYPE) | 8,
		TYPE_TIMESTAMP = TYPE_MASK_TIMESTAMP_BASE | 8,
		TYPE_MICRO_TIMESTAMP = TYPE_MASK_TIMESTAMP_EXT | 8,
		TYPE_NANO_TIMESTAMP = TYPE_MASK_TIMESTAMP_EXT | 9,
		TYPE_BOOL = (4 << TYPE_BITS_SUB_TYPE) | 1,
		TYPE_NUMERIC = (5 << TYPE_BITS_SUB_TYPE) | 0,
		TYPE_ANY = TYPE_MASK_VAR | (0 << TYPE_BITS_SUB_TYPE) | 10,
		TYPE_STRING = TYPE_MASK_VAR | (1 << TYPE_BITS_SUB_TYPE) | 8,
		TYPE_GEOMETRY = TYPE_MASK_VAR | (2 << TYPE_BITS_SUB_TYPE) | 8,
		TYPE_BLOB = TYPE_MASK_VAR | (3 << TYPE_BITS_SUB_TYPE) | 8
	};

	typedef uint16_t TupleColumnType;
	typedef uint64_t Position;
	typedef LocalTempStore Store;
	typedef LocalTempStore::Group Group;
	typedef LocalTempStore::GroupId GroupId;
	typedef LocalTempStore::ResourceId ResourceId;
	typedef LocalTempStore::BlockId BlockId;
	typedef LocalTempStore::Block Block;
	struct Info;
	class Column; 
	struct LobInfo; 

	typedef std::vector< LocalTempStore::BlockId, util::StdAllocator<
	  LocalTempStore::BlockId, TupleValueVarUtils::VarSizeAllocator> > BlockIdArray;

	static const uint32_t BLOCK_HEADER_SIZE;
	static const uint32_t RESERVED_SIZE_FOR_MAX_SINGLE_VAR;
	static const BlockId UNDEF_BLOCKID;

	TupleList(LocalTempStore &store, const ResourceId &id, const Info *info);
	TupleList(const Group &storeGroup, const Info &info);

	~TupleList();

	ResourceId detach();

	const Info& getInfo() const;
	LocalTempStore& getStore();

	GroupId getGroupId() const;
	ResourceId getResourceId() const;
	uint32_t getBlockSize() const;
	uint32_t getBlockExpSize() const;

	void close();

	bool isActive();

	uint64_t getBlockCount() const;

	uint64_t getReferenceCount() const;

	LocalTempStore::BlockId getActiveTopBlockNth() const;
	void setActiveTopBlockNth(LocalTempStore::BlockId nth);

	void append(const Block &block);

	bool hasVarType() const;

	BlockId getBlockId(uint64_t pos);
	bool getBlockIdList(uint64_t pos, uint64_t count, BlockIdArray &blockIdList);
	void invalidateBlockId(uint64_t pos);

	static bool isPartial(const Block &Block);

	static bool isEmpty(const Block &Block);
	static size_t tupleCount(const Block &Block);
	static size_t tupleCount(const void* blockAddr);
	static int32_t contiguousBlockCount(const Block &Block);
	static int32_t contiguousBlockCount(const void* blockAddr);
	static uint32_t getBlockExpSize(const Block &Block);

	static uint8_t *getVarTopAddr(const Block &Block);
	static uint32_t getNextVarDataOffset(const Block &Block);
	static void setNextVarDataOffset(Block &Block, uint32_t offset);
	static void initializeBlock(Block &block, uint32_t blockSize, uint16_t columnCount, uint32_t blockId);
	static int32_t compareBlockContents(const Block &block1, const Block &block2);

private:
	class Body;

	struct TupleBlockHeader;
	struct BlockLobDataHeader;

	TupleList(const TupleList&);
	TupleList& operator=(const TupleList&);

	void resetBlock(LocalTempStore::Block &block);

	uint64_t getAllocatedBlockCount() const;
	uint64_t addAllocatedBlockCount();
	void setValidBlockCount(uint64_t count);

	bool blockAppendable() const;
	void setBlockAppendable(bool flag);
	bool tupleAppendable() const;
	void setTupleAppendable(bool flag);

	uint64_t addReference();
	uint64_t removeReference();

	bool blockReaderDetached();
	bool readerDetached();
	bool writerDetached();

	void setBlockReaderDetached(bool flag);
	void setWriterDetached(bool flag);

	size_t registReader(Reader &reader);
	void attachReader(size_t id, Reader &reader);
	void detachReader(size_t id);
	void setReaderTopNth(size_t id, uint64_t nth);
	void closeReader(size_t id);

	size_t getReaderCount() const;
	bool isReaderStarted() const;

	uint64_t getActiveReaderCount() const;
	uint64_t getDetachedReaderCount() const;
	uint64_t getAccessTopMin() const;

	Body *body_;
	BlockReader *blockReader_;
	Writer *writer_;
	WriterHandler *writerHandler_;
};

struct TupleList::Info {
	static const size_t COLUMN_COUNT_LIMIT;

	Info();

	void setColumnTypeList(const TupleColumnType *list, size_t count);
	const TupleColumnType* getColumnTypeList() const;

	void getColumns(Column *column, size_t count) const;

	bool isSameColumns(const Info &another) const;

	bool isNullable() const;
	bool hasVarType() const;

	size_t getFixedPartSize() const;
	size_t getNullBitOffset() const;

	static bool checkColumnCount(size_t count);

	const TupleColumnType *columnTypeList_;

	uint32_t blockSize_;

	size_t columnCount_;
};

class TupleList::Column {
	friend class ReadableTuple;
	friend class WritableTuple;
	friend class Reader;
	friend class Writer;
	friend class BlockReader;
	friend class BlockWriter;
	friend class TupleValue;
	friend struct TupleList::Info;

public:
	Column() : offset_(0), type_(0), pos_(0) {}

	inline TupleColumnType getType() const {
		return type_;
	};
	inline uint32_t getPosition() const {
		return pos_;
	}

	uint32_t offset_;
	TupleColumnType type_;
	uint16_t pos_;
};


class TupleString;
class TupleValue {
public:
	struct VarData;
	struct StoreLobHeader;
	struct StoreMultiVarData;
	struct StoreMultiVarBlockIdList;
	struct StackAllocLobHeader;
	struct StackAllocMultiVarData;
	struct TimestampTag {};
	class VarContext;
	class SingleVarBuilder;
	class StackAllocSingleVarBuilder;
	class VarArrayBuilder;
	class LobBuilder;
	class StackAllocLobBuilder;
	class VarArrayReader;
	class LobReader;
	class NanoTimestampBuilder;
	template<typename Base> class Coder;

	friend class TupleList::Reader;
	friend class TupleList::Writer;

	TupleValue();

	template<typename T> explicit TupleValue(const T &value);

	TupleValue(const void *rawData, TupleList::TupleColumnType type);

	TupleValue(const int64_t &ts, const TimestampTag&);

	TupleList::TupleColumnType getType() const;

	template<typename T> T get() const;

	const void* fixedData() const;
	size_t varSize() const;
	const void* varData() const;

	size_t getElementCount();
	const void* getElementFixedPart(size_t index);

	enum {
		MAX_VAR_HEAD_SIZE, /* implementation dependent */
		MAX_VAR_ARRAY_HEAD_SIZE /* implementation dependent */
	};

	static size_t getVarHeadSize(size_t bodySize);
	static size_t setVarHead(void *head, size_t bodySize);

	static size_t getArrayHeadSize(size_t elemCount, TupleList::TupleColumnType type);
	static size_t setArrayHead(void *head, size_t elemCount, TupleList::TupleColumnType type);

	bool testEquals(const TupleValue &another) const;

	const void* getRawData() const;
	void* getRawData();

	template<typename Base>
	static Coder<Base> coder(const Base &baseCoder, VarContext *cxt);

private:
	const void* varData(size_t &headerSize, size_t &dataSize) const;

	static const size_t DEFAULT_INITIAL_CAPACITY = 1 * 1024 * 1024; 

	union {
		int64_t longValue_;    
		double doubleValue_;   
		uint8_t fixedData_[8]; 
		const void *varData_;  
		void *rawData_; 
	} data_;

	int32_t type_;
};


class TupleValue::VarContext {
	friend class TupleValue::SingleVarBuilder;
	friend class TupleValue::VarArrayBuilder;
	friend class TupleValue::LobBuilder;
public:
	VarContext();
	~VarContext();

	void setVarAllocator(TupleValueVarUtils::VarSizeAllocator *varAlloc);
	TupleValueVarUtils::VarSizeAllocator* getVarAllocator() const;

	void setStackAllocator(TupleValueVarUtils::StackAllocator *stackAlloc);
	TupleValueVarUtils::StackAllocator* getStackAllocator() const;

	void setInterruptionChecker(InterruptionChecker *checker);
	InterruptionChecker* getInterruptionChecker() const;

	void setGroup(LocalTempStore::Group *group);
	LocalTempStore::Group& getGroup();

	TupleValueVarUtils::VarData* allocate(
		TupleValueVarUtils ::VarDataType type, size_t minBodySize, TupleValueVarUtils::VarData *base = NULL);
	TupleValueVarUtils::VarData* allocateRelated(size_t minBodySize, TupleValueVarUtils::VarData *base);

	TupleValueVarUtils::VarData* getHead();

	void clear();
	void destroyValue(TupleValue &value);

	TupleValueVarUtils::BaseVarContext& getBaseVarContext() {
		return baseCxt_;
	};

	void moveValueToParent(size_t depth, TupleValue &value);

	class Scope {
	public:
		explicit Scope(VarContext &cxt);
		~Scope();

		void moveValueToParent(TupleValue &value);
	private:
		VarContext &cxt_;
		TupleValueVarUtils::BaseVarContext::Scope scope_;
	};


private:
	VarContext(const VarContext&);
	VarContext& operator=(const VarContext&);

	void resetStoreValue();
	static void resetStoreValue(TupleValueVarUtils::VarData *varData);

	void moveValueToParent(
			TupleValueVarUtils::BaseVarContext::Scope *scope, size_t depth,
			TupleValue &value);

	TupleValueVarUtils::BaseVarContext baseCxt_;
	LocalTempStore::Group *group_;
	LocalTempStore::Block currentBlock_;
	uint32_t currentOffset_;
	InterruptionChecker *interruptionChecker_;
};


class BaseObject;
class TupleValue::SingleVarBuilder { 
public:

	SingleVarBuilder(VarContext &cxt, TupleList::TupleColumnType type, size_t initialCapacity);

	void append(const void *data, size_t size);

	uint32_t append(uint32_t maxSize);

	void* lastData();

	TupleValue build();

private:
	size_t newBlock(LocalTempStore &store);

	TupleValue::VarContext& cxt_;
	TupleList::TupleColumnType type_;
	size_t initialCapacity_;

	uint64_t totalSize_;
	uint64_t partCount_;
	void* lastData_;
	uint32_t lastSize_;
};


class TupleValue::StackAllocSingleVarBuilder {
public:

	StackAllocSingleVarBuilder(util::StackAllocator &alloc, TupleList::TupleColumnType type, size_t initialCapacity);

	void append(const void *data, size_t size);

	uint32_t append(uint32_t maxSize);

	void* lastData();

	TupleValue build();

private:
	size_t newBlock(LocalTempStore &store);

	util::StackAllocator& alloc_;
	TupleList::TupleColumnType type_;
	size_t initialCapacity_;

	uint64_t totalSize_;
	uint64_t partCount_;
	void* lastData_;
	uint32_t lastSize_;
};


template<typename Base>
class TupleValue::Coder : public util::ObjectCoderBase<
		TupleValue::Coder<Base>, typename Base::Allocator, Base> {
public:
	typedef Base BaseCoder;
	typedef typename Base::Allocator Allocator;
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoderBase<
			TupleValue::Coder<Base>,
			typename Base::Allocator, Base> BaseType;

	Coder(const BaseCoder &base, TupleValue::VarContext *cxt);

	template<typename Org, typename S, typename T, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::encodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename T, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, T &value, const Attribute &attr,
			const Traits&) const {
		BaseType::decodeBy(orgCoder, stream, value, attr, Traits());
	}

	template<typename Org, typename S, typename Traits>
	void encodeBy(
			Org &orgCoder, S &stream, const TupleValue &value,
			const Attribute &attr, const Traits&) const;

	template<typename Org, typename S, typename Traits>
	void decodeBy(
			Org &orgCoder, S &stream, TupleValue &value,
			const Attribute &attr, const Traits&) const;

private:
	typedef TupleList::TupleColumnType TupleColumnType;
	typedef TupleList::TupleColumnTypeCoder TupleColumnTypeCoder;

	static const util::ObjectCoder::NumericPrecision DEFAULT_PRECISION =
			util::ObjectCoder::PRECISION_EXACT;

	static const Attribute ATTRIBUTE_TYPE;
	static const Attribute ATTRIBUTE_VALUE;

	template<typename S>
	void encodeTupleValue(
			S &stream, const TupleValue &value, const Attribute &attr,
			TupleColumnType type) const;

	template<typename S>
	void decodeTupleValue(
			S &stream, TupleValue &value, const Attribute &attr,
			TupleColumnType type) const;

	TupleValue::VarContext& getContext() const;

	template<typename T>
	static T getNumericValue(const TupleValue &value);

	TupleValue::VarContext *cxt_;
};


struct TupleValueTempStoreVarUtils {
	enum StoreVarDataType {
		LTS_VAR_DATA_NONE = 80,
		LTS_VAR_DATA_SINGLE_VAR,
		LTS_VAR_DATA_MULTI_VAR_ON_TEMP_STORE,
		LTS_VAR_DATA_MULTI_VAR_ON_TUPLE_LIST,
		LTS_VAR_DATA_MULTI_VAR_ON_STACK_ALLOCATOR
	};

	struct TempStoreLobData;
	struct TempStoreLobReader;
	struct TempStoreVarArrayReader;
};


struct TupleValue::StoreLobHeader {

	uint64_t getPartDataOffset(uint64_t index) const;
	void setPartDataOffset(uint64_t index, uint64_t partDataOffset);

	void resetPartDataOffset();

	TupleValueTempStoreVarUtils::StoreVarDataType storeVarDataType_;
	uint32_t blockSize_;
	uint64_t reservedCount_;
	uint64_t validCount_;
	uint64_t offsetList_;
};


struct TupleValue::StoreMultiVarBlockIdList {

	LocalTempStore::BlockId getBlockId(uint64_t index) const;
	void setBlockId(uint64_t index, LocalTempStore::BlockId blockId);

	void resetBlockIdList();

	static size_t calcAllocateSize(uint64_t blockCount);

	uint64_t reservedCount_;
	uint64_t blockIdList_;
};


struct TupleValue::StoreMultiVarData {
public:

	LocalTempStore* getLocalTempStore() const;

	uint64_t getLobPartCount() const;

	const TupleValue::StoreLobHeader* getStoreLobHeader() const;
	TupleValue::StoreLobHeader* getStoreLobHeader();

	static TupleValueVarUtils::VarData* create(TupleValueVarUtils::BaseVarContext &baseCxt,
		LocalTempStore &store, uint64_t initialCapacity);

	static TupleValueVarUtils::VarData* createTupleListVarData(
		TupleValueVarUtils::BaseVarContext &cxt, LocalTempStore &store,
		uint32_t blockSize,
		uint64_t blockCount, const LocalTempStore::BlockId* blockIdList,
		uint64_t partCount, const uint64_t* partOffsetList);

	LocalTempStore::BlockId getBlockId(uint64_t index) const;
	void setBlockId(uint64_t index, LocalTempStore::BlockId blockId);

	void appendLobPartOffset(
		TupleValueVarUtils::BaseVarContext &cxt,
		TupleValueVarUtils::VarData *topVarData,
		uint64_t partCount, uint64_t offset);

	static TupleValue::StoreLobHeader* updateLobPartOffset(
		TupleValueVarUtils::BaseVarContext &cxt,
		TupleValueVarUtils::VarData* topVarData,
		TupleValue::StoreLobHeader* lobHeader,
		uint64_t partCount, uint64_t offset);

	void appendBlockId(TupleValueVarUtils::BaseVarContext &cxt,
						   TupleValueVarUtils::VarData *topVarData,
						   uint64_t blockIndex, LocalTempStore::BlockId blockId);

	void releaseStoreBlock(); 



private:
	TupleValueTempStoreVarUtils::StoreVarDataType getDataType() const;

	uint64_t getBlockIdListCount() const;

	uint64_t appendBlock();
	void resetBlockIdList();

	void setBlockIdListCount(uint64_t blockIdListCount);
	void setBlockIdList(TupleValue::StoreMultiVarBlockIdList* blockIdList);
	void reallocBlockIdList(TupleValueVarUtils::BaseVarContext &cxt,
							TupleValueVarUtils::VarData *topVarData);
	size_t getBlockIdListAllocSize(size_t blockCount);

	void setStoreLobHeader(TupleValue::StoreLobHeader* storeLobHeader);


	uint8_t dataType_; 
	LocalTempStore* store_;
	TupleValue::StoreMultiVarBlockIdList* blockIdList_;
	TupleValue::StoreLobHeader* storeLobHeader_;
};


class TupleValue::LobBuilder { 
public:
	LobBuilder(VarContext &cxt, TupleList::TupleColumnType type, size_t initialCapacity);
	~LobBuilder();

	void append(const void *data, size_t size);

	size_t append(size_t maxSize);

	size_t getMaxAllocateSize();

	void* lastData();

	TupleValue build();

	static TupleValue fromDataStore(VarContext &cxt, const BaseObject &fieldObject, const void *data);

private:
	LobBuilder(const LobBuilder&);
	LobBuilder& operator=(const LobBuilder&);

	uint32_t newBlock(LocalTempStore &store);

	TupleValue::VarContext& cxt_;
	TupleList::TupleColumnType type_;
	size_t initialCapacity_;

	TupleValueVarUtils::VarData* topVarData_;
	uint64_t totalSize_;
	uint64_t blockCount_;
	uint64_t partCount_;
	void* lastData_;
	uint32_t lastSize_;
};




struct TupleValueStackAllocVarUtils {
	enum StoreVarDataType {
		LTS_VAR_DATA_NONE = 80,
		LTS_VAR_DATA_SINGLE_VAR,
		LTS_VAR_DATA_MULTI_VAR_ON_STACK_ALLOC
	};

	struct StackAllocLobData;
	struct StackAllocLobReader;
	struct StackAllocVarArrayReader;
};


struct TupleValue::StackAllocLobHeader {

	void* getPartDataAddr(uint64_t index) const;
	void setPartDataAddr(uint64_t index, void* partDataAddr);

	void resetPartDataAddr();

	TupleValueTempStoreVarUtils::StoreVarDataType storeVarDataType_;
	uint64_t reservedCount_;
	uint64_t validCount_;
	void* addrList_;
};


struct TupleValue::StackAllocMultiVarData {
public:

	uint64_t getLobPartCount() const;

	const TupleValue::StackAllocLobHeader* getStackAllocLobHeader() const;
	TupleValue::StackAllocLobHeader* getStackAllocLobHeader();

	static TupleValueVarUtils::VarData* create(TupleValueVarUtils::BaseVarContext &baseCxt,
		uint64_t initialCapacity);

	void appendLobPartAddr(
		TupleValueVarUtils::BaseVarContext &cxt,
		TupleValueVarUtils::VarData *topVarData,
		uint64_t partCount, void* partData);

	static TupleValue::StackAllocLobHeader* updateLobPartAddr(
		TupleValueVarUtils::BaseVarContext &cxt,
		TupleValueVarUtils::VarData* topVarData,
		TupleValue::StackAllocLobHeader* lobHeader,
		uint64_t partCount, void* partData);

private:
	TupleValueTempStoreVarUtils::StoreVarDataType getDataType() const;

	void setStackAllocLobHeader(TupleValue::StackAllocLobHeader* stackAllocLobHeader);


	uint8_t dataType_; 
	TupleValue::StackAllocLobHeader* stackAllocLobHeader_;
};


struct TupleValueStackAllocVarUtils::StackAllocLobReader {
public:
	void initialize(const TupleValue::StackAllocMultiVarData *multiVarData,
					const TupleValue::StackAllocLobHeader *lobHeader);
	void destroy();
	bool next(const void *&data, size_t &size);
	void reset();
	uint64_t getPartCount();

private:
	const TupleValue::StackAllocMultiVarData *multiVarData_;
	const TupleValue::StackAllocLobHeader *lobHeader_;
	uint64_t currentPos_;
};


class TupleValue::StackAllocLobBuilder {
public:
	StackAllocLobBuilder(VarContext &cxt, TupleList::TupleColumnType type, size_t initialCapacity);
	~StackAllocLobBuilder();

	void append(const void *data, size_t size);

	size_t append(size_t maxSize);

	void* lastData();

	TupleValue build();

	static TupleValue fromDataStore(VarContext &cxt, const BaseObject &fieldObject, const void *data);

private:
	StackAllocLobBuilder(const LobBuilder&);
	StackAllocLobBuilder& operator=(const LobBuilder&);

	uint32_t newBlock(LocalTempStore &store);

	TupleValue::VarContext& cxt_;
	TupleList::TupleColumnType type_;
	size_t initialCapacity_;

	TupleValueVarUtils::VarData* topVarData_;
	uint64_t totalSize_;
	uint64_t partCount_;
	void* lastData_;
	uint32_t lastSize_;
};



struct TupleValueTempStoreVarUtils::TempStoreLobReader {
public:
	void initialize(LocalTempStore &store,
					const TupleValue::StoreMultiVarData *multiVarData,
					const TupleValue::StoreLobHeader *lobHeader);
	void destroy();
	bool next(const void *&data, size_t &size);
	void reset();
	LocalTempStore::Block getCurrentPartBlock();
	uint64_t getPartCount();

private:
	const TupleValue::StoreMultiVarData *multiVarData_;
	const TupleValue::StoreLobHeader *lobHeader_;
	LocalTempStore::BlockId currentBlockId_;
	LocalTempStore *store_;
	uint64_t currentPos_;
	uint32_t blockSize_;
};

struct TupleValueTempStoreVarUtils::TempStoreVarArrayReader {
public:
	void initialize();
	void destroy();
	bool next(const void *&data, size_t &size);

private:
};



class TupleValue::LobReader { 
public:
	explicit LobReader(const TupleValue &value);
	~LobReader();

	bool next(const void *&data, size_t &size);

	void reset();

	uint64_t getPartCount();

	LocalTempStore::Block getCurrentPartBlock();

	void dumpContents(std::ostream &ostr);
private:
	union ReaderImpl {
		TupleValueVarUtils::ContainerVarArrayReader containerReader_;
		TupleValueTempStoreVarUtils::TempStoreLobReader tempStoreReader_;
		TupleValueStackAllocVarUtils::StackAllocLobReader stackAllocReader_;
	} readerImpl_;

	TupleValueVarUtils::VarDataType varDataType_;
};


class TupleValue::VarArrayReader { 
public:
	VarArrayReader(VarContext &cxt, const TupleValue &value);

	bool next(const void *&data, size_t &size);
};

class TupleValue::NanoTimestampBuilder {
public:
	explicit NanoTimestampBuilder(VarContext &cxt);

	void append(const void *data, size_t size);
	void setValue(const NanoTimestamp &ts);

	const NanoTimestamp* build();

private:
	VarContext &cxt_;
	uint8_t data_[sizeof(NanoTimestamp)];
	size_t pos_;
};


class TupleNanoTimestamp {
public:
	TupleNanoTimestamp();
	explicit TupleNanoTimestamp(const NanoTimestamp *ts);
	explicit TupleNanoTimestamp(const TupleValue &value);

	operator TupleValue() const;
	operator NanoTimestamp() const;

	const NanoTimestamp& get() const;
	const void* data() const;

private:
	struct Constants {
		static const NanoTimestamp EMPTY_VALUE;
	};

	static NanoTimestamp makeEmptyValue();

	const NanoTimestamp *ts_;
};


class TupleString {
public:
	typedef std::pair<const char8_t*, size_t> BufferInfo;

	TupleString();
	explicit TupleString(const void *data);
	TupleString(const TupleValue &value);

	operator TupleValue() const;

	BufferInfo getBuffer() const;
	const void* data() const;

private:
	static const void* emptyData();

	const void *data_;
};

class TupleList::BlockReader {
public:
	class Image;
	friend class Image;

	BlockReader();
	explicit BlockReader(TupleList &tupleList);

	BlockReader(TupleList &tupleList, Image &image);

	~BlockReader();

	TupleList& getTupleList() const;
	void close();

	void setOnce();

	bool next(Block &tupleBlock);

	bool isExists();

	void isExists(bool &isExists, bool &isAvailable);

	void getBlockCount(uint64_t &blockCount, uint64_t &blockNth);

	void detach(TupleList::BlockReader::Image &image);

private:
	BlockReader(const BlockReader&);
	BlockReader& operator=(const BlockReader&);

	void initialize(TupleList &tupleList);

	TupleList* tupleList_;
	LocalTempStore::BlockId currentBlockNth_;
	bool isOnce_;
	bool isActive_;
	bool isDetached_;
};

class TupleList::BlockReader::Image {
	friend class TupleList;
	friend class TupleList::BlockReader;

public:
	Image(util::StackAllocator &alloc);

private:
	void clear();

	SQLAllocator &alloc_;
	ResourceId resourceId_;   
	uint64_t blockCount_;      

	LocalTempStore::BlockId currentBlockNth_;
	LocalTempStore::BlockId activeTopBlockNth_;
	bool isOnce_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			resourceId_, blockCount_, currentBlockNth_, activeTopBlockNth_,
			UTIL_OBJECT_CODER_OPTIONAL(isOnce_, false));
};


class TupleList::BlockWriter {
public:
	BlockWriter();
	explicit BlockWriter(TupleList &tupleList);
	~BlockWriter();

	TupleList& getTupleList() const;
	void close();

	void append(const Block &tupleBlock);

private:
	BlockWriter(const BlockWriter&);
	BlockWriter& operator=(const BlockWriter&);

	TupleList* tupleList_;
	bool isActive_;
};

class TupleValue;
class TupleString;
class TupleList::Writer {
	friend class WritableTuple;
public:
	class Image;
	friend class Image;

	static const uint32_t CONTIGUOUS_BLOCK_THRESHOLD = 1;

	Writer();

	explicit Writer(TupleList &tupleList);

	Writer(TupleList &tupleList, const Image &image);

	~Writer();

	TupleList& getTupleList() const;
	void close();

	void setBlockHandler(WriterHandler &handler);

	inline void next();
	void nextDetail();

	WritableTuple get(); 

	void setVarContext(TupleValue::VarContext &context);
	TupleValue::VarContext& getVarContext();

	void detach(Image& image);

private:
	typedef std::vector< TupleList::Column, util::StdAllocator<
	  TupleList::Column, LocalTempStore::LTSVariableSizeAllocator> > ColumnList;

	Writer(const Writer&);
	Writer& operator=(const Writer&);

	void initialize(TupleList &tupleList);

	void allocateNewBlock();

	uint32_t getFixedPartSize() const {
		return fixedPartSize_;
	}

	void setupFirstAppend();
	void nextBlock();
	void setSingleVarData(const Column &column, const TupleValue &value);
	void setSingleVarData(const Column &column, const TupleString &value);
	void setLobVarData(const Column &column, const TupleValue &value);
	void copyVarData(Reader &reader);

	void setVarDataOffset(void *fixedAddr, Column column, TupleColumnType type, uint64_t varOffset);

	uint64_t appendPartDataArea(size_t requestSize,
		bool splittable, uint8_t* &reservedAddr, size_t &reservedSize);

	void appendLobPart(const void* partData, size_t partSize, size_t &partCount);

	void reserveLobPartInfo(size_t initialSize);

	void updateLobPartDataOffset(size_t index, size_t offset);

	void getLobPartInfo(void *&lobInfoAddr, size_t &size);

	uint8_t* getFixedAddr(uint64_t pos);

	TupleList* tupleList_;
	uint8_t* fixedAddr_;  
	uint8_t* varTopAddr_;  
	uint8_t* varTailAddr_; 
	TupleValue::VarContext* cxt_;
	TupleValue::StoreLobHeader *storeLobHeader_; 
	ColumnList columnList_;
	LocalTempStore::Block block_;    
	LocalTempStore::Block varBlock_; 
	LocalTempStore::Block tempBlock_; 
	LocalTempStore::BlockId currentBlockNth_;
	size_t nullBitOffset_;
	uint32_t tupleCount_; 
	uint32_t contiguousBlockCount_; 
	const uint32_t fixedPartSize_;
	const uint32_t blockSize_;
	const uint32_t maxAvailableSize_;
	bool isNullable_;
	bool isActive_;
	bool isDetached_;
};

class TupleList::Writer::Image {
	friend class TupleList;
	friend class TupleList::Writer;

public:
	Image(util::StackAllocator &alloc);

private:
	void clear();

	SQLAllocator &alloc_;
	ResourceId resourceId_;   
	uint64_t blockCount_;      
	LocalTempStore::BlockId currentBlockNth_;
	uint32_t tupleCount_; 
	uint32_t contiguousBlockCount_; 
	bool isNullable_;

	UTIL_OBJECT_CODER_ALLOC_CONSTRUCTOR;
	UTIL_OBJECT_CODER_MEMBERS(
			resourceId_, blockCount_, currentBlockNth_,
			tupleCount_, contiguousBlockCount_,
			UTIL_OBJECT_CODER_OPTIONAL(isNullable_, false));
};

class TupleList::WriterHandler {
public:
	WriterHandler();
	virtual ~WriterHandler();

	virtual void operator()() = 0;
};


class TupleList::TupleColumnTypeCoder {
public:
	const char8_t* operator()(TupleColumnType type) const;
	bool operator()(const char8_t *name, TupleColumnType &type) const;
};


class TupleList::ReadableTuple {
public:
	explicit ReadableTuple(const util::FalseType&);
	explicit ReadableTuple(Reader &reader);
	TupleValue get(const Column &column) const;

	template<typename T> T getAs(const Column &column) const;
	bool checkIsNull(uint16_t colNumber) const;

	Reader *reader_;
#if defined(NDEBUG)
	const uint8_t *addr_;
	uint64_t blockNth_;
#else
	uint64_t pos_;
#endif
};


class TupleList::WritableTuple {
public:
	explicit WritableTuple(const util::FalseType&);
	explicit WritableTuple(Writer &writer);

	void set(const Column &column, const TupleValue &value);

	void setBy(const Column &column, const TupleValue &value);
	void setBy(const Column &column, const TupleString &value);
	void setBy(const Column &column, const int32_t &value);
	void setBy(const Column &column, const int64_t &value);
	void setBy(const Column &column, const float &value);
	void setBy(const Column &column, const double &value);
	void setBy(const Column &column, const MicroTimestamp &value);
	void setBy(const Column &column, const NanoTimestamp &value);

	void setIsNull(uint16_t colNumber, bool nullValue = true);

	bool checkIsNull(uint16_t colNumber);
private:
	Writer *writer_;
#ifdef NDEBUG
	uint8_t *addr_;
#else
	uint64_t pos_;
#endif
};

class TupleList::Reader {
	friend class TupleList::Writer;
	friend class ReadableTuple;
public:
	class Image;
	friend class Image;

	enum AccessOrder {
		ORDER_SEQUENTIAL,
		ORDER_RANDOM
	};

	static const BlockId UNDEF_BLOCKID;

	Reader();
	Reader(TupleList &tupleList, AccessOrder order);
	Reader(TupleList &tupleList, const TupleList::Reader::Image &image);
	~Reader();

	TupleList& getTupleList() const;
	void close();

	void setVarContext(TupleValue::VarContext &context);
	TupleValue::VarContext& getVarContext();

	bool exists();

	void next();

	bool existsDetail();
	bool nextDetail();

	ReadableTuple get();

	void assign(const Reader &reader);

	void discard();

	void detach(TupleList::Reader::Image &image);

	uint64_t getFollowingBlockCount() const;
	uint64_t getReferencingBlockCount() const;

	template<typename T> T getAs(const Column &column);

	bool checkIsNull(uint16_t colNumber);
	void countLatchedBlock(
			uint64_t &latchedBlockCount, uint64_t &keepTop, uint64_t &keepMax) const;

	int32_t getCurrentGOBSize();

	uint64_t calcBlockNth(uint64_t pos) const;
	uint32_t calcBlockOffset(uint64_t pos) const;

	AccessOrder getAccessOrder() {
		return accessOrder_;
	}
	bool isStarted() {
		return (currentBlockNth_ != 0);
	}
private:
	const uint8_t* getBlockTopAddr(uint64_t pos) const;

	const uint8_t* getBlockTopAddrDirect(uint64_t blockNth) const;

	bool latchHeadBlock();

	void positionWithLatch(LocalTempStore::BlockId newBlockNth);

	void updateKeepInfo();

	struct LatchedBlockInfo {
		LatchedBlockInfo() : addr_(NULL) {}

		LocalTempStore::Block block_;   
		void* addr_;    
	};

	typedef std::deque< LatchedBlockInfo, util::StdAllocator<
	  LatchedBlockInfo, LocalTempStore::LTSVariableSizeAllocator> > LatchedBlockInfoList;

	Reader(const Reader&);
	Reader& operator=(const Reader&);

	void initialize(TupleList &tupleList);

	void nextBlock(int32_t contiguousBlockCount);

	bool existsDirect();

	const uint8_t* getFixedTopAddr(); 

	const uint8_t* getCurrentBlockAddr() const;

	const uint8_t* createSingleVarValue(
			uint64_t baseBlockNth, uint64_t varOffset);

	TupleValue createMultiVarValue(
			uint64_t baseBlockNth, uint64_t varOffset,
			TupleList::TupleColumnType type);

	uint32_t getFixedPartSize() const {
		return fixedPartSize_;
	}

	TupleList* tupleList_;
	const uint8_t* fixedAddr_;      
	const uint8_t* fixedEndAddr_;   
	LatchedBlockInfoList blockInfoArray_; 
	TupleValue::VarContext* cxt_;
	AccessOrder accessOrder_;
	uint64_t currentBlockNth_;
	uint64_t keepTopBlockNth_;
	uint64_t keepMaxBlockNth_;
	uint64_t headBlockNth_;		
	size_t readerId_;			
	const uint32_t fixedPartSize_;
	const uint32_t blockSize_;
	const uint32_t blockExpSize_;
	const Position positionOffsetMask_;
	const size_t nullBitOffset_;
	const bool isNullable_;
	bool isActive_;
	bool isDetached_;
	bool positionCalled_;
};


class TupleList::Reader::Image {
	friend class TupleList;
	friend class TupleList::Reader;

public:
	Image(util::StackAllocator &alloc);

private:
	void clear();

	ResourceId resourceId_;		
	uint64_t blockCount_;		

	uint64_t readerId_;
	AccessOrder accessOrder_;
	uint64_t currentBlockNth_;
	uint64_t keepTopBlockNth_;
	uint64_t keepMaxBlockNth_;
	uint64_t headBlockNth_;		
	LocalTempStore::BlockId activeTopBlockNth_;

	uint32_t fixedOffset_;
	bool positionCalled_;

	UTIL_OBJECT_CODER_MEMBERS(
			resourceId_, blockCount_,
			readerId_, accessOrder_, currentBlockNth_,
			keepTopBlockNth_, keepMaxBlockNth_,
			headBlockNth_,
			activeTopBlockNth_,
			fixedOffset_,
			UTIL_OBJECT_CODER_OPTIONAL(positionCalled_, false)
		);
};



struct TupleColumnTypeUtils {
	static bool isAny(TupleList::TupleColumnType type);
	static bool isNullable(TupleList::TupleColumnType type);

	static bool isNull(TupleList::TupleColumnType type);
	static bool isSomeFixed(TupleList::TupleColumnType type);
	static bool isNormalFixed(TupleList::TupleColumnType type);
	static bool isLargeFixed(TupleList::TupleColumnType type);
	static bool isSingleVar(TupleList::TupleColumnType type);
	static bool isSingleVarOrLarge(TupleList::TupleColumnType type);
	static bool isArray(TupleList::TupleColumnType type);
	static bool isFixedArray(TupleList::TupleColumnType type);
	static bool isLob(TupleList::TupleColumnType type);

	static bool isNumeric(TupleList::TupleColumnType type);
	static bool isIntegral(TupleList::TupleColumnType type);
	static bool isFloating(TupleList::TupleColumnType type);
	static bool isTimestampFamily(TupleList::TupleColumnType type);

	static bool isAbstract(TupleList::TupleColumnType type);
	static bool isDeclarable(TupleList::TupleColumnType type);
	static bool isSupported(TupleList::TupleColumnType type);

	static size_t getFixedSize(TupleList::TupleColumnType type);
};


struct TupleValueUtils {
	static const uint32_t INT32_MAX_ENCODE_SIZE = 4;
	static const uint32_t INT64_MAX_ENCODE_SIZE = 8;

	static size_t encodeInt32(uint32_t input, uint8_t *output);

	static size_t decodeInt32(const void *input, uint32_t &output);

	static const uint32_t OBJECT_MAX_1BYTE_LENGTH_VALUE = 127;  

	static const uint32_t VAR_SIZE_1BYTE_THRESHOLD = 128;
	static const uint32_t VAR_SIZE_4BYTE_THRESHOLD = UINT32_C(1) << 30;
	static const uint64_t VAR_SIZE_8BYTE_THRESHOLD = UINT64_C(1) << 62;

	static bool varSizeIs1Byte(const void* ptr);
	static bool varSizeIs4Byte(const void* ptr);
	static bool varSizeIs8Byte(const void* ptr);

	static bool varSizeIs1Byte(uint8_t val);
	static bool varSizeIs4Byte(uint8_t val);
	static bool varSizeIs8Byte(uint8_t val);

	static uint32_t decode1ByteVarSize(uint8_t val);
	static uint32_t decode4ByteVarSize(uint32_t val);
	static uint64_t decode8ByteVarSize(uint64_t val);

	static uint32_t get1ByteVarSize(const void *ptr);
	static uint32_t get4ByteVarSize(const void *ptr);
	static uint64_t getOIdVarSize(const void *ptr);

	static uint32_t decodeVarSize(const void *ptr);
	static uint64_t decodeVarSizeOrOId(const void *ptr, bool &isOId);

	static uint32_t getEncodedVarSize(const void *ptr);
	static uint32_t getEncodedVarSize(uint32_t val);

	static uint32_t encodeVarSize(uint32_t val);
	static uint64_t encodeVarSizeOId(uint64_t val);

	static uint32_t calcNullsByteSize(uint32_t columnNum);


	static void memWrite(void* &destAddr, const void* srcAddr, size_t size);

	static void setPartData(void* destAddr, const void* srcData, uint32_t size);
	static void getPartData(const void* rawAddr, const void* &data, uint32_t &size);
};


struct TupleBlockOperation {
	static void getFixedData();
	static void getLobPartInfo();
	static void reserveArea(size_t maxSize);
	static void setData(const void* addr, size_t size);
	static void appendLobPartInfo();
	static void updateLobPartInfo();
};

inline TupleList::ReadableTuple::ReadableTuple(const util::FalseType&) {
	reader_ = NULL;
#if defined(NDEBUG)
	addr_ = NULL;
	blockNth_ = 0;
#else
	pos_ = 0;
#endif
}

inline TupleList::ReadableTuple::ReadableTuple(Reader &reader) {
	assert(reader.existsDirect());
	if (TupleList::Reader::ORDER_RANDOM == reader.accessOrder_) {
		reader.updateKeepInfo();
	}
	reader_ = &reader;
#if defined(NDEBUG)
	addr_ = reader_->fixedAddr_;
	blockNth_ = reader_->currentBlockNth_;
#else
	size_t blockOffset = static_cast<size_t>(reader_->fixedAddr_ - reader_->getCurrentBlockAddr());
	assert(blockOffset < reader_->blockSize_);

	pos_ = (reader_->currentBlockNth_ << reader_->blockExpSize_) | blockOffset;
	assert((pos_ >> reader_->blockExpSize_) == reader_->currentBlockNth_);
	assert((reader_->positionOffsetMask_ & pos_) == blockOffset);
#endif
}

inline
TupleValue TupleList::ReadableTuple::get(const Column &column) const{
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	TupleColumnType type = column.type_;
	if (TupleColumnTypeUtils::isNullable(type)) {
		if (checkIsNull(column.pos_)) {
			return TupleValue();
		}
		type &= static_cast<TupleColumnType>(~TYPE_MASK_NULLABLE);
	}
	if (TupleColumnTypeUtils::isSomeFixed(type)) {
		return TupleValue(addr, type);
	}
	else if (TupleColumnTypeUtils::isAny(type)) {
		memcpy(&type, addr, sizeof(type));
		addr += sizeof(type);
		if (TupleColumnTypeUtils::isSomeFixed(type)) {
			return TupleValue(addr, type);
		}
	}
	assert(!TupleColumnTypeUtils::isAny(type));
#if defined(NDEBUG)
	const uint64_t baseBlockNth = blockNth_;
	const uint8_t* blockTopAddr = reader_->getBlockTopAddrDirect(blockNth_);
#else
	const uint64_t baseBlockNth = reader_->calcBlockNth(pos_);
#endif
	uint64_t varOffset;
	memcpy(&varOffset, addr, sizeof(uint64_t));
	if (TupleColumnTypeUtils::isSingleVar(type)) {
		if (varOffset < reader_->blockSize_) {
			const uint8_t* varTop = blockTopAddr + varOffset;
			return TupleValue(varTop, type);
		}
		else {
			const uint8_t *varTop = reader_->createSingleVarValue(baseBlockNth, varOffset);
			return TupleValue(varTop, type);
		}
	}
	else {
		assert(reader_->cxt_);
		return reader_->createMultiVarValue(baseBlockNth, varOffset, type);
	}
}

template<>
inline TupleValue TupleList::ReadableTuple::getAs(const Column &column) const {
	return get(column);
}

template<>
inline TupleString TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_STRING ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* blockTopAddr = reader_->getBlockTopAddrDirect(blockNth_);
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	assert(!TupleColumnTypeUtils::isSomeFixed(column.type_));
	assert(!TupleColumnTypeUtils::isAny(column.type_));
	assert(TupleColumnTypeUtils::isSingleVar(column.type_));
	uint64_t varOffset;
	memcpy(&varOffset, addr, sizeof(uint64_t));
	if (varOffset < reader_->blockSize_) {
		const uint8_t* varTop = blockTopAddr + varOffset;
		return TupleString(varTop);
	}
	else {
#if defined(NDEBUG)
		const uint64_t baseBlockNth = blockNth_;
#else
		const uint64_t baseBlockNth = reader_->calcBlockNth(pos_);
#endif
		const uint8_t *newVarTop = reader_->createSingleVarValue(baseBlockNth, varOffset);
		return TupleString(newVarTop);
	}
}

template<>
inline int32_t TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_INTEGER ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	return *reinterpret_cast<const int32_t*>(addr);
}

template<>
inline int64_t TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_LONG ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE) ||
			TupleList::TYPE_TIMESTAMP ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	return *reinterpret_cast<const int64_t*>(addr);
}

template<>
inline float TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_FLOAT ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	return *reinterpret_cast<const float*>(addr);
}

template<>
inline double TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_DOUBLE ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	return *reinterpret_cast<const double*>(addr);
}

template<>
inline MicroTimestamp TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_MICRO_TIMESTAMP ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	return *reinterpret_cast<const MicroTimestamp*>(addr);
}

template<>
inline TupleNanoTimestamp TupleList::ReadableTuple::getAs(const Column &column) const {
#ifndef NDEBUG
	if (!reader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"Reader is not exist(or already closed).");
	}
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_NANO_TIMESTAMP ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
#endif
#if defined(NDEBUG)
	const uint8_t* addr = addr_ + column.offset_;
#else
	const uint8_t* blockTopAddr = reader_->getBlockTopAddr(pos_);
	const uint8_t* addr = blockTopAddr + reader_->calcBlockOffset(pos_) + column.offset_;
#endif
	return TupleNanoTimestamp(reinterpret_cast<const NanoTimestamp*>(addr));
}

inline bool TupleList::ReadableTuple::checkIsNull(uint16_t colNumber) const {
	assert(reader_->tupleList_);
#if defined(NDEBUG)
	const uint8_t* fixedAddr = addr_;
#else
	const uint8_t* fixedAddr = reader_->getBlockTopAddr(pos_)
			+ reader_->calcBlockOffset(pos_);
#endif
	if (reader_->isNullable_) {
		const uint8_t* nullbit = fixedAddr + reader_->nullBitOffset_;
		uint16_t byteCount = colNumber / 8;
		uint8_t bitMask = static_cast<uint8_t>(1U << (colNumber % 8));
		return (*(nullbit + byteCount) & bitMask) != 0;
	}
	else {
		return false;
	}
}


inline bool TupleList::Reader::existsDirect() {
	InterruptionChecker maskChecker;
	return exists();
}

inline const uint8_t* TupleList::Reader::getFixedTopAddr() {
	assert(tupleList_);
	assert(existsDirect());
	return fixedAddr_;
}

inline uint64_t TupleList::Reader::calcBlockNth(uint64_t pos) const {
	return (pos >> blockExpSize_);
}

inline uint32_t TupleList::Reader::calcBlockOffset(uint64_t pos) const {
	uint32_t posOffset = static_cast<uint32_t>(
			pos & positionOffsetMask_);
	assert(posOffset < blockSize_);
	assert(TupleList::BLOCK_HEADER_SIZE <= posOffset);
	return posOffset;
}

inline const uint8_t* TupleList::Reader::getCurrentBlockAddr() const {
	if (blockInfoArray_.size() > 0) {
		assert(keepTopBlockNth_ <= currentBlockNth_);
		uint64_t arrayOffset = currentBlockNth_ - keepTopBlockNth_;
		assert(arrayOffset < blockInfoArray_.size());
		assert(blockInfoArray_[arrayOffset].addr_);
		return static_cast<const uint8_t*>(blockInfoArray_[arrayOffset].addr_);
	}
	else {
		return NULL;
	}
}

inline const uint8_t* TupleList::Reader::getBlockTopAddrDirect(uint64_t blockNth) const {
	const uint8_t* blockTopAddr = NULL;
	assert((keepTopBlockNth_ <= blockNth) && (blockNth <= keepMaxBlockNth_));
	uint64_t arrayOffset = blockNth - keepTopBlockNth_;
	assert(arrayOffset < blockInfoArray_.size());
	blockTopAddr = static_cast<const uint8_t*>(blockInfoArray_[arrayOffset].addr_);

	assert(blockTopAddr);
	return blockTopAddr;
}

inline TupleList::ReadableTuple TupleList::Reader::get() {
	assert(existsDirect());
	return ReadableTuple(*this);
}

inline bool TupleList::Reader::checkIsNull(uint16_t colNumber) {
	assert(tupleList_);
	assert(existsDirect());
	assert(fixedAddr_);
	if (isNullable_) {
		const uint8_t* nullbit = fixedAddr_ + nullBitOffset_;
		uint16_t byteCount = colNumber / 8;
		uint8_t bitMask = static_cast<uint8_t>(1U << (colNumber % 8));
		return (*(nullbit + byteCount) & bitMask) != 0;
	}
	else {
		return false;
	}
}

template<> inline TupleValue TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(fixedAddr_);
	const uint8_t* addr = fixedAddr_ + column.offset_;
	TupleColumnType type = column.type_;
	if (TupleColumnTypeUtils::isNullable(type)) {
		assert(isNullable_);
		if (checkIsNull(column.pos_)) {
			return TupleValue();
		}
		type &= static_cast<TupleColumnType>(~TYPE_MASK_NULLABLE);
	}
	if (TupleColumnTypeUtils::isSomeFixed(type)) {
		return TupleValue(addr, type);
	}
	else if (TupleColumnTypeUtils::isAny(type)) {
		memcpy(&type, addr, sizeof(type));
		addr += sizeof(type);
		if (TupleColumnTypeUtils::isSomeFixed(type)) {
			return TupleValue(addr, type);
		}
	}
	assert(!TupleColumnTypeUtils::isAny(type));
	uint64_t varOffset;
	memcpy(&varOffset, addr, sizeof(uint64_t));
	if (TupleColumnTypeUtils::isSingleVar(type)) {
		const uint8_t* varTop = getCurrentBlockAddr() + varOffset;
		if (varOffset < blockSize_) {
			return TupleValue(varTop, type);
		}
		else {
			const uint8_t *varTop = createSingleVarValue(currentBlockNth_, varOffset);
			return TupleValue(varTop, type);
		}
	}
	else {
		assert(cxt_);
		return createMultiVarValue(currentBlockNth_, varOffset, type);
	}
}

template<> inline TupleString TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	assert(!TupleColumnTypeUtils::isSomeFixed(column.type_));
	assert(!TupleColumnTypeUtils::isAny(column.type_));
	assert(TupleColumnTypeUtils::isSingleVar(column.type_));
	uint64_t varOffset;
	memcpy(&varOffset, addr, sizeof(uint64_t));
	if (varOffset < blockSize_) {
		const uint8_t* varTop = getCurrentBlockAddr() + varOffset;
		return TupleString(varTop);
	}
	else {
		const uint8_t *newVarTop = createSingleVarValue(currentBlockNth_, varOffset);
		return TupleString(newVarTop);
	}
}

template<> inline int32_t TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_INTEGER ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	return *reinterpret_cast<const int32_t*>(addr);
}

template<> inline int64_t TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	assert((TupleList::TYPE_LONG ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE)) ||
			(TupleList::TYPE_TIMESTAMP ==
					(column.type_ & ~TupleList::TYPE_MASK_NULLABLE)));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	return *reinterpret_cast<const int64_t*>(addr);
}

template<> inline float TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_FLOAT ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	return *reinterpret_cast<const float*>(addr);
}

template<> inline double TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	assert(TupleList::TYPE_DOUBLE ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	return *reinterpret_cast<const double*>(addr);
}

template<> inline MicroTimestamp TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	assert((TupleList::TYPE_MICRO_TIMESTAMP ==
					(column.type_ & ~TupleList::TYPE_MASK_NULLABLE)));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	return *reinterpret_cast<const MicroTimestamp*>(addr);
}

template<> inline TupleNanoTimestamp TupleList::Reader::getAs(const Column &column) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (!existsDirect()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_READER_NOT_SUPPORTED,
			"Tuple is not exist.");
	}
#endif
	assert(!checkIsNull(column.pos_));
	const uint8_t* addr = fixedAddr_ + column.offset_;
	return TupleNanoTimestamp(reinterpret_cast<const NanoTimestamp*>(addr));
}

inline bool TupleList::Reader::exists() {
	if (fixedAddr_ >= fixedEndAddr_) {
		return existsDetail();
	}
	return true;
}

inline void TupleList::Reader::next() {
	if ((fixedAddr_ + fixedPartSize_) >= fixedEndAddr_) {
		if (!nextDetail()) {
			return;
		}
	}
	fixedAddr_ += fixedPartSize_;
}





inline void TupleList::Writer::setVarDataOffset(void *fixedAddr, Column column, TupleColumnType type, uint64_t varOffset) {
	void* addr = static_cast<uint8_t*>(fixedAddr) + column.offset_;
	if (TupleColumnTypeUtils::isAny(column.type_)) {
		TupleValueUtils::memWrite(addr, &type, sizeof(type));
	}
	memcpy(addr, &varOffset, sizeof(uint64_t));
}


inline void TupleList::Writer::next() {
	assert(!isDetached_);

	if ((fixedAddr_ + fixedPartSize_ * 2 >= varTopAddr_)
		|| (contiguousBlockCount_ > CONTIGUOUS_BLOCK_THRESHOLD)
		){
		nextDetail();
	}
	else {
		fixedAddr_ += fixedPartSize_;
	}
	if (isNullable_) {
		size_t nullBitSize = (columnList_.size() + 7) / 8;
		memset(fixedAddr_ + nullBitOffset_, 0, nullBitSize);
	}
	++tupleCount_;

	assert((fixedAddr_ + fixedPartSize_) < varTopAddr_);
	assert(!block_.isSwapped());
}


inline TupleList::WritableTuple TupleList::Writer::get() {
	return WritableTuple(*this);
}

inline TupleList::WritableTuple::WritableTuple(const util::FalseType&) {
	writer_ = NULL;
#ifdef NDEBUG
	addr_ = NULL;
#else
	pos_ = 0;
#endif
}

inline TupleList::WritableTuple::WritableTuple(Writer &writer) {
	writer_ = &writer;
#ifdef NDEBUG
	addr_ = writer_->fixedAddr_;
#else
	uint64_t blockOffset = static_cast<uint64_t>(writer_->fixedAddr_
			- static_cast<uint8_t*>(writer_->block_.data()));
	assert(blockOffset < writer_->blockSize_);

	const uint64_t blockExpSize = writer_->tupleList_->getBlockExpSize();
	const uint64_t positionOffsetMask = (1UL << blockExpSize) - 1;
	pos_ = (writer_->currentBlockNth_ << blockExpSize) | blockOffset;

	assert((pos_ >> blockExpSize) == writer_->currentBlockNth_);
	assert((positionOffsetMask & pos_) == blockOffset);
#endif
}

inline void TupleList::WritableTuple::set(const Column &column, const TupleValue &value) {
#ifndef NDEBUG
	if (!TupleColumnTypeUtils::isAny(column.type_)) {
		if ((value.getType() != TYPE_NULL &&
				value.getType() != (column.type_ & ~TYPE_MASK_NULLABLE)) ||
				(value.getType() == TYPE_NULL &&
						!TupleColumnTypeUtils::isNullable(column.type_))) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
					"Column type does not match (columnType=" << column.type_
					<< ", valueType=" << value.getType() << ")");
		}
	}
#endif
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, TupleColumnTypeUtils::isNull(value.getType()));
	}

	if (TupleColumnTypeUtils::isSomeFixed(value.getType())) {
		void* addr = fixedAddr + column.offset_;
		if (TupleColumnTypeUtils::isAny(column.type_)) {
			const TupleColumnType &type = value.getType();
			TupleValueUtils::memWrite(addr, &type, sizeof(type));
		}
		const void *srcAddr = value.fixedData();
		if (TupleColumnTypeUtils::isLargeFixed(value.getType())) {
			srcAddr = value.getRawData();
		}
		memcpy(
				addr, srcAddr,
				TupleColumnTypeUtils::getFixedSize(value.getType()));
	}
	else {
		if (TupleList::TYPE_STRING == value.getType()) {
			writer_->setSingleVarData(column, value);
		}
		else {
			writer_->setLobVarData(column, value);
		}
	}
	assert(!writer_->block_.isSwapped());
}

inline void TupleList::WritableTuple::setBy(const Column &column, const TupleValue &value) {
	set(column, value);
}

inline void TupleList::WritableTuple::setBy(const Column &column, const TupleString &value) {
	assert(TupleList::TYPE_STRING ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
	writer_->setSingleVarData(column, value);
	assert(!writer_->block_.isSwapped());
}

inline void TupleList::WritableTuple::setBy(const Column &column, const int32_t &value) {
	assert(TupleList::TYPE_INTEGER ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	{
		void* addr = fixedAddr + column.offset_;
		memcpy(addr, &value, sizeof(value));
	}
	assert(!writer_->block_.isSwapped());
}

inline void TupleList::WritableTuple::setBy(const Column &column, const int64_t &value) {
	assert(TupleList::TYPE_LONG ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE) ||
			TupleList::TYPE_TIMESTAMP ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
#if defined(NDEBUG)
	*reinterpret_cast<int64_t*>(addr_ + column.offset_) = value;
#else
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	{
		void* addr = fixedAddr + column.offset_;
		memcpy(addr, &value, sizeof(value));
	}
	assert(!writer_->block_.isSwapped());
#endif
}

inline void TupleList::WritableTuple::setBy(const Column &column, const float &value) {
	assert(TupleList::TYPE_FLOAT ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	{
		void* addr = fixedAddr + column.offset_;
		memcpy(addr, &value, sizeof(value));
	}
	assert(!writer_->block_.isSwapped());
}

inline void TupleList::WritableTuple::setBy(const Column &column, const double &value) {
	assert(TupleList::TYPE_DOUBLE ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	{
		void* addr = fixedAddr + column.offset_;
		memcpy(addr, &value, sizeof(value));
	}
	assert(!writer_->block_.isSwapped());
}

inline void TupleList::WritableTuple::setBy(
		const Column &column, const MicroTimestamp &value) {
	assert(TupleList::TYPE_MICRO_TIMESTAMP ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
#if defined(NDEBUG)
	*reinterpret_cast<MicroTimestamp*>(addr_ + column.offset_) = value;
#else
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	{
		void* addr = fixedAddr + column.offset_;
		memcpy(addr, &value, sizeof(value));
	}
	assert(!writer_->block_.isSwapped());
#endif
}

inline void TupleList::WritableTuple::setBy(const Column &column, const NanoTimestamp &value) {
	assert(TupleList::TYPE_NANO_TIMESTAMP ==
			(column.type_ & ~TupleList::TYPE_MASK_NULLABLE));
	if (TupleColumnTypeUtils::isNullable(column.type_)) {
		assert(writer_->isNullable_);
		setIsNull(column.pos_, false);
	}
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	{
		void* addr = fixedAddr + column.offset_;
		memcpy(addr, &value, sizeof(value));
	}
	assert(!writer_->block_.isSwapped());
}


inline void TupleList::WritableTuple::setIsNull(uint16_t colNumber, bool nullValue) {
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	assert(writer_->isNullable_);
	uint8_t* nullbit = fixedAddr + writer_->nullBitOffset_;
	uint16_t byteCount = colNumber / 8;
	uint8_t bitMask = static_cast<uint8_t>(1U << (colNumber % 8));
	if (nullValue) {
		*(nullbit + byteCount) |= bitMask;
	}
	else {
		*(nullbit + byteCount) &= static_cast<uint8_t>(~bitMask);
	}
}

inline bool TupleList::WritableTuple::checkIsNull(uint16_t colNumber) {
#ifdef NDEBUG
	uint8_t* fixedAddr = addr_;
#else
	uint8_t* fixedAddr = writer_->getFixedAddr(pos_);
#endif
	assert(fixedAddr);
	if (writer_->isNullable_) {
		uint8_t* nullbit = fixedAddr + writer_->nullBitOffset_;
		uint16_t byteCount = colNumber / 8;
		uint8_t bitMask = static_cast<uint8_t>(1U << (colNumber % 8));
		return (*(nullbit + byteCount) & bitMask) != 0;
	}
	else {
		return false;
	}
}


inline bool TupleColumnTypeUtils::isAny(TupleList::TupleColumnType type) {
	return TupleList::TYPE_ANY == type;
}

inline bool TupleColumnTypeUtils::isNullable(TupleList::TupleColumnType type) {
	return 0 != (TupleList::TYPE_MASK_NULLABLE & type);
}

inline bool TupleColumnTypeUtils::isNull(TupleList::TupleColumnType type) {
	return TupleList::TYPE_NULL == type;
}

inline bool TupleColumnTypeUtils::isSomeFixed(TupleList::TupleColumnType type) {
	return 0 == (TupleList::TYPE_MASK_VAR & type);
}

inline bool TupleColumnTypeUtils::isNormalFixed(TupleList::TupleColumnType type) {
	if (!isSomeFixed(type)) {
		return false;
	}
	const size_t rawFixedSize = type & TupleList::TYPE_MASK_FIXED_SIZE;
	const size_t normalFixedSize = 1 << TupleList::TYPE_BITS_NORMAL_SIZE;
	return rawFixedSize <= normalFixedSize;
}

inline bool TupleColumnTypeUtils::isLargeFixed(TupleList::TupleColumnType type) {
	if (!isSomeFixed(type)) {
		return false;
	}
	const size_t rawFixedSize = type & TupleList::TYPE_MASK_FIXED_SIZE;
	const size_t normalFixedSize = 1 << TupleList::TYPE_BITS_NORMAL_SIZE;
	return rawFixedSize > normalFixedSize;
}

inline bool TupleColumnTypeUtils::isSingleVar(TupleList::TupleColumnType type) {
	return (0 != (TupleList::TYPE_MASK_VAR & type))
		&& (0 == (TupleList::TYPE_MASK_ARRAY & type))
		&& (type != TupleList::TYPE_BLOB);
}

inline bool TupleColumnTypeUtils::isSingleVarOrLarge(
		TupleList::TupleColumnType type) {
	return isSingleVar(type) || isLargeFixed(type);
}

inline bool TupleColumnTypeUtils::isArray(TupleList::TupleColumnType type) {
	return (0 != (TupleList::TYPE_MASK_ARRAY & type));
}

inline bool TupleColumnTypeUtils::isFixedArray(TupleList::TupleColumnType type) {
	return (0 != (TupleList::TYPE_MASK_ARRAY & type))
		&& (0 == (TupleList::TYPE_MASK_VAR & type));
}

inline bool TupleColumnTypeUtils::isLob(TupleList::TupleColumnType type) {
	return type == TupleList::TYPE_BLOB;
}

inline bool TupleColumnTypeUtils::isNumeric(TupleList::TupleColumnType type) {
	return (0 == (TupleList::TYPE_MASK_VAR & type))
		&& ((1 == ((type & TupleList::TYPE_MASK_SUB) >> TupleList::TYPE_BITS_SUB_TYPE))
			|| (2 == ((type & TupleList::TYPE_MASK_SUB) >> TupleList::TYPE_BITS_SUB_TYPE))
			|| (5 == ((type & TupleList::TYPE_MASK_SUB) >> TupleList::TYPE_BITS_SUB_TYPE)));
}

inline bool TupleColumnTypeUtils::isIntegral(TupleList::TupleColumnType type) {
	return (0 == (TupleList::TYPE_MASK_VAR & type))
		&& (1 == ((type & TupleList::TYPE_MASK_SUB) >> TupleList::TYPE_BITS_SUB_TYPE));
}

inline bool TupleColumnTypeUtils::isFloating(TupleList::TupleColumnType type) {
	return (0 == (TupleList::TYPE_MASK_VAR & type))
		&& (2 == ((type & TupleList::TYPE_MASK_SUB) >> TupleList::TYPE_BITS_SUB_TYPE));
}

inline bool TupleColumnTypeUtils::isTimestampFamily(
		TupleList::TupleColumnType type) {
	const TupleList::TupleColumnType extMask =
			(~(TupleList::TYPE_MASK_TIMESTAMP_BASE ^
					TupleList::TYPE_MASK_TIMESTAMP_EXT)) &
			(~(TupleList::TYPE_MASK_FIXED_SIZE |
			TupleList::TYPE_MASK_NULLABLE));
	return (0 == ((TupleList::TYPE_TIMESTAMP ^ type) & extMask));
}

inline bool TupleColumnTypeUtils::isAbstract(TupleList::TupleColumnType type) {
	return TupleList::TYPE_ANY == type || TupleList::TYPE_NUMERIC == type
			|| isNullable(type);
}

inline bool TupleColumnTypeUtils::isDeclarable(TupleList::TupleColumnType type) {
	return !isAbstract(type) && (TupleList::TYPE_NULL != type);
}

inline bool TupleColumnTypeUtils::isSupported(TupleList::TupleColumnType type) {
	return TupleList::TYPE_GEOMETRY != type;
}

inline size_t TupleColumnTypeUtils::getFixedSize(TupleList::TupleColumnType type) {
	size_t fixedSize = type & TupleList::TYPE_MASK_FIXED_SIZE;
	if (fixedSize > 127) {
		size_t log2size = fixedSize & 0x7f;
		fixedSize = static_cast<size_t>(1) << log2size;
	}
	return fixedSize;
}


inline TupleValue::TupleValue()
: data_(), type_(TupleList::TYPE_NULL) {
}

inline TupleValue::TupleValue(const void *rawData, TupleList::TupleColumnType type)
: data_(), type_(type) {
	switch (type) {
		case TupleList::TYPE_NULL:
			break;
		case TupleList::TYPE_BYTE:
			memcpy(data_.fixedData_, rawData, sizeof(int8_t));
			break;
		case TupleList::TYPE_SHORT:
			memcpy(data_.fixedData_, rawData, sizeof(int16_t));
			break;
		case TupleList::TYPE_INTEGER:
			memcpy(data_.fixedData_, rawData, sizeof(int32_t));
			break;
		case TupleList::TYPE_LONG:
			memcpy(data_.fixedData_, rawData, sizeof(int64_t));
			break;
		case TupleList::TYPE_FLOAT:
			memcpy(data_.fixedData_, rawData, sizeof(float));
			break;
		case TupleList::TYPE_DOUBLE:
			memcpy(data_.fixedData_, rawData, sizeof(double));
			break;
		case TupleList::TYPE_TIMESTAMP:
			memcpy(data_.fixedData_, rawData, sizeof(int64_t));
			break;
		case TupleList::TYPE_MICRO_TIMESTAMP:
			memcpy(data_.fixedData_, rawData, sizeof(int64_t));
			break;
		case TupleList::TYPE_NANO_TIMESTAMP:
			data_.varData_ = rawData;
			break;
		case TupleList::TYPE_BOOL:
			memcpy(data_.fixedData_, rawData, sizeof(int8_t));
			break;
		case TupleList::TYPE_ANY:
			{
				const uint8_t* addr = static_cast<const uint8_t*>(rawData);
				TupleList::TupleColumnType destType;
				memcpy(&destType, rawData, sizeof(destType));
				type_ = destType;
				addr += sizeof(destType);
				if (TupleColumnTypeUtils::isNormalFixed(getType())) {
					memcpy(data_.fixedData_, addr, TupleColumnTypeUtils::getFixedSize(getType()));
				}
				else if (TupleList::TYPE_STRING == getType()) {
					data_.varData_ = addr;
				}
				else {
					assert(TupleColumnTypeUtils::isLob(getType()) ||
							TupleColumnTypeUtils::isLargeFixed(getType()));
					data_.varData_ = addr;
				}
			}
			break;
		case TupleList::TYPE_STRING:
			data_.varData_ = rawData;
			break;
		case TupleList::TYPE_GEOMETRY:
			assert(false);
			break;
		case TupleList::TYPE_BLOB:
			data_.varData_ = rawData;
			break;
		default:
			assert(false);
			break;
	}
}


template<> inline TupleValue::TupleValue(const bool &b)
: data_(), type_(TupleList::TYPE_BOOL) {
	data_.longValue_ = static_cast<int64_t>(b);
}

template<> inline TupleValue::TupleValue(const int8_t &b)
: data_(), type_(TupleList::TYPE_BYTE) {
	memcpy(data_.fixedData_, &b, sizeof(int8_t));
}

template<> inline TupleValue::TupleValue(const int16_t &i)
: data_(), type_(TupleList::TYPE_SHORT) {
	memcpy(data_.fixedData_, &i, sizeof(int16_t));
}

template<> inline TupleValue::TupleValue(const int32_t &i)
: data_(), type_(TupleList::TYPE_INTEGER) {
	memcpy(data_.fixedData_, &i, sizeof(int32_t));
}

template<> inline TupleValue::TupleValue(const int64_t &i)
: data_(), type_(TupleList::TYPE_LONG) {
	data_.longValue_ = i;
}

template<> inline TupleValue::TupleValue(const float &f)
: data_(), type_(TupleList::TYPE_FLOAT) {
	memcpy(data_.fixedData_, &f, sizeof(float));
}

template<> inline TupleValue::TupleValue(const double &d)
: data_(), type_(TupleList::TYPE_DOUBLE) {
	data_.doubleValue_ = d;
}

template<> inline TupleValue::TupleValue(const TupleString &s)
: data_(), type_(TupleList::TYPE_STRING) {
	data_.varData_ = s.data();
}

template<> inline TupleValue::TupleValue(const MicroTimestamp &ts) :
		data_(), type_(TupleList::TYPE_MICRO_TIMESTAMP) {
	data_.longValue_ = ts.value_;
}

template<> inline TupleValue::TupleValue(const TupleNanoTimestamp &ts) :
		data_(), type_(TupleList::TYPE_NANO_TIMESTAMP) {
	data_.varData_ = &ts.get();
}

inline TupleValue::TupleValue(const int64_t &ts, const TimestampTag&) :
		data_(), type_(TupleList::TYPE_TIMESTAMP) {
	data_.longValue_ = ts;
}


template<> inline float TupleValue::get() const {
	assert(TupleList::TYPE_FLOAT == type_);
	float val;
	memcpy(&val, data_.fixedData_, sizeof(float));
	return val;
}

template<> inline double TupleValue::get() const {
	assert(TupleList::TYPE_DOUBLE == type_);
	return data_.doubleValue_;
}

template<> inline int64_t TupleValue::get() const {
	assert(TupleList::TYPE_LONG == type_ || TupleList::TYPE_TIMESTAMP == type_ ||
			TupleList::TYPE_BOOL == type_);
	return data_.longValue_;
}

template<> inline MicroTimestamp TupleValue::get() const {
	assert(TupleList::TYPE_MICRO_TIMESTAMP == type_);
	MicroTimestamp ts;
	ts.value_ = data_.longValue_;
	return ts;
}

template<> inline TupleNanoTimestamp TupleValue::get() const {
	assert(TupleList::TYPE_NANO_TIMESTAMP == type_);
	return TupleNanoTimestamp(
			static_cast<const NanoTimestamp*>(data_.rawData_));
}

template<> inline const void* TupleValue::get() const {
	return data_.varData_;
}

inline bool TupleValue::testEquals(const TupleValue &another) const {
	return (type_ == another.type_)
		&& (data_.longValue_ == another.data_.longValue_);
}

inline TupleList::TupleColumnType TupleValue::getType() const {
	return static_cast<TupleList::TupleColumnType>(type_);
}

inline const void* TupleValue::fixedData() const {
	assert(TupleColumnTypeUtils::isNormalFixed(getType()));
	return data_.fixedData_;
}

inline size_t TupleValue::varSize() const {
	assert(!TupleColumnTypeUtils::isSomeFixed(getType()));
	if (data_.varData_) {
		uint32_t len;
		TupleValueUtils::decodeInt32(data_.varData_, len);
		return static_cast<size_t>(len);
	}
	else {
		return 0;
	}
}

inline const void* TupleValue::varData(size_t &headerSize, size_t &dataSize) const {
	assert(TupleColumnTypeUtils::isSingleVar(getType()));
	if (data_.varData_) {
		uint32_t value = 0;
		headerSize = TupleValueUtils::decodeInt32(data_.varData_, value);
		dataSize = static_cast<size_t>(value);
		return static_cast<const uint8_t*>(data_.varData_) + headerSize;
	}
	else {
		headerSize = 0;
		dataSize = 0;
		return NULL;
	}
}

inline const void* TupleValue::varData() const {
	assert(TupleColumnTypeUtils::isSingleVar(getType()));
	if (data_.varData_) {
		uint32_t value;
		size_t headerSize = TupleValueUtils::decodeInt32(data_.varData_, value);
		return static_cast<const uint8_t*>(data_.varData_) + headerSize;
	}
	else {
		return NULL;
	}
}

inline const void* TupleValue::getRawData() const {
	return data_.rawData_;
}

inline void* TupleValue::getRawData() {
	return data_.rawData_;
}

template<typename Base>
TupleValue::Coder<Base> TupleValue::coder(
		const Base &baseCoder, VarContext *cxt) {
	return TupleValue::Coder<Base>(baseCoder, cxt);
}

inline TupleValue::VarContext::Scope::Scope(VarContext &cxt) :
		cxt_(cxt), scope_(cxt.getBaseVarContext()) {
}

inline TupleValue::VarContext::Scope::~Scope() {
	if (!cxt_.getBaseVarContext().isEmpty()) {
		cxt_.clear();
	}
}

template<typename Base>
const typename TupleValue::Coder<Base>::Attribute
TupleValue::Coder<Base>::ATTRIBUTE_TYPE = Attribute("type");

template<typename Base>
const typename TupleValue::Coder<Base>::Attribute
TupleValue::Coder<Base>::ATTRIBUTE_VALUE = Attribute("value");

template<typename Base>
TupleValue::Coder<Base>::Coder(
		const BaseCoder &base, TupleValue::VarContext *cxt) :
		BaseType(base.getAllocator(), base),
		cxt_(cxt) {
}

template<typename Base>
template<typename Org, typename S, typename Traits>
void TupleValue::Coder<Base>::encodeBy(
		Org &orgCoder, S &stream, const TupleValue &value,
		const Attribute &attr, const Traits&) const {
	static_cast<void>(orgCoder);

	const TupleColumnType type = value.getType();

	if (stream.isTyped()) {
		switch (type) {
		case TupleList::TYPE_NULL:
		case TupleList::TYPE_DOUBLE:
		case TupleList::TYPE_BOOL:
		case TupleList::TYPE_STRING:
			encodeTupleValue(stream, value, attr, type);
			return;
		}
	}

	{
		typename S::ObjectScope scope(stream, attr);
		scope.stream().writeEnum(
				util::EnumCoder()(type, TupleColumnTypeCoder()).coder(),
				ATTRIBUTE_TYPE);

		encodeTupleValue(scope.stream(), value, ATTRIBUTE_VALUE, type);
	}
}

template<typename Base>
template<typename Org, typename S, typename Traits>
void TupleValue::Coder<Base>::decodeBy(
		Org &orgCoder, S &stream, TupleValue &value,
		const Attribute &attr, const Traits&) const {
	static_cast<void>(orgCoder);

	util::ObjectCoder::Type coderType;
	if (!stream.peekType(coderType, attr) ||
			coderType == util::ObjectCoder::TYPE_OBJECT) {
		typename S::ObjectScope scope(stream, attr);

		TupleColumnType type;
		scope.stream().readEnum(
				util::EnumCoder()(type, TupleColumnTypeCoder()).coder(),
				ATTRIBUTE_TYPE);
		decodeTupleValue(scope.stream(), value, ATTRIBUTE_VALUE, type);
	}
	else {
		switch (coderType) {
		case util::ObjectCoder::TYPE_NULL:
			decodeTupleValue(stream, value, attr, TupleList::TYPE_NULL);
			break;
		case util::ObjectCoder::TYPE_BOOL:
			decodeTupleValue(stream, value, attr, TupleList::TYPE_BOOL);
			break;
		case util::ObjectCoder::TYPE_NUMERIC:
			decodeTupleValue(stream, value, attr, TupleList::TYPE_DOUBLE);
			break;
		case util::ObjectCoder::TYPE_STRING:
			decodeTupleValue(stream, value, attr, TupleList::TYPE_STRING);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
					"Internal error by unknown value type in stream");
			break;
		}
	}
}

template<typename Base>
template<typename S>
void TupleValue::Coder<Base>::encodeTupleValue(
		S &stream, const TupleValue &value, const Attribute &attr,
		TupleColumnType type) const {
	switch (type) {
	case TupleList::TYPE_NULL:
		break;
	case TupleList::TYPE_BYTE:
		stream.writeNumeric(
				getNumericValue<int8_t>(value), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_SHORT:
		stream.writeNumeric(
				getNumericValue<int16_t>(value), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_INTEGER:
		stream.writeNumeric(
				getNumericValue<int32_t>(value), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_LONG:
		stream.writeNumeric(value.get<int64_t>(), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_FLOAT:
		stream.writeNumeric(
				getNumericValue<float>(value), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_DOUBLE:
		stream.writeNumeric(value.get<double>(), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_TIMESTAMP:
		stream.writeNumeric(value.get<int64_t>(), attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_MICRO_TIMESTAMP:
		stream.writeNumeric(
				value.get<MicroTimestamp>().value_, attr, DEFAULT_PRECISION);
		break;
	case TupleList::TYPE_NANO_TIMESTAMP:
		stream.writeBinary(
				value.get<TupleNanoTimestamp>().data(),
				sizeof(NanoTimestamp), attr);
		break;
	case TupleList::TYPE_BOOL:
		stream.writeBool(!!getNumericValue<int8_t>(value), attr);
		break;
	case TupleList::TYPE_STRING:
		{
			const TupleString::BufferInfo &info = TupleString(value).getBuffer();
			stream.writeString(info.first, info.second, attr);
		}
		break;
	case TupleList::TYPE_GEOMETRY:
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
				"Internal error by unsupported type");
	case TupleList::TYPE_BLOB:
		{
			TupleValue::LobReader reader(value);
			const uint64_t count = reader.getPartCount();
			const void *data;
			size_t size;
			if (count == 0) {
				stream.writeBinary(NULL, 0, attr);
			}
			else if (count == 1) {
				reader.next(data, size);
				stream.writeBinary(data, size, attr);
			}
			else {
				util::NormalXArray<uint8_t> buffer;

				while (reader.next(data, size)) {
					const size_t offset = buffer.size();
					buffer.resize(offset + size);
					memcpy(buffer.data() + offset, data, size);
				}

				stream.writeBinary(buffer.data(), buffer.size(), attr);
			}
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
				"Internal error by unsupported type");
	}
}

template<typename Base>
template<typename S>
void TupleValue::Coder<Base>::decodeTupleValue(
		S &stream, TupleValue &value, const Attribute &attr,
		TupleColumnType type) const {
	switch (type) {
	case TupleList::TYPE_NULL:
		value = TupleValue();
		break;
	case TupleList::TYPE_BYTE:
		value = TupleValue(stream.template readNumeric<int8_t>(
				attr, DEFAULT_PRECISION));
		break;
	case TupleList::TYPE_SHORT:
		value = TupleValue(stream.template readNumeric<int16_t>(
				attr, DEFAULT_PRECISION));
		break;
	case TupleList::TYPE_INTEGER:
		value = TupleValue(stream.template readNumeric<int32_t>(
				attr, DEFAULT_PRECISION));
		break;
	case TupleList::TYPE_LONG:
		value = TupleValue(stream.template readNumeric<int64_t>(
				attr, DEFAULT_PRECISION));
		break;
	case TupleList::TYPE_FLOAT:
		value = TupleValue(stream.template readNumeric<float>(
				attr, DEFAULT_PRECISION));
		break;
	case TupleList::TYPE_DOUBLE:
		value = TupleValue(stream.template readNumeric<double>(
				attr, DEFAULT_PRECISION));
		break;
	case TupleList::TYPE_TIMESTAMP:
		{
			const int64_t tsValue = stream.template readNumeric<int64_t>(
					attr, DEFAULT_PRECISION);
			value = TupleValue(tsValue, TimestampTag());
		}
		break;
	case TupleList::TYPE_MICRO_TIMESTAMP:
		{
			MicroTimestamp tsValue;
			tsValue.value_ = stream.template readNumeric<int64_t>(
					attr, DEFAULT_PRECISION);
			value = TupleValue(tsValue);
		}
		break;
	case TupleList::TYPE_NANO_TIMESTAMP:
		{
			NanoTimestampBuilder tsBuilder(getContext());
			util::ObjectCoder::StringBuilder<
					NanoTimestampBuilder, uint8_t> builder(tsBuilder);
			stream.readBinary(builder, attr);
			value = TupleNanoTimestamp(tsBuilder.build());
		}
		break;
	case TupleList::TYPE_BOOL:
		value = TupleValue(stream.readBool(attr));
		break;
	case TupleList::TYPE_STRING:
		{
			SingleVarBuilder varBuilder(getContext(), type, 0);
			util::ObjectCoder::StringBuilder<SingleVarBuilder, char8_t> builder(
					varBuilder);

			stream.readString(builder, attr);
			value = varBuilder.build();
		}
		break;
	case TupleList::TYPE_GEOMETRY:
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
				"Internal error by unsupported type");
	case TupleList::TYPE_BLOB:
		{
			LobBuilder lobBuilder(getContext(), type, 0);
			util::ObjectCoder::StringBuilder<LobBuilder, uint8_t> builder(
					lobBuilder);

			stream.readBinary(builder, attr);
			value = lobBuilder.build();
		}
		break;
	default:
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
				"Internal error by unsupported type");
	}
}

template<typename Base>
TupleValue::VarContext& TupleValue::Coder<Base>::getContext() const {
	if (cxt_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR,
				"Internal error by empty context");
	}

	return *cxt_;
}

template<typename Base>
template<typename T>
T TupleValue::Coder<Base>::getNumericValue(const TupleValue &value) {
	T dest;
	assert(TupleColumnTypeUtils::getFixedSize(value.getType()) == sizeof(dest));
	memcpy(&dest, value.fixedData(), sizeof(dest));
	return dest;
}

inline size_t TupleValueUtils::encodeInt32(uint32_t input, uint8_t *output) {
	uint32_t encodedVarSize = encodeVarSize(input);
	size_t bytesOut = 4;
	if (input <= OBJECT_MAX_1BYTE_LENGTH_VALUE) {
		uint8_t val = static_cast<uint8_t>(encodedVarSize);
		memcpy(output, &val, sizeof(val));
		bytesOut = 1;
	}
	else {
		memcpy(output, &encodedVarSize, sizeof(encodedVarSize));
	}
	return bytesOut;
}

inline size_t TupleValueUtils::decodeInt32(const void *input, uint32_t &output) {
	size_t bytesIn = varSizeIs1Byte(input) ? 1 : 4;
	output = decodeVarSize(input);
	return bytesIn;
}


inline bool TupleValueUtils::varSizeIs1Byte(const void* ptr) {
	return ((*static_cast<const uint8_t*>(ptr) & 0x01) == 0x01);
}

inline bool TupleValueUtils::varSizeIs4Byte(const void* ptr) {
	return ((*reinterpret_cast<const uint8_t*>(ptr) & 0x03) == 0x00);
}

inline bool TupleValueUtils::varSizeIs8Byte(const void* ptr) {
	return ((*reinterpret_cast<const uint8_t*>(ptr) & 0x03) == 0x02);
}

inline bool TupleValueUtils::varSizeIs1Byte(uint8_t val) {
	return ((val & 0x01) == 0x01);
}
inline bool TupleValueUtils::varSizeIs4Byte(uint8_t val) {
	return ((val & 0x03) == 0x00);
}
inline bool TupleValueUtils::varSizeIs8Byte(uint8_t val) {
	return ((val & 0x03) == 0x02);
}

inline uint32_t TupleValueUtils::decode1ByteVarSize(uint8_t val) {
	assert(val != 0);
	return val >> 1;
}
inline uint32_t TupleValueUtils::decode4ByteVarSize(uint32_t val) {
	assert(val != 0);
	return val >> 2;
}
inline uint64_t TupleValueUtils::decode8ByteVarSize(uint64_t val) {
	assert(val != 0);
	return val >> 2;
}

inline uint32_t TupleValueUtils::get1ByteVarSize(const void *ptr) {
	assert(*reinterpret_cast<const uint8_t*>(ptr) != 0);
	return (*reinterpret_cast<const uint8_t*>(ptr) >> 1);
}
inline uint32_t TupleValueUtils::get4ByteVarSize(const void *ptr) {
	assert(*reinterpret_cast<const uint32_t*>(ptr) != 0);
	return (*reinterpret_cast<const uint32_t*>(ptr) >> 2);
}
inline uint64_t TupleValueUtils::getOIdVarSize(const void *ptr) {
	assert(*reinterpret_cast<const uint64_t*>(ptr) != 0);
	return (*reinterpret_cast<const uint64_t*>(ptr) >> 2);
}

inline uint32_t TupleValueUtils::decodeVarSize(const void *ptr) {
	if (varSizeIs1Byte(ptr)) {
		return get1ByteVarSize(ptr);
	}
	else {
		assert(varSizeIs4Byte(ptr));
		return get4ByteVarSize(ptr);
	}
}

inline uint64_t TupleValueUtils::decodeVarSizeOrOId(const void *ptr, bool &isOId) {
	isOId = false;
	if (varSizeIs1Byte(ptr)) {
		return get1ByteVarSize(ptr);
	}
	else if (varSizeIs4Byte(ptr)) {
		return get4ByteVarSize(ptr);
	}
	else {
		assert(varSizeIs8Byte(ptr));
		isOId = true;
		return getOIdVarSize(ptr);
	}
}

inline uint32_t TupleValueUtils::getEncodedVarSize(const void *ptr) {
	if (varSizeIs1Byte(ptr)) {
		return 1;
	}
	else if (varSizeIs4Byte(ptr)) {
		return 4;
	}
	else {
		assert(varSizeIs8Byte(ptr));
		return 8;
	}
}

inline uint32_t TupleValueUtils::getEncodedVarSize(uint32_t val) {
	if (val < VAR_SIZE_1BYTE_THRESHOLD) {
		return 1;
	}
	else {
		return (val < VAR_SIZE_4BYTE_THRESHOLD ? 4 : 8);
	}
}

inline uint32_t TupleValueUtils::encodeVarSize(uint32_t val) {
	assert(val < 0x40000000L);
	if (val <= OBJECT_MAX_1BYTE_LENGTH_VALUE) {
		return ((val << 1L) | 0x01);
	}
	else {
		return (val << 2L);
	}
}


inline uint64_t TupleValueUtils::encodeVarSizeOId(uint64_t val) {
	return (val << 2L) | 0x02;
}

inline uint32_t TupleValueUtils::calcNullsByteSize(uint32_t columnNum) {
	return (columnNum + 7) / 8;
}


inline void TupleValueUtils::memWrite(void* &destAddr, const void* srcAddr, size_t size) {
	assert(destAddr);
	assert(srcAddr);
	memcpy(destAddr, srcAddr, size);
	destAddr = static_cast<uint8_t*>(destAddr) + size;
}


inline void TupleValueUtils::setPartData(void* destAddr, const void* srcAddr, uint32_t size) {
	assert(destAddr);
	assert(srcAddr);
	const uint32_t encodedSize = TupleValueUtils::getEncodedVarSize(size);
	const uint32_t encodedResult = TupleValueUtils::encodeVarSize(size);
	memcpy(destAddr, &encodedResult, encodedSize);
	uint8_t *addr = static_cast<uint8_t*>(destAddr) + encodedSize;
	memcpy(addr, srcAddr, size);
}

inline void TupleValueUtils::getPartData(const void* rawAddr, const void* &data, uint32_t &size) {
	assert(rawAddr);
	size = TupleValueUtils::decodeVarSize(rawAddr);
	const uint32_t encodedSize = TupleValueUtils::getEncodedVarSize(size);
	const uint8_t* addr = static_cast<const uint8_t*>(rawAddr) + encodedSize;
	data = addr;
}


inline TupleNanoTimestamp::TupleNanoTimestamp() :
		ts_(&Constants::EMPTY_VALUE) {
}

inline TupleNanoTimestamp::TupleNanoTimestamp(const NanoTimestamp *ts) :
		ts_(ts) {
	assert(ts != NULL);
}

inline TupleNanoTimestamp::TupleNanoTimestamp(const TupleValue &value) :
		ts_(static_cast<const NanoTimestamp*>(value.getRawData())) {
}

inline TupleNanoTimestamp::operator TupleValue() const {
	return TupleValue(*this);
}

inline TupleNanoTimestamp::operator NanoTimestamp() const {
	return get();
}

inline const NanoTimestamp& TupleNanoTimestamp::get() const {
	assert(ts_ != NULL);
	return *ts_;
}

inline const void* TupleNanoTimestamp::data() const {
	return ts_;
}

inline TupleString::TupleString() :
		data_(emptyData()) {
}

inline TupleString::TupleString(const void *data) :
		data_(data) {
	assert(data != NULL);
}

inline TupleString::TupleString(const TupleValue &value) :
		data_(value.get<const void*>()) {
	assert(value.getType() == TupleList::TYPE_STRING);
	assert(data_ != NULL);
}

inline TupleString::BufferInfo TupleString::getBuffer() const {
	if (TupleValueUtils::varSizeIs1Byte(data_)) {
		return BufferInfo(
				static_cast<const char8_t*>(data_) + 1,
				TupleValueUtils::get1ByteVarSize(data_));
	}
	else {
		assert(TupleValueUtils::varSizeIs4Byte(data_));
		return BufferInfo(
				static_cast<const char8_t*>(data_) + 4,
				TupleValueUtils::get4ByteVarSize(data_));
	}
}

inline const void* TupleString::data() const {
	return data_;
}

inline const void* TupleString::emptyData() {
	static uint8_t data[] = { 0x1 };
	return data;
}

inline bool TupleList::Info::checkColumnCount(size_t count) {
	return count <= COLUMN_COUNT_LIMIT;
}

#endif 
