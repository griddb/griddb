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

#include "sql_tuple.h"
#include "util/container.h" 
#include <iostream>

UTIL_TRACER_DECLARE(TUPLE_LIST);
const LocalTempStore::BlockId TupleList::UNDEF_BLOCKID = LocalTempStore::UNDEF_BLOCKID;
const LocalTempStore::BlockId TupleList::Reader::UNDEF_BLOCKID = LocalTempStore::UNDEF_BLOCKID;
const size_t TupleList::Info::COLUMN_COUNT_LIMIT = 65000;





struct TupleList::TupleBlockHeader {
	static const uint32_t TUPLE_COUNT_OFFSET = LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET + sizeof(uint32_t);
	static const uint32_t COLUMN_NUM_OFFSET = TUPLE_COUNT_OFFSET + sizeof(uint32_t);
	static const uint32_t COLUMN_LIST_CHECKSUM_OFFSET = COLUMN_NUM_OFFSET + sizeof(uint16_t);
	static const uint32_t NEXT_FIX_DATA_OFFSET = COLUMN_LIST_CHECKSUM_OFFSET + sizeof(uint16_t);
	static const uint32_t NEXT_VAR_DATA_OFFSET = NEXT_FIX_DATA_OFFSET + sizeof(uint32_t);

	static const uint32_t BLOCK_HEADER_SIZE = NEXT_VAR_DATA_OFFSET + 4;
	static const uint16_t MAGIC_NUMBER = LocalTempStore::Block::Header::MAGIC_NUMBER;
	static const uint16_t VERSION_NUMBER = LocalTempStore::Block::Header::VERSION_NUMBER;

	TupleBlockHeader();
	inline static uint32_t getHeaderCheckSum(const void *block) {
		return LocalTempStore::Block::Header::getHeaderCheckSum(block);
	}
	inline static void updateHeaderCheckSum(void *block) {
		LocalTempStore::Block::Header::updateHeaderCheckSum(block);
	}
	inline static uint32_t calcBlockCheckSum(void *block) {
		return LocalTempStore::Block::Header::calcBlockCheckSum(block);
	}
	inline static uint16_t getMagic(const void *block) {
		return LocalTempStore::Block::Header::getMagic(block);
	}
	inline static void setMagic(void *block) {
		LocalTempStore::Block::Header::setMagic(block);
	}
	inline static uint16_t getVersion(const void *block) {
		return LocalTempStore::Block::Header::getVersion(block);
	}
	inline static void setVersion(void *block) {
		LocalTempStore::Block::Header::setVersion(block);
	}
	inline static uint32_t getBlockExpSize(const void *block) {
		return LocalTempStore::Block::Header::getBlockExpSize(block);
	}
	inline static void setBlockExpSize(void *block, uint32_t blockExpSize) {
		LocalTempStore::Block::Header::setBlockExpSize(block, blockExpSize);
	}
	inline static BlockId getBlockId(const void *block) {
		return LocalTempStore::Block::Header::getBlockId(block);
	}
	inline static void setBlockId(void *block, uint32_t blockId) {
		LocalTempStore::Block::Header::setBlockId(block, blockId);
	}
	inline static int32_t getContiguousBlockNum(const void *block) {
		return LocalTempStore::Block::Header::getContiguousBlockNum(block);
	}
	inline static void setContiguousBlockNum(void *block, int32_t contiguousBlockNum) {
		LocalTempStore::Block::Header::setContiguousBlockNum(block, contiguousBlockNum);
	}
	inline static uint32_t getTupleCount(const void *block) {
		return *reinterpret_cast<const uint32_t*>(
			static_cast<const uint8_t*>(block) + TUPLE_COUNT_OFFSET);
	}
	inline static void setTupleCount(void *block, uint32_t tupleCount) {
		memcpy(static_cast<uint8_t*>(block) + TUPLE_COUNT_OFFSET, &tupleCount, sizeof(uint32_t));
	}
	inline static uint16_t getColumnNum(const void *block) {
		return *reinterpret_cast<const uint16_t*>(
			static_cast<const uint8_t*>(block) + COLUMN_NUM_OFFSET);
	}
	inline static void setColumnNum(void *block, uint16_t columnNum) {
		memcpy(static_cast<uint8_t*>(block) + COLUMN_NUM_OFFSET, &columnNum, sizeof(uint16_t));
	}
	inline static uint16_t getColumnListChecksum(const void *block) {
		return *reinterpret_cast<const uint16_t*>(
			static_cast<const uint8_t*>(block) + COLUMN_LIST_CHECKSUM_OFFSET);
	}
	inline static void setColumnListChecksum(void *block, uint16_t colListChecksum) {
		memcpy(static_cast<uint8_t*>(block) + COLUMN_LIST_CHECKSUM_OFFSET, &colListChecksum, sizeof(uint16_t));
	}
	inline static uint32_t getNextFixDataOffset(const void *block) {
		return *reinterpret_cast<const uint32_t*>(
			static_cast<const uint8_t*>(block) + NEXT_FIX_DATA_OFFSET);
	}
	inline static void setNextFixDataOffset(void *block, uint32_t offset) {
		memcpy(static_cast<uint8_t*>(block) + NEXT_FIX_DATA_OFFSET, &offset, sizeof(uint32_t));
	}
	inline static uint32_t getNextVarDataOffset(const void *block) {
		return *reinterpret_cast<const uint32_t*>(
			static_cast<const uint8_t*>(block) + NEXT_VAR_DATA_OFFSET);
	}
	inline static void setNextVarDataOffset(void *block, uint32_t offset) {
		memcpy(static_cast<uint8_t*>(block) + NEXT_VAR_DATA_OFFSET, &offset, sizeof(uint32_t));
	}
};

const uint32_t TupleList::BLOCK_HEADER_SIZE = TupleList::TupleBlockHeader::BLOCK_HEADER_SIZE;
const uint32_t TupleList::RESERVED_SIZE_FOR_MAX_SINGLE_VAR = TupleList::BLOCK_HEADER_SIZE + 4; 

struct TupleList::BlockLobDataHeader {
	uint64_t partCount_; 
	uint64_t lobPartData_; 

	static const size_t LOB_PART_COUNT_SIZE = sizeof(uint64_t);
	static const size_t LOB_PART_INFO_SIZE = sizeof(uint64_t);
	static const size_t LOB_PART_HEADER_SIZE = sizeof(uint64_t); 
};



class TupleList::Body {
public:
	Body(LocalTempStore &store, const GroupId &groupId, const ResourceId &resourceId, const Info &info);
	Body(const Group &group, const ResourceId &id, const Info &info);
	~Body();

	const TupleList::Info& getInfo() const;
	LocalTempStore& getStore() const;
	LocalTempStore::GroupId getGroupId() const;
	LocalTempStore::ResourceId getResourceId() const;
	void close();

	uint64_t getBlockCount() const;
	void setValidBlockCount(uint64_t count);
	uint64_t getAllocatedBlockCount() const;
	uint64_t addAllocatedBlockCount();

	uint32_t getBlockExpSize() const;
	uint32_t getBlockSize() const;

	void append(const LocalTempStore::Block &block);

	BlockId getBlockId(uint64_t pos);
	bool getBlockIdList(uint64_t pos, uint64_t count, BlockIdArray &blockIdList);
	void invalidateBlockId(uint64_t pos);
	void invalidateBlockId(uint64_t start, uint64_t end);
	void appendBlockId(LocalTempStore::BlockId id);

	void resetBlock(LocalTempStore::Block &block);

	LocalTempStore::BlockId getActiveTopBlockNth() const;
	void setActiveTopBlockNth(LocalTempStore::BlockId nth);

	uint64_t addReference();
	uint64_t removeReference();
	uint64_t getReferenceCount() const;

	bool blockAppendable() const;
	void setBlockAppendable(bool flag);
	bool tupleAppendable() const;
	void setTupleAppendable(bool flag);

	bool blockReaderDetached() const;
	void setBlockReaderDetached(bool flag);
	bool readerDetached() const;
	void setReaderDetached(bool flag);
	bool writerDetached() const;
	void setWriterDetached(bool flag);

	size_t registReader(Reader &reader);
	void attachReader(size_t id, Reader &reader);
	void detachReader(size_t id);
	void setReaderTopNth(size_t id, uint64_t nth);
	void closeReader(size_t id);
	size_t getReaderCount() const;
	bool isReaderStarted() const;
	bool detached() const;
	void setDetached(bool flag);

	uint64_t getActiveReaderCount() const;
	uint64_t getDetachedReaderCount() const;
	uint64_t getAccessTopMin() const;

	util::Mutex& getLock();
private:
	class AccessorManager;
	friend class AccessorManager;

	Body(const Body&);
	Body& operator=(const Body&);

	typedef std::deque<BlockId,
			util::StdAllocator<BlockId, LocalTempStore::LTSVariableSizeAllocator> > TupleBlockIdList;

	typedef TupleBlockIdList::iterator TupleBlockIdListItr;

	typedef std::vector< TupleList::TupleColumnType, util::StdAllocator<
	  TupleList::TupleColumnType, LocalTempStore::LTSVariableSizeAllocator> > TupleColumnTypeList;

	util::Mutex mutex_;
	TupleList::Info info_;
	LocalTempStore *store_;
	AccessorManager *accessorManager_;
	util::Atomic<uint64_t> refCount_;
	util::Atomic<uint64_t> blockCount_;
	util::Atomic<uint64_t> allocatedBlockCount_;
	const LocalTempStore::GroupId groupId_;
	const LocalTempStore::ResourceId resourceId_;
	LocalTempStore::BlockId activeTopBlockNth_;
	TupleColumnTypeList columnTypeList_;
	TupleBlockIdList tupleBlockIdList_;
	bool blockAppendable_;
	bool tupleAppendable_;

	bool blockReaderDetached_;
	bool writerDetached_;
	bool detached_;
};

class TupleList::Body::AccessorManager {
public:
	typedef std::vector<Reader*,
			util::StdAllocator<Reader*, LocalTempStore::LTSVariableSizeAllocator> > ReaderList;

	typedef std::vector<uint64_t,
			util::StdAllocator<uint64_t, LocalTempStore::LTSVariableSizeAllocator> > ReaderTopNthList;


	explicit AccessorManager(TupleList::Body* tupleListBody);
	~AccessorManager();

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

private:
	AccessorManager(const AccessorManager&);
	AccessorManager& operator=(const AccessorManager&);

	TupleList::Body *tupleListBody_;
	ReaderList readerList_;
	ReaderTopNthList readerTopNthList_;
	bool isReaderStarted_;
	uint64_t activeReaderCount_;
	uint64_t detachedReaderCount_;
	uint64_t accessTopMin_;
	TupleList::Reader::AccessOrder readerAccessOrder_;
};


TupleList::Body::Body(
		LocalTempStore &store, const GroupId &groupId,
		const ResourceId &resourceId, const Info &info)
: info_(info), store_(&store), accessorManager_(NULL), refCount_(0)
, blockCount_(0), allocatedBlockCount_(0)
, groupId_(groupId), resourceId_(resourceId), activeTopBlockNth_(0)
, columnTypeList_(store.getVarAllocator())
, tupleBlockIdList_(store.getVarAllocator())
, blockAppendable_(true), tupleAppendable_(true)
, blockReaderDetached_(false)
, writerDetached_(false), detached_(false) {
	if (!TupleList::Info::checkColumnCount(info_.columnCount_)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_LIMIT_EXCEEDED,
					"Column count exceeds maximum size : " << info_.columnCount_);
	}
	info_.blockSize_ = static_cast<uint32_t>(
			1UL << store_->getDefaultBlockExpSize());
	if (info_.getFixedPartSize() >= (info_.blockSize_ - TupleList::BLOCK_HEADER_SIZE)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_LIMIT_EXCEEDED,
					"Tuple size exceeds maximum size : " << info_.getFixedPartSize());
	}
	assert(info.columnTypeList_);
	columnTypeList_.reserve(sizeof(TupleList::TupleColumnType) * info.columnCount_);
	columnTypeList_.assign(
			info.columnTypeList_, info.columnTypeList_ + info.columnCount_);
	info_.columnTypeList_ = &columnTypeList_[0];
	accessorManager_ = UTIL_NEW AccessorManager(this);
}
TupleList::Body::Body(const Group &group, const ResourceId &id, const Info &info)
: info_(info), store_(&group.getStore()), accessorManager_(NULL), refCount_(0)
, blockCount_(0), allocatedBlockCount_(0)
, groupId_(group.getId()), resourceId_(id), activeTopBlockNth_(0)
, columnTypeList_(group.getStore().getVarAllocator())
, tupleBlockIdList_(group.getStore().getVarAllocator())
, blockAppendable_(true), tupleAppendable_(true)
, blockReaderDetached_(false), writerDetached_(false) {
	if (!TupleList::Info::checkColumnCount(info_.columnCount_)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_LIMIT_EXCEEDED,
					"Column count exceeds maximum size : " << info_.columnCount_);
	}
	info_.blockSize_ = static_cast<uint32_t>(
			1UL << store_->getDefaultBlockExpSize());
	if (info_.getFixedPartSize() >= (info_.blockSize_ - TupleList::BLOCK_HEADER_SIZE)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_LIMIT_EXCEEDED,
					"Tuple size exceeds maximum size : " << info_.getFixedPartSize());
	}
	assert(info.columnTypeList_);
	columnTypeList_.reserve(sizeof(TupleList::TupleColumnType) * info.columnCount_);
	columnTypeList_.assign(
			info.columnTypeList_, info.columnTypeList_ + info.columnCount_);
	info_.columnTypeList_ = &columnTypeList_[0];
	accessorManager_ = UTIL_NEW AccessorManager(this);
}

TupleList::Body::~Body() try {
	if (store_) {
		TupleBlockIdList::iterator itr = tupleBlockIdList_.begin();
		for (; itr != tupleBlockIdList_.end(); ++itr) {
			if (UNDEF_BLOCKID != *itr) {
				store_->getBufferManager().removeAssignment(*itr);
			}
		}
	}
	delete accessorManager_;
	accessorManager_ = NULL;
}
catch (...) {
}

const TupleList::Info& TupleList::Body::getInfo() const {
	return info_;
}

LocalTempStore& TupleList::Body::getStore() const {
	return *store_;
}

LocalTempStore::ResourceId TupleList::Body::getGroupId() const {
	return groupId_;
}

LocalTempStore::ResourceId TupleList::Body::getResourceId() const {
	return resourceId_;
}
void TupleList::Body::close() {
}
uint64_t TupleList::Body::getBlockCount() const {
	return blockCount_;
}

void TupleList::Body::setValidBlockCount(uint64_t count) {
	assert(count <= allocatedBlockCount_);
	blockCount_ = count;
}

uint64_t TupleList::Body::getAllocatedBlockCount() const {
	assert(tupleBlockIdList_.size() == allocatedBlockCount_);
	return tupleBlockIdList_.size();
}

uint64_t TupleList::Body::addAllocatedBlockCount() {
	return ++allocatedBlockCount_;
}

uint32_t TupleList::Body::getBlockExpSize() const {
	return util::ilog2(info_.blockSize_);
}

uint32_t TupleList::Body::getBlockSize() const {
	return info_.blockSize_;
}

LocalTempStore::BlockId TupleList::Body::getActiveTopBlockNth() const {
	return activeTopBlockNth_;
}

void TupleList::Body::setActiveTopBlockNth(LocalTempStore::BlockId blockNth) {
	activeTopBlockNth_ = blockNth;
}

void TupleList::Body::append(const LocalTempStore::Block &block) {

	store_->getBufferManager().addAssignment(block.getBlockId()); 

	appendBlockId(block.getBlockId());
	LocalTempStore::Block *blockPtr = const_cast<LocalTempStore::Block *>(&block);
	blockPtr->setGroupId(groupId_);
	uint64_t activeCount = (tupleBlockIdList_.size() > activeTopBlockNth_) ?
			tupleBlockIdList_.size() - activeTopBlockNth_ : 0;
	store_->setActiveBlockCount(groupId_, activeCount);
}


LocalTempStore::BlockId TupleList::Body::getBlockId(uint64_t pos) {
	util::LockGuard<util::Mutex> guard(mutex_);
	assert(pos < tupleBlockIdList_.size());
	return tupleBlockIdList_[static_cast<size_t>(pos)];
}

bool TupleList::Body::getBlockIdList(uint64_t pos, uint64_t count, BlockIdArray &blockIdList) {
	blockIdList.clear();
	util::LockGuard<util::Mutex> guard(mutex_);
	if ((pos + count) <= tupleBlockIdList_.size()) {
		blockIdList.reserve(count);
		TupleBlockIdList::const_iterator startItr = tupleBlockIdList_.begin() + pos;
		TupleBlockIdList::const_iterator endItr = tupleBlockIdList_.begin() + pos + count;
		blockIdList.assign(startItr, endItr);
		return true;
	}
	else { 
		return false;
	} 
}

void TupleList::Body::invalidateBlockId(uint64_t pos) {
	util::LockGuard<util::Mutex> guard(mutex_);
	assert(pos < tupleBlockIdList_.size());
	tupleBlockIdList_[static_cast<size_t>(pos)] = UNDEF_BLOCKID;
}

void TupleList::Body::invalidateBlockId(uint64_t start, uint64_t end0) {
	util::LockGuard<util::Mutex> guard(mutex_);
	uint64_t end = (end0 < tupleBlockIdList_.size()) ? end0 : tupleBlockIdList_.size();
	for (uint64_t pos = start; pos < end; ++pos) {
		BlockId blockId = tupleBlockIdList_[static_cast<size_t>(pos)];
		assert(store_->getBufferManager().getAssignmentCount(blockId) > 0);
		if (store_->getBufferManager().getAssignmentCount(blockId) > 0) {
			store_->getBufferManager().removeAssignment(blockId);
		}
		tupleBlockIdList_[static_cast<size_t>(pos)] = UNDEF_BLOCKID;
	}
}

void TupleList::Body::appendBlockId(LocalTempStore::BlockId id) {
	util::LockGuard<util::Mutex> guard(mutex_);
	tupleBlockIdList_.push_back(id);
}


void TupleList::Body::resetBlock(LocalTempStore::Block &block) {
	TupleBlockHeader::setMagic(block.data());
	TupleBlockHeader::setVersion(block.data());
	TupleBlockHeader::setBlockExpSize(block.data(), getBlockExpSize());
	TupleBlockHeader::setColumnNum(block.data(), static_cast<uint16_t>(info_.columnCount_));
	TupleBlockHeader::setContiguousBlockNum(block.data(), 0);
	TupleBlockHeader::setTupleCount(block.data(), 0);
	TupleBlockHeader::setColumnListChecksum(block.data(), 0);
	TupleBlockHeader::setNextFixDataOffset(block.data(), TupleList::BLOCK_HEADER_SIZE);
	TupleBlockHeader::setNextVarDataOffset(block.data(), getBlockSize());
}


uint64_t TupleList::Body::addReference() {
	return ++refCount_;
}

uint64_t TupleList::Body::removeReference() {
	assert(refCount_ > 0);
	return --refCount_;
}

uint64_t TupleList::Body::getReferenceCount() const {
	return refCount_;
}


bool TupleList::Body::blockAppendable() const {
	return blockAppendable_;
}

void TupleList::Body::setBlockAppendable(bool flag) {
	blockAppendable_ = flag;
}

bool TupleList::Body::tupleAppendable() const {
	return tupleAppendable_;
}

bool TupleList::Body::blockReaderDetached() const {
	return blockReaderDetached_;
}
void TupleList::Body::setBlockReaderDetached(bool flag) {
	blockReaderDetached_ = flag;
}

bool TupleList::Body::readerDetached() const {
	return (accessorManager_->getDetachedReaderCount() > 0);
}
bool TupleList::Body::writerDetached() const {
	return writerDetached_;
}
void TupleList::Body::setWriterDetached(bool flag) {
	writerDetached_ = flag;
}

size_t TupleList::Body::registReader(Reader &reader) {
	return accessorManager_->registReader(reader);
}
void TupleList::Body::attachReader(size_t id, Reader &reader) {
	accessorManager_->attachReader(id, reader);
}
void TupleList::Body::detachReader(size_t id) {
	accessorManager_->detachReader(id);
}
void TupleList::Body::setReaderTopNth(size_t id, uint64_t nth) {
	accessorManager_->setReaderTopNth(id, nth);
}
void TupleList::Body::closeReader(size_t id) {
	accessorManager_->closeReader(id);
}
size_t TupleList::Body::getReaderCount() const {
	return accessorManager_->getReaderCount();
}
bool TupleList::Body::isReaderStarted() const {
	return accessorManager_->isReaderStarted();
}
uint64_t TupleList::Body::getActiveReaderCount() const {
	return accessorManager_->getActiveReaderCount();
}
uint64_t TupleList::Body::getDetachedReaderCount() const {
	return accessorManager_->getDetachedReaderCount();
}
uint64_t TupleList::Body::getAccessTopMin() const {
	return accessorManager_->getAccessTopMin();
}

bool TupleList::Body::detached() const {
	return detached_;
}
void TupleList::Body::setDetached(bool flag) {
	detached_ = flag;
}

void TupleList::Body::setTupleAppendable(bool flag) {
	tupleAppendable_ = flag;
}

util::Mutex& TupleList::Body::getLock() {
	return mutex_;
}
TupleList::TupleList(LocalTempStore &store, const ResourceId &resourceId, const Info *info)
: body_(NULL), blockReader_(NULL) 
  , writer_(NULL), writerHandler_(NULL) {
	LocalTempStore::ResourceInfo &resourceInfo = store.getResourceInfo(resourceId);
	if (resourceInfo.resource_) {
		Body* body = static_cast<Body*>(resourceInfo.resource_);
		if (body == NULL) { 
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"Specified resourceId is invalid: ResourceId=" << resourceId);
		} 
		util::LockGuard<util::Mutex> guard(body->getLock());
		if (!body->detached()) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"Specified resourceId is not detached: ResourceId=" << resourceId);
		}
		body_ = body;
		body_->setDetached(false);
	}
	else {
		assert(info);
		body_ = ALLOC_VAR_SIZE_NEW(store.getVarAllocator())
		  TupleList::Body(store, resourceInfo.groupId_, resourceId, *info);
		resourceInfo.resource_ = body_;
		assert(0 == resourceInfo.blockExpSize_);
		resourceInfo.blockExpSize_ = body_->getBlockExpSize();
		body_->addReference();
	}
}

TupleList::TupleList(const Group &group, const Info &info)
: body_(NULL), blockReader_(NULL) 
  , writer_(NULL), writerHandler_(NULL) {
	LocalTempStore::ResourceId resourceId =
		group.getStore().allocateResource(LocalTempStore::RESOURCE_TUPLE_LIST, group.getId());
	LocalTempStore::ResourceInfo &resourceInfo = group.getStore().getResourceInfo(resourceId);
	body_ = ALLOC_VAR_SIZE_NEW(group.getStore().getVarAllocator())
		TupleList::Body(group.getStore(), resourceInfo.groupId_, resourceId, info);
	body_->addReference();
	resourceInfo.resource_ = body_;
	assert(0 == resourceInfo.blockExpSize_);
	resourceInfo.blockExpSize_ = body_->getBlockExpSize();
}

TupleList::~TupleList() try {
	if (body_ && !body_->detached()) {
		uint64_t refCount = body_->removeReference();
		if (0 == refCount) {
			body_->getStore().deallocateResource(body_->getResourceId());
			ALLOC_VAR_SIZE_DELETE(getStore().getVarAllocator(), body_);
			body_ = NULL;
		}
	}
}
catch (...) { 
} 

TupleList& TupleList::operator=(const TupleList &another) { 
	if (this != &another) {
		if (body_) {
			body_->removeReference();
		}
		body_ = another.body_;
		body_->addReference();
		blockReader_ = NULL;
		writer_ = NULL;
		writerHandler_ = NULL;
	}
	return *this;
} 

LocalTempStore::ResourceId TupleList::detach() {
	if (body_) {
		util::LockGuard<util::Mutex> guard(body_->getLock());
		if (body_->detached()) { 
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"Already detached");
		} 
		if (blockReader_ && !blockReaderDetached()) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"BlockReader is active");
		}
		if (getReaderCount() > 0 && !readerDetached()) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"Reader is active");
		}
		if (writer_ && !writerDetached()) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"Writer is active");
		}
		body_->setDetached(true);
		ResourceId resourceId = body_->getResourceId();
		body_ = NULL;
		return resourceId;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
				"Already detached");
	}
}

bool TupleList::blockReaderDetached() {
	assert(body_);
	return body_->blockReaderDetached();
}

bool TupleList::readerDetached() {
	assert(body_);
	return body_->readerDetached();
}

bool TupleList::writerDetached() {
	assert(body_);
	return body_->writerDetached();
}

void TupleList::setBlockReaderDetached(bool flag) {
	assert(body_);
	body_->setBlockReaderDetached(flag);
}
void TupleList::setWriterDetached(bool flag) {
	assert(body_);
	body_->setWriterDetached(flag);
}

size_t TupleList::registReader(Reader &reader) {
	if (blockReader_ || blockReaderDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Another blockreader has already exist.");
	}
	if (writer_ || writerDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Another writer has already exist.");
	}
	assert(body_);
	return body_->registReader(reader);
}
void TupleList::attachReader(size_t id, Reader &reader) {
	assert(body_);
	body_->attachReader(id, reader);
}
void TupleList::detachReader(size_t id) {
	assert(body_);
	body_->detachReader(id);
}
void TupleList::setReaderTopNth(size_t id, uint64_t blockNth) {
	assert(body_);
	body_->setReaderTopNth(id, blockNth);
}
void TupleList::closeReader(size_t id) {
	assert(body_);
	body_->closeReader(id);
}
size_t TupleList::getReaderCount() const {
	assert(body_);
	return body_->getReaderCount();
}
bool TupleList::isReaderStarted() const {
	assert(body_);
	return body_->isReaderStarted();
}
uint64_t TupleList::getActiveReaderCount() const{
	assert(body_);
	return body_->getActiveReaderCount();
}
uint64_t TupleList::getDetachedReaderCount() const{
	assert(body_);
	return body_->getDetachedReaderCount();
}
uint64_t TupleList::getAccessTopMin() const{
	assert(body_);
	return body_->getAccessTopMin();
}

bool TupleList::isActive() {
	return (body_ != NULL);
}

const TupleList::Info& TupleList::getInfo() const {
	return body_->getInfo();
}

LocalTempStore& TupleList::getStore() {
	return body_->getStore();
}

LocalTempStore::GroupId TupleList::getGroupId() const {
	return body_->getGroupId();
}

LocalTempStore::ResourceId TupleList::getResourceId() const {
	return body_->getResourceId();
}

uint32_t TupleList::getBlockExpSize() const {
	return body_->getBlockExpSize();
}

uint32_t TupleList::getBlockSize() const {
	return static_cast<uint32_t>(1UL << body_->getBlockExpSize());
}

void TupleList::close() {
	if (body_ && body_->detached()) { 
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
				"Detached tupleList cannot close");
	} 
}

uint64_t TupleList::getBlockCount() const {
	return body_->getBlockCount();
}

uint64_t TupleList::getAllocatedBlockCount() const {
	return body_->getAllocatedBlockCount();
}

uint64_t TupleList::addAllocatedBlockCount() {
	return body_->addAllocatedBlockCount();
}

void TupleList::setValidBlockCount(uint64_t count) {
	body_->setValidBlockCount(count);
}

LocalTempStore::BlockId TupleList::getActiveTopBlockNth() const {
	return body_->getActiveTopBlockNth();
}

void TupleList::setActiveTopBlockNth(LocalTempStore::BlockId blockNth) {
	body_->setActiveTopBlockNth(blockNth);
}

void TupleList::append(const LocalTempStore::Block &block) {
	body_->append(block);
	addAllocatedBlockCount();
	setValidBlockCount(getAllocatedBlockCount());
	getStore().incrementAppendBlockCount(getGroupId());
}

LocalTempStore::BlockId TupleList::getBlockId(uint64_t pos) {
	return body_->getBlockId(pos);
}

bool TupleList::getBlockIdList(uint64_t pos, uint64_t count, BlockIdArray &blockIdList) {
	return body_->getBlockIdList(pos, count, blockIdList);
}

void TupleList::invalidateBlockId(uint64_t pos) {
	body_->invalidateBlockId(pos);
}

void TupleList::resetBlock(LocalTempStore::Block &block) {
	body_->resetBlock(block);
}

uint64_t TupleList::addReference() {
	return body_->addReference();
}

uint64_t TupleList::removeReference() {
	return body_->removeReference();
}

uint64_t TupleList::getReferenceCount() const {
	return body_->getReferenceCount();
}

bool TupleList::blockAppendable() const {
	return body_->blockAppendable();
}

void TupleList::setBlockAppendable(bool flag) {
	body_->setBlockAppendable(flag);
}

bool TupleList::tupleAppendable() const {
	return body_->tupleAppendable();
}

void TupleList::setTupleAppendable(bool flag) {
	body_->setTupleAppendable(flag);
}

bool TupleList::isPartial(const LocalTempStore::Block &block) {
	int32_t contiguousBlockNum = TupleBlockHeader::getContiguousBlockNum(static_cast<const uint8_t*>(block.data()));
	return contiguousBlockNum > 0;
}

bool TupleList::isEmpty(const LocalTempStore::Block &block) {
	int32_t contiguousBlockNum = TupleBlockHeader::getContiguousBlockNum(static_cast<const uint8_t*>(block.data()));
	uint32_t tupleCount = TupleBlockHeader::getTupleCount(static_cast<const uint8_t*>(block.data()));
	return (contiguousBlockNum == 0) && (tupleCount == 0);
}

size_t TupleList::tupleCount(const LocalTempStore::Block &block) {
	return TupleBlockHeader::getTupleCount(static_cast<const uint8_t*>(block.data()));
}
size_t TupleList::tupleCount(const void* blockAddr) {
	return TupleBlockHeader::getTupleCount(static_cast<const uint8_t*>(blockAddr));
}

int32_t TupleList::contiguousBlockCount(const LocalTempStore::Block &block) {
	return TupleBlockHeader::getContiguousBlockNum(static_cast<const uint8_t*>(block.data()));
}
int32_t TupleList::contiguousBlockCount(const void* blockAddr) {
	return TupleBlockHeader::getContiguousBlockNum(static_cast<const uint8_t*>(blockAddr));
}


uint32_t TupleList::getBlockExpSize(const LocalTempStore::Block &block) {
	return TupleBlockHeader::getBlockExpSize(static_cast<const uint8_t*>(block.data()));
}

uint32_t TupleList::getNextVarDataOffset(const Block &block) {
	return TupleBlockHeader::getNextVarDataOffset(static_cast<const uint8_t*>(block.data()));
}

void TupleList::setNextVarDataOffset(Block &block, uint32_t offset) {
	TupleBlockHeader::setNextVarDataOffset(static_cast<uint8_t*>(block.data()), offset);
}

void TupleList::initializeBlock(Block &block, uint32_t blockExpSize, uint16_t columnCount, uint32_t blockId) {
	uint8_t *blockTop = static_cast<uint8_t*>(block.data());
	TupleBlockHeader::setMagic(blockTop);
	TupleBlockHeader::setVersion(blockTop);
	TupleBlockHeader::setBlockExpSize(blockTop, blockExpSize);
	TupleBlockHeader::setColumnNum(blockTop, columnCount);
	TupleBlockHeader::setBlockId(blockTop, blockId);
	TupleBlockHeader::setContiguousBlockNum(blockTop, 0);
	TupleBlockHeader::setTupleCount(blockTop, 0);
	TupleBlockHeader::setColumnListChecksum(blockTop, 0);
	TupleBlockHeader::setNextFixDataOffset(blockTop, TupleList::BLOCK_HEADER_SIZE);
	TupleBlockHeader::setNextVarDataOffset(blockTop, 1U << blockExpSize);
}

int32_t TupleList::compareBlockContents(const Block &block1, const Block &block2) {
	const size_t blockSize1 = 1U << LocalTempStore::Block::Header::getBlockExpSize(block1.data());
	const size_t blockSize2 = 1U << LocalTempStore::Block::Header::getBlockExpSize(block2.data());
	if (blockSize1 != blockSize2) {
		return 1;
	}
	const uint8_t* addr1 = static_cast<const uint8_t*>(block1.data())
	  + LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
	const uint8_t* addr2 = static_cast<const uint8_t*>(block2.data())
	  + LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;

	const int32_t contiguousBlockNum1 =
	  LocalTempStore::Block::Header::getContiguousBlockNum(block1.data());
	const int32_t contiguousBlockNum2 =
	  LocalTempStore::Block::Header::getContiguousBlockNum(block2.data());
	const uint32_t fixOffset1 =
	  LocalTempStore::Block::Header::getNextFixDataOffset(block1.data());
	const uint32_t fixOffset2 =
	  LocalTempStore::Block::Header::getNextFixDataOffset(block1.data());
	const uint32_t varOffset1 =
	  LocalTempStore::Block::Header::getNextVarDataOffset(block1.data());
	const uint32_t varOffset2 =
	  LocalTempStore::Block::Header::getNextVarDataOffset(block2.data());

	if (contiguousBlockNum1 >= 0) {
		if (contiguousBlockNum2 < 0) {
			return 1;
		}
		if (fixOffset1 != fixOffset2) {
			return 1;
		}
		if (varOffset1 != varOffset2) {
			return 1;
		}
		if (blockSize1 == varOffset1) {
			const size_t size = fixOffset1 - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
			return memcmp(addr1, addr2, size);
		}
		else {
			const size_t size1 = fixOffset1 - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
			if (memcmp(addr1, addr2, size1) != 0) {
				return 1;
			}
			size_t varPartSize = blockSize1 - varOffset1;
			addr1 += (varOffset1 - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET);
			addr2 += (varOffset2 - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET);
			return memcmp(addr1, addr2, varPartSize);
		}
	}
	else {
		const size_t size = varOffset1 - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
		return memcmp(addr1, addr2, size);
	}
}


TupleList::Info::Info()
: columnTypeList_(NULL), blockSize_(0), columnCount_(0) {
}

bool TupleList::Info::isSameColumns(const Info &another) const {
	if (columnCount_ != another.columnCount_) {
		return false;
	}
	if (!columnTypeList_ || !another.columnTypeList_) {
		return false;
	}
	return (memcmp(columnTypeList_, another.columnTypeList_, columnCount_*2) == 0);
}

void TupleList::Info::getColumns(Column *column, size_t count) const {
	assert(column);
	assert(count <= columnCount_);
	if (!TupleList::Info::checkColumnCount(columnCount_)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_LIMIT_EXCEEDED,
					"Column count exceeds maximum size : " << columnCount_);
	}
	uint32_t offset = 0;
	for (size_t col = 0; col < count; ++col) {
		column->offset_ = offset;
		column->type_ = columnTypeList_[col];
		column->pos_ = static_cast<uint16_t>(col);
		offset += static_cast<uint32_t>(TupleColumnTypeUtils::getFixedSize(columnTypeList_[col]));
		++column;
	}
}

bool TupleList::Info::isNullable() const {
	for (size_t column = 0; column < columnCount_; ++column) {
		if (TupleColumnTypeUtils::isNullable(columnTypeList_[column])) {
			return true;
		}
	}
	return false;
}

bool TupleList::Info::hasVarType() const {
	for (size_t column = 0; column < columnCount_; ++column) {
		if (!TupleColumnTypeUtils::isSomeFixed(columnTypeList_[column])) {
			return true;
		}
	}
	return false;
}

size_t TupleList::Info::getFixedPartSize() const {
	size_t size = 0;
	for (size_t column = 0; column < columnCount_; ++column) {
		size += TupleColumnTypeUtils::getFixedSize(columnTypeList_[column]);
	}
	size_t nullBitSize = 0;
	if (isNullable()) {
		nullBitSize = (columnCount_ + 7) / 8;
	}
	return size + nullBitSize;
}

size_t TupleList::Info::getNullBitOffset() const {
	size_t offset = 0;
	for (size_t column = 0; column < columnCount_; ++column) {
		offset += TupleColumnTypeUtils::getFixedSize(columnTypeList_[column]);
	}
	return offset;
}


NanoTimestamp TupleNanoTimestamp::makeEmptyValue() {
	NanoTimestamp ts;
	ts.assign(0, 0);
	return ts;
}


const NanoTimestamp TupleNanoTimestamp::Constants::EMPTY_VALUE = makeEmptyValue();


TupleList::BlockReader::BlockReader()
: tupleList_(NULL)
, currentBlockNth_(0), isOnce_(false), isActive_(false), isDetached_(false) {
}

void TupleList::BlockReader::initialize(TupleList &tupleList) {
	if (tupleList.blockReader_ || tupleList.blockReaderDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKREADER_CREATE_FAILED,
			"Another blockreader has already exist.");
	}
	if (tupleList.getReaderCount() > 0 || tupleList.readerDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKREADER_CREATE_FAILED,
			"Another reader has already exist.");
	}
}

TupleList::BlockReader::BlockReader(TupleList &tupleList)
: tupleList_(&tupleList)
, currentBlockNth_(0), isOnce_(false), isActive_(false), isDetached_(false) {

	initialize(tupleList);

	if (tupleList.getActiveTopBlockNth() > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKREADER_CREATE_FAILED,
			"This tuplelist has already destroyed.");
	}
	tupleList.blockReader_ = this;
	isActive_ = true;
}

TupleList::BlockReader::BlockReader(
		TupleList &tupleList, TupleList::BlockReader::Image &image)
: tupleList_(&tupleList)
, currentBlockNth_(0), isOnce_(false), isActive_(false), isDetached_(false) {

	if (!tupleList_->blockReaderDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKREADER_CREATE_FAILED,
			"Detached blockreader has not existed.");
	}
	if (tupleList_->getActiveTopBlockNth() != image.activeTopBlockNth_) { 
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKREADER_CREATE_FAILED,
			"Attach failed. TupleList status has changed. "
			"(activeTopBlockNth: detached=" << image.activeTopBlockNth_ <<
			", current=" << tupleList.getActiveTopBlockNth() << ")");
	} 
	tupleList_->setBlockReaderDetached(false);
	initialize(tupleList);

	currentBlockNth_ = image.currentBlockNth_;
	isOnce_ = image.isOnce_;

	tupleList.blockReader_ = this;
	isActive_ = true;
}

void TupleList::BlockReader::detach(TupleList::BlockReader::Image &image) {
	image.clear();
	image.resourceId_ = tupleList_->getResourceId();
	image.blockCount_ = tupleList_->getBlockCount();
	image.currentBlockNth_ = currentBlockNth_;
	image.activeTopBlockNth_ = tupleList_->getActiveTopBlockNth();
	image.isOnce_ = isOnce_;

	tupleList_->setBlockReaderDetached(true);
	tupleList_->blockReader_ = NULL;

	isActive_ = false;
	isDetached_ = true;
}

TupleList::BlockReader::Image::Image(util::StackAllocator &alloc)
: alloc_(alloc), resourceId_(0), blockCount_(0),
  currentBlockNth_(0), activeTopBlockNth_(0), isOnce_(false) {
}

void TupleList::BlockReader::Image::clear() {
	resourceId_ = 0;
	blockCount_ = 0;
	currentBlockNth_ = 0;
	activeTopBlockNth_ = 0;
	isOnce_ = false;
}

TupleList::BlockReader::~BlockReader() try {
	close();
}
catch (...) { 
} 

TupleList& TupleList::BlockReader::getTupleList() const {
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist.");
	}
	return *tupleList_;
}

void TupleList::BlockReader::close() {
	if (isDetached_) {
		return;
	}
	isActive_ = false;
	currentBlockNth_ = 0;
	if (tupleList_) {
		tupleList_->blockReader_ = NULL;
		if (isOnce_) {
			BlockId blockNth = tupleList_->getActiveTopBlockNth();
			for (; blockNth < tupleList_->getBlockCount(); ++blockNth) {
				BlockId blockId = tupleList_->getBlockId(blockNth);
				if (UNDEF_BLOCKID != blockId) {
					tupleList_->getStore().getBufferManager().removeAssignment(blockId);
					tupleList_->invalidateBlockId(blockNth);
				}
			}
			tupleList_->setActiveTopBlockNth(blockNth + 1);
		}
	}
	tupleList_ = NULL;
}

void TupleList::BlockReader::setOnce() {
	if (currentBlockNth_ != 0) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_SETTING_CAN_NOT_CHANGE,
			"Setting can not change. (BlockReader has already started.)");
	}
	isOnce_ = true;
}

void TupleList::BlockReader::getBlockCount(uint64_t &blockCount, uint64_t &blockNth) {
	blockCount = tupleList_->getBlockCount();
	blockNth = currentBlockNth_;
}

bool TupleList::BlockReader::next(Block &tupleBlock) {
	assert(!isDetached_);
	uint64_t blockCount = tupleList_->getBlockCount();
	if (currentBlockNth_ == blockCount) {
		return false;
	}
	else {
		BlockId blockId = tupleList_->getBlockId(currentBlockNth_);
		assert(UNDEF_BLOCKID != blockId);
		Block nextBlock(tupleList_->getStore(), blockId); 
		if (nextBlock.data() == NULL) {
			return false;
		}
		else {
			int32_t contiguousBlockCount = TupleList::contiguousBlockCount(nextBlock);
			if (contiguousBlockCount > 0) {
				if (currentBlockNth_ + contiguousBlockCount >= blockCount) {
					return false;
				}
			}
			if (isOnce_ && currentBlockNth_ > 0) {
				BlockId blockId = tupleList_->getBlockId(currentBlockNth_ - 1);
				assert(UNDEF_BLOCKID != blockId);
				tupleList_->getStore().getBufferManager().removeAssignment(blockId);
				assert(currentBlockNth_ >= tupleList_->getActiveTopBlockNth());
				tupleList_->setActiveTopBlockNth(currentBlockNth_);
				tupleList_->invalidateBlockId(currentBlockNth_ - 1);
			}
			tupleBlock = nextBlock;
			++currentBlockNth_;
			tupleList_->getStore().incrementReadBlockCount(tupleList_->getGroupId());
		}
	}
	return true;
}

bool TupleList::BlockReader::isExists() {
	if (!tupleList_) {
		return false;
	}
	uint64_t blockCount = tupleList_->getBlockCount();
	if (currentBlockNth_ == blockCount) {
		return false;
	}
	else {
		BlockId blockId = tupleList_->getBlockId(currentBlockNth_);
		if (blockId == UNDEF_BLOCKID) {
			return false;
		}
		assert(UNDEF_BLOCKID != blockId);
		Block nextBlock(tupleList_->getStore(), blockId); 
		if (nextBlock.data() == NULL) {
			return false;
		}
	}
	return true;
}


void TupleList::BlockReader::isExists(bool &isExists, bool &isAvailable) {

	isExists = false;
	isAvailable = false;
	
	if (!tupleList_) {
		return;
	}
	uint64_t blockCount = tupleList_->getBlockCount();
	if (currentBlockNth_ == blockCount) {
		return;
	}
	else {
		BlockId blockId = tupleList_->getBlockId(currentBlockNth_);
		if (blockId == UNDEF_BLOCKID) {
			return;
		}
		assert(UNDEF_BLOCKID != blockId);
		Block nextBlock(tupleList_->getStore(), blockId); 
		if (nextBlock.data() == NULL) {
			return;
		}
		else {
			isExists = true;
			int32_t contiguousBlockCount = TupleList::contiguousBlockCount(nextBlock);
			if (contiguousBlockCount > 0) {
				if (currentBlockNth_ + contiguousBlockCount >= blockCount) {
					return;
				}
				else {
					isAvailable = true;
				}
			}
		}
	}
}



TupleList::BlockWriter::BlockWriter()
: tupleList_(NULL)
, isActive_(false) {
}

TupleList::BlockWriter::BlockWriter(TupleList &tupleList)
: tupleList_(&tupleList)
, isActive_(false) {
	if (tupleList.blockReader_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKWRITER_CREATE_FAILED,
			"Another blockreader has already exist.");
	}
	if (tupleList.getReaderCount() > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKWRITER_CREATE_FAILED,
			"Another reader has already exist.");
	}
	if (!tupleList.blockAppendable()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_BLOCKWRITER_CREATE_FAILED,
			"This tuplelist is not able to append block.");
	}
	tupleList.setTupleAppendable(false); 
	isActive_ = true;
}

TupleList::BlockWriter::~BlockWriter() try {
	close();
}
catch (...) {  
}  

TupleList& TupleList::BlockWriter::getTupleList() const {
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist.");
	}
	return *tupleList_;
}

void TupleList::BlockWriter::close() {
	isActive_ = false;
	tupleList_ = NULL;
}

void TupleList::BlockWriter::append(const Block &tupleBlock) {
	assert(tupleList_);
	tupleList_->append(tupleBlock);
}





TupleList::Reader::Reader(TupleList &tupleList, AccessOrder order)
: tupleList_(&tupleList), fixedAddr_(NULL), fixedEndAddr_(NULL)
, blockInfoArray_(tupleList.getStore().getVarAllocator())
, cxt_(NULL)
, accessOrder_(order), currentBlockNth_(0)
, keepTopBlockNth_(UNDEF_BLOCKID), keepMaxBlockNth_(0)
, headBlockNth_(0)
, fixedPartSize_(static_cast<uint32_t>(tupleList.getInfo().getFixedPartSize()))
, blockSize_(static_cast<uint32_t>(1UL << tupleList.getBlockExpSize()))
, blockExpSize_(tupleList.getBlockExpSize())
, positionOffsetMask_((1UL << tupleList.getBlockExpSize()) - 1)
, nullBitOffset_(tupleList.getInfo().getNullBitOffset())
, isNullable_(tupleList_->getInfo().isNullable())
, isActive_(false), isDetached_(false)
, positionCalled_(false)
{

	initialize(tupleList);

	if (tupleList.getActiveTopBlockNth() > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
				"This tuplelist has already destroyed.");
	}
	if (tupleList_->getBlockCount() > 0) {
		latchHeadBlock();
	}
	readerId_ = tupleList.registReader(*this);
	isActive_ = true;
}

TupleList::Reader::Reader(TupleList &tupleList, const TupleList::Reader::Image &image)
: tupleList_(&tupleList), fixedAddr_(NULL), fixedEndAddr_(NULL)
, blockInfoArray_(tupleList.getStore().getVarAllocator())
, cxt_(NULL)
, accessOrder_(image.accessOrder_), currentBlockNth_(image.currentBlockNth_)
, keepTopBlockNth_(image.keepTopBlockNth_), keepMaxBlockNth_(image.keepMaxBlockNth_)
, headBlockNth_(image.headBlockNth_)
, fixedPartSize_(static_cast<uint32_t>(tupleList.getInfo().getFixedPartSize()))
, blockSize_(static_cast<uint32_t>(1UL << tupleList.getBlockExpSize()))
, blockExpSize_(tupleList.getBlockExpSize())
, positionOffsetMask_((1UL << tupleList.getBlockExpSize()) - 1)
, nullBitOffset_(tupleList.getInfo().getNullBitOffset())
, isNullable_(tupleList_->getInfo().isNullable())
, isActive_(false), isDetached_(false)
, positionCalled_(false)
{

	if (!tupleList_->readerDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Detached reader has not existed.");
	}
	initialize(tupleList);

	if (image.resourceId_ != tupleList_->getResourceId()) { 
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Attach failed. TupleList status has changed. "
			"(ResourceId: detached=" << image.resourceId_ <<
			", current=" << tupleList_->getResourceId() << ")");
	}
	if (image.blockCount_ > tupleList_->getBlockCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Attach failed. TupleList status has changed. "
			"(blockCount: detached=" << image.blockCount_ <<
			", current=" << tupleList_->getBlockCount() << ")");
	}
	if (tupleList.getActiveTopBlockNth() != image.activeTopBlockNth_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Attach failed. TupleList status has changed. "
			"(activeTopBlockNth: detached=" << image.activeTopBlockNth_ <<
			", current=" << tupleList.getActiveTopBlockNth() << ")");
	} 
	readerId_ = image.readerId_;
	accessOrder_ = image.accessOrder_;
	currentBlockNth_ = image.currentBlockNth_;
	positionCalled_ = image.positionCalled_;

	if (image.fixedOffset_ == 0) {
		fixedAddr_ = NULL;
		fixedEndAddr_ = NULL;
		tupleList_->attachReader(readerId_, *this);
		isActive_ = true;
		return;
	}
	if (tupleList_->getBlockCount() > 0) {
		if (keepTopBlockNth_ != UNDEF_BLOCKID) {
			uint64_t maxPos = std::max(keepTopBlockNth_, keepMaxBlockNth_);
			assert(maxPos != UNDEF_BLOCKID);
			UNUSED_VARIABLE(maxPos);

			size_t initSize = (keepTopBlockNth_ < keepMaxBlockNth_) ?
					(keepMaxBlockNth_ - keepTopBlockNth_ + 1) : 1;
			blockInfoArray_.assign(initSize, LatchedBlockInfo());

			BlockId blockId = tupleList_->getBlockId(keepTopBlockNth_);
			assert(blockId != UNDEF_BLOCKID);
			blockInfoArray_[0].block_ = LocalTempStore::Block(tupleList_->getStore(), blockId);
			blockInfoArray_[0].addr_ = blockInfoArray_[0].block_.data();
			int32_t contiguousBlockNum =
					TupleBlockHeader::getContiguousBlockNum(blockInfoArray_[0].addr_);
			assert(contiguousBlockNum >= 0);
			if (initSize == 1 && contiguousBlockNum > 0) {
				blockInfoArray_.resize(contiguousBlockNum + 1);
			}
		}
		if (keepTopBlockNth_ != UNDEF_BLOCKID) {
			assert(keepTopBlockNth_ <= currentBlockNth_);
			assert(currentBlockNth_ <= keepMaxBlockNth_);
			if (keepTopBlockNth_ != currentBlockNth_) {
				uint64_t arrayOffset = currentBlockNth_ - keepTopBlockNth_;
				BlockId blockId = tupleList_->getBlockId(currentBlockNth_);
				assert(blockId != UNDEF_BLOCKID);
				assert(arrayOffset < blockInfoArray_.size());
				blockInfoArray_[arrayOffset].block_ =
						LocalTempStore::Block(tupleList_->getStore(), blockId);
				blockInfoArray_[arrayOffset].addr_ = blockInfoArray_[arrayOffset].block_.data();
				assert(blockInfoArray_[arrayOffset].addr_);
			}
			const uint8_t* blockAddr = getCurrentBlockAddr();
			assert(blockAddr);
			const int32_t contiguousBlockCount = TupleList::contiguousBlockCount(blockAddr);
			assert(contiguousBlockCount >= 0);
			if (currentBlockNth_ + contiguousBlockCount < tupleList_->getBlockCount()) {
				fixedAddr_ = blockAddr + TupleList::BLOCK_HEADER_SIZE;
				fixedEndAddr_ =
						fixedAddr_ + TupleList::tupleCount(blockAddr) * fixedPartSize_;
				assert(image.fixedOffset_ < tupleList_->getBlockSize());
				if (image.fixedOffset_ > 0) {
					assert(TupleList::BLOCK_HEADER_SIZE <= image.fixedOffset_);
					fixedAddr_ = blockAddr + image.fixedOffset_;
				}
				else { 
					fixedAddr_ = blockAddr + TupleList::BLOCK_HEADER_SIZE;
				}
			}
			else {
				fixedAddr_ = NULL;
				fixedEndAddr_ = NULL;
			}
		}
		else {
			fixedAddr_ = NULL;
			fixedEndAddr_ = NULL;
		} 
	}

	tupleList_->attachReader(readerId_, *this);
	isActive_ = true;
}

TupleList::Reader::~Reader() try {
	close();
}
catch (...) { 
} 

void TupleList::Reader::initialize(TupleList &tupleList) {
	assert(blockSize_ >= static_cast<uint32_t>(1UL << LocalTempStore::MIN_BLOCK_EXP_SIZE));
	assert(blockSize_ <= static_cast<uint32_t>(1UL << LocalTempStore::MAX_BLOCK_EXP_SIZE));
	if (tupleList.blockReader_ || tupleList.blockReaderDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Another blockreader has already exist.");
	}
	if (tupleList.writer_ || tupleList.writerDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
			"Another writer has already exist.");
	}
	blockInfoArray_.clear();
}

void TupleList::Reader::detach(TupleList::Reader::Image &image) {
	if (!tupleList_ || !tupleList_->isActive()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	image.clear();
	image.resourceId_ = tupleList_->getResourceId();
	image.blockCount_ = tupleList_->getBlockCount();
	image.currentBlockNth_ = currentBlockNth_;
	image.readerId_ = readerId_;
	if (fixedAddr_) {
		assert(getCurrentBlockAddr());
		image.fixedOffset_ = static_cast<uint32_t>(
				fixedAddr_ - getCurrentBlockAddr());
	}
	else {
		image.fixedOffset_ = 0;
	}
	assert(image.fixedOffset_ < tupleList_->getBlockSize());

	image.keepTopBlockNth_ = keepTopBlockNth_;
	image.keepMaxBlockNth_ = keepMaxBlockNth_;
	image.headBlockNth_ = headBlockNth_;
	image.activeTopBlockNth_ = tupleList_->getActiveTopBlockNth();

	image.accessOrder_ = accessOrder_;
	image.positionCalled_ = positionCalled_;

	tupleList_->detachReader(readerId_);
	blockInfoArray_.clear();

	isDetached_ = true;
	isActive_ = false;
}

bool TupleList::Reader::latchHeadBlock() {
	assert(tupleList_->getBlockCount() > 0);
	BlockId blockId = tupleList_->getBlockId(currentBlockNth_);
	assert(UNDEF_BLOCKID != blockId);
	LocalTempStore::Block block(tupleList_->getStore(), blockId);

	const int32_t contiguousBlockCount = TupleList::contiguousBlockCount(block);
	assert(contiguousBlockCount >= 0);
	if (currentBlockNth_ + contiguousBlockCount < tupleList_->getBlockCount()) {
		keepTopBlockNth_ = currentBlockNth_;
		keepMaxBlockNth_ = currentBlockNth_ + contiguousBlockCount;
		blockInfoArray_.assign(contiguousBlockCount + 1, LatchedBlockInfo());

		blockInfoArray_[0].block_ = block;
		blockInfoArray_[0].addr_ = block.data();
		assert(blockInfoArray_[0].block_.getBlockId() == block.getBlockId());
		const uint8_t* blockAddr = getCurrentBlockAddr();
		assert(blockAddr);
		assert(TupleList::tupleCount(blockAddr) > 0);
		fixedAddr_ = blockAddr + TupleList::BLOCK_HEADER_SIZE;
		fixedEndAddr_ = fixedAddr_ 
				+ TupleList::tupleCount(blockAddr) * fixedPartSize_;

		tupleList_->getStore().incrementReadBlockCount(tupleList_->getGroupId());
		return true;
	}
	else {
		return false;
	}
}

TupleList::Reader::Image::Image(util::StackAllocator &alloc)
: resourceId_(0), blockCount_(0), readerId_(-1),
  accessOrder_(ORDER_SEQUENTIAL), currentBlockNth_(0),
  keepTopBlockNth_(UNDEF_BLOCKID), keepMaxBlockNth_(0),
  headBlockNth_(0),
  activeTopBlockNth_(0),
  positionCalled_(false) {
	static_cast<void>(alloc);
}

void TupleList::Reader::Image::clear() {
	resourceId_ = 0;
	blockCount_ = 0;
	readerId_ = -1;
	accessOrder_ = ORDER_SEQUENTIAL;
	currentBlockNth_ = 0;
	keepTopBlockNth_ = UNDEF_BLOCKID;
	keepMaxBlockNth_ = 0;
	headBlockNth_ = 0;
	activeTopBlockNth_ = 0;
	positionCalled_ = false;
}


TupleList& TupleList::Reader::getTupleList() const {
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist.");
	}
	return *tupleList_;
}

uint64_t TupleList::Reader::getFollowingBlockCount() const {
	if (!tupleList_) {
		assert(false);
		return 0;
	}

	const uint64_t totalCount = tupleList_->getBlockCount();
	return totalCount - std::min(totalCount, currentBlockNth_);
}

uint64_t TupleList::Reader::getReferencingBlockCount() const {
	if (!tupleList_) {
		assert(false);
		return 0;
	}
	return blockInfoArray_.size();
}


void TupleList::Reader::positionWithLatch(BlockId newBlockNth) {
	assert(newBlockNth < tupleList_->getBlockCount());

	const size_t baseOffset = static_cast<size_t>(newBlockNth - keepTopBlockNth_);
	assert(baseOffset < blockInfoArray_.size());

	LocalTempStore::Block &newBlock = blockInfoArray_[baseOffset].block_;
	assert(!newBlock.data());

	BlockId blockId = tupleList_->getBlockId(newBlockNth);
	assert(UNDEF_BLOCKID != blockId);
	newBlock = LocalTempStore::Block(tupleList_->getStore(), blockId);
	blockInfoArray_[baseOffset].addr_ = newBlock.data();
	assert(newBlock.data());

	currentBlockNth_ = newBlockNth;
	assert(newBlock.getBlockId() == tupleList_->getBlockId(newBlockNth));
	tupleList_->getStore().incrementReadBlockCount(tupleList_->getGroupId());
	fixedEndAddr_ = static_cast<const uint8_t*>(newBlock.data())
			+ TupleList::BLOCK_HEADER_SIZE
			+ TupleList::tupleCount(newBlock) * fixedPartSize_;
}


void TupleList::Reader::close() {
	if (isDetached_) {
		return;
	}
	if (tupleList_) {
		if ((ORDER_RANDOM == accessOrder_)
				&& (UNDEF_BLOCKID != keepTopBlockNth_)) {
			discard();
		}
		blockInfoArray_.clear();
		tupleList_->setReaderTopNth(readerId_, UINT64_MAX);
		tupleList_->closeReader(readerId_);
	}
	tupleList_ = NULL;
	fixedAddr_ = NULL;
	fixedEndAddr_ = NULL;
	currentBlockNth_ = 0;
	keepTopBlockNth_ = UNDEF_BLOCKID;
	keepMaxBlockNth_ = 0;
	isActive_ = false;
}

void TupleList::Reader::setVarContext(TupleValue::VarContext &context) {
	cxt_ = &context;
}

TupleValue::VarContext& TupleList::Reader::getVarContext() {
	assert(cxt_);
	return *cxt_;
}


void TupleList::Reader::assign(const Reader &reader){
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (ORDER_RANDOM != accessOrder_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
			"This operation is only supported in random access mode.");
	}
#endif
	uint64_t newBlockNth = reader.currentBlockNth_;
	size_t newBlockOffset = static_cast<size_t>(reader.fixedAddr_ - reader.getCurrentBlockAddr());
	assert(newBlockOffset < blockSize_);

	const uint8_t* currentBlockAddr = NULL;
	if (newBlockNth == currentBlockNth_) {
		currentBlockAddr = getCurrentBlockAddr();
		if (!currentBlockAddr) {
			latchHeadBlock();
			currentBlockAddr = getCurrentBlockAddr();
		}
	}
	else {
		currentBlockNth_ = newBlockNth;
		latchHeadBlock();
		currentBlockAddr = getCurrentBlockAddr();
	}
	assert(currentBlockAddr);
	assert(TupleList::BLOCK_HEADER_SIZE <= newBlockOffset);

	fixedAddr_ = currentBlockAddr + newBlockOffset;

	assert((fixedEndAddr_ - fixedAddr_) < static_cast<ptrdiff_t>(blockSize_));
}


void TupleList::Reader::nextBlock(int32_t contiguousBlockCount) {
	if (!tupleList_) { 
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	} 
	assert(keepTopBlockNth_ <= currentBlockNth_);

	if (currentBlockNth_ + contiguousBlockCount + 1 >= tupleList_->getBlockCount()) {
		return;
	}
	BlockId nextBlockNth = currentBlockNth_ +
			static_cast<BlockId>(contiguousBlockCount + 1);
	assert(nextBlockNth > keepTopBlockNth_);
	void *nextTopAddr = NULL;
	size_t nextBaseOffset = static_cast<size_t>(nextBlockNth - keepTopBlockNth_);
	if (nextBaseOffset < blockInfoArray_.size()) {
		nextTopAddr = blockInfoArray_[nextBaseOffset].addr_;
	}
	int32_t nextContiguousBlockCount = -1;
	if (!nextTopAddr) {
		LocalTempStore::Block nextTopBlock =
				LocalTempStore::Block(tupleList_->getStore(), tupleList_->getBlockId(nextBlockNth));
		nextContiguousBlockCount = TupleList::contiguousBlockCount(nextTopBlock);
		if (nextBaseOffset >= blockInfoArray_.size()) {
			blockInfoArray_.resize(nextBaseOffset + 1);
		}
		blockInfoArray_[nextBaseOffset].block_ = nextTopBlock;
		blockInfoArray_[nextBaseOffset].addr_ = nextTopBlock.data();
		tupleList_->getStore().incrementReadBlockCount(tupleList_->getGroupId());
	}
	else {
		nextContiguousBlockCount = TupleList::contiguousBlockCount(nextTopAddr);
	}
	assert(nextContiguousBlockCount >= 0);
	if ((nextBlockNth + nextContiguousBlockCount) >= tupleList_->getBlockCount()) {
		return;
	}
	if (positionCalled_) {
		if (blockInfoArray_.size() <= (nextBlockNth + nextContiguousBlockCount)) {
			blockInfoArray_.resize(nextBaseOffset + nextContiguousBlockCount + 1);
			assert(blockInfoArray_[nextBaseOffset].addr_);
		}
		else {
			assert(blockInfoArray_[nextBaseOffset].addr_);
		}
		if (keepMaxBlockNth_ < (nextBlockNth + nextContiguousBlockCount)) {
			keepMaxBlockNth_ = nextBlockNth + nextContiguousBlockCount;
		}
		assert(blockInfoArray_.size() == (keepMaxBlockNth_ - keepTopBlockNth_ + 1));
	}
	else {
		LocalTempStore::Block nextTopBlock =
				blockInfoArray_[nextBaseOffset].block_;
		assert(nextTopBlock.data());
		blockInfoArray_.clear();
		blockInfoArray_.resize(nextContiguousBlockCount + 1);
		blockInfoArray_[0].block_ = nextTopBlock;
		blockInfoArray_[0].addr_ = nextTopBlock.data();
		keepTopBlockNth_ = nextBlockNth;
		keepMaxBlockNth_ = nextBlockNth + nextContiguousBlockCount;
	}
	if ((ORDER_SEQUENTIAL == accessOrder_)) {
		tupleList_->setReaderTopNth(readerId_, nextBlockNth);
		headBlockNth_ = nextBlockNth; 
	}
	currentBlockNth_ = nextBlockNth;
	assert(getCurrentBlockAddr());

	fixedAddr_ = getCurrentBlockAddr() + TupleList::BLOCK_HEADER_SIZE;
	fixedEndAddr_ = fixedAddr_ + TupleList::tupleCount(getCurrentBlockAddr()) * fixedPartSize_;
}

void TupleList::Reader::updateKeepInfo() {
	if (TupleList::Reader::ORDER_RANDOM == accessOrder_) {
		size_t baseOffset = static_cast<size_t>(currentBlockNth_ - keepTopBlockNth_);
		if (!positionCalled_) {
			positionCalled_ = true;
			keepTopBlockNth_ = currentBlockNth_;
			keepMaxBlockNth_ = currentBlockNth_;
			baseOffset = 0;

			const int32_t contiguousBlockCount =
					TupleList::contiguousBlockCount(getCurrentBlockAddr());
			assert(contiguousBlockCount >= 0);
			if (contiguousBlockCount > 0) {
				keepMaxBlockNth_ += contiguousBlockCount;
				assert(blockInfoArray_.size() > static_cast<size_t>(contiguousBlockCount));
			}
			else {
				assert(blockInfoArray_.size() > 0);
			}
		}
		else {
			if (keepMaxBlockNth_ < currentBlockNth_) {
				assert(baseOffset < blockInfoArray_.size());
				assert(blockInfoArray_[baseOffset].addr_);

				const int32_t contiguousBlockCount =
						TupleList::contiguousBlockCount(blockInfoArray_[baseOffset].addr_);
				assert(contiguousBlockCount >= 0);
				if (contiguousBlockCount > 0) {
					keepMaxBlockNth_ = currentBlockNth_ + contiguousBlockCount;
					assert(baseOffset + contiguousBlockCount < blockInfoArray_.size());
				}
				else {
					keepMaxBlockNth_ = currentBlockNth_;
				}
			}
		}
		assert(currentBlockNth_ >= keepTopBlockNth_);
		assert(fixedAddr_ > getCurrentBlockAddr());
	}
}

void TupleList::Reader::discard() {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
	if (ORDER_RANDOM != accessOrder_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
			"This operation is only supported in random access mode.");
	}
#endif
	if (currentBlockNth_ ==  0) {
		return;
	}
	tupleList_->setReaderTopNth(readerId_, currentBlockNth_);
	headBlockNth_ = currentBlockNth_; 

	if (keepTopBlockNth_ < currentBlockNth_) {
		size_t diff = currentBlockNth_ - keepTopBlockNth_;
		for (size_t pos = 0; pos < diff; ++pos) {
			if (blockInfoArray_.size() > 0) {
				blockInfoArray_.pop_front();
			}
		}
		keepTopBlockNth_ = currentBlockNth_;
		assert(blockInfoArray_[0].block_.getBlockId() == tupleList_->getBlockId(currentBlockNth_));
	}
}

const uint8_t* TupleList::Reader::getBlockTopAddr(uint64_t pos) const {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
#endif
	LocalTempStore::BlockId posBlockNth =
			static_cast<LocalTempStore::BlockId>(pos >> blockExpSize_);

	const uint8_t* blockTopAddr = NULL;
	if (posBlockNth == currentBlockNth_) {
		blockTopAddr = getCurrentBlockAddr();
	}
	else {
		if ((keepTopBlockNth_ <= posBlockNth)
				&& (posBlockNth <= keepMaxBlockNth_)) {
			uint64_t arrayOffset = posBlockNth - keepTopBlockNth_;
			assert(arrayOffset < blockInfoArray_.size());
			if (!blockInfoArray_[arrayOffset].addr_) {
				GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
						"not latched");
			}
			blockTopAddr = static_cast<const uint8_t*>(blockInfoArray_[arrayOffset].addr_);
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
					"not latched");
		}
	}
	assert(blockTopAddr);
	return blockTopAddr;
}


const uint8_t* TupleList::Reader::createSingleVarValue(uint64_t baseBlockNth, uint64_t varOffset) {
	uint32_t blockCount = static_cast<uint32_t>(varOffset >> blockExpSize_);
	uint64_t offset = varOffset & positionOffsetMask_;
	assert(blockCount > 0);
	assert(keepTopBlockNth_ <= baseBlockNth);

	const size_t baseOffset = static_cast<size_t>(baseBlockNth - keepTopBlockNth_);
	assert(baseOffset < blockInfoArray_.size());
	assert(blockInfoArray_[baseOffset].addr_);

	if (baseOffset + blockCount >= blockInfoArray_.size()) {  
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_LTS_INVALID_POSITION,
				"VarOffset is out of range: varOffset=" << varOffset <<
				", blockCount=" << blockCount <<
				", arraySize=" << (blockInfoArray_.size() - baseOffset));
	}  
	const uint8_t *varTop = NULL;
	uint8_t *addr = static_cast<uint8_t*>(
			blockInfoArray_[baseOffset + blockCount].addr_);
	if (!addr) {
		BlockId blockId = tupleList_->getBlockId(baseBlockNth + blockCount);
		assert(UNDEF_BLOCKID != blockId);
		blockInfoArray_[baseOffset + blockCount].block_ =
				LocalTempStore::Block(tupleList_->getStore(), blockId);
		LocalTempStore::Block &targetBlock =
				blockInfoArray_[baseOffset + blockCount].block_;
		blockInfoArray_[baseOffset + blockCount].addr_ = targetBlock.data();

		varTop = static_cast<const uint8_t*>(targetBlock.data()) + offset;
		tupleList_->getStore().incrementReadBlockCount(tupleList_->getGroupId());
	}
	else {
		varTop = addr + offset;
	}
	return varTop;
}


TupleValue TupleList::Reader::createMultiVarValue(
	uint64_t baseBlockNth, uint64_t varOffset, TupleList::TupleColumnType type) {
	assert(cxt_);
	assert(keepTopBlockNth_ <= baseBlockNth);
	const size_t baseOffset = static_cast<size_t>(baseBlockNth - keepTopBlockNth_);
	assert(baseOffset < blockInfoArray_.size());
	assert(blockInfoArray_[baseOffset].addr_);

	const uint8_t* baseBlockTop = static_cast<const uint8_t*>(
			blockInfoArray_[baseOffset].addr_);

	const void* varTop = NULL;
	if (varOffset < blockSize_) {
		varTop = baseBlockTop + varOffset;
	}
	else {
		uint32_t blockCount = static_cast<uint32_t>(varOffset >> blockExpSize_);
		uint64_t offset = varOffset & positionOffsetMask_;
		assert(blockCount > 0);

		if (baseOffset + blockCount > blockInfoArray_.size()) {  
			assert(false);
			GS_THROW_USER_ERROR(GS_ERROR_LTS_INVALID_POSITION,
					"VarOffset is out of range: varOffset=" << varOffset <<
					", blockCount=" << blockCount <<
					", arraySize=" << (blockInfoArray_.size() - baseOffset));
		}  
		const uint8_t *addr = static_cast<const uint8_t*>(
				blockInfoArray_[baseOffset + blockCount].addr_);

		if (!addr) {
			BlockId blockId = tupleList_->getBlockId(baseBlockNth + blockCount);
			assert(UNDEF_BLOCKID != blockId);
			blockInfoArray_[baseOffset + blockCount].block_ =
					LocalTempStore::Block(tupleList_->getStore(), blockId);
			LocalTempStore::Block &targetBlock =
					blockInfoArray_[baseOffset + blockCount].block_;
			blockInfoArray_[baseOffset + blockCount].addr_ = targetBlock.data();

			varTop = static_cast<const uint8_t*>(targetBlock.data()) + offset;
			tupleList_->getStore().incrementReadBlockCount(tupleList_->getGroupId());
		}
		else {
			varTop = addr + offset;
		}
	}
	assert(varTop);
	uint64_t blockCount = static_cast<uint64_t>(
			TupleList::contiguousBlockCount(baseBlockTop)) + 1;
	TupleList::BlockIdArray blockIdList(*cxt_->getVarAllocator());
	tupleList_->getBlockIdList(baseBlockNth, blockCount, blockIdList);

	const uint64_t *partOffsetList = static_cast<const uint64_t *>(varTop);
	const uint64_t partCount = *partOffsetList++;

	TupleValueVarUtils::VarData *topVarData =
			TupleValue::StoreMultiVarData::createTupleListVarData(
					cxt_->getBaseVarContext(), tupleList_->getStore(),
					blockSize_, blockCount, &blockIdList[0],
					partCount, partOffsetList);

	return TupleValue(topVarData, type);
}

bool TupleList::Reader::existsDetail() {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
#endif
	assert(!isDetached_);
	if (isDetached_) {
		return false;
	}
	if (!fixedAddr_) {
		if (tupleList_->getBlockCount() > 0) {
			if (!latchHeadBlock()) {
				return false;
			}
		}
		else {
			return false;
		}
	}
	assert(fixedAddr_);
	assert(getCurrentBlockAddr());
	if (fixedAddr_ < fixedEndAddr_) {
		return true;
	}
	else {
		const uint8_t* blockAddr = getCurrentBlockAddr();
		fixedEndAddr_ = blockAddr + TupleList::BLOCK_HEADER_SIZE
				+ TupleList::tupleCount(blockAddr) * fixedPartSize_;

		if (fixedAddr_ < fixedEndAddr_) {
			return true;
		}
		const int32_t contiguousBlockCount = TupleList::contiguousBlockCount(blockAddr);
		assert(contiguousBlockCount >= 0);
		if (currentBlockNth_ + contiguousBlockCount + 1 < tupleList_->getBlockCount()) {
			nextBlock(contiguousBlockCount);
			return (fixedAddr_ < fixedEndAddr_);
		}
		else {
			return false;
		}
	}
}

bool TupleList::Reader::nextDetail() {
	{
		{
			if (!exists()) { 
				return false;
			}
			assert(fixedAddr_);
		}
		if ((fixedAddr_ + fixedPartSize_) < fixedEndAddr_) {
			fixedAddr_ += fixedPartSize_;
			return false;
		}
		else {
			fixedAddr_ += fixedPartSize_;
			const int32_t contiguousBlockCount = TupleList::contiguousBlockCount(
					getCurrentBlockAddr());
			assert(contiguousBlockCount >= 0);
			if (currentBlockNth_ + contiguousBlockCount + 1 < tupleList_->getBlockCount()) {
				nextBlock(contiguousBlockCount);
				return false;
			}
			else {
				return false;
			}
		}
	}
	return true;
}


TupleList::Writer::Writer(TupleList &tupleList)
: tupleList_(&tupleList), fixedAddr_(NULL), varTopAddr_(NULL), varTailAddr_(NULL)
, cxt_(NULL), storeLobHeader_(NULL)
, columnList_(tupleList.getStore().getVarAllocator())
, currentBlockNth_(0), nullBitOffset_(tupleList.getInfo().getNullBitOffset())
, tupleCount_(0), contiguousBlockCount_(0)
, fixedPartSize_(static_cast<uint32_t>(tupleList.getInfo().getFixedPartSize()))
, blockSize_(static_cast<uint32_t>(1UL << tupleList.getBlockExpSize()))
, maxAvailableSize_(static_cast<uint32_t>(1UL << tupleList.getBlockExpSize()) - TupleList::BLOCK_HEADER_SIZE)
, isNullable_(tupleList_->getInfo().isNullable()), isActive_(false), isDetached_(false)
{
	initialize(tupleList);

	isActive_ = true;
}

TupleList::Writer::Writer(TupleList &tupleList, const TupleList::Writer::Image &image)
: tupleList_(&tupleList), fixedAddr_(NULL), varTopAddr_(NULL), varTailAddr_(NULL)
, cxt_(NULL), storeLobHeader_(NULL)
, columnList_(tupleList.getStore().getVarAllocator())
, currentBlockNth_(0), nullBitOffset_(tupleList.getInfo().getNullBitOffset())
, tupleCount_(0), contiguousBlockCount_(0)
, fixedPartSize_(static_cast<uint32_t>(tupleList.getInfo().getFixedPartSize()))
, blockSize_(static_cast<uint32_t>(1UL << tupleList.getBlockExpSize()))
, maxAvailableSize_(static_cast<uint32_t>(1UL << tupleList.getBlockExpSize()) - TupleList::BLOCK_HEADER_SIZE)
, isNullable_(tupleList.getInfo().isNullable()), isActive_(false), isDetached_(false)
{
	if (!tupleList_->writerDetached()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
			"Detached writer has not existed.");
	}
	tupleList_->setWriterDetached(false);

	initialize(tupleList);
	if (image.resourceId_ != tupleList_->getResourceId()) {  
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
			"Attach failed. TupleList status has changed. "
			"(ResourceId: detached=" << image.resourceId_ <<
			", current=" << tupleList_->getResourceId() << ")");
	}
	if (image.blockCount_ != tupleList_->getAllocatedBlockCount()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
			"Attach failed. TupleList status has changed. "
			"(blockCount: detached=" << image.blockCount_ <<
			", current=" << tupleList_->getAllocatedBlockCount() << ")");
	}  
	contiguousBlockCount_ = image.contiguousBlockCount_;
	tupleCount_ = image.tupleCount_;
	isNullable_ = image.isNullable_;

	currentBlockNth_ = image.currentBlockNth_;

	if (tupleList_->getAllocatedBlockCount() > 0) {
		block_ = LocalTempStore::Block(tupleList_->getStore(),
				tupleList_->getBlockId(currentBlockNth_));
		int32_t contiguousBlockNum = TupleBlockHeader::getContiguousBlockNum(block_.data());

		if (image.tupleCount_ != TupleBlockHeader::getTupleCount(block_.data())) {  
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
				"Attach failed. TupleList status has changed. "
				"(tupleCount: detached=" << image.tupleCount_ <<
				", current=" << TupleBlockHeader::getTupleCount(block_.data()) << ")");
		}
		if (image.contiguousBlockCount_ != static_cast<uint32_t>(contiguousBlockNum)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
				"Attach failed. TupleList status has changed. "
				"(contiguousBlockCount: detached=" <<
				image.contiguousBlockCount_ <<
				", current=" << contiguousBlockNum << ")");
		}  
	}
	fixedAddr_ = NULL;
	varTopAddr_ = NULL;
	varTailAddr_ = NULL;
	tupleList_->writer_ = this;
	isActive_ = true;
}

TupleList::Writer::~Writer() {
	try {
		close();
	}
	catch (...) {  
	}  
}

void TupleList::Writer::initialize(TupleList &tupleList) {
	assert(blockSize_ >= static_cast<uint32_t>(1UL << LocalTempStore::MIN_BLOCK_EXP_SIZE));
	assert(blockSize_ <= static_cast<uint32_t>(1UL << LocalTempStore::MAX_BLOCK_EXP_SIZE));
	if (tupleList.writer_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
			"Another writer has already exist.");
	}
	if (!tupleList.tupleAppendable()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
			"This tuplelist can not use writer (block append mode or another writer has closed).");
	}
	if (tupleList.getReaderCount() > 0) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_WRITER_CREATE_FAILED,
			"Another reader has already exist.");
	}
	tupleList.setBlockAppendable(false); 
	tupleList.writer_ = this;
	const TupleList::Info &info = tupleList_->getInfo();
	TupleList::Column dummy;
	columnList_.assign(info.columnCount_, dummy);
	info.getColumns(&columnList_[0], info.columnCount_);
}

TupleList& TupleList::Writer::getTupleList() const {
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist.");
	}
	return *tupleList_;
}

void TupleList::Writer::close() {
	if (isDetached_) {
		return;
	}
	isActive_ = false;
	if (tupleList_) {
		if (block_.data()) {
			block_.setSwapped(false);
			TupleBlockHeader::setTupleCount(block_.data(), tupleCount_);
			TupleBlockHeader::setNextFixDataOffset(block_.data(),
				TupleList::BLOCK_HEADER_SIZE + fixedPartSize_ * tupleCount_);
			if (varTopAddr_) {
				if (!varTailAddr_) {
					uint32_t nextVarDataOffset = static_cast<uint32_t>(
						varTopAddr_ - static_cast<uint8_t*>(block_.data()));
					TupleBlockHeader::setNextVarDataOffset(block_.data(), nextVarDataOffset);
					assert(nextVarDataOffset <= blockSize_);
				}
				else {
					assert(!varBlock_.isSwapped());
					uint32_t nextVarDataOffset = static_cast<uint32_t>(
						varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));
					TupleBlockHeader::setNextVarDataOffset(varBlock_.data(), nextVarDataOffset);
					assert(nextVarDataOffset <= blockSize_);
				}
			}
			TupleBlockHeader::setContiguousBlockNum(block_.data(), contiguousBlockCount_);
			tupleList_->setValidBlockCount(tupleList_->getAllocatedBlockCount());
			if (tupleCount_ > 0 && tupleList_->writerHandler_) {
				try {
					(*tupleList_->writerHandler_)();
				}
				catch (std::exception &) {
				}
			}
		}
		tupleList_->writerHandler_ = NULL;
		tupleList_->writer_ = NULL;
		tupleList_->setTupleAppendable(false); 
	}
	currentBlockNth_ = UNDEF_BLOCKID;
	tupleCount_ = 0;
	contiguousBlockCount_ = 0;
	tupleList_ = NULL;
	fixedAddr_ = NULL;
	varTopAddr_ = NULL;
	varTailAddr_ = NULL;
}

void TupleList::Writer::setBlockHandler(WriterHandler &handler) {
	tupleList_->writerHandler_ = &handler;
}

void TupleList::Writer::nextDetail() {
	{
		if (!fixedAddr_ && tupleList_->getAllocatedBlockCount() > 0) {
			setupFirstAppend();
			if ((fixedAddr_ + fixedPartSize_ >= varTopAddr_)
				|| (contiguousBlockCount_ > CONTIGUOUS_BLOCK_THRESHOLD)) {
				nextBlock();
			}
		}
		else {
			nextBlock();
		}
	}
}

void TupleList::Writer::setVarContext(TupleValue::VarContext &context) {
	cxt_ = &context;
}

TupleValue::VarContext& TupleList::Writer::getVarContext() {
	assert(cxt_);
	return *cxt_;
}

void TupleList::Writer::allocateNewBlock() {
	LocalTempStore::Block block(tupleList_->getStore(), tupleList_->getBlockSize(),
			tupleList_->getGroupId()); 
	block.setSwapped(false);

	tupleList_->resetBlock(block);
	tupleList_->append(block); 

	currentBlockNth_ = static_cast<BlockId>(tupleList_->getAllocatedBlockCount() - 1);
	fixedAddr_ = static_cast<uint8_t*>(block.data())
			+ TupleList::BLOCK_HEADER_SIZE;
	varTopAddr_ = static_cast<uint8_t*>(block.data()) + blockSize_;
	varTailAddr_ = NULL;
	tupleCount_ = 0;
	uint8_t *blockTop = static_cast<uint8_t*>(block.data());
	TupleBlockHeader::setTupleCount(blockTop, 0);
	TupleBlockHeader::setNextFixDataOffset(blockTop, TupleList::BLOCK_HEADER_SIZE);
	TupleBlockHeader::setNextVarDataOffset(blockTop, blockSize_);
	contiguousBlockCount_ = 0;
	block_ = block;
	varBlock_ = block;
	assert(!block_.isSwapped());
}

void TupleList::Writer::setupFirstAppend() {
	currentBlockNth_ = static_cast<BlockId>(
			tupleList_->getAllocatedBlockCount() - 1);
	block_ = LocalTempStore::Block(tupleList_->getStore(),
			tupleList_->getBlockId(currentBlockNth_));
	int32_t contiguousBlockNum = TupleBlockHeader::getContiguousBlockNum(block_.data());
	if (contiguousBlockNum < 0) {
		allocateNewBlock(); 
		tupleCount_ = 0;
		tupleList_->setValidBlockCount(currentBlockNth_); 
		if (tupleList_->writerHandler_) {
				(*tupleList_->writerHandler_)();
			}
		return;
	}
	block_.setSwapped(false);
	tupleCount_ = TupleBlockHeader::getTupleCount(block_.data());
	tupleList_->setValidBlockCount(currentBlockNth_);

	fixedAddr_ = static_cast<uint8_t*>(block_.data())
	  + TupleList::BLOCK_HEADER_SIZE
		+ fixedPartSize_ * tupleCount_;
	varBlock_ = block_;
	varTailAddr_ = NULL;
	contiguousBlockCount_ = 0;
	varTopAddr_ = static_cast<uint8_t*>(block_.data())
	  + TupleBlockHeader::getNextVarDataOffset(block_.data());

}

uint64_t TupleList::Writer::appendPartDataArea(
		size_t requestSize, bool splittable,
		uint8_t* &reservedAddr, size_t &reservedSize) {

	assert(!block_.isSwapped());
	if (!splittable && (requestSize > maxAvailableSize_)) { 
		GS_THROW_USER_ERROR(GS_ERROR_LTS_NOT_IMPLEMENTED,
				"The size of data is too large. blockSize=" << blockSize_ <<
				", requestSize=" << requestSize);
	} 
	reservedAddr = NULL;
	reservedSize = 0;
	if (!varTailAddr_) {


		if (static_cast<size_t>(varTopAddr_ - (fixedAddr_ + fixedPartSize_)) > requestSize) {
			reservedSize = requestSize;

			uint8_t *dataTopAddr = static_cast<uint8_t*>(block_.data());
			const int32_t fixSum = static_cast<int32_t>(
					fixedAddr_ + fixedPartSize_ - dataTopAddr - TupleList::BLOCK_HEADER_SIZE);
			assert(fixSum >= 0);
			const int32_t varSum = static_cast<int32_t>(
					dataTopAddr + blockSize_ - varTopAddr_);
			assert(varSum >= 0);
			if (fixSum < (static_cast<int32_t>(blockSize_ / 2) - varSum)) {
				varTopAddr_ -= requestSize;
				assert(fixedAddr_ + fixedPartSize_ <= varTopAddr_);
				reservedAddr = varTopAddr_;
				uint64_t varOffset = static_cast<uint64_t>(
						varTopAddr_ - static_cast<uint8_t*>(block_.data()));
				return varOffset;
			}
		}
	}
	else {
		assert(!varBlock_.isSwapped());
		assert(static_cast<uint8_t*>(varBlock_.data()) < varTailAddr_);
		uint8_t *blockEnd = static_cast<uint8_t*>(varBlock_.data())
				+ blockSize_;
		size_t availableSize = static_cast<size_t>(blockEnd - varTailAddr_);
		if (availableSize > requestSize) {
			assert(varTailAddr_ <= blockEnd);
			reservedSize = requestSize;
			reservedAddr = varTailAddr_;

			uint64_t varOffset = static_cast<uint64_t>(
					contiguousBlockCount_ * blockSize_ +
					varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));

			varTailAddr_ += requestSize;
			assert(static_cast<uint32_t>(varTailAddr_ - static_cast<uint8_t*>(varBlock_.data())) <= blockSize_);
			return varOffset;
		}
		else {
			if (splittable && (availableSize > TupleValueUtils::INT32_MAX_ENCODE_SIZE)) {
				reservedSize = availableSize;
				reservedAddr = varTailAddr_;

				uint64_t varOffset = static_cast<uint64_t>(
						contiguousBlockCount_ * blockSize_ +
						varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));

				varTailAddr_ += availableSize;
				assert(static_cast<uint32_t>(varTailAddr_ - static_cast<uint8_t*>(varBlock_.data())) <= blockSize_);
				return varOffset;
			}
		}
	}
	if (!varTailAddr_) {
		uint32_t nextVarDataOffset = static_cast<uint32_t>(
			varTopAddr_ - static_cast<uint8_t*>(block_.data()));
		TupleBlockHeader::setNextVarDataOffset(block_.data(), nextVarDataOffset);
		assert(nextVarDataOffset <= blockSize_);
	}
	else {
		uint32_t nextVarDataOffset = static_cast<uint32_t>(
			varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));
		TupleBlockHeader::setNextVarDataOffset(varBlock_.data(), nextVarDataOffset);
		assert(nextVarDataOffset <= blockSize_);
	}

	LocalTempStore::Block block(
			tupleList_->getStore(), tupleList_->getBlockSize(),
			tupleList_->getGroupId()); 
	block.setSwapped(false);
	tupleList_->resetBlock(block);
	tupleList_->append(block); 
	TupleBlockHeader::setNextVarDataOffset(block.data(), TupleList::BLOCK_HEADER_SIZE);
	varBlock_ = block;
	varTailAddr_ = static_cast<uint8_t*>(varBlock_.data()) + TupleList::BLOCK_HEADER_SIZE;

	++contiguousBlockCount_;
	TupleBlockHeader::setContiguousBlockNum(block_.data(), contiguousBlockCount_);
	int32_t distance = 0 - static_cast<int32_t>(contiguousBlockCount_);
	TupleBlockHeader::setContiguousBlockNum(varBlock_.data(), distance);

	reservedSize = (requestSize < maxAvailableSize_) ? requestSize : maxAvailableSize_;
	reservedAddr = varTailAddr_;
	assert((static_cast<uint8_t*>(varBlock_.data()) + blockSize_) >= (varTailAddr_ + reservedSize));

	uint32_t varBlockOffset = static_cast<uint32_t>(
			varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));
	assert(varBlockOffset <= blockSize_);

	uint64_t varOffset = static_cast<uint64_t>(
			contiguousBlockCount_ * blockSize_ + varBlockOffset);

	varTailAddr_ += reservedSize;
	assert(static_cast<uint32_t>(varTailAddr_ - static_cast<uint8_t*>(varBlock_.data())) <= blockSize_);
	varBlockOffset = static_cast<uint32_t>(
			varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));
	assert(varBlockOffset <= blockSize_);
	TupleBlockHeader::setNextVarDataOffset(varBlock_.data(), varBlockOffset);

	return varOffset;
}


void TupleList::Writer::reserveLobPartInfo(size_t initialSize) {
	assert(initialSize > 0);
	assert(cxt_);
	size_t allocateSize = sizeof(TupleValue::StoreLobHeader) +
			(initialSize - 1) * sizeof(uint64_t) + 1;

	const TupleValueVarUtils::VarDataType type =
			TupleValueVarUtils::VAR_DATA_TEMP_STORE;
	TupleValueVarUtils::VarData *varData = cxt_->allocate(type, allocateSize);

	storeLobHeader_ = static_cast<TupleValue::StoreLobHeader*>(varData->getBody());
	storeLobHeader_->storeVarDataType_ =
			TupleValueTempStoreVarUtils::LTS_VAR_DATA_NONE;
	storeLobHeader_->blockSize_ = 0;
	storeLobHeader_->reservedCount_ = initialSize;
	storeLobHeader_->validCount_ = 0;
	storeLobHeader_->resetPartDataOffset();
}

void TupleList::Writer::updateLobPartDataOffset(size_t index, size_t offset) {
	TupleValue::StoreLobHeader* newLobHeader =
			TupleValue::StoreMultiVarData::updateLobPartOffset(
					cxt_->getBaseVarContext(), NULL, storeLobHeader_, index, offset);
	if (newLobHeader) { 
		storeLobHeader_ = newLobHeader;
	} 
}

void TupleList::Writer::getLobPartInfo(void *&lobInfoAddr, size_t &byteSize) {
	assert(storeLobHeader_);
	lobInfoAddr = &storeLobHeader_->validCount_;
	byteSize = static_cast<size_t>(storeLobHeader_->validCount_ + 1) * sizeof(uint64_t);
}


void TupleList::Writer::nextBlock() {
	if (fixedAddr_) {
		block_.setSwapped(false);
		TupleBlockHeader::setTupleCount(block_.data(), tupleCount_);
		TupleBlockHeader::setNextFixDataOffset(block_.data(),
			TupleList::BLOCK_HEADER_SIZE + fixedPartSize_ * tupleCount_);
		TupleBlockHeader::setContiguousBlockNum(block_.data(), contiguousBlockCount_);
		if (varTailAddr_) {
			assert(varTailAddr_ <= (static_cast<uint8_t*>(varBlock_.data()) + blockSize_));
			uint32_t varTopDataOffset = static_cast<uint32_t>(
				varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));
			TupleBlockHeader::setNextVarDataOffset(varBlock_.data(), varTopDataOffset);
		}
		else {
			if (varTopAddr_) {
				uint32_t varTopDataOffset = static_cast<uint32_t>(
						varTopAddr_ - static_cast<uint8_t*>(block_.data()));
				TupleBlockHeader::setNextVarDataOffset(block_.data(), varTopDataOffset);
				assert(varTopDataOffset <= blockSize_);
			}
		}
	}
	allocateNewBlock(); 
	tupleCount_ = 0;
	tupleList_->setValidBlockCount(currentBlockNth_); 
	if (tupleList_->writerHandler_ && (tupleList_->getBlockCount() > 0)) {
			(*tupleList_->writerHandler_)();
		}
}


void TupleList::Writer::setSingleVarData(const Column &column, const TupleValue &value) {
	assert(!block_.isSwapped());
	size_t dataSize;
	size_t headerSize;
	const uint8_t* varDataAddr = static_cast<const uint8_t*>(value.varData(headerSize, dataSize));
	const size_t storedSize = dataSize + headerSize;
	assert(storedSize <= (blockSize_ - TupleList::BLOCK_HEADER_SIZE));

	uint8_t* reservedAddr;
	size_t reservedSize;
	uint64_t varOffset = appendPartDataArea(storedSize, false, reservedAddr, reservedSize);
	assert(storedSize == reservedSize);
	memcpy(reservedAddr, (varDataAddr - headerSize), reservedSize);
	setVarDataOffset(fixedAddr_, column, value.getType(), varOffset);
}

void TupleList::Writer::setSingleVarData(const Column &column, const TupleString &value) {
	assert(!block_.isSwapped());
	TupleString::BufferInfo bufferInfo = value.getBuffer();
	const uint8_t* dataAddr = static_cast<const uint8_t*>(value.data());
	const size_t headerSize = TupleValueUtils::varSizeIs1Byte(dataAddr) ? 1 : 4;
	const size_t dataSize = bufferInfo.second;
	const size_t storedSize = dataSize + headerSize;
	assert(storedSize <= (blockSize_ - TupleList::BLOCK_HEADER_SIZE));

	uint8_t* reservedAddr;
	size_t reservedSize;
	uint64_t varOffset = appendPartDataArea(storedSize, false, reservedAddr, reservedSize);
	assert(storedSize == reservedSize);
	memcpy(reservedAddr, dataAddr, reservedSize);
	setVarDataOffset(fixedAddr_, column, TupleList::TYPE_STRING, varOffset);
}

void TupleList::Writer::appendLobPart(const void* partData, size_t partSize, size_t &partCount) {
	size_t remain = partSize;
	const uint8_t* srcAddr = static_cast<const uint8_t*>(partData);
	while (remain > 0) {
		uint8_t* reservedAddr;
		size_t reservedSize;
		uint64_t varOffset = appendPartDataArea(
				remain + TupleValueUtils::INT32_MAX_ENCODE_SIZE,
				true, reservedAddr, reservedSize);
		size_t reservedBodySize =
				reservedSize - TupleValueUtils::INT32_MAX_ENCODE_SIZE;
		TupleValueUtils::setPartData(
				reservedAddr, srcAddr, static_cast<uint32_t>(reservedBodySize));
		remain -= reservedBodySize;
		srcAddr += reservedBodySize;

		updateLobPartDataOffset(partCount, static_cast<size_t>(varOffset));
		++partCount;
	}
}

void TupleList::Writer::setLobVarData(const Column &column, const TupleValue &value) {
	assert(!block_.isSwapped());
	assert(cxt_);
	TupleValue::LobReader lobReader(value);
	const void* partData;
	size_t partSize;
	size_t partCount = 0;
	size_t srcPartCount = static_cast<size_t>(lobReader.getPartCount());
	reserveLobPartInfo(srcPartCount + 1);

	size_t memcpySize = 0;
	size_t destSize = 0;
	if (!tempBlock_.data()) {
		LocalTempStore::Block block(
				tupleList_->getStore(), tupleList_->getBlockSize(),
				tupleList_->getGroupId());
		tempBlock_ = block;
	}
	uint8_t* tempTop = static_cast<uint8_t*>(tempBlock_.data());
	assert(tempTop);
	uint8_t* tempTail = tempTop + blockSize_;
	UNUSED_VARIABLE(tempTail);
	tempTop += LocalTempStore::Block::Header::BLOCK_HEADER_SIZE;
	const size_t tempLimit = blockSize_ - LocalTempStore::Block::Header::BLOCK_HEADER_SIZE;
	const size_t tempSaveLimit = tempLimit / 2;
	uint8_t* tempAddr = tempTop;
	size_t tempRemain = tempLimit;

	while (lobReader.next(partData, partSize)) {
		if (partSize < tempRemain) {
			memcpy(tempAddr, partData, partSize);
			tempAddr += partSize;
			assert(tempAddr <= tempTail);
			tempRemain -= partSize;
			memcpySize += partSize;
		}
		else {
			size_t tempSavedSize = tempAddr - tempTop;
			if (tempSavedSize > 0) {
				assert(tempSavedSize <= tempLimit);
				appendLobPart(tempTop, tempSavedSize, partCount);
				tempAddr = tempTop;
				tempRemain = tempLimit;
				destSize += tempSavedSize;
			}
			assert(tempRemain == tempLimit);
			if (tempSaveLimit < partSize) {
				appendLobPart(partData, partSize, partCount);
				destSize += partSize;
			}
			else {
				memcpy(tempAddr, partData, partSize);
				tempAddr += partSize;
				assert(tempAddr <= tempTail);
				assert(partSize < tempRemain);
				tempRemain -= partSize;
				memcpySize += partSize;
			}
		}
	}
	assert(tempTop <= tempAddr);
	size_t tempSavedSize = tempAddr - tempTop;
	if (tempSavedSize > 0) {
		assert(tempSavedSize <= tempLimit);
		appendLobPart(tempTop, tempSavedSize, partCount);
		destSize += tempSavedSize;
	}
	void* lobPartInfoAddr;
	size_t lobPartInfoByteSize;
	getLobPartInfo(lobPartInfoAddr, lobPartInfoByteSize);

	uint8_t* reservedAddr;
	size_t reservedSize;
	uint64_t varOffset = appendPartDataArea(lobPartInfoByteSize, false,
		reservedAddr, reservedSize);
	assert(reservedSize == lobPartInfoByteSize);
	memcpy(reservedAddr, lobPartInfoAddr, lobPartInfoByteSize);
	setVarDataOffset(fixedAddr_, column, value.getType(), varOffset);
}


uint8_t* TupleList::Writer::getFixedAddr(uint64_t pos) {
#ifndef NDEBUG
	if (!tupleList_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_IS_NOT_EXIST,
			"TupleList is not exist(or already closed).");
	}
#endif
	LocalTempStore::BlockId newBlockNth =
			static_cast<LocalTempStore::BlockId>(pos >> tupleList_->getBlockExpSize());

	uint8_t* currentBlockAddr = NULL;
	if (newBlockNth == currentBlockNth_) {
		currentBlockAddr = static_cast<uint8_t*>(block_.data());
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
				"not latched");
	}
	assert(currentBlockAddr);

	const uint32_t positionOffsetMask = static_cast<uint32_t>(
			(1UL << tupleList_->getBlockExpSize()) - 1);
	uint32_t newOffset = static_cast<uint32_t>(
			pos & positionOffsetMask);
	assert(newOffset < blockSize_);
	assert(TupleList::BLOCK_HEADER_SIZE <= newOffset);
	assert(newOffset == (pos - newBlockNth * blockSize_));

	uint8_t* fixedAddr = currentBlockAddr + newOffset;
	if (fixedAddr != fixedAddr_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_OPERATION_NOT_SUPPORTED,
				"Invalid cursor");
	}
	return fixedAddr;
}


void TupleList::Writer::detach(TupleList::Writer::Image& image) {
	image.clear();
	image.resourceId_ = tupleList_->getResourceId();
	image.blockCount_ = tupleList_->getAllocatedBlockCount();
	image.currentBlockNth_ = currentBlockNth_;
	image.tupleCount_ = tupleCount_;
	image.contiguousBlockCount_ = contiguousBlockCount_;
	image.isNullable_ = isNullable_;

	if (tupleList_) {
		if (block_.data()) {
			block_.setSwapped(false);
			TupleBlockHeader::setTupleCount(block_.data(), tupleCount_);
			TupleBlockHeader::setNextFixDataOffset(block_.data(),
				TupleList::BLOCK_HEADER_SIZE + fixedPartSize_ * tupleCount_);
			if (varTopAddr_) {
				if (!varTailAddr_) {
					uint32_t nextVarDataOffset = static_cast<uint32_t>(
						varTopAddr_ - static_cast<uint8_t*>(block_.data()));
					TupleBlockHeader::setNextVarDataOffset(block_.data(), nextVarDataOffset);
					assert(nextVarDataOffset <= blockSize_);
				}
				else {
					uint32_t nextVarDataOffset = static_cast<uint32_t>(
						varTailAddr_ - static_cast<uint8_t*>(varBlock_.data()));
					TupleBlockHeader::setNextVarDataOffset(varBlock_.data(), nextVarDataOffset);
					assert(nextVarDataOffset <= blockSize_);
				}
			}
			TupleBlockHeader::setContiguousBlockNum(block_.data(), contiguousBlockCount_);
		}
		tupleList_->setWriterDetached(true);
		tupleList_->writer_ = NULL;
	}
	isDetached_ = true;
	isActive_ = false;
}

TupleList::Writer::Image::Image(util::StackAllocator &alloc)
: alloc_(alloc), resourceId_(0), blockCount_(0),
  currentBlockNth_(0), tupleCount_(0),
  contiguousBlockCount_(0), isNullable_(false) {
}

void TupleList::Writer::Image::clear() {
	resourceId_ = 0;
	blockCount_ = 0;
	currentBlockNth_ = 0;
	tupleCount_ = 0;
	contiguousBlockCount_ = 0;
	isNullable_ = false;
}


TupleList::WriterHandler::WriterHandler() {
}

TupleList::WriterHandler::~WriterHandler() {
}
const char8_t* TupleList::TupleColumnTypeCoder::operator()(TupleColumnType type) const {
	switch (type) {
	case TYPE_NULL:
		return "NULL";
	case TYPE_ANY:
		return "ANY";
	}

	const size_t off =
			((type & TYPE_MASK_NULLABLE) == 0 ? strlen("NULLABLE_") : 0);
	const bool a = ((type & TYPE_MASK_ARRAY) != 0);

	switch (type & ~(TYPE_MASK_NULLABLE | TYPE_MASK_ARRAY)) {
	case TYPE_BYTE:
		return off + (a ? "NULLABLE_BYTE_ARRAY" : "NULLABLE_BYTE");
	case TYPE_SHORT:
		return off + (a ? "NULLABLE_SHORT_ARRAY" : "NULLABLE_SHORT");
	case TYPE_INTEGER:
		return off + (a ? "NULLABLE_INTEGER_ARRAY" : "NULLABLE_INTEGER");
	case TYPE_LONG:
		return off + (a ? "NULLABLE_LONG_ARRAY" : "NULLABLE_LONG");
	case TYPE_FLOAT:
		return off + (a ? "NULLABLE_FLOAT_ARRAY" : "NULLABLE_FLOAT");
	case TYPE_DOUBLE:
		return off + (a ? "NULLABLE_DOUBLE_ARRAY" : "NULLABLE_DOUBLE");
	case TYPE_TIMESTAMP:
		return off + (a ? "NULLABLE_TIMESTAMP_ARRAY" : "NULLABLE_TIMESTAMP");
	case TYPE_MICRO_TIMESTAMP:
		return off + (a ? "NULLABLE_TIMESTAMP_ARRAY(6)" : "NULLABLE_TIMESTAMP(6)");
	case TYPE_NANO_TIMESTAMP:
		return off + (a ? "NULLABLE_TIMESTAMP_ARRAY(9)" : "NULLABLE_TIMESTAMP(9)");
	case TYPE_BOOL:
		return off + (a ? "NULLABLE_BOOL_ARRAY" : "NULLABLE_BOOL");
	case TYPE_STRING:
		return off + (a ? "NULLABLE_STRING_ARRAY" : "NULLABLE_STRING");
	case TYPE_GEOMETRY:
		return off + (a ? "NULLABLE_GEOMETRY_ARRAY" : "NULLABLE_GEOMETRY");
	case TYPE_BLOB:
		return off + (a ? "NULLABLE_BLOB_ARRAY" : "NULLABLE_BLOB");
	case TYPE_NUMERIC:
		return off + (a ? "NULLABLE_NUMERIC_ARRAY" : "NULLABLE_NUMERIC");
	default:
		return NULL;
	}
}

bool TupleList::TupleColumnTypeCoder::operator()(
		const char8_t *name, TupleColumnType &type) const {
	if (strcmp(name, "NULL") == 0) {
		type = TYPE_NULL;
		return true;
	}
	else if (strcmp(name, "ANY") == 0) {
		type = TYPE_ANY;
		return true;
	}

	const char8_t *subName = name;

	const char8_t *nullablePrefix = "NULLABLE_";
	TupleColumnType mask = 0;
	if (strstr(name, nullablePrefix) == name) {
		subName += strlen(nullablePrefix);
		mask |= TupleList::TYPE_MASK_NULLABLE;
	}

	typedef std::pair<const char8_t*, TupleColumnType> Entry;
	const Entry entryList[] = {
		Entry("BYTE", TYPE_BYTE),
		Entry("SHORT", TYPE_SHORT),
		Entry("INTEGER", TYPE_INTEGER),
		Entry("LONG", TYPE_LONG),
		Entry("FLOAT", TYPE_FLOAT),
		Entry("DOUBLE", TYPE_DOUBLE),
		Entry("TIMESTAMP", TYPE_TIMESTAMP),
		Entry("TIMESTAMP(6)", TYPE_MICRO_TIMESTAMP),
		Entry("TIMESTAMP(9)", TYPE_NANO_TIMESTAMP),
		Entry("BOOL", TYPE_BOOL),
		Entry("STRING", TYPE_STRING),
		Entry("GEOMETRY", TYPE_GEOMETRY),
		Entry("BLOB", TYPE_BLOB),
		Entry("NUMERIC", TYPE_NUMERIC),
		Entry()
	};

	for (const Entry *entry = entryList; entry->first != NULL; ++entry) {
		if (strstr(subName, entry->first) == subName) {
			type = mask | entry->second;

			const char8_t *suffix = subName + strlen(entry->first);
			if (*suffix != '\0') {
				if (strcmp(suffix, "_ARRAY") != 0) {
					break;
				}
				type |= TYPE_MASK_ARRAY;
			}
			return true;
		}
	}

	type = TYPE_NULL;
	return false;
}



TupleValue::VarContext::VarContext() :
		group_(NULL),
		currentOffset_(0),
		interruptionChecker_(NULL) {
}

TupleValue::VarContext::~VarContext() try {
	clear();
}
catch (...) { 
}

void TupleValue::VarContext::setGroup(LocalTempStore::Group *group) {
	assert(group);
	group_ = group;
}

LocalTempStore::Group& TupleValue::VarContext::getGroup() {
	return *group_;
}

void TupleValue::VarContext::setVarAllocator(TupleValueVarUtils::VarSizeAllocator *varAlloc) {
	baseCxt_.setVarAllocator(varAlloc);
}

TupleValueVarUtils::VarSizeAllocator* TupleValue::VarContext::getVarAllocator() const {
	return baseCxt_.getVarAllocator();
}

void TupleValue::VarContext::setStackAllocator(TupleValueVarUtils::StackAllocator *stackAlloc) {
	baseCxt_.setStackAllocator(stackAlloc);
}

TupleValueVarUtils::StackAllocator* TupleValue::VarContext::getStackAllocator() const {
	return baseCxt_.getStackAllocator();
}
void TupleValue::VarContext::setInterruptionChecker(InterruptionChecker *checker) {
	interruptionChecker_ = checker;
}

InterruptionChecker* TupleValue::VarContext::getInterruptionChecker() const {
	return interruptionChecker_;
}
TupleValueVarUtils::VarData* TupleValue::VarContext::allocate(
	TupleValueVarUtils::VarDataType type, size_t minBodySize, TupleValueVarUtils::VarData *base) {
	return baseCxt_.allocate(type, minBodySize, base);
}
TupleValueVarUtils::VarData* TupleValue::VarContext::allocateRelated(size_t minBodySize, TupleValueVarUtils::VarData *base) {
	return baseCxt_.allocateRelated(minBodySize, base);
}

TupleValueVarUtils::VarData* TupleValue::VarContext::getHead() {
	return baseCxt_.getHead();
}


void TupleValue::VarContext::resetStoreValue() {
	TupleValueVarUtils::VarData* cursor = baseCxt_.getHead();
	while (cursor) {
		resetStoreValue(cursor);
		cursor = cursor->getNext();
	}
}

void TupleValue::VarContext::resetStoreValue(
		TupleValueVarUtils::VarData *varData) {

	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varData->getType()) {
		TupleValue::StoreMultiVarData *multiVarData =
				static_cast<TupleValue::StoreMultiVarData*>(varData->getBody());
		multiVarData->releaseStoreBlock();
	}
	else {
	}
}

void TupleValue::VarContext::clear() {
	currentBlock_ = LocalTempStore::Block(); 
	resetStoreValue();
	baseCxt_.clear();
}

void TupleValue::VarContext::destroyValue(TupleValue &value) {
	TupleValueVarUtils::VarData *varData;
	assert(!TupleColumnTypeUtils::isAny(value.getType()));
	if (TupleColumnTypeUtils::isSingleVarOrLarge(value.getType())) {
		uint8_t *body = static_cast<uint8_t*>(value.getRawData()) - 1;
		if (*body != TupleValueTempStoreVarUtils::LTS_VAR_DATA_SINGLE_VAR) {
			assert(false);
			return;
		}

		void *head = body - TupleValueVarUtils::VarData::getHeadSize();
		varData = static_cast<TupleValueVarUtils::VarData*>(head);

		assert(TupleValueVarUtils::VAR_DATA_TEMP_STORE == varData->getType());
	}
	else if (TupleColumnTypeUtils::isLob(value.getType())) {
		varData =
				static_cast<TupleValueVarUtils::VarData*>(value.getRawData());
		resetStoreValue(varData);
	}
	else {
		return;
	}
	baseCxt_.clearElement(varData);
	value.data_.varData_ = NULL;
}

void TupleValue::VarContext::moveValueToParent(
		size_t depth, TupleValue &value) {
	moveValueToParent(NULL, depth, value);
}

void TupleValue::VarContext::moveValueToParent(
		TupleValueVarUtils::BaseVarContext::Scope *scope, size_t depth,
		TupleValue &value) {
	TupleValueVarUtils::VarData *varData;
	assert(!TupleColumnTypeUtils::isAny(value.getType()));
	if (TupleColumnTypeUtils::isSingleVar(value.getType())) {
		size_t size = value.varSize();
		size_t headerSize = 1 + TupleValueUtils::getEncodedVarSize(static_cast<uint32_t>(size));
		const TupleValueVarUtils::VarDataType type =
				TupleValueVarUtils::VAR_DATA_TEMP_STORE;
		varData = allocate(type, headerSize + size);
		assert(varData->getBodySize(getBaseVarContext()) >= (headerSize + size));
		uint8_t *addr = static_cast<uint8_t*>(varData->getBody());
		*addr = TupleValueTempStoreVarUtils::LTS_VAR_DATA_SINGLE_VAR;
		++addr;
		size_t encodeSize = TupleValueUtils::encodeInt32(static_cast<uint32_t>(size), addr);
		assert(1 + encodeSize == headerSize);
		memcpy(addr + encodeSize, value.varData(), size);
		value.data_.varData_ = addr;
	}
	else if (TupleColumnTypeUtils::isLargeFixed(value.getType())) {
		const uint8_t dataType = TupleValueTempStoreVarUtils::LTS_VAR_DATA_SINGLE_VAR;

		const size_t size = TupleColumnTypeUtils::getFixedSize(value.getType());
		const TupleValueVarUtils::VarDataType type =
				TupleValueVarUtils::VAR_DATA_TEMP_STORE;

		const size_t headerSize = sizeof(dataType);
		varData = allocate(type, headerSize + size);
		assert(varData->getBodySize(getBaseVarContext()) >= headerSize + size);

		void *addr = varData->getBody();
		*static_cast<uint8_t*>(addr) = dataType;
		addr = static_cast<uint8_t*>(addr) + headerSize;

		memcpy(addr, value.getRawData(), size);
		value.data_.rawData_ = addr;
	}
	else if (TupleColumnTypeUtils::isLob(value.getType())) {
		for (;;) {
			varData =
					static_cast<TupleValueVarUtils::VarData*>(value.getRawData());

			if (varData->getType() != TupleValueVarUtils::VAR_DATA_CONTAINER) {
				break;
			}

			LobBuilder builder(*this, value.getType(), 0);
			{
				LobReader reader(value);
				const void *data;
				size_t size;
				while (reader.next(data, size)) {
					builder.append(data, size);
				}
			}
			value = builder.build();
		}
	}
	else {
		return;
	}

	if (scope == NULL) {
		baseCxt_.moveToParent(depth, varData);
	}
	else {
		scope->move(varData);
	}
}


void TupleValue::VarContext::Scope::moveValueToParent(TupleValue &value) {
	cxt_.moveValueToParent(&scope_, 0, value);
}


TupleValue::SingleVarBuilder::SingleVarBuilder(VarContext &cxt, TupleList::TupleColumnType type, size_t initialCapacity)
: cxt_(cxt), type_(type), initialCapacity_(initialCapacity),
totalSize_(0), partCount_(0), lastData_(NULL), lastSize_(0) {
}

void TupleValue::SingleVarBuilder::append(const void *data, size_t size) {
	assert(partCount_ == 0);

	size_t headerSize = 1 + TupleValueUtils::getEncodedVarSize(static_cast<uint32_t>(size));

	const TupleValueVarUtils::VarDataType type =
			TupleValueVarUtils::VAR_DATA_TEMP_STORE;
	TupleValueVarUtils::VarData *varData = cxt_.allocate(type, headerSize + size);
	assert(varData->getBodySize(cxt_.getBaseVarContext()) >= (headerSize + size));
	uint8_t *addr = static_cast<uint8_t*>(varData->getBody());
	*addr = TupleValueTempStoreVarUtils::LTS_VAR_DATA_SINGLE_VAR;
	++addr;
	lastData_ = addr;
	size_t encodeSize = TupleValueUtils::encodeInt32(static_cast<uint32_t>(size), addr);
	addr += encodeSize;
	assert(1 + encodeSize == headerSize);
	memcpy(addr, data, size);
	++partCount_;

	totalSize_ += size;
	lastSize_ = static_cast<uint32_t>(size);
}


uint32_t TupleValue::SingleVarBuilder::append(uint32_t maxSize) {
	assert(partCount_ == 0);

	size_t headerSize = 1 + TupleValueUtils::getEncodedVarSize(maxSize);

	const TupleValueVarUtils::VarDataType type =
			TupleValueVarUtils::VAR_DATA_TEMP_STORE;
	TupleValueVarUtils::VarData *varData = cxt_.allocate(type, headerSize + maxSize);
	assert(varData->getBodySize(cxt_.getBaseVarContext()) >= (headerSize + maxSize));
	uint8_t *addr = static_cast<uint8_t*>(varData->getBody());
	*addr = TupleValueTempStoreVarUtils::LTS_VAR_DATA_SINGLE_VAR;
	++addr;
	lastData_ = addr;

	++partCount_;
	lastSize_ = maxSize;
	return lastSize_;
}


TupleValue TupleValue::SingleVarBuilder::build() {
	return TupleValue(TupleString(lastData_));
}



TupleValue::StackAllocSingleVarBuilder::StackAllocSingleVarBuilder(
	util::StackAllocator &alloc, TupleList::TupleColumnType type, size_t initialCapacity)
: alloc_(alloc), type_(type), initialCapacity_(initialCapacity),
totalSize_(0), partCount_(0), lastData_(NULL), lastSize_(0) {
}

void TupleValue::StackAllocSingleVarBuilder::append(const void *data, size_t size) {
	assert(partCount_ == 0);

	size_t headerSize = TupleValueUtils::getEncodedVarSize(static_cast<uint32_t>(size));
	uint8_t *addr = static_cast<uint8_t*>(alloc_.allocate(headerSize + size));
	lastData_ = addr;
	size_t encodeSize = TupleValueUtils::encodeInt32(static_cast<uint32_t>(size), addr);
	addr += encodeSize;
	memcpy(addr, data, size);
	++partCount_;

	totalSize_ += size;
	lastSize_ = static_cast<uint32_t>(size);
}


uint32_t TupleValue::StackAllocSingleVarBuilder::append(uint32_t maxSize) {
	assert(partCount_ == 0);

	size_t headerSize = TupleValueUtils::getEncodedVarSize(static_cast<uint32_t>(maxSize));
	uint8_t *addr = static_cast<uint8_t*>(alloc_.allocate(headerSize + maxSize));
	lastData_ = addr;

	++partCount_;
	lastSize_ = maxSize;
	return lastSize_;
}


TupleValue TupleValue::StackAllocSingleVarBuilder::build() {
	return TupleValue(TupleString(lastData_));
}


TupleValue::LobBuilder::LobBuilder(VarContext &cxt, TupleList::TupleColumnType type, size_t initialCapacity)
: cxt_(cxt), type_(type), initialCapacity_(initialCapacity)
, topVarData_(NULL), totalSize_(0), blockCount_(0), partCount_(0)
, lastData_(NULL), lastSize_(0) {
	topVarData_ = TupleValue::StoreMultiVarData::create(cxt.getBaseVarContext(),
		cxt.getGroup().getStore(), initialCapacity);
}

TupleValue::LobBuilder::~LobBuilder() {
	cxt_.currentBlock_ = LocalTempStore::Block(); 
}

void TupleValue::LobBuilder::append(const void *data, size_t size) {
	size_t remain = size;
	const uint8_t *addr = static_cast<const uint8_t*>(data);
	size_t offset = 0;
	while (remain > 0) {
		size_t reserved = append(remain);
		size_t partSize = reserved;
		memcpy(lastData_, addr + offset, partSize);
		remain -= partSize;
		offset += partSize;
	}
}

size_t TupleValue::LobBuilder::append(size_t maxSize) {
	assert(maxSize <= UINT32_MAX);
	LocalTempStore &store = cxt_.getGroup().getStore();
	uint32_t requestSize = 0;
	if (maxSize > TupleValueUtils::VAR_SIZE_4BYTE_THRESHOLD) {
		requestSize = TupleValueUtils::VAR_SIZE_4BYTE_THRESHOLD;
	}
	else {
		requestSize = static_cast<uint32_t>(maxSize);
	}
	TupleValue::StoreMultiVarData *multiVarData =
		static_cast<TupleValue::StoreMultiVarData*>(topVarData_->getBody());
	const uint32_t blockSize = static_cast<uint32_t>(1UL << store.getDefaultBlockExpSize());

	uint32_t availableSize = blockSize - cxt_.currentOffset_;
	if (!lastData_) {
		availableSize = newBlock(store);
	}
	lastData_ = static_cast<uint8_t*>(cxt_.currentBlock_.data()) + cxt_.currentOffset_;
	if (availableSize < sizeof(uint64_t)) {
		availableSize = newBlock(store);
	}
	assert(blockCount_ > 0);
	uint64_t offset = (blockCount_ - 1) * blockSize + cxt_.currentOffset_;
	uint32_t lobPartHeaderSize = TupleValueUtils::getEncodedVarSize(requestSize);
	availableSize -= lobPartHeaderSize;
	lastSize_ = (availableSize >= requestSize) ? requestSize : availableSize;
	uint32_t partSize = lastSize_;
	lobPartHeaderSize = TupleValueUtils::getEncodedVarSize(partSize); 
	uint32_t encodedSize = TupleValueUtils::encodeVarSize(partSize);
	memcpy(lastData_, &encodedSize, lobPartHeaderSize);  
	lastData_ = static_cast<uint8_t*>(lastData_) + lobPartHeaderSize;
	cxt_.currentOffset_ += lobPartHeaderSize + lastSize_;

	multiVarData->appendLobPartOffset(cxt_.getBaseVarContext(), topVarData_, partCount_, offset);

	++partCount_;
	totalSize_ += lastSize_;

	return lastSize_;
}

size_t TupleValue::LobBuilder::getMaxAllocateSize() {
	LocalTempStore &store = cxt_.getGroup().getStore();

	const uint32_t blockSize = static_cast<uint32_t>(1UL << store.getDefaultBlockExpSize());

	uint32_t availableSize = blockSize - cxt_.currentOffset_;
	if (!lastData_) {
		availableSize = store.getDefaultBlockSize() - TupleList::BLOCK_HEADER_SIZE;
	}
	if (availableSize < sizeof(uint64_t)) {
		availableSize = store.getDefaultBlockSize() - TupleList::BLOCK_HEADER_SIZE;
	}
	uint32_t lobPartHeaderSize = 4;
	availableSize -= lobPartHeaderSize;
	return availableSize;
}

void* TupleValue::LobBuilder::lastData() {
	return lastData_;
}

uint32_t TupleValue::LobBuilder::newBlock(LocalTempStore &store) {
	LocalTempStore::Block block(
		store, store.getDefaultBlockSize(),
		LocalTempStore::UNDEF_GROUPID);
	store.getBufferManager().addAssignment(block.getBlockId());
	block.setSwapped(false);
	TupleValue::StoreMultiVarData *multiVarData =
		static_cast<TupleValue::StoreMultiVarData*>(topVarData_->getBody());
	multiVarData->appendBlockId(cxt_.getBaseVarContext(), topVarData_, blockCount_, block.getBlockId());

	cxt_.currentBlock_ = block;
	uint32_t availableSize = store.getDefaultBlockSize() - TupleList::BLOCK_HEADER_SIZE;
	lastData_ = static_cast<uint8_t*>(block.data()) + TupleList::BLOCK_HEADER_SIZE;
	cxt_.currentOffset_ = TupleList::BLOCK_HEADER_SIZE;
	++blockCount_;
	return availableSize;
}

TupleValue TupleValue::LobBuilder::fromDataStore(
		VarContext &cxt, const BaseObject &fieldObject, const void *data) {
	TupleValueVarUtils::VarData* varData = cxt.allocate(
		TupleValueVarUtils::VAR_DATA_CONTAINER, sizeof(TupleValueVarUtils::ContainerVarArray));
	TupleValueVarUtils::ContainerVarArray* containerVarArray =
		static_cast<TupleValueVarUtils::ContainerVarArray*>(varData->getBody());
	containerVarArray->initialize(fieldObject, data);
	return TupleValue(varData, TupleList::TYPE_BLOB);
}

TupleValue TupleValue::LobBuilder::build() {
	return TupleValue(topVarData_, TupleList::TYPE_BLOB);
}



TupleValue::LobReader::LobReader(const TupleValue &value) {
	const TupleValueVarUtils::VarData* varData =
	  static_cast<const TupleValueVarUtils::VarData*>(value.getRawData());
	assert(varData);
	varDataType_ = varData->getType();
	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varDataType_) {
		const TupleValue::StoreMultiVarData *multiVarData =
			static_cast<const TupleValue::StoreMultiVarData*>(varData->getBody());
		const TupleValue::StoreLobHeader *lobHeader = multiVarData->getStoreLobHeader();
		readerImpl_.tempStoreReader_.initialize(*multiVarData->getLocalTempStore(), multiVarData, lobHeader);
	}
	else if (TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR == varDataType_) {
		const TupleValue::StackAllocMultiVarData *multiVarData =
			static_cast<const TupleValue::StackAllocMultiVarData*>(varData->getBody());
		const TupleValue::StackAllocLobHeader *lobHeader = multiVarData->getStackAllocLobHeader();
		readerImpl_.stackAllocReader_.initialize(multiVarData, lobHeader);
	}
	else { 
		assert(TupleValueVarUtils::VAR_DATA_CONTAINER == varDataType_);
		const TupleValueVarUtils::ContainerVarArray* containerVarArray =
		  static_cast<const TupleValueVarUtils::ContainerVarArray*>(varData->getBody());
		readerImpl_.containerReader_.initialize(*containerVarArray);
	} 
}

TupleValue::LobReader::~LobReader() try {
	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varDataType_) {
		readerImpl_.tempStoreReader_.destroy();
	}
	else if (TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR == varDataType_) {
		readerImpl_.stackAllocReader_.destroy();
	}
	else { 
		assert(TupleValueVarUtils::VAR_DATA_CONTAINER == varDataType_);
		readerImpl_.containerReader_.destroy();
	}
}
catch (...) {
} 


bool TupleValue::LobReader::next(const void *&data, size_t &size) {
	data = NULL;
	size = 0;
	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varDataType_) {
		return readerImpl_.tempStoreReader_.next(data, size);
	}
	else if (TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR == varDataType_) {
		return readerImpl_.stackAllocReader_.next(data, size);
	}
	else { 
		assert(TupleValueVarUtils::VAR_DATA_CONTAINER == varDataType_);
		return readerImpl_.containerReader_.next(data, size);
	} 
}

void TupleValue::LobReader::reset() {
	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varDataType_) {
		return readerImpl_.tempStoreReader_.reset();
	}
	else if (TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR == varDataType_) {
		return readerImpl_.stackAllocReader_.reset();
	}
	else { 
		assert(TupleValueVarUtils::VAR_DATA_CONTAINER == varDataType_);
		return readerImpl_.containerReader_.reset();
	} 
}

LocalTempStore::Block TupleValue::LobReader::getCurrentPartBlock() {
	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varDataType_) {
		return readerImpl_.tempStoreReader_.getCurrentPartBlock();
	}
	else if (TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR == varDataType_) {
		return LocalTempStore::Block();
	}
	else { 
		assert(TupleValueVarUtils::VAR_DATA_CONTAINER == varDataType_);
		return LocalTempStore::Block();
	} 
}

uint64_t TupleValue::LobReader::getPartCount() {
	if (TupleValueVarUtils::VAR_DATA_TEMP_STORE == varDataType_) {
		return readerImpl_.tempStoreReader_.getPartCount();
	}
	else if (TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR == varDataType_) {
		return readerImpl_.stackAllocReader_.getPartCount();
	}
	else { 
		assert(TupleValueVarUtils::VAR_DATA_CONTAINER == varDataType_);
		return readerImpl_.containerReader_.length();
	} 
}
void TupleValue::LobReader::dumpContents(std::ostream &ostr) {
	const void* data;
	size_t size;
	size_t partCount = 0;
	while (next(data, size)) {
		ostr << "partCount=" << partCount << std::endl;
		LocalTempStore::dumpMemory(ostr, data, size);
		++partCount;
	}
}

uint64_t TupleValue::StoreLobHeader::getPartDataOffset(uint64_t index) const {
	assert(index < validCount_);
	const uint64_t* top = &offsetList_;
	return *(top + index);
}

void TupleValue::StoreLobHeader::setPartDataOffset(uint64_t index, uint64_t offset) {
	assert(index < validCount_);
	uint64_t* top = &offsetList_;
	*(top + index) = offset;
}

void TupleValue::StoreLobHeader::resetPartDataOffset() {
	uint64_t* top = &offsetList_;
	for (size_t index = 0; index < reservedCount_; ++index) {
		*(top + index) = 0;
	}
}



uint64_t TupleValue::StoreMultiVarBlockIdList::getBlockId(uint64_t index) const {
	assert(index < reservedCount_);
	const uint64_t *top = &blockIdList_;
	return *(top + index);
}

void TupleValue::StoreMultiVarBlockIdList::setBlockId(uint64_t index, uint64_t blockId) {
	assert(index < reservedCount_);
	uint64_t *top = &blockIdList_;
	*(top + index) = blockId;
}

size_t TupleValue::StoreMultiVarBlockIdList::calcAllocateSize(uint64_t blockCount) {
	return static_cast<size_t>(sizeof(uint64_t) * (blockCount + 1));
}

void TupleValue::StoreMultiVarBlockIdList::resetBlockIdList() {
	uint64_t *top = &blockIdList_;
	for (size_t index = 0; index < reservedCount_; ++index) {
		*(top + index) = LocalTempStore::UNDEF_BLOCKID;
	}
}


uint64_t TupleValue::StoreMultiVarData::getBlockIdListCount() const {
	assert(blockIdList_);
	return blockIdList_->reservedCount_;
}

void TupleValue::StoreMultiVarData::setBlockIdListCount(uint64_t blockIdListCount) {
	assert(blockIdList_);
	blockIdList_->reservedCount_ = blockIdListCount;
}

uint64_t TupleValue::StoreMultiVarData::getBlockId(uint64_t index) const {
	assert(blockIdList_);
	assert(index < blockIdList_->reservedCount_);
	return blockIdList_->getBlockId(index);
}

void TupleValue::StoreMultiVarData::setBlockId(uint64_t index, uint64_t blockId) {
	assert(blockIdList_);
	assert(index < blockIdList_->reservedCount_);
	blockIdList_->setBlockId(index, blockId);
}

void TupleValue::StoreMultiVarData::resetBlockIdList() {
	blockIdList_->resetBlockIdList();
}


LocalTempStore* TupleValue::StoreMultiVarData::getLocalTempStore() const {
	assert(store_);
	return store_;
}
TupleValueTempStoreVarUtils::StoreVarDataType TupleValue::StoreMultiVarData::getDataType() const {
	return static_cast<TupleValueTempStoreVarUtils::StoreVarDataType>(dataType_);
}

TupleValue::StoreLobHeader* TupleValue::StoreMultiVarData::getStoreLobHeader() {
	return storeLobHeader_;
}

const TupleValue::StoreLobHeader* TupleValue::StoreMultiVarData::getStoreLobHeader() const {
	return storeLobHeader_;
}

void TupleValue::StoreMultiVarData::setStoreLobHeader(TupleValue::StoreLobHeader* header) {
	storeLobHeader_ = header;
}

size_t TupleValue::StoreMultiVarData::getBlockIdListAllocSize(size_t blockCount) {
	return TupleValue::StoreMultiVarBlockIdList::calcAllocateSize(blockCount);
}


uint64_t TupleValue::StoreMultiVarData::getLobPartCount() const {
	if (storeLobHeader_) {
		return storeLobHeader_->validCount_;
	}
	else {
		return 0;
	}
}

TupleValueVarUtils::VarData* TupleValue::StoreMultiVarData::create(
	TupleValueVarUtils::BaseVarContext &cxt, LocalTempStore &store, uint64_t initialCapacity) {

	static const size_t DEFAULT_LOB_PART_COUNT = 3;
	const uint32_t defaultBlockSize = static_cast<uint32_t>(1UL << store.getDefaultBlockExpSize());
	size_t initialBlockCount =
		static_cast<size_t>(initialCapacity + defaultBlockSize + TupleList::BLOCK_HEADER_SIZE - 1) / defaultBlockSize;

	TupleValueVarUtils::VarData* topVarData =
		cxt.allocate(TupleValueVarUtils::VAR_DATA_TEMP_STORE, sizeof(TupleValue::StoreMultiVarData));
	TupleValue::StoreMultiVarData* storeMultiVarData =
		static_cast<TupleValue::StoreMultiVarData*>(topVarData->getBody());
	storeMultiVarData->dataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_TEMP_STORE;
	storeMultiVarData->store_ = &store;

	size_t blockIdListAllocSize = storeMultiVarData->getBlockIdListAllocSize(initialBlockCount);
	TupleValueVarUtils::VarData *blockIdListData =
	  cxt.allocateRelated(blockIdListAllocSize, topVarData);

	storeMultiVarData->blockIdList_ =
		static_cast<TupleValue::StoreMultiVarBlockIdList*>(blockIdListData->getBody());
	storeMultiVarData->setBlockIdListCount(initialBlockCount);
	storeMultiVarData->resetBlockIdList();

	size_t initialPartCount = std::max(DEFAULT_LOB_PART_COUNT, initialBlockCount);
	size_t storeLobHeaderSize = sizeof(TupleValue::StoreLobHeader)
		+ initialPartCount * sizeof(uint64_t);
	TupleValueVarUtils::VarData *varData = cxt.allocateRelated(storeLobHeaderSize, topVarData);

	TupleValue::StoreLobHeader* storeLobHeader =
		static_cast<TupleValue::StoreLobHeader*>(varData->getBody());
	storeLobHeader->storeVarDataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_TEMP_STORE;
	storeLobHeader->blockSize_ = defaultBlockSize;
	storeLobHeader->reservedCount_ = initialPartCount;
	storeLobHeader->validCount_ = 0;
	storeLobHeader->resetPartDataOffset();

	storeMultiVarData->storeLobHeader_ = storeLobHeader;

	return topVarData;
}


TupleValueVarUtils::VarData* TupleValue::StoreMultiVarData::createTupleListVarData(
	TupleValueVarUtils::BaseVarContext &cxt, LocalTempStore &store,
	uint32_t blockSize,
	uint64_t blockCount, const LocalTempStore::BlockId* blockIdList,
	uint64_t partCount, const uint64_t* partOffsetList) {

	TupleValueVarUtils::VarData* topVarData =
		cxt.allocate(TupleValueVarUtils::VAR_DATA_TEMP_STORE, sizeof(TupleValue::StoreMultiVarData));
	TupleValue::StoreMultiVarData* storeMultiVarData =
		static_cast<TupleValue::StoreMultiVarData*>(topVarData->getBody());
	storeMultiVarData->dataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_TUPLE_LIST;
	storeMultiVarData->store_ = &store;

	size_t blockIdListAllocSize = storeMultiVarData->getBlockIdListAllocSize(static_cast<size_t>(blockCount));
	TupleValueVarUtils::VarData *blockIdListData =
	  cxt.allocateRelated(blockIdListAllocSize, topVarData);

	storeMultiVarData->blockIdList_ =
		static_cast<TupleValue::StoreMultiVarBlockIdList*>(blockIdListData->getBody());
	storeMultiVarData->setBlockIdListCount(blockCount);

	memcpy(&storeMultiVarData->blockIdList_->blockIdList_, blockIdList,
		sizeof(LocalTempStore::BlockId) * static_cast<size_t>(blockCount));

	size_t storeLobHeaderSize = sizeof(TupleValue::StoreLobHeader)
		+ static_cast<size_t>(partCount) * sizeof(uint64_t);
	TupleValueVarUtils::VarData *varData = cxt.allocateRelated(storeLobHeaderSize, topVarData);

	TupleValue::StoreLobHeader* storeLobHeader =
		static_cast<TupleValue::StoreLobHeader*>(varData->getBody());
	storeLobHeader->storeVarDataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_TUPLE_LIST;
	storeLobHeader->blockSize_ = blockSize;
	storeLobHeader->reservedCount_ = partCount;
	storeLobHeader->validCount_ = partCount;
	memcpy(&storeLobHeader->offsetList_, partOffsetList, sizeof(uint64_t) * static_cast<size_t>(partCount));

	storeMultiVarData->storeLobHeader_ = storeLobHeader;

	return topVarData;
}




void TupleValue::StoreMultiVarData::releaseStoreBlock() {
	if (TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_TEMP_STORE
		== dataType_) {
		for (uint64_t index = 0; index < getBlockIdListCount(); ++index) {
			uint64_t blockId = getBlockId(index);
			if (LocalTempStore::UNDEF_BLOCKID != blockId) {
				store_->getBufferManager().removeAssignment(blockId);
			}
		}
	}
}

TupleValue::StoreLobHeader* TupleValue::StoreMultiVarData::updateLobPartOffset(
	TupleValueVarUtils::BaseVarContext &cxt,
	TupleValueVarUtils::VarData* topVarData,
	TupleValue::StoreLobHeader* lobHeader,
	uint64_t partCount, uint64_t offset) {

	TupleValue::StoreLobHeader* newLobHeader = NULL;
	uint64_t allocatedPartCount = lobHeader->reservedCount_;
	if (allocatedPartCount < partCount) {
		size_t oldStoreLobHeaderSize = sizeof(TupleValue::StoreLobHeader)
				+ static_cast<size_t>(allocatedPartCount) * sizeof(uint64_t);

		uint64_t newReservePartCount = allocatedPartCount * 2;

		size_t newStoreLobHeaderSize = sizeof(TupleValue::StoreLobHeader)
				+ static_cast<size_t>(newReservePartCount) * sizeof(uint64_t);
		TupleValueVarUtils::VarData *varData = cxt.allocateRelated(newStoreLobHeaderSize, topVarData);
		newLobHeader = static_cast<TupleValue::StoreLobHeader*>(varData->getBody());
		memset(newLobHeader, 0, newStoreLobHeaderSize);
		memcpy(newLobHeader, lobHeader, oldStoreLobHeaderSize);
		newLobHeader->storeVarDataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_NONE;
		newLobHeader->reservedCount_ = newReservePartCount;

		assert(newLobHeader->reservedCount_ > partCount);
		lobHeader = newLobHeader;
	}
	lobHeader->validCount_ = partCount + 1;
	lobHeader->setPartDataOffset(partCount, offset);
	return newLobHeader;
}

void TupleValue::StoreMultiVarData::appendLobPartOffset(
	TupleValueVarUtils::BaseVarContext &cxt,
	TupleValueVarUtils::VarData *topVarData,
	uint64_t partCount, uint64_t offset) {

	TupleValue::StoreMultiVarData *multiVarData =
			static_cast<TupleValue::StoreMultiVarData*>(topVarData->getBody());

	TupleValue::StoreLobHeader* lobHeader = multiVarData->getStoreLobHeader();
	TupleValue::StoreLobHeader* newLobHeader = updateLobPartOffset(
			cxt, topVarData, lobHeader, partCount, offset);
	if (newLobHeader) {
		multiVarData->setStoreLobHeader(newLobHeader);
	}
}

void TupleValue::StoreMultiVarData::appendBlockId(
	TupleValueVarUtils::BaseVarContext &cxt,
	TupleValueVarUtils::VarData *topVarData,
	uint64_t index, LocalTempStore::BlockId blockId) {
	if (index >= getBlockIdListCount()) {
		reallocBlockIdList(cxt, topVarData);
	}
	setBlockId(index, blockId);
}


void TupleValue::StoreMultiVarData::reallocBlockIdList(
	TupleValueVarUtils::BaseVarContext &cxt,
	TupleValueVarUtils::VarData *topVarData) {

	uint64_t oldBlockCount = getBlockIdListCount();
	size_t oldAllocSize =
			TupleValue::StoreMultiVarData::getBlockIdListAllocSize(
					static_cast<size_t>(oldBlockCount));
	TupleValue::StoreMultiVarBlockIdList* oldBlockIdList = blockIdList_;

	uint64_t newBlockCount = oldBlockCount * 2;
	size_t newAllocSize =
			TupleValue::StoreMultiVarData::getBlockIdListAllocSize(
					static_cast<size_t>(newBlockCount));

	TupleValueVarUtils::VarData *newBlockIdListData =
			cxt.allocateRelated(newAllocSize, topVarData);
	TupleValue::StoreMultiVarBlockIdList* newBlockIdList =
			static_cast<TupleValue::StoreMultiVarBlockIdList*>(newBlockIdListData->getBody());

	newBlockIdList->reservedCount_ = newBlockCount;
	newBlockIdList->resetBlockIdList();
	memcpy(newBlockIdList, oldBlockIdList, oldAllocSize);
	newBlockIdList->reservedCount_ = newBlockCount;
	blockIdList_ = newBlockIdList;
	oldBlockIdList->resetBlockIdList();
}



void TupleValueTempStoreVarUtils::TempStoreLobReader::initialize(
	LocalTempStore &store, const TupleValue::StoreMultiVarData *multiVarData,
	const TupleValue::StoreLobHeader *lobHeader) {
	multiVarData_ = multiVarData;
	lobHeader_ = lobHeader;
	blockSize_ = lobHeader->blockSize_;
	store_ = &store;
	currentPos_ = 0;
	currentBlockId_ = LocalTempStore::UNDEF_BLOCKID;
	if (0 != lobHeader_->validCount_) {
		uint64_t offset = lobHeader_->getPartDataOffset(0);
		uint64_t targetBlockNth = offset / blockSize_;
		uint64_t blockId = multiVarData_->getBlockId(targetBlockNth);
		if (LocalTempStore::UNDEF_BLOCKID != blockId) {
			LocalTempStore::Block block(*store_, blockId);
			store.getBufferManager().addReference(*block.getBlockInfo());
			currentBlockId_ = blockId;
		}
	}
}

void TupleValueTempStoreVarUtils::TempStoreLobReader::destroy() {
	if (LocalTempStore::UNDEF_BLOCKID != currentBlockId_) {
		LocalTempStore::Block block(*store_, currentBlockId_);
		if (block.getReferenceCount() > 0) {
			store_->getBufferManager().removeReference(*block.getBlockInfo());
		}
		currentBlockId_ = LocalTempStore::UNDEF_BLOCKID;
	}
}

bool TupleValueTempStoreVarUtils::TempStoreLobReader::next(const void *&data, size_t &size) {
	if ((LocalTempStore::UNDEF_BLOCKID == currentBlockId_)
		|| currentPos_ >= lobHeader_->validCount_) {
		return false;
	}
	else {
		uint64_t offset = lobHeader_->getPartDataOffset(currentPos_);
		uint64_t blockNth = offset / blockSize_;
		uint64_t blockOffset = offset - blockNth * blockSize_;
		LocalTempStore::Block block(*store_, currentBlockId_);
		if (currentBlockId_ != multiVarData_->getBlockId(blockNth)) {
			if (block.getReferenceCount() > 0) {
				store_->getBufferManager().removeReference(*block.getBlockInfo());
			}
			currentBlockId_ = multiVarData_->getBlockId(blockNth);
			block = LocalTempStore::Block(*store_, currentBlockId_);
			store_->getBufferManager().addReference(*block.getBlockInfo());
		}
		const uint8_t* addr = static_cast<const uint8_t*>(block.data());
		addr += blockOffset;
		uint32_t partLobSize;
		size_t partHeaderSize = TupleValueUtils::decodeInt32(addr, partLobSize);
		size = static_cast<size_t>(partLobSize);
		data = addr + partHeaderSize;
		++currentPos_;
		return true;
	}
}

void TupleValueTempStoreVarUtils::TempStoreLobReader::reset() {
	const TupleValue::StoreMultiVarData *multiVarData = multiVarData_;
	const TupleValue::StoreLobHeader *lobHeader = lobHeader_;
	LocalTempStore *store = store_;
	destroy();
	initialize(*store, multiVarData, lobHeader);
}

LocalTempStore::Block TupleValueTempStoreVarUtils::TempStoreLobReader::getCurrentPartBlock() {
	if ((LocalTempStore::UNDEF_BLOCKID == currentBlockId_)
		|| currentPos_ >= lobHeader_->validCount_) {
		return LocalTempStore::Block();
	}
	else {
		return LocalTempStore::Block(*store_, currentBlockId_);
	}
}

uint64_t TupleValueTempStoreVarUtils::TempStoreLobReader::getPartCount() {
	if (multiVarData_) {
		return multiVarData_->getLobPartCount();
	}
	else {
		return 0;
	}
}




TupleValueTempStoreVarUtils::StoreVarDataType TupleValue::StackAllocMultiVarData::getDataType() const {
	return static_cast<TupleValueTempStoreVarUtils::StoreVarDataType>(dataType_);
}



TupleValue::StackAllocLobHeader* TupleValue::StackAllocMultiVarData::getStackAllocLobHeader() {
	return stackAllocLobHeader_;
}

const TupleValue::StackAllocLobHeader* TupleValue::StackAllocMultiVarData::getStackAllocLobHeader() const {
	return stackAllocLobHeader_;
}

void TupleValue::StackAllocMultiVarData::setStackAllocLobHeader(TupleValue::StackAllocLobHeader* header) {
	stackAllocLobHeader_ = header;
}

uint64_t TupleValue::StackAllocMultiVarData::getLobPartCount() const {
	if (stackAllocLobHeader_) {
		return stackAllocLobHeader_->validCount_;
	}
	else {
		return 0;
	}
}

TupleValueVarUtils::VarData* TupleValue::StackAllocMultiVarData::create(
	TupleValueVarUtils::BaseVarContext &cxt, uint64_t initialCapacity) {

	static const size_t DEFAULT_LOB_PART_COUNT = 1;
	size_t initialBlockCount = 1;
	static_cast<void>(initialCapacity);
	TupleValueVarUtils::VarData* topVarData =
		cxt.allocate(TupleValueVarUtils::VAR_DATA_STACK_ALLOCATOR, sizeof(TupleValue::StackAllocMultiVarData));
	TupleValue::StackAllocMultiVarData* stackAllocMultiVarData =
		static_cast<TupleValue::StackAllocMultiVarData*>(topVarData->getBody());
	stackAllocMultiVarData->dataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_STACK_ALLOCATOR;

	size_t initialPartCount = std::max(DEFAULT_LOB_PART_COUNT, initialBlockCount);
	size_t stackAllocLobHeaderSize = sizeof(TupleValue::StackAllocLobHeader)
		+ initialPartCount * sizeof(void*);
	TupleValueVarUtils::VarData *varData = cxt.allocateRelated(stackAllocLobHeaderSize, topVarData);

	TupleValue::StackAllocLobHeader* stackAllocLobHeader =
		static_cast<TupleValue::StackAllocLobHeader*>(varData->getBody());
	stackAllocLobHeader->storeVarDataType_ =
	  TupleValueTempStoreVarUtils::LTS_VAR_DATA_MULTI_VAR_ON_STACK_ALLOCATOR;
	stackAllocLobHeader->reservedCount_ = initialPartCount;
	stackAllocLobHeader->validCount_ = 0;
	stackAllocLobHeader->resetPartDataAddr();

	stackAllocMultiVarData->stackAllocLobHeader_ = stackAllocLobHeader;

	return topVarData;
}


TupleValue::StackAllocLobHeader* TupleValue::StackAllocMultiVarData::updateLobPartAddr(
	TupleValueVarUtils::BaseVarContext &cxt,
	TupleValueVarUtils::VarData* topVarData,
	TupleValue::StackAllocLobHeader* lobHeader,
	uint64_t partCount, void *addr) {

	TupleValue::StackAllocLobHeader* newLobHeader = NULL;
	uint64_t allocatedPartCount = lobHeader->reservedCount_;
	if (allocatedPartCount < partCount) {
		size_t oldStackAllocLobHeaderSize = sizeof(TupleValue::StackAllocLobHeader)
		  + static_cast<size_t>(allocatedPartCount) * sizeof(void *);

		uint64_t newReservePartCount = allocatedPartCount * 2;

		size_t newStackAllocLobHeaderSize = sizeof(TupleValue::StackAllocLobHeader)
		  + static_cast<size_t>(newReservePartCount) * sizeof(void *);
		TupleValueVarUtils::VarData *varData = cxt.allocateRelated(newStackAllocLobHeaderSize, topVarData);
		newLobHeader = static_cast<TupleValue::StackAllocLobHeader*>(varData->getBody());
		memset(newLobHeader, 0, newStackAllocLobHeaderSize);
		memcpy(newLobHeader, lobHeader, oldStackAllocLobHeaderSize);
		newLobHeader->storeVarDataType_ = TupleValueTempStoreVarUtils::LTS_VAR_DATA_NONE;
		newLobHeader->reservedCount_ = newReservePartCount;

		assert(newLobHeader->reservedCount_ >= partCount);
		lobHeader = newLobHeader;
	}
	lobHeader->validCount_ = partCount + 1;
	lobHeader->setPartDataAddr(partCount, addr);
	return newLobHeader;
}

void TupleValue::StackAllocMultiVarData::appendLobPartAddr(
	TupleValueVarUtils::BaseVarContext &cxt,
	TupleValueVarUtils::VarData *topVarData,
	uint64_t partCount, void *addr) {

	TupleValue::StackAllocMultiVarData *multiVarData =
		static_cast<TupleValue::StackAllocMultiVarData*>(topVarData->getBody());

	TupleValue::StackAllocLobHeader* lobHeader = multiVarData->getStackAllocLobHeader();
	TupleValue::StackAllocLobHeader* newLobHeader = updateLobPartAddr(
		cxt, topVarData, lobHeader, partCount, addr);
	if (newLobHeader) {
		multiVarData->setStackAllocLobHeader(newLobHeader);
	}
}


void* TupleValue::StackAllocLobHeader::getPartDataAddr(uint64_t index) const {
	assert(index < validCount_);
	return *(&addrList_ + index);
}

void TupleValue::StackAllocLobHeader::setPartDataAddr(uint64_t index, void* addr) {
	assert(index < validCount_);
	void **listAddr = &addrList_ + index;
	memcpy(listAddr, &addr, sizeof(void*));
}

void TupleValue::StackAllocLobHeader::resetPartDataAddr() {
	memset(&addrList_, 0, static_cast<size_t>(reservedCount_) * sizeof(void*));
}



void TupleValueStackAllocVarUtils::StackAllocLobReader::initialize(
	const TupleValue::StackAllocMultiVarData *multiVarData,
	const TupleValue::StackAllocLobHeader *lobHeader) {
	multiVarData_ = multiVarData;
	lobHeader_ = lobHeader;
	currentPos_ = 0;
}

void TupleValueStackAllocVarUtils::StackAllocLobReader::destroy() {
}

bool TupleValueStackAllocVarUtils::StackAllocLobReader::next(const void *&data, size_t &size) {
	if (currentPos_ >= lobHeader_->validCount_) {
		return false;
	}
	else {
		const uint8_t* addr = static_cast<const uint8_t*>(lobHeader_->getPartDataAddr(currentPos_));
		uint32_t partLobSize;
		size_t partHeaderSize = TupleValueUtils::decodeInt32(addr, partLobSize);
		size = static_cast<size_t>(partLobSize);
		data = addr + partHeaderSize;
		++currentPos_;
		return true;
	}
}

void TupleValueStackAllocVarUtils::StackAllocLobReader::reset() {
	currentPos_ = 0;
}

uint64_t TupleValueStackAllocVarUtils::StackAllocLobReader::getPartCount() {
	if (multiVarData_) {
		return multiVarData_->getLobPartCount();
	}
	else {
		return 0;
	}
}


TupleValue::StackAllocLobBuilder::StackAllocLobBuilder(
	VarContext &cxt, TupleList::TupleColumnType type, size_t initialCapacity)
: cxt_(cxt), type_(type), initialCapacity_(initialCapacity)
, topVarData_(NULL), totalSize_(0), partCount_(0)
, lastData_(NULL), lastSize_(0) {
	topVarData_ = TupleValue::StackAllocMultiVarData::create(cxt.getBaseVarContext(),
		initialCapacity);
}

TupleValue::StackAllocLobBuilder::~StackAllocLobBuilder() {
}

void TupleValue::StackAllocLobBuilder::append(const void *data, size_t size) {
	size_t remain = size;
	const uint8_t *addr = static_cast<const uint8_t*>(data);
	size_t offset = 0;
	while (remain > 0) {
		size_t reserved = append(remain);
		size_t partSize = reserved;
		memcpy(lastData_, addr + offset, partSize);
		remain -= partSize;
		offset += partSize;
	}
	lastSize_ = 0;
	lastData_ = NULL;
}

size_t TupleValue::StackAllocLobBuilder::append(size_t maxSize) {
	assert(maxSize <= UINT32_MAX);

	TupleValue::StackAllocMultiVarData *multiVarData =
		static_cast<TupleValue::StackAllocMultiVarData*>(topVarData_->getBody());

	if (maxSize >= TupleValueUtils::VAR_SIZE_4BYTE_THRESHOLD) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_LIMIT_EXCEEDED,
					"BLOB size limit exceeded: requestSize=" << maxSize);
	}
	uint32_t requestSize = static_cast<uint32_t>(maxSize);
	lastSize_ = requestSize;

	uint32_t lobPartHeaderSize = TupleValueUtils::getEncodedVarSize(requestSize);
	assert(cxt_.getStackAllocator());
	lastData_ = static_cast<uint8_t*>(
		cxt_.getStackAllocator()->allocate(maxSize + lobPartHeaderSize));

	uint8_t* partAddr = static_cast<uint8_t*>(lastData_);
	uint32_t encodedSize = TupleValueUtils::encodeVarSize(requestSize);
	memcpy(lastData_, &encodedSize, lobPartHeaderSize);  
	lastData_ = static_cast<uint8_t*>(lastData_) + lobPartHeaderSize;
	multiVarData->appendLobPartAddr(cxt_.getBaseVarContext(), topVarData_, partCount_, partAddr);

	++partCount_;
	totalSize_ += lastSize_;

	return lastSize_;
}

TupleValue TupleValue::StackAllocLobBuilder::fromDataStore(
		VarContext &cxt, const BaseObject &fieldObject, const void *data) {
	TupleValueVarUtils::VarData* varData = cxt.allocate(
		TupleValueVarUtils::VAR_DATA_CONTAINER, sizeof(TupleValueVarUtils::ContainerVarArray));
	TupleValueVarUtils::ContainerVarArray* containerVarArray =
		static_cast<TupleValueVarUtils::ContainerVarArray*>(varData->getBody());
	containerVarArray->initialize(fieldObject, data);
	return TupleValue(varData, TupleList::TYPE_BLOB);
}

TupleValue TupleValue::StackAllocLobBuilder::build() {
	return TupleValue(topVarData_, TupleList::TYPE_BLOB);
}

TupleValue::NanoTimestampBuilder::NanoTimestampBuilder(VarContext &cxt) :
		cxt_(cxt),
		pos_(0) {
}

void TupleValue::NanoTimestampBuilder::append(const void *data, size_t size) {
	if (size > sizeof(NanoTimestamp) ||
			pos_ + size > sizeof(NanoTimestamp)) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
				"Internal error by unexpected value size");
	}
	memcpy(data_ + pos_, data, size);
	pos_ += size;
}

void TupleValue::NanoTimestampBuilder::setValue(const NanoTimestamp &ts) {
	memcpy(data_, &ts, sizeof(ts));
	pos_ = sizeof(ts);
}

const NanoTimestamp* TupleValue::NanoTimestampBuilder::build() {
	const uint32_t valueSize = sizeof(NanoTimestamp);
	if (pos_ != valueSize) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TYPE_NOT_MATCH,
				"Internal error by state");
	}

	const uint8_t dataType = TupleValueTempStoreVarUtils::LTS_VAR_DATA_SINGLE_VAR;

	const uint32_t headerSize = sizeof(dataType);
	const uint32_t totalSize = headerSize + valueSize;

	const TupleValueVarUtils::VarDataType type =
			TupleValueVarUtils::VAR_DATA_TEMP_STORE;
	TupleValueVarUtils::VarData *varData = cxt_.allocate(type, totalSize);
	assert(varData->getBodySize(cxt_.getBaseVarContext()) >= totalSize);

	void *addr = varData->getBody();
	*static_cast<uint8_t*>(addr) = dataType;
	addr = static_cast<uint8_t*>(addr) + headerSize;

	memcpy(addr, data_, valueSize);
	return static_cast<const NanoTimestamp*>(addr);
}


TupleList::Body::AccessorManager::AccessorManager(TupleList::Body *body)
: tupleListBody_(body)
, readerList_(body->store_->getVarAllocator())
, readerTopNthList_(body->store_->getVarAllocator())
, isReaderStarted_(false)
, activeReaderCount_(0)
, detachedReaderCount_(0)
, accessTopMin_(0)
, readerAccessOrder_(TupleList::Reader::ORDER_SEQUENTIAL)
{
}

TupleList::Body::AccessorManager::~AccessorManager(){};

size_t TupleList::Body::AccessorManager::registReader(Reader &reader) {
	if (readerList_.size() == 0) {
		readerAccessOrder_ = reader.getAccessOrder();
	}
	else {
		if (reader.getAccessOrder() != readerAccessOrder_) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
					"Access mode is not same.");
		}
	}
	if (!isReaderStarted_) {
		ReaderList::iterator itr = readerList_.begin();
		for (; itr != readerList_.end(); ++itr) {
			if ((*itr) && (*itr)->isStarted()) {
				isReaderStarted_ = true;
				break;
			}
		}
	}
	if (isReaderStarted_) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
				"Another reader has already started.");
	}
	readerList_.push_back(&reader);
	readerTopNthList_.push_back(0);
	++activeReaderCount_;
	return readerList_.size() - 1;
}

void TupleList::Body::AccessorManager::attachReader(size_t id, Reader &reader) {
	if (detachedReaderCount_ > 0) {
		if (readerList_.size() <= id) {
			readerList_.resize(id + 1, NULL);
		}
		assert(!readerList_[id]);
		readerList_[id] = &reader;
		++activeReaderCount_;
		--detachedReaderCount_;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
				"Specified id is out of range (id=" << id <<")");
	}
}

void TupleList::Body::AccessorManager::detachReader(size_t id) {
	assert(readerList_.size() > id);
	if (readerList_.size() > id) {
		assert(readerList_[id]);
		readerList_[id] = NULL;
		--activeReaderCount_;
		++detachedReaderCount_;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
				"Specified id is out of range (id=" << id <<")");
	}
}

void TupleList::Body::AccessorManager::setReaderTopNth(size_t id, uint64_t blockNth) {
	assert(id < readerList_.size());
	assert(id < readerTopNthList_.size());
	uint64_t prevTop = UINT64_MAX;
	if (id < readerList_.size()) {
		prevTop = readerTopNthList_[id];
		readerTopNthList_[id] = blockNth;
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
				"Specified id is out of range (id=" << id <<")");
	}
	if (prevTop == accessTopMin_) {
		uint64_t min = UINT64_MAX;
		ReaderTopNthList::const_iterator itr = readerTopNthList_.begin();
		for (; itr != readerTopNthList_.end(); ++itr) {
			if ((*itr) == accessTopMin_) {
				min = (*itr);
				break;
			}
			else if ((*itr) < min) {
				min = (*itr);
			}
			assert(accessTopMin_ <= (*itr));
		}
		if (accessTopMin_ < min) {
			tupleListBody_->setActiveTopBlockNth(min);
			tupleListBody_->invalidateBlockId(accessTopMin_, min);
			accessTopMin_ = min;
		}
	}
}

void TupleList::Body::AccessorManager::closeReader(size_t id) {
	assert(readerList_.size() > id);
	if (readerList_.size() > id) {
		assert(readerList_[id]);
		readerList_[id] = NULL;
		--activeReaderCount_;
		if (activeReaderCount_ == 0) {
			readerList_.clear();
		}
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_TUPLELIST_READER_CREATE_FAILED,
				"Specified id is out of range (id=" << id <<")");
	}
}

size_t TupleList::Body::AccessorManager::getReaderCount() const {
	return readerList_.size();
}

bool TupleList::Body::AccessorManager::isReaderStarted() const {
	return isReaderStarted_;
}

uint64_t TupleList::Body::AccessorManager::getActiveReaderCount() const {
	return activeReaderCount_;
}

uint64_t TupleList::Body::AccessorManager::getDetachedReaderCount() const {
	return detachedReaderCount_;
}

uint64_t TupleList::Body::AccessorManager::getAccessTopMin() const {
	return accessTopMin_;
}
