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
	@brief Definition of local tempolary store
*/

#ifndef LOCAL_TEMP_STORE_H_
#define LOCAL_TEMP_STORE_H_

#include "config_table.h"
#include "util/allocator.h"
#include "util/code.h"
#include "util/container.h"
#include "util/trace.h"

#include <deque>
#include <list>
#include <vector>
#include <map>
#if UTIL_CXX11_SUPPORTED
#include <unordered_map>
#endif

#include "sql_common.h"

#include "event_engine.h"


UTIL_TRACER_DECLARE(SQL_TEMP_STORE);




typedef util::StackAllocator SQLAllocator;
typedef util::VariableSizeAllocator<> SQLVarSizeAllocator;

namespace picojson {
class value;
}

class LocalTempStore {
public:
	static const uint32_t DEFAULT_BLOCK_SIZE;
	static const uint32_t MAX_BLOCK_EXP_SIZE; 
	static const uint32_t MIN_BLOCK_EXP_SIZE; 

	static const int32_t DEFAULT_STORE_MEMORY_LIMIT_MB;
	static const int32_t DEFAULT_STORE_SWAP_FILE_SIZE_LIMIT_MB;
	static const std::string DEFAULT_SWAP_FILES_TOP_DIR;

	static const int32_t DEFAULT_STORE_SWAP_SYNC_SIZE_MB;
	static const int32_t DEFAULT_STORE_SWAP_SYNC_INTERVAL;

	static const uint64_t SWAP_FILE_IO_MAX_RETRY_COUNT;

	static const uint32_t DEFAULT_IO_WARNING_THRESHOLD_MILLIS;

	static const char8_t* const SWAP_FILE_BASE_NAME;
	static const char8_t* const SWAP_FILE_EXTENSION;
	static const char8_t SWAP_FILE_SEPARATOR;

	static const size_t BLOCK_INFO_POOL_FREE_LIMIT;

	static const size_t MIN_INITIAL_BUCKETS; 

	static const uint64_t MASK_ALREADY_SWAPPED = 0x8000000000000000ULL;

	enum GroupStatus {
		GROUP_NONE,
		GROUP_ACTIVE,
		GROUP_INACTIVE,
		GROUP_DELETED
	};

	enum ResourceStatus {
		RESOURCE_NONE,
		RESOURCE_INITIALIZED,
		RESOURCE_ACTIVE,
		RESOURCE_INACTIVE,
		RESOURCE_DELETED
	};

	typedef uint64_t BlockId;
	typedef uint64_t FileId;
	typedef uint64_t FileBlockId;
	typedef uint64_t GroupId;
	typedef uint64_t ResourceId;

	struct Config;
	class Group;
	class Block;
	class GroupInfoManager;
	class ResourceInfoManager;
	class BufferManager;
	class BlockInfoTable;

	static const GroupId UNDEF_GROUP_ID = UINT64_MAX;
	static const ResourceId UNDEF_RESOURCE_ID = UINT64_MAX;
	struct BaseBlockInfo { 
		uint64_t assignmentCount_;  
	};

	class BlockInfo {
		friend class LocalTempStore::BufferManager;
		friend class LocalTempStore::Block;
		friend class LocalTempStore::BlockInfoTable;

	public:
		~BlockInfo();

		const void* data() const;
		void* data();

		LocalTempStore& getStore();

		LocalTempStore::BlockId getBlockId() const;

		void dumpContents(std::ostream &ostr) const;

		void lock() { mutex_.lock(); };
		void unlock() { mutex_.unlock(); };
		bool checkLocked(bool getLock);

		void swapOut(LocalTempStore &store, BlockId id);
		void setup(LocalTempStore &store, BlockId id, bool isNew);

	private:
		BlockInfo(LocalTempStore &store, BlockId blockId);

		void initialize(LocalTempStore &store, BlockId id);
		void assign(const void *data, size_t size, bool isPartial);

		uint64_t addReference();
		uint64_t removeReference();
		uint64_t getReferenceCount() const;

		void clear();

		uint8_t* data_;
		BaseBlockInfo* baseBlockInfo_;
		LocalTempStore* store_;
		LocalTempStore::BlockId blockId_;
		util::Mutex mutex_;
		uint64_t refCount_;  
	};


	enum ResourceType {
		RESOURCE_TUPLE_LIST,
		RESOURCE_JSON_LIST
	};

	struct GroupInfo {
		GroupId groupId_;
		util::Atomic<int64_t> resourceCount_;
		GroupStatus status_;

		GroupInfo() : groupId_(UNDEF_GROUPID), resourceCount_(0), status_(GROUP_NONE) {}
	};


	struct ResourceInfo {
		ResourceId id_;
		GroupId groupId_;
		void* resource_;
		ResourceType type_;
		ResourceStatus status_;
		uint32_t blockExpSize_;

		ResourceInfo() : id_(UNDEF_RESOURCEID), groupId_(UNDEF_GROUPID),
						 resource_(NULL),
						 type_(RESOURCE_TUPLE_LIST), status_(RESOURCE_NONE),
						 blockExpSize_(0) {}
	};


	typedef util::FixedSizeAllocator<util::Mutex> LTSFixedSizeAllocator;
	typedef util::VariableSizeAllocator<util::Mutex> LTSVariableSizeAllocator;

	static const GroupId UNDEF_GROUPID;
	static const ResourceId UNDEF_RESOURCEID;
	static const BlockId UNDEF_BLOCKID;
	static const uint64_t UNDEF_FILEBLOCKID;

	LocalTempStore(
			const Config &config, LTSVariableSizeAllocator &varAllocator,
			bool autoUseDefault = true);
	~LocalTempStore();

	void setSwapFileSizeLimit(uint64_t size);
	void setStableMemoryLimit(uint64_t size);

	GroupId allocateGroup();
	void deallocateGroup(GroupId groupId);

	void incrementGroupResourceCount(GroupId groupId);
	void decrementGroupResourceCount(GroupId groupId);
	int64_t getGroupResourceCount(GroupId groupId);

	ResourceId allocateResource(ResourceType type, GroupId groupId = UNDEF_GROUPID);

	void deallocateResource(ResourceId resourceId);

	void useBlockSize(uint32_t blockSize);

	LTSVariableSizeAllocator& getVarAllocator();

	ResourceInfo& getResourceInfo(ResourceId resourceId);
	BlockInfo& getBlockInfo(BlockId blockId);
	const BlockInfo& getBlockInfo(BlockId blockId) const;

	size_t getVariableAllocatorStat(size_t &totalSize, size_t &freeSize, size_t &hugeCount, size_t &hugeSize);

	BufferManager& getBufferManager();

	uint32_t getDefaultBlockSize();
	uint32_t getDefaultBlockExpSize();

	std::string& getSwapFilesTopDir();

	uint64_t getTotalSwapFileSize();

	uint64_t getTotalSwapReadBlockCount();
	uint64_t getTotalSwapWriteBlockCount();

	uint64_t getTotalSwapReadOperation();
	uint64_t getTotalSwapReadSize();
	uint64_t getTotalSwapReadTime();
	uint64_t getTotalSwapWriteOperation();
	uint64_t getTotalSwapWriteSize();
	uint64_t getTotalSwapWriteTime();

	uint64_t getActiveBlockCount();
	uint64_t getMaxActiveBlockCount();


	void countBlockInfo(uint64_t &freeCount, uint64_t &latchedCount,
						uint64_t &unlatchedCount, uint64_t &noneCount);

	void countFileBlock(uint64_t &totalCount, uint64_t &freeCount, uint64_t &fileSize);
	void dumpActiveBlockId(std::ostream &ostr);

	void dumpBlockInfo();
	void dumpBlockInfoContents();

	static uint32_t getBlockExpSize(BlockId blockId);
	static BlockId getBlockNth(BlockId blockId);
	static BlockId makeBlockId(BlockId blockNth, uint32_t blockExpSize);

	static void dumpMemory(std::ostream &ostr, const void* addr, size_t size);
	static void dumpBackTrace();

private:
	LocalTempStore(const LocalTempStore&);
	LocalTempStore& operator=(const LocalTempStore&);

	util::Mutex mutex_;

	LTSVariableSizeAllocator* varAlloc_;

	BufferManager* bufferManager_;

	ResourceInfoManager* resourceInfoManager_;

	std::string swapFilesTopDir_;

	uint32_t defaultBlockExpSize_;

	util::Atomic<uint64_t> maxGroupId_;
};

struct LocalTempStore::Config {
	Config(const ConfigTable &config);
	Config();

	uint32_t blockSize_;

	uint64_t swapFileSizeLimit_;
	uint64_t stableMemoryLimit_;

	std::string swapFilesTopDir_;

	uint32_t swapSyncInterval_;
	uint64_t swapSyncSize_;
	uint32_t swapReleaseInterval_;
};


class LocalTempStore::Group {
public:
	explicit Group(LocalTempStore &store);
	~Group();

	LocalTempStore& getStore() const;
	LocalTempStore::GroupId getId() const;

private:
	Group(const Group&);
	Group& operator=(const Group&);

	LocalTempStore &store_;
	GroupId groupId_;
};


class LocalTempStore::Block {
	friend class LocalTempStore::BufferManager;
public:
	struct Header;

	static const uint8_t FLAG_FULL_BLOCK;      
	static const uint8_t FLAG_PARTIAL_BLOCK_1; 
	static const uint8_t FLAG_PARTIAL_BLOCK_2; 

	Block();
	Block(LocalTempStore &store, LocalTempStore::BlockId blockId);    
	Block(LocalTempStore &store, size_t blockSize, uint64_t affinity);  
	Block(LocalTempStore &store, const void *data, size_t size);      
	explicit Block(BlockInfo &blockInfo);      
	~Block();

	Block(const Block &block);
	Block& operator=(const Block &block);

	const void* data() const;
	void* data();

	LocalTempStore::BlockId getBlockId() const;

	const BlockInfo* getBlockInfo() const;
	BlockInfo* getBlockInfo();

	uint64_t getReferenceCount() const;
	uint64_t getAssignmentCount() const;

	bool isSwapped() const;
	void setSwapped(bool flag);

	bool operator<(const Block &another) const {
		return (getBlockId() < another.getBlockId());
	};
	bool operator==(const Block &another) const {
		return (getBlockId() == another.getBlockId());
	};


	void dumpContents(std::ostream &ostr) const;
	void encode(EventByteOutStream &out, uint32_t checkRatio = 99);
	void decode(util::StackAllocator &alloc, EventByteInStream &in);
	void encodeMain(EventByteOutStream &out, uint32_t checkRatio);
	void decodeMain(util::StackAllocator &alloc, util::ArrayByteInStream &in);
private:
	uint64_t addReference();
	uint64_t removeReference();

	uint64_t addAssignment();
	uint64_t removeAssignment();

	void initialize(LocalTempStore &store);
	void assign(const void *data, size_t size, bool isPartial);
	void clear();

	BlockInfo *blockInfo_;
};

struct LocalTempStore::Block::Header {
	static const uint32_t CHECKSUM_OFFSET = 0;
	static const uint32_t MAGIC_OFFSET = CHECKSUM_OFFSET + sizeof(uint32_t);
	static const uint32_t VERSION_OFFSET = MAGIC_OFFSET + sizeof(uint16_t);
	static const uint32_t BLOCK_ID_OFFSET = VERSION_OFFSET + sizeof(uint16_t);
	static const uint32_t BLOCK_EXP_SIZE_OFFSET = BLOCK_ID_OFFSET + sizeof(uint64_t);
	static const uint32_t CONTIGUOUS_BLOCK_NUM_OFFSET = BLOCK_EXP_SIZE_OFFSET + sizeof(uint32_t);

	static const uint32_t TUPLE_COUNT_OFFSET = CONTIGUOUS_BLOCK_NUM_OFFSET + sizeof(uint32_t);
	static const uint32_t COLUMN_NUM_OFFSET = TUPLE_COUNT_OFFSET + sizeof(uint32_t);
	static const uint32_t COLUMN_LIST_CHECKSUM_OFFSET = COLUMN_NUM_OFFSET + sizeof(uint16_t);
	static const uint32_t NEXT_FIX_DATA_OFFSET = COLUMN_LIST_CHECKSUM_OFFSET + sizeof(uint16_t);
	static const uint32_t NEXT_VAR_DATA_OFFSET = NEXT_FIX_DATA_OFFSET + sizeof(uint32_t);

	static const uint32_t BLOCK_HEADER_SIZE = NEXT_VAR_DATA_OFFSET + 4;

	static const uint16_t MAGIC_NUMBER = 0xce17;
	static const uint16_t VERSION_NUMBER = 0x0003;

	Header();
	static uint32_t getHeaderCheckSum(const void *block);
	static void setHeaderCheckSum(void *block, uint32_t value);
	static void updateHeaderCheckSum(void *block);
	static uint32_t calcBlockCheckSum(void *block);
	static uint16_t getMagic(const void *block);
	static void setMagic(void *block);
	static uint16_t getVersion(const void *block);
	static void setVersion(void *block);
	static uint64_t getBlockId(const void *block);
	static void setBlockId(void *block, uint64_t blockId);
	static uint32_t getBlockExpSize(const void *block);
	static void setBlockExpSize(void *block, uint32_t blockExpSize);
	static int32_t getContiguousBlockNum(const void *block);
	static void setContiguousBlockNum(void *block, int32_t contiguousBlockNum);

	static void resetHeader(void *block, uint32_t blockExpSize);
	static void validateHeader(void *block, uint32_t blockExpSize, bool verifyCheckSum);

	static uint32_t getTupleCount(const void *block);
	static void setTupleCount(void *block, uint32_t tupleCount);
	static uint16_t getColumnNum(const void *block);
	static void setColumnNum(void *block, uint16_t columnNum);
	static uint16_t getColumnListChecksum(const void *block);
	static void setColumnListChecksum(void *block, uint16_t colListChecksum);
	static uint32_t getNextFixDataOffset(const void *block);
	static void setNextFixDataOffset(void *block, uint32_t offset);
	static uint32_t getNextVarDataOffset(const void *block);
	static void setNextVarDataOffset(void *block, uint32_t offset);
};


class LocalTempStore::BlockInfoTable {
	friend class LocalTempStore::BufferManager;
public:
	static const uint64_t MASK_ALREADY_SWAPPED = LocalTempStore::MASK_ALREADY_SWAPPED;

	BlockInfoTable(
			LTSVariableSizeAllocator &varAlloc, LocalTempStore &store,
			uint32_t blockExpSize, uint64_t stableMemoryLimit
			, uint64_t swapFileSizeLimit
	);

	~BlockInfoTable();

	void insert(BlockId id, BlockInfo*& block);

	BlockInfo* lookup(BlockId id);

	void remove(BlockId id);

	LocalTempStore::BlockInfo* getNextTarget();

	void update(BlockInfo* &reuseBlockInfo, BlockId newId);

	void setFixedAllocator(LTSFixedSizeAllocator *fixedAlloc);

	BlockId allocateBlockNth(bool force = false);
	size_t freeBlockNth(BlockId start);
	BaseBlockInfo* lookupBaseInfo(BlockId id);
	const BaseBlockInfo* lookupBaseInfo(BlockId id) const;

	BlockId getCurrentMaxInuseBlockNth(BlockId id);

	uint64_t getMaxActiveBlockCount();

	void dumpActiveBlockId(std::ostream &ostr, uint32_t blockExpSize);

	void countBlockInfo(uint32_t blockExpSize,
						uint64_t &freeCount, uint64_t &latchedCount,
						uint64_t &unlatchedCount, uint64_t &noneCount);
private:
	BlockInfoTable(const BlockInfoTable&);
	BlockInfoTable& operator=(const BlockInfoTable&);

	typedef std::list< LocalTempStore::BlockInfo*, util::StdAllocator<
	  LocalTempStore::BlockInfo*, LocalTempStore::LTSVariableSizeAllocator> > BlockInfoList;
	typedef std::pair< LocalTempStore::BlockId, BlockInfoList::iterator > BlockInfoMapEntry;
#if UTIL_CXX11_SUPPORTED
	typedef std::unordered_map< LocalTempStore::BlockId, BlockInfoList::iterator,
	std::hash<LocalTempStore::BlockId>, std::equal_to<LocalTempStore::BlockId>,
	util::StdAllocator<BlockInfoMapEntry, LTSVariableSizeAllocator> > BlockInfoMap;
#else
	typedef std::map< LocalTempStore::BlockId, BlockInfoList::iterator,
	std::less<LocalTempStore::BlockId>,
	util::StdAllocator<BlockInfoMapEntry, LTSVariableSizeAllocator> > BlockInfoMap;
#endif
	typedef BlockInfoMap::iterator BlockInfoMapItr;

	typedef std::deque< BaseBlockInfo, util::StdAllocator<
	  BaseBlockInfo, LocalTempStore::LTSVariableSizeAllocator> > BaseBlockInfoArray;

	typedef std::set< LocalTempStore::BlockId, std::less<LocalTempStore::BlockId>,
	util::StdAllocator< LocalTempStore::BlockId, LocalTempStore::LTSVariableSizeAllocator> > BlockIdSet;

	LTSVariableSizeAllocator &varAlloc_;
	LTSFixedSizeAllocator *fixedAlloc_;
	LocalTempStore &store_;

	util::ObjectPool<LocalTempStore::BlockInfo> *blockInfoPool_;
	BlockInfoList blockInfoList_;
	BlockInfoMap blockInfoMap_;
	BlockIdSet freeBlockIdSet_;	
	BaseBlockInfoArray baseBlockInfoArray_;
	const uint32_t blockExpSize_;
	util::Atomic<uint64_t> stableMemoryLimit_;
	util::Atomic<uint64_t> blockIdMaxLimit_;
};

class LocalTempStore::BufferManager {
public:
	class File;
	friend class File;
public:
	static const uint64_t MASK_ALREADY_SWAPPED = LocalTempStore::MASK_ALREADY_SWAPPED;
	static const size_t SWAP_FILE_TRIM_UNIT_SIZE = 16 * 1024 * 1024; 

	BufferManager(
	  LTSVariableSizeAllocator &varAlloc,
	  const LocalTempStore::Config &config,
	  LocalTempStore &store);
	~BufferManager();

	void allocate(
			BlockInfo* &blockInfo, uint64_t affinity = 0, bool force = false);
	void release(BlockInfo &blockInfo, uint64_t affinity = 0);
	void get(BlockId id, BlockInfo* &blockInfo);

	void incrementActiveBlockCount(); 
	void decrementActiveBlockCount(); 

	void swapIn(BlockId id, BlockInfo &blockInfo);
	void swapOut(BlockInfo &blockInfo);

	size_t getAllocatedMemory();

	uint64_t addAssignment(BlockId blockId);
	uint64_t removeAssignment(BlockId blockId);
	uint64_t getAssignmentCount(BlockId blockId);

	uint64_t addReference(BlockInfo &blockInfo);
	uint64_t removeReference(BlockInfo &blockInfo);
	uint64_t getReferenceCount(BlockId blockId);

	bool isAlreadySwapped(BlockId blockId);
	void setAlreadySwapped(BlockId blockId, bool flag);

	uint64_t getSwapFileSize();

	uint64_t getReadBlockCount();
	uint64_t getReadOperation();
	uint64_t getReadSize();
	uint64_t getReadTime();
	uint64_t getWriteBlockCount();
	uint64_t getWriteOperation();
	uint64_t getWriteSize();
	uint64_t getWriteTime();

	void setSwapFileSizeLimit(uint64_t size);
	void setStableMemoryLimit(uint64_t size);

	uint64_t getCurrentTotalMemory();
	uint64_t getPeakTotalMemory();
	bool checkTotalMemory();

	void addCurrentMemory();
	void subCurrentMemory();

	uint64_t getActiveBlockCount();
	uint64_t getMaxActiveBlockCount();

	void countBlockInfo(uint64_t &freeCount, uint64_t &latchedCount,
						uint64_t &unlatchedCount, uint64_t &noneCount);

	void countFileBlock(uint64_t &totalCount, uint64_t &fileSize);

	void dumpActiveBlockId(std::ostream &ostr);

	void dumpBlockInfo();
	void dumpBlockInfoContents();

private:
	template<typename L>
	class LockGuardVariant {
	public:
		explicit LockGuardVariant(L &lockObject);
		~LockGuardVariant();
		void acquire();
		void release();
		bool isLocked(); 

	private:
		LockGuardVariant(const LockGuardVariant&);
		LockGuardVariant& operator=(const LockGuardVariant&);

		L &lockObject_;
		util::Atomic<int32_t> count_;
	};

	BufferManager(const BufferManager&);
	BufferManager& operator=(const BufferManager&);

	bool allocateMemory(BlockId id, BlockInfo* &blockInfo);
	void allocateMemoryMain(BlockId id, BlockInfo* &blockInfo);

	LocalTempStore::BaseBlockInfo* lookupBaseInfo(BlockId blockId);
#ifndef NDEBUG
	LocalTempStore::BlockInfo* lookup(BlockId blockId);
#endif

	typedef std::vector< LocalTempStore::BlockInfo*, util::StdAllocator<
	  LocalTempStore::BlockInfo*, LocalTempStore::LTSVariableSizeAllocator> > BlockInfoVector;

	LTSVariableSizeAllocator &varAlloc_;
	LocalTempStore &store_;

	uint64_t swapFileSizeLimit_;
	File *file_;
	util::Mutex mutex_;
	LocalTempStore::BlockInfoTable blockInfoTable_;	

	size_t maxBlockNth_;
	BlockId inuseMaxBlockNth_;
	BlockId wroteMaxBlockNth_;
	BlockId trimSwapFileThreshold_;

	const uint32_t blockExpSize_;
	const uint32_t blockSize_;

	void allocateBlock(BlockInfo &block);

	util::Atomic<uint64_t> stableMemoryLimit_;
	util::Atomic<uint64_t> currentTotalMemory_;
	util::Atomic<uint64_t> peakTotalMemory_;

	uint64_t activeBlockCount_;
};


class LocalTempStore::BufferManager::File {
public:
	File(LTSVariableSizeAllocator &varAlloc, LocalTempStore &store,
			const LocalTempStore::Config &config,
			uint32_t blockExpSize, const std::string &topDir);
	~File();

	size_t readBlock(FileBlockId fileBlockId, size_t count, void *buffer);
	size_t writeBlock(FileBlockId fileBlockId, size_t count, void *buffer);

	uint64_t getReadBlockCount();
	uint64_t getWriteBlockCount();
	void resetReadBlockCount();
	void resetWriteBlockCount();

	uint64_t getReadOperation();
	uint64_t getReadSize();
	uint64_t getReadTime();
	uint64_t getWriteOperation();
	uint64_t getWriteSize();
	uint64_t getWriteTime();

	bool open();
	void close();
	void flush();
	void trim(uint64_t size);

	void advise(int32_t advice); 

	inline uint64_t getBlockNum() const {
		return blockNum_;
	}
	inline void setIOWarningThresholdMillis(uint32_t millis) {
		ioWarningThresholdMillis_ = millis;
	}
	inline uint32_t getIOWarningThresholdMillis() {
		return ioWarningThresholdMillis_;
	}

	std::string dumpUsedChunkInfo();

	void countFileBlock(uint64_t &totalCount, uint64_t &fileSize);

	static std::string makeFileName(const std::string &topDir, uint64_t fileId);

	uint64_t getFileSize();
	std::string dump();

private:
	File(const File&);
	File& operator=(const File&);

	LTSVariableSizeAllocator &varAlloc_;
	LocalTempStore &store_;

	std::string swapFilesTopDir_;

	util::NamedFile *file_;
	std::string fileName_;
	const size_t blockExpSize_;
	const size_t blockSize_;

	uint64_t blockNum_;
	util::Atomic<uint64_t> readBlockCount_;
	util::Atomic<uint64_t> readOperation_;
	util::Atomic<uint64_t> readSize_;
	util::Atomic<uint64_t> readTime_;
	util::Atomic<uint64_t> writeBlockCount_;
	util::Atomic<uint64_t> writeOperation_;
	util::Atomic<uint64_t> writeSize_;
	util::Atomic<uint64_t> writeTime_;

	uint32_t swapSyncInterval_;
	uint64_t swapSyncCount_;
	uint64_t swapSyncSize_;
	uint64_t swapReleaseInterval_;

	uint32_t ioWarningThresholdMillis_;
};


class LocalTempStore::ResourceInfoManager {
	typedef LocalTempStore::ResourceId ResourceId;
	typedef LocalTempStore::ResourceInfo ResourceInfo;
public:
	ResourceInfoManager(LocalTempStore &store);
	~ResourceInfoManager();

	GroupId allocateGroup();
	GroupInfo& getGroupInfo(GroupId id);
	void freeGroup(GroupId id);

	void incrementGroupResourceCount(GroupId groupId);
	void decrementGroupResourceCount(GroupId groupId);
	int64_t getGroupResourceCount(GroupId groupId);

	ResourceId allocateResource(LocalTempStore::ResourceType type, LocalTempStore::GroupId groupId);
	ResourceInfo& getResourceInfo(ResourceId id);
	void freeResource(ResourceId id);
private:
	typedef std::pair< GroupId, GroupInfo > GroupInfoEntry;
	typedef std::pair< ResourceId, ResourceInfo > ResourceInfoEntry;
#if UTIL_CXX11_SUPPORTED
	typedef std::unordered_map< GroupId, GroupInfo,
		std::hash<LocalTempStore::GroupId>, std::equal_to<LocalTempStore::GroupId>,
		util::StdAllocator<GroupInfoEntry, LTSVariableSizeAllocator> > GroupInfoMap;
	typedef std::unordered_map< ResourceId, ResourceInfo,
		std::hash<LocalTempStore::ResourceId>, std::equal_to<LocalTempStore::ResourceId>,
		util::StdAllocator<ResourceInfoEntry, LTSVariableSizeAllocator> > ResourceInfoMap;
#else
	typedef std::map< GroupId, GroupInfo,
		std::less<GroupId>,
		util::StdAllocator<GroupInfoEntry, LTSVariableSizeAllocator> > GroupInfoMap;
	typedef std::map< ResourceId, ResourceInfo,
		std::less<ResourceId>,
		util::StdAllocator<ResourceInfoEntry, LTSVariableSizeAllocator> > ResourceInfoMap;
#endif
	typedef GroupInfoMap::iterator GroupInfoMapItr;
	typedef ResourceInfoMap::iterator ResourceInfoMapItr;

	util::Mutex mutex_;
	LocalTempStore &store_;
	GroupInfoMap groupInfoMap_;
	ResourceInfoMap resourceInfoMap_;
	uint64_t maxGroupId_;
	uint64_t maxResourceId_;
};


inline uint32_t LocalTempStore::getBlockExpSize(BlockId blockId) {
	return static_cast<uint32_t>(blockId & 0xffUL);
}

inline LocalTempStore::BlockId LocalTempStore::getBlockNth(BlockId blockId) {
	return (blockId >> 8);
}

inline LocalTempStore::BlockId LocalTempStore::makeBlockId(BlockId blockNth, uint32_t blockExpSize) {
	return (blockNth << 8UL | blockExpSize);
}

inline uint32_t LocalTempStore::getDefaultBlockSize() {
	return static_cast<uint32_t>(1UL << defaultBlockExpSize_);
}

inline uint32_t LocalTempStore::getDefaultBlockExpSize() {
	return defaultBlockExpSize_;
}

inline LocalTempStore::LTSVariableSizeAllocator& LocalTempStore::getVarAllocator() {
	return *varAlloc_;
}

inline LocalTempStore::BufferManager& LocalTempStore::getBufferManager() {
	return *bufferManager_;
}


inline uint64_t LocalTempStore::getTotalSwapFileSize() {
	return bufferManager_->getSwapFileSize();
}

inline uint64_t LocalTempStore::getTotalSwapReadBlockCount() {
	return bufferManager_->getReadBlockCount();
}

inline uint64_t LocalTempStore::getTotalSwapReadOperation() {
	return bufferManager_->getReadOperation();
}

inline uint64_t LocalTempStore::getTotalSwapReadSize() {
	return bufferManager_->getReadSize();
}

inline uint64_t LocalTempStore::getTotalSwapReadTime() {
	return bufferManager_->getReadTime();
}

inline uint64_t LocalTempStore::getTotalSwapWriteBlockCount() {
	return bufferManager_->getWriteBlockCount();
}

inline uint64_t LocalTempStore::getTotalSwapWriteOperation() {
	return bufferManager_->getWriteOperation();
}

inline uint64_t LocalTempStore::getTotalSwapWriteSize() {
	return bufferManager_->getWriteSize();
}

inline uint64_t LocalTempStore::getTotalSwapWriteTime() {
	return bufferManager_->getWriteTime();
}

inline uint64_t LocalTempStore::getActiveBlockCount() {
	return bufferManager_->getActiveBlockCount();
}
inline uint64_t LocalTempStore::getMaxActiveBlockCount() {
	return bufferManager_->getMaxActiveBlockCount();
}

inline LocalTempStore::Block::Block(const Block &block) {
	blockInfo_ = block.blockInfo_;
	if (blockInfo_) {
		blockInfo_->getStore().getBufferManager().addReference(*blockInfo_);
	}
}

inline LocalTempStore::Block& LocalTempStore::Block::operator=(const Block &another) {
	if (this != &another) {
		if (blockInfo_) {
			blockInfo_->getStore().getBufferManager().removeReference(*blockInfo_);
		}
		blockInfo_ = another.blockInfo_;
		if (blockInfo_) {
			blockInfo_->getStore().getBufferManager().addReference(*blockInfo_);
		}
	}
	return *this;
}


inline LocalTempStore::Block::~Block() try {
	if (blockInfo_) {
		blockInfo_->getStore().getBufferManager().removeReference(*blockInfo_);
	}
	blockInfo_ = NULL;
}
catch (...) {
}

inline const void* LocalTempStore::Block::data() const {
	return (blockInfo_) ? blockInfo_->data() : NULL;
}

inline void* LocalTempStore::Block::data() {
	return (blockInfo_) ? blockInfo_->data() : NULL;
}

inline LocalTempStore::BlockId LocalTempStore::Block::getBlockId() const {
	assert(blockInfo_);
	return blockInfo_->getBlockId();
}

inline const LocalTempStore::BlockInfo* LocalTempStore::Block::getBlockInfo() const {
	return blockInfo_;
}

inline LocalTempStore::BlockInfo* LocalTempStore::Block::getBlockInfo() {
	return blockInfo_;
}

inline uint64_t LocalTempStore::Block::addReference() {
	assert(blockInfo_);
	return blockInfo_->getStore().getBufferManager().addReference(*blockInfo_);
}

inline uint64_t LocalTempStore::Block::removeReference() {
	assert(blockInfo_);
	return blockInfo_->getStore().getBufferManager().removeReference(*blockInfo_);
}

inline uint64_t LocalTempStore::Block::getReferenceCount() const {
	assert(blockInfo_);
	return blockInfo_->getReferenceCount();
}

inline uint64_t LocalTempStore::Block::addAssignment() {
	assert(blockInfo_);
	return blockInfo_->getStore().getBufferManager().addAssignment(blockInfo_->getBlockId());
}

inline uint64_t LocalTempStore::Block::removeAssignment() {
	assert(blockInfo_);
	return blockInfo_->getStore().getBufferManager().removeAssignment(blockInfo_->getBlockId());
}

inline uint64_t LocalTempStore::Block::getAssignmentCount() const {
	assert(blockInfo_);
	return blockInfo_->getStore().getBufferManager().getAssignmentCount(blockInfo_->getBlockId());
}

inline bool LocalTempStore::Block::isSwapped() const {
	assert(blockInfo_);
	return blockInfo_->getStore().getBufferManager().isAlreadySwapped(blockInfo_->getBlockId());
}
inline void LocalTempStore::Block::setSwapped(bool flag) {
	assert(blockInfo_);
	blockInfo_->getStore().getBufferManager().setAlreadySwapped(blockInfo_->getBlockId(), flag);
}


inline uint32_t LocalTempStore::Block::Header::getHeaderCheckSum(const void *block) {
	return *reinterpret_cast<const uint32_t*>(
		static_cast<const uint8_t*>(block) + CHECKSUM_OFFSET);
}

inline void LocalTempStore::Block::Header::setHeaderCheckSum(void *block, uint32_t value) {
	memcpy(static_cast<uint8_t*>(block) + CHECKSUM_OFFSET, &value, sizeof(uint32_t));
}

inline void LocalTempStore::Block::Header::updateHeaderCheckSum(void *block) {
	const size_t blockSize = 1 << getBlockExpSize(block);
	uint32_t resetValue = 0;
	setHeaderCheckSum(block, resetValue);
	uint32_t checkSum = util::fletcher32(block, blockSize);
	memcpy(static_cast<uint8_t*>(block) + CHECKSUM_OFFSET, &checkSum, sizeof(uint32_t));
}

inline uint32_t LocalTempStore::Block::Header::calcBlockCheckSum(void *block) {
	const size_t blockSize = 1 << getBlockExpSize(block);
	uint32_t origCheckSum = getHeaderCheckSum(block);
	uint32_t resetValue = 0;
	setHeaderCheckSum(block, resetValue);
	uint32_t currentCheckSum = util::fletcher32(block, blockSize);
	setHeaderCheckSum(block, origCheckSum);
	return currentCheckSum;
}

inline uint16_t LocalTempStore::Block::Header::getMagic(const void *block) {
	return *reinterpret_cast<const uint16_t*>(
		static_cast<const uint8_t*>(block) + MAGIC_OFFSET);
}

inline void LocalTempStore::Block::Header::setMagic(void *block) {
	static const uint16_t magic_number = MAGIC_NUMBER;
	memcpy(static_cast<uint8_t*>(block) + MAGIC_OFFSET, &magic_number, sizeof(uint16_t));
}

inline uint16_t LocalTempStore::Block::Header::getVersion(const void *block) {
	return *reinterpret_cast<const uint16_t*>(
		static_cast<const uint8_t*>(block) + VERSION_OFFSET);
}

inline void LocalTempStore::Block::Header::setVersion(void *block) {
	static const uint16_t version_number = VERSION_NUMBER;
	memcpy(static_cast<uint8_t*>(block) + VERSION_OFFSET, &version_number, sizeof(uint16_t));
}

inline uint32_t LocalTempStore::Block::Header::getBlockExpSize(const void *block) {
	return *reinterpret_cast<const uint32_t*>(
		static_cast<const uint8_t*>(block) + BLOCK_EXP_SIZE_OFFSET);
}
inline void LocalTempStore::Block::Header::setBlockExpSize(void *block, uint32_t blockExpSize) {
	assert(LocalTempStore::MIN_BLOCK_EXP_SIZE <= blockExpSize
			&& blockExpSize <= LocalTempStore::MAX_BLOCK_EXP_SIZE);
	memcpy(static_cast<uint8_t*>(block) + BLOCK_EXP_SIZE_OFFSET, &blockExpSize, sizeof(uint32_t));
}
inline uint64_t LocalTempStore::Block::Header::getBlockId(const void *block) {
	return *reinterpret_cast<const uint64_t*>(
		static_cast<const uint8_t*>(block) + BLOCK_ID_OFFSET);
}
inline void LocalTempStore::Block::Header::setBlockId(void *block, uint64_t blockId) {
	memcpy(static_cast<uint8_t*>(block) + BLOCK_ID_OFFSET, &blockId, sizeof(uint64_t));
}
inline int32_t LocalTempStore::Block::Header::getContiguousBlockNum(const void *block) {
	return *reinterpret_cast<const int32_t*>(
		static_cast<const uint8_t*>(block) + CONTIGUOUS_BLOCK_NUM_OFFSET);
}
inline void LocalTempStore::Block::Header::setContiguousBlockNum(void *block, int32_t contiguousBlockNum) {
	memcpy(static_cast<uint8_t*>(block) + CONTIGUOUS_BLOCK_NUM_OFFSET, &contiguousBlockNum, sizeof(int32_t));
}

inline void LocalTempStore::Block::Header::resetHeader(void *block, uint32_t blockExpSize) {
	setHeaderCheckSum(block, 0);
	setMagic(block);
	setVersion(block);
	setBlockId(block, 0);
	setBlockExpSize(block, blockExpSize);
	setContiguousBlockNum(block, 0);

	setTupleCount(block, 0);
	setColumnNum(block, 0);
	setColumnListChecksum(block, 0);
	setNextFixDataOffset(block, 0);
	setNextVarDataOffset(block, 0);
}

inline uint32_t LocalTempStore::Block::Header::getTupleCount(const void *block) {
	return *reinterpret_cast<const uint32_t*>(
		static_cast<const uint8_t*>(block) + TUPLE_COUNT_OFFSET);
}
inline void LocalTempStore::Block::Header::setTupleCount(void *block, uint32_t tupleCount) {
	memcpy(static_cast<uint8_t*>(block) + TUPLE_COUNT_OFFSET, &tupleCount, sizeof(uint32_t));
}
inline uint16_t LocalTempStore::Block::Header::getColumnNum(const void *block) {
	return *reinterpret_cast<const uint16_t*>(
		static_cast<const uint8_t*>(block) + COLUMN_NUM_OFFSET);
}
inline void LocalTempStore::Block::Header::setColumnNum(void *block, uint16_t columnNum) {
	memcpy(static_cast<uint8_t*>(block) + COLUMN_NUM_OFFSET, &columnNum, sizeof(uint16_t));
}
inline uint16_t LocalTempStore::Block::Header::getColumnListChecksum(const void *block) {
	return *reinterpret_cast<const uint16_t*>(
		static_cast<const uint8_t*>(block) + COLUMN_LIST_CHECKSUM_OFFSET);
}
inline void LocalTempStore::Block::Header::setColumnListChecksum(void *block, uint16_t colListChecksum) {
	memcpy(static_cast<uint8_t*>(block) + COLUMN_LIST_CHECKSUM_OFFSET, &colListChecksum, sizeof(uint16_t));
}
inline uint32_t LocalTempStore::Block::Header::getNextFixDataOffset(const void *block) {
	return *reinterpret_cast<const uint32_t*>(
		static_cast<const uint8_t*>(block) + NEXT_FIX_DATA_OFFSET);
}
inline void LocalTempStore::Block::Header::setNextFixDataOffset(void *block, uint32_t offset) {
	memcpy(static_cast<uint8_t*>(block) + NEXT_FIX_DATA_OFFSET, &offset, sizeof(uint32_t));
}
inline uint32_t LocalTempStore::Block::Header::getNextVarDataOffset(const void *block) {
	return *reinterpret_cast<const uint32_t*>(
		static_cast<const uint8_t*>(block) + NEXT_VAR_DATA_OFFSET);
}
inline void LocalTempStore::Block::Header::setNextVarDataOffset(void *block, uint32_t offset) {
	memcpy(static_cast<uint8_t*>(block) + NEXT_VAR_DATA_OFFSET, &offset, sizeof(uint32_t));
}

inline LocalTempStore::BlockInfo::~BlockInfo() {
}

inline const void* LocalTempStore::BlockInfo::data() const {
	return data_;
}

inline void* LocalTempStore::BlockInfo::data() {
	return data_;
}

inline LocalTempStore& LocalTempStore::BlockInfo::getStore() {
	assert(store_);
	return *store_;
}

inline LocalTempStore::BlockId LocalTempStore::BlockInfo::getBlockId() const {
	return blockId_;
}

inline void LocalTempStore::BlockInfo::clear() {
	data_ = NULL;
	baseBlockInfo_ = NULL;
	store_ = NULL;
	blockId_ = LocalTempStore::UNDEF_BLOCKID;
}

inline uint64_t LocalTempStore::BlockInfo::addReference() {
	return ++refCount_;
}

inline uint64_t LocalTempStore::BlockInfo::removeReference() {
	assert(refCount_ > 0);
	return --refCount_;
}

inline uint64_t LocalTempStore::BlockInfo::getReferenceCount() const {
	return refCount_;
}


inline uint64_t LocalTempStore::BufferManager::getCurrentTotalMemory() {
	return currentTotalMemory_;
}

inline uint64_t LocalTempStore::BufferManager::getPeakTotalMemory() {
	return peakTotalMemory_;
}

inline void LocalTempStore::BufferManager::addCurrentMemory() {
	currentTotalMemory_ += blockSize_;
	if (currentTotalMemory_ > peakTotalMemory_) {
		peakTotalMemory_ = currentTotalMemory_;
	}
}

inline void LocalTempStore::BufferManager::subCurrentMemory() {
	assert(currentTotalMemory_ >= blockSize_);
	currentTotalMemory_ -= blockSize_;
}

inline bool LocalTempStore::BufferManager::checkTotalMemory() {
	return (currentTotalMemory_ < stableMemoryLimit_);
}

inline uint64_t LocalTempStore::BufferManager::getActiveBlockCount() {
	util::LockGuard<util::Mutex> guard(mutex_);
	return activeBlockCount_;
}
inline uint64_t LocalTempStore::BufferManager::getMaxActiveBlockCount() {
	return blockInfoTable_.getMaxActiveBlockCount();
}


inline void LocalTempStore::BufferManager::incrementActiveBlockCount() {
	++activeBlockCount_;
}

inline LocalTempStore::BlockId LocalTempStore::BlockInfoTable::getCurrentMaxInuseBlockNth(BlockId id) {
	static_cast<void>(id);
	BlockIdSet::reverse_iterator rItr = freeBlockIdSet_.rbegin();
	BlockId maxInuseNth = LocalTempStore::getBlockNth(*rItr);
	for (; rItr != freeBlockIdSet_.rend(); ++rItr) {
		--maxInuseNth;
		if (LocalTempStore::getBlockNth(*rItr) != maxInuseNth) {
			++maxInuseNth;
			break;
		}
	}
	return maxInuseNth;
}


inline uint64_t LocalTempStore::BlockInfoTable::getMaxActiveBlockCount() {
	return static_cast<uint64_t>(baseBlockInfoArray_.size());
}

inline LocalTempStore::BlockId LocalTempStore::BlockInfoTable::allocateBlockNth(bool force) {
	BlockId blockNth = LocalTempStore::UNDEF_BLOCKID;
	if (freeBlockIdSet_.size() > 0) {
		BlockIdSet::iterator itr = freeBlockIdSet_.begin();
		blockNth = LocalTempStore::getBlockNth(*itr);
		if (!force && (blockNth > blockIdMaxLimit_)) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_LIMIT_EXCEEDED,
				"Swap file size limit exceeded. blockId=" << (*itr) <<
				", blockNth=" << blockNth);
		}
		freeBlockIdSet_.erase(itr);
	}
	else {
		if (!force && (baseBlockInfoArray_.size() > (blockIdMaxLimit_ + 1))) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_LIMIT_EXCEEDED,
				"Swap file size limit exceeded. blockNth=" <<
				baseBlockInfoArray_.size());
		}
		BaseBlockInfo baseInfo;
		baseBlockInfoArray_.push_back(baseInfo);
		blockNth = baseBlockInfoArray_.size() - 1;
	}
	baseBlockInfoArray_[static_cast<size_t>(blockNth)].assignmentCount_ = 0;
	return blockNth;
}

inline uint64_t LocalTempStore::BufferManager::addAssignment(BlockId blockId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	BaseBlockInfo *baseBlockInfo = blockInfoTable_.lookupBaseInfo(blockId);
	assert(baseBlockInfo);
	uint64_t maskVal = MASK_ALREADY_SWAPPED & baseBlockInfo->assignmentCount_;
	uint64_t newCount = ((~MASK_ALREADY_SWAPPED) & baseBlockInfo->assignmentCount_) + 1;
	baseBlockInfo->assignmentCount_ = (maskVal | newCount);
	return baseBlockInfo->assignmentCount_;
}

inline uint64_t LocalTempStore::BufferManager::removeAssignment(BlockId blockId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	BaseBlockInfo *baseBlockInfo = blockInfoTable_.lookupBaseInfo(blockId);
	assert(baseBlockInfo);
	uint64_t maskVal = MASK_ALREADY_SWAPPED & baseBlockInfo->assignmentCount_;
	uint64_t count = ((~MASK_ALREADY_SWAPPED) & baseBlockInfo->assignmentCount_);
	assert(count > 0);
	--count;
	if (0 == count) {
		BlockInfo *blockInfo = blockInfoTable_.lookup(blockId);
		if (blockInfo) {
			if (blockInfo->getReferenceCount() == 0) {
				release(*blockInfo, 0);
			}
		}
		else {
			blockInfoTable_.remove(blockId);
			decrementActiveBlockCount();
		}
		maskVal = 0;
	}
	baseBlockInfo->assignmentCount_ = (maskVal | count);
	return baseBlockInfo->assignmentCount_;
}

inline uint64_t LocalTempStore::BufferManager::getAssignmentCount(BlockId blockId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	const BaseBlockInfo *baseBlockInfo = blockInfoTable_.lookupBaseInfo(blockId);
	assert(baseBlockInfo);
	return ((~MASK_ALREADY_SWAPPED) & baseBlockInfo->assignmentCount_);
}

inline bool LocalTempStore::BufferManager::isAlreadySwapped(BlockId blockId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	const BaseBlockInfo *baseBlockInfo = blockInfoTable_.lookupBaseInfo(blockId);
	assert(baseBlockInfo);
	return ((MASK_ALREADY_SWAPPED) & baseBlockInfo->assignmentCount_) != 0;
}

inline void LocalTempStore::BufferManager::setAlreadySwapped(
		BlockId blockId, bool flag) {
	util::LockGuard<util::Mutex> guard(mutex_);
	BaseBlockInfo *baseBlockInfo = blockInfoTable_.lookupBaseInfo(blockId);
	assert(baseBlockInfo);
	if (flag) {
		baseBlockInfo->assignmentCount_ |= MASK_ALREADY_SWAPPED;
	}
	else {
		baseBlockInfo->assignmentCount_ &= (~MASK_ALREADY_SWAPPED);
	}
}

inline uint64_t LocalTempStore::BufferManager::addReference(BlockInfo &blockInfo) {
	util::LockGuard<util::Mutex> guard(mutex_);
	return blockInfo.addReference();
}

inline uint64_t LocalTempStore::BufferManager::removeReference(BlockInfo &blockInfo) {
	util::LockGuard<util::Mutex> guard(mutex_);
	uint64_t result =  blockInfo.removeReference();
	if (0 == result) {
		if (0 == ((~MASK_ALREADY_SWAPPED) & blockInfo.baseBlockInfo_->assignmentCount_)) {
			release(blockInfo, 0);
		}
	}
	return result;
}

inline uint64_t LocalTempStore::BufferManager::getReferenceCount(BlockId blockId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	const BlockInfo *blockInfo = blockInfoTable_.lookup(blockId);
	assert(blockInfo);
	return blockInfo->getReferenceCount();
}

inline LocalTempStore::BaseBlockInfo*
		LocalTempStore::BufferManager::lookupBaseInfo(BlockId blockId) {
	return blockInfoTable_.lookupBaseInfo(blockId);
}

#ifndef NDEBUG
inline LocalTempStore::BlockInfo*
		LocalTempStore::BufferManager::lookup(BlockId blockId) {
	return blockInfoTable_.lookup(blockId);
}
#endif


inline LocalTempStore::BlockInfo* LocalTempStore::BlockInfoTable::lookup(BlockId id) {
	BlockInfoMap::iterator itr = blockInfoMap_.find(id);
	if (blockInfoMap_.end() == itr) {
		return NULL;
	}
	else {
		BlockInfoList::iterator listItr = itr->second;
		blockInfoList_.splice(blockInfoList_.end(), blockInfoList_, listItr); 
		return *listItr;
	}
}

inline void LocalTempStore::BlockInfoTable::insert(BlockId id, BlockInfo* &blockInfo) {
	blockInfo = blockInfoPool_->poll();
	if (blockInfo == NULL) {
		blockInfo = UTIL_OBJECT_POOL_NEW(*blockInfoPool_) LocalTempStore::BlockInfo(store_, id);
	}
	blockInfo->initialize(store_, id);
	blockInfo->baseBlockInfo_ = lookupBaseInfo(id);
	blockInfoList_.push_back(blockInfo); 
	blockInfoMap_[id] = --blockInfoList_.end();
}

inline void LocalTempStore::BlockInfoTable::remove(BlockId id) {
	BlockInfoMap::iterator itr = blockInfoMap_.find(id);
	if (blockInfoMap_.end() != itr) {
		BlockInfoList::iterator listItr = itr->second;
		LocalTempStore::BlockInfo* blockInfo = *listItr;
		UTIL_OBJECT_POOL_DELETE(*blockInfoPool_, blockInfo);
		blockInfoMap_.erase(itr);
		blockInfoList_.erase(listItr);
	}
	freeBlockIdSet_.insert(id);
}

inline void LocalTempStore::BlockInfoTable::update(BlockInfo* &reuseBlockInfo, BlockId newId) {
	BlockInfoMap::iterator itr = blockInfoMap_.find(reuseBlockInfo->getBlockId());
	assert(blockInfoMap_.end() != itr);
	BlockInfoList::iterator listItr = itr->second;
	assert((*listItr) == reuseBlockInfo);
	blockInfoMap_.erase(itr);

	itr = blockInfoMap_.find(newId);
	assert(blockInfoMap_.end() == itr);
	blockInfoList_.splice(blockInfoList_.end(), blockInfoList_, listItr); 
	blockInfoMap_[newId] = --blockInfoList_.end();
#ifndef NDEBUG
	BlockInfo *checkBlockInfo = *(blockInfoMap_[newId]);
	assert(checkBlockInfo->getBlockId() == reuseBlockInfo->getBlockId());
#endif
}


inline LocalTempStore::BaseBlockInfo* LocalTempStore::BlockInfoTable::lookupBaseInfo(BlockId id) {
	uint64_t nth = LocalTempStore::getBlockNth(id);
	assert(nth < baseBlockInfoArray_.size());
	return &(baseBlockInfoArray_[static_cast<size_t>(nth)]);
}


inline LocalTempStore::BlockInfo* LocalTempStore::BlockInfoTable::getNextTarget() {
	LocalTempStore::BlockInfo* target = NULL;
	if (blockInfoList_.size() == 0) {
		return NULL;
	}
	BlockInfoList::iterator itr = blockInfoList_.begin();
	for (; itr != blockInfoList_.end(); ++itr) {
		if ((*itr)->refCount_ == 0) {
			target = *itr;
#ifndef NDEBUG
			assert((*itr)->data_);
			BlockId headerBlockId =
				LocalTempStore::Block::Header::getBlockId((*itr)->data_);
			if (headerBlockId != 0) {
				assert(headerBlockId == (*itr)->blockId_);
			}
#endif
			break;
		}
	}
	return target;
}



class LTSEventBufferManager : public EventEngine::BufferManager {
public:
	typedef EventEngine::BufferId BufferId;
	typedef EventEngine::VariableSizeAllocator VariableSizeAllocator;
	explicit LTSEventBufferManager(LocalTempStore &store);
	virtual ~LTSEventBufferManager();

	size_t getUnitSize();

	std::pair<BufferId, void*> allocate();
	void deallocate(const BufferId &id);

	void* latch(const BufferId &id);
	void unlatch(const BufferId &id);

private:
	LTSEventBufferManager(const LTSEventBufferManager&);
	LTSEventBufferManager& operator=(const LTSEventBufferManager&);

	static void* getBlockBody(LocalTempStore::BlockInfo &blockInfo);

	LocalTempStore &store_;
	LocalTempStore::Group group_;
};


template<typename L>
inline LocalTempStore::BufferManager::LockGuardVariant<L>::LockGuardVariant(L &lockObject)
: lockObject_(lockObject), count_(0) {
	acquire();
}

template<typename L>
inline void LocalTempStore::BufferManager::LockGuardVariant<L>::acquire() {
	lockObject_.lock();
	assert(count_ == 0);
	count_ = 1;
}

template<typename L>
inline void LocalTempStore::BufferManager::LockGuardVariant<L>::release() {
	if (count_ > 0) {
		count_ = 0;
		lockObject_.unlock();
	}
}

template<typename L>
inline LocalTempStore::BufferManager::LockGuardVariant<L>::~LockGuardVariant() {
	release();
}

template<typename L>
inline bool LocalTempStore::BufferManager::LockGuardVariant<L>::isLocked() {
	return (count_ > 0);
}
#endif 
