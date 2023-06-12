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
	@brief Definition of ChunkManager
*/
#ifndef CHUNK_MANAGER_H_
#define CHUNK_MANAGER_H_

#include "util/container.h"
#include "utility_v5.h"
#include "data_type.h"
#include "data_store_common.h"
#include "config_table.h"
#include <vector>
#include <string>
#include <memory>
#include <unordered_map>
#include "log_manager.h" 




class VirtualFileBase;
/*!
	@brief 物理チャンク情報
*/
class RawChunkInfo {
private:
	int64_t chunkId_;
	DSGroupId groupId_;
	int64_t offset_;
	int8_t vacancy_;
	int64_t dummy_;
	int64_t logversion_;
	bool cumulativeDirty_;
	bool differentialDirty_;

public:
	RawChunkInfo()
	: chunkId_(-1), groupId_(-1), offset_(-1), vacancy_(-1), dummy_(0),
	  logversion_(-1), cumulativeDirty_(false), differentialDirty_(false) {}

	inline RawChunkInfo& setChunkId(int64_t chunkId) {
		chunkId_ = chunkId;
		return *this;
	}

	inline int64_t getChunkId() const {
		return chunkId_;
	}

	inline RawChunkInfo& setGroupId(DSGroupId groupId) {
		groupId_ = groupId;
		return *this;
	}

	inline DSGroupId getGroupId() const {
		return groupId_;
	}

	inline RawChunkInfo& setOffset(int64_t offset) {
		offset_ = offset;
		return *this;
	}

	inline int64_t getOffset() const {
		return offset_;
	}

	RawChunkInfo& setVacancy(int8_t vacancy) {
		vacancy_ = vacancy;
		return *this;
	}

	inline int8_t getVacancy() const {
		return vacancy_;
	}

	RawChunkInfo& setLogVersion(int64_t logversion) {
		logversion_ = logversion;
		return *this;
	}

	inline int64_t getLogVersion() const {
		return logversion_;
	}

	RawChunkInfo& setCumulativeDirty(bool flag) {
		cumulativeDirty_ = flag;
		return *this;
	}

	inline bool isCumulativeDirty() const {
		return cumulativeDirty_;
	}

	RawChunkInfo& setDifferentialDirty(bool flag) {
		differentialDirty_ = flag;
		return *this;
	}

	inline bool isDifferentialDirty() const {
		return differentialDirty_;
	}

	inline int64_t getSignature() const {
		return 911 * (getChunkId() + 1) + 373 * (getGroupId() + 1) + 19 * getVacancy();
	}

	std::string toString() const;
};

/*!
	@brief チャンクエクステント

	@note extentSize_数分
	@note ・ログバージョン
	@note ・量子化された空き情報
	@note ・オフセット
*/
class ChunkExtent {
private:
	int16_t extentSize_;
	bool use_;
	DSGroupId groupId_;
	int64_t baseLogVersion_;
	std::vector<int8_t> deltaLogVersions_;
	std::vector<int8_t> vacancies_;
	std::vector<uint8_t> flags_;
	MimicLongArray* offsets_;
	int16_t free_;
	int16_t numUsed_;

	static const uint8_t CUMULATIVE_DIRTY_FLAG_BIT = UINT8_C(1) << 0;
	static const uint8_t DIFFERENTIAL_DIRTY_FLAG_BIT = UINT8_C(1) << 1;
public:
	ChunkExtent(int16_t extentsize);
	~ChunkExtent();

	int16_t getExtentSize() const;

	ChunkExtent& use(DSGroupId groupId);

	ChunkExtent& unuse();

	bool isUsed() const;

	void put0(int16_t pos, int64_t offset, int8_t vacancy);

	void free0(int16_t pos);

	ChunkExtent& initFreeChain();

	int16_t peek() const;

	int16_t allocate();

	void tryAllocate(int16_t pos); 

	void free(int16_t pos);

	int16_t getNumUsed() const;

	DSGroupId getGroupId() const;

	int64_t getOffset(int16_t pos) const;

	void setOffset(int16_t targetpos, int64_t offset);

	int16_t getNext(int16_t pos) const;

	void setNext(int16_t pos, int16_t next);

	void use(int16_t pos);

	void unuse(int16_t pos);

	bool isUsed(int16_t pos) const;

	int8_t getVacancy(int16_t pos) const;

	void setVacancy(int16_t pos, int8_t vacancy);

	int64_t getLogVersion(int16_t pos) const;

	void setLogVersion(int16_t targetpos, int64_t logversion);

	bool isDifferentialDirty(int16_t pos) const;

	void setDifferentialDirty(int16_t pos, bool flag);

	bool isCumulativeDirty(int16_t pos) const;

	void setCumulativeDirty(int16_t pos, bool flag);

	uint8_t getFlags(int16_t pos) const;

	std::string toString() const;
};


/*!
	@brief チャンクグループの統計値
*/
struct ChunkGroupStats {
	enum Param {
		GROUP_STAT_USE_BLOCK_COUNT,
		GROUP_STAT_USE_BUFFER_COUNT,
		GROUP_STAT_SWAP_READ_COUNT,
		GROUP_STAT_SWAP_WRITE_COUNT,
		GROUP_STAT_END
	};

	typedef LocalStatTable<Param, GROUP_STAT_END> Table;
	typedef Table::Mapper Mapper;

	explicit ChunkGroupStats(const Mapper *mapper);

	Table table_;
};

/*!
	@brief メッシュドチャンクテーブル

	@note ・エクステント配列
			・エクステント群 extents_
	@note グループに対する
			・所有エクステントリンク群 chaintable_
			・空きチャンクを含むエクステントリンク群 freetable_
	@note ・グループ集合 groupMap_
	@note ・空きエクステント chain0_
*/
class MeshedChunkTable {
	friend class StoreV5Impl;
public:
	class Group;
	class StableChunkCursor;
	class CsectionChunkCursor;

private:
	static const int16_t DEFAULT_EXTENT_SIZE = 64; 

	int16_t extentPower_;
	int16_t extentPosMask_;
	int16_t extentSize_;
	std::deque<ChunkExtent*> extents_;
	DLTable* chainTable_;
	DLTable* freeTable_;
	std::unordered_map<DSGroupId, Group*> groupMap_;
	DLTable::Chain* chain0_;

public:
	MeshedChunkTable();
	MeshedChunkTable(int16_t extentsize);
	virtual ~MeshedChunkTable();

	void init(int16_t extentsize);

	void redo(int64_t chunkId, DSGroupId groupId, int64_t offset, int8_t vacancy);

	int64_t reinit();

	int64_t getFreeSpace(std::deque<int64_t>& frees);

	int32_t allocateExtent(DSGroupId groupId);

	bool tryAllocateExtent(DSGroupId groupId, int32_t index);

	std::string toString();

	std::vector<RawChunkInfo> dump();

	void extentChecker();

	Group* createGroup();

	Group* putGroup(DSGroupId groupId);

	Group* checkGroup(DSGroupId groupId);

	void removeGroupFromMap(DSGroupId groupId);

	void resetIncrementalDirtyFlags(bool resetAll);

private:
	int64_t getChunkId(int32_t index, int16_t pos);

	int32_t getIndex(int64_t chunkId);

	int16_t getPos(int64_t chunkId);

public:
	StableChunkCursor createStableChunkCursor();

	CsectionChunkCursor createCsectionChunkCursor();
};

/*!
	@brief グループに対する操作
*/
class MeshedChunkTable::Group {
	friend MeshedChunkTable;
public:
	class StableGroupChunkCursor;

	Group(DSGroupId groupId, MeshedChunkTable& mct);
	virtual ~Group();

	StableGroupChunkCursor createStableGroupChunkCursor();

	void allocate(RawChunkInfo &rawInfo);

	bool isAllocated(int64_t chunkId);

	void tryAllocate(int64_t chunkId, RawChunkInfo &rawInfo);

	RawChunkInfo get(int64_t chunkId);

	ChunkExtent* get(int64_t chunkId, int16_t &pos);
	void get(int64_t chunkId, RawChunkInfo &info);

	void remove(int64_t chunkId, int64_t logversion);

	void set(int64_t chunkId, int64_t offset, int64_t logversion, bool flags);

	void setVacancy(int64_t chunkId, int8_t vacancy);

	int8_t getVacancy(int64_t chunkId);

	void setDirtyFlags(int64_t chunkId, bool isDirty);

	int64_t getOffset(int64_t chunkId);

	DSGroupId getId() {return groupId_;}

	ChunkGroupStats::Table& stats() { return stats_.table_; }

	uint64_t getBlockCount() {
		return stats()(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT).get();
	}

	void dump(std::vector<RawChunkInfo>& chunkinfos);

	std::string toString();

private:
	void addExtentChain(int32_t index);

	void addExtentFree(int32_t index);

	void extentChecker();

	DSGroupId groupId_;
	DLTable::Chain* chain_;
	DLTable::Chain* free_;
	MeshedChunkTable& mct_;
	ChunkGroupStats stats_;
};

/*!
	@brief 単一グループチャンク走査
*/
class MeshedChunkTable::Group::StableGroupChunkCursor {
private:
	MeshedChunkTable& mct_;
	Group& group_;
	int32_t index_;
	int16_t pos_;
	int32_t counter_;

public:
	StableGroupChunkCursor(Group& group)
		: mct_(group.mct_), group_(group), index_(0), pos_(0), counter_(0) {
		index_ = group.chain_->peek();
	}
	~StableGroupChunkCursor() {};
	void set(int64_t chunkId);

	ChunkExtent* next(int64_t &chunkId, int16_t &pos);

	bool next(RawChunkInfo &info);

	bool current(RawChunkInfo &info);
	bool advance(RawChunkInfo &info);
	bool next(bool advance, RawChunkInfo &info);

	std::string toString() const;
private:
	void setRawInfo(ChunkExtent* extent, int16_t pos, RawChunkInfo& rawInfo);
};


/*!
	@brief グループ横断チャンク走査

	@note 主にフルチェックポイントで使われる
*/
class MeshedChunkTable::StableChunkCursor {
private:
	MeshedChunkTable& mct_;
	int32_t index_;
	int16_t pos_;

public:
	StableChunkCursor(MeshedChunkTable& mct);
	~StableChunkCursor() {};

	ChunkExtent* next(int64_t &chunkId, int16_t &pos);
	bool next(RawChunkInfo &info);
};

/*!
	@brief 断面性の高いチャンクカーソル

	@note 主にパーシャルチェックポイントで使われる。耐停止性、終わりがある。
*/
class MeshedChunkTable::CsectionChunkCursor {
private:
	MeshedChunkTable& mct_;
	int32_t index_;
	int16_t pos_;
	int32_t maxindex_;

public:
	CsectionChunkCursor(MeshedChunkTable& mct);

	bool hasNext();

	bool next(RawChunkInfo& info);
	ChunkExtent* next(int64_t &chunkId, int16_t &pos);
};


class ChunkBuffer;
/*!
	@brief チャンク操作クラス

	@note チャンクマネージャと上位アプリをつなぐインターフェイスクラス
*/
class ChunkAccessor {
private:
	MeshedChunkTable* chunkTable_;
	ChunkBuffer* chunkBuffer_;
	PartitionId pId_;
	DSGroupId groupId_;

	int64_t chunkId_;
	Chunk* chunk_;
	ChunkBufferFrameRef frameRef_;

public:
	ChunkAccessor() :
			chunkTable_(NULL),
			chunkBuffer_(NULL),
			pId_(-1),
			groupId_(-1),
			chunkId_(-1),
			chunk_(NULL) {
	}

	~ChunkAccessor();

	bool isEmpty();

	void set(
			MeshedChunkTable *chunkTable, ChunkBuffer *chunkBuffer,
			PartitionId pId, DSGroupId groupId, int64_t chunkId,
			const ChunkBufferFrameRef &frameRef);

	Chunk* getChunk();

	PartitionId getPartitionId();
	DSGroupId getGroupId();
	int64_t getChunkId();
	ChunkBufferFrameRef getFrameRef();

	void setVacancy(int8_t vacancy);
	int8_t getVacancy();

	void unpin();
	void unpin(bool dirty);

	int64_t getPinCount(int64_t offset);

	void reset();

private:
	ChunkAccessor(const ChunkAccessor&);
	ChunkAccessor& operator=(const ChunkAccessor&);

	void forceReset();
	void assign(
			MeshedChunkTable *chunkTable, ChunkBuffer *chunkBuffer,
			PartitionId pId, DSGroupId groupId, int64_t chunkId,
			Chunk *chunk, const ChunkBufferFrameRef &frameRef);
};

struct ChunkManagerStats;

/*!
	@brief フリースペース管理
*/
class FreeSpace {
private:
	std::set<int64_t> frees_;
	int64_t maxoffset_;
	Locker& locker_;
	ChunkManagerStats& chunkManagerStats_;

public:
	FreeSpace(Locker& locker, ChunkManagerStats& chunkManagerStats);

	void init(std::deque<int64_t>& frees, int64_t maxoffset);

	void free(int64_t offset);

	int64_t allocate();

	int64_t getMaxOffset() { return maxoffset_; };

	const std::set<int64_t>& getFrees();

	std::string toString() const;
};

#define EXEC_FAILURE(errorNo)

/*!
	@brief チャンクマネージャ統計値
*/
struct ChunkManagerStats {
	enum Param {
		CHUNK_STAT_USE_CHUNK_COUNT,
		CHUNK_STAT_CP_FILE_USE_COUNT,
		CHUNK_STAT_CP_FILE_SIZE,
		CHUNK_STAT_CP_FILE_FLUSH_COUNT,
		CHUNK_STAT_CP_FILE_FLUSH_TIME,
		CHUNK_STAT_END
	};

	typedef TimeStatTable<Param, CHUNK_STAT_END> TimeTable;
	typedef TimeTable::BaseTable Table;
	typedef Table::Mapper Mapper;

	explicit ChunkManagerStats(const Mapper *mapper);

	static void setUpMapper(Mapper &mapper, uint32_t chunkSize);

	static void setUpBasicMapper(Mapper &mapper, uint32_t chunkSize);
	static void setUpCPMapper(Mapper &mapper);

	Table table_;
	TimeTable timeTable_;
};

class Log;
class AffinityManager;
class ChunkCopyContext;
/*!
	@brief チャンクマネージャ
*/
class ChunkManager {
public:
	enum FreeMode {
		SELF_FREE_MODE = 0,  
		BATCH_FREE_MODE = 1  
	};

	enum SearchMode {
		SMALL_SIZE_SKIP_SEARCH_MODE = 0,  
		SMALL_SIZE_SEARCH_MODE = 1,  
	};
	/*!
		@brief Attribute of Chunk Category
	*/
	struct ChunkCategoryAttribute {
		ChunkCategoryAttribute() : freeMode_(SELF_FREE_MODE), searchMode_(SMALL_SIZE_SKIP_SEARCH_MODE){}
		FreeMode freeMode_;
		SearchMode searchMode_;
	};

	struct Coders;

	class Config : public ConfigTable::ParamHandler {
		friend class ChunkManager;
	public:
		Config(PartitionGroupId partitionGroupNum, PartitionId partitionNum,
				ChunkCategoryId chunkCategoryNum, int32_t chunkSize,
				int32_t affinitySize, uint64_t storeMemoryLimit
			    , ConfigTable &configTable
			   , double atomicStoreMemoryColdRate_
			   , uint64_t cpFileFlushSize
			   , bool cpFileFlushAutoClearCache
				, double bufferHashTableSizeRate
			   , uint32_t cpFileSplitCount
			   , uint32_t cpFileSplitStripeSize
			);
		~Config();

		PartitionGroupId getPartitionGroupId(PartitionId pId) const;
		PartitionGroupId getPartitionGroupNum() const;
		PartitionId getPartitionNum() const;
		ChunkCategoryId getChunkCategoryNum() const;
		uint8_t getChunkExpSize() const;
		uint32_t getChunkSize() const;
		bool setAtomicStoreMemoryLimit(uint64_t memoryLimitByte);
		uint64_t getAtomicStoreMemoryLimit();
		uint64_t getAtomicStoreMemoryLimitNum();
		bool setAtomicCheckpointMemoryLimit(uint64_t memoryLimitByte);
		uint64_t getAtomicCheckpointMemoryLimit();
		uint64_t getAtomicCheckpointMemoryLimitNum();

		double getAtomicStoreMemoryColdRate();
		bool setAtomicStoreMemoryColdRate(double rate);
		uint64_t getAtomicCpFileFlushSize();
		bool setAtomicCpFileFlushSize(uint64_t size);

		uint64_t getAtomicCpFileFlushInterval();

		bool getAtomicCpFileAutoClearCache();
		bool setAtomicCpFileAutoClearCache(bool flag);
		double getBufferHashTableSizeRate();
		static std::vector<std::string> parseCpFileDirList(ConfigTable &configTable);

		void setCpFileDirList(const char* jsonArray);

		uint32_t getCpFileSplitCount();
		uint32_t getCpFileSplitStripeSize();
		const std::vector<std::string> &getCpFileDirList();
		uint32_t getAffinitySize();

		static ChunkCompressionTypes::Mode getCompressionMode(
				const ConfigTable& configTable);

		std::string dump();
	private:
		const PartitionGroupId partitionGroupNum_; 
		const PartitionId partitionNum_;
		const ChunkCategoryId categoryNum_;
		const uint8_t chunkExpSize_;
		const uint32_t chunkSize_; 
		util::Atomic<uint32_t>
			atomicAffinitySize_;  
		const bool isWarmStart_;
		const uint32_t maxOnceSwapNum_; 

		util::Atomic<uint64_t> atomicStoreMemoryColdRate_;
		std::vector<PartitionGroupId> pgIdList_;
		util::Atomic<uint64_t> atomicStorePoolLimit_;
		util::Atomic<uint32_t> atomicStorePoolLimitNum_;
		util::Atomic<uint64_t> atomicCheckpointPoolLimit_;
		util::Atomic<uint32_t> atomicCheckpointPoolLimitNum_;
		util::Atomic<uint64_t> atomicCpFileFlushSize_;
		util::Atomic<uint64_t> atomicCpFileFlushInterval_;
		util::Atomic<bool> atomicCpFileAutoClearCache_;
		double bufferHashTableSizeRate_;  
		std::vector<std::string> cpFileDirList_; 
		uint32_t cpFileSplitCount_; 
		uint32_t cpFileSplitStripeSize_; 
		bool isWarmStart() const {
			return isWarmStart_;
		}
		uint32_t getMaxOnceSwapNum() const {
			return maxOnceSwapNum_;
		}
		void appendPartitionGroupId(PartitionGroupId pgId) {
			pgIdList_.push_back(pgId);
		}
		bool isValid();
		void setUpConfigHandler(ConfigTable& configTable);
		void operator()(ConfigTable::ParamId id, const ParamValue& value);
		Config(const Config&);
		Config& operator=(const Config&);
	};

public:
	static const size_t CHUNKKEY_BIT_NUM = 22;
	static const size_t CHUNKKEY_BITS = 0x3FFFFF;

	static const uint8_t MAX_CHUNK_EXP_SIZE_ = 26;
	static const int32_t MIN_CHUNK_EXP_SIZE_ = 15;
	static const int32_t CHUNK_CATEGORY_NUM = 8;

	static const int32_t CHUNK_CATEGORY_MASK = 7;
	static ChunkCategoryId getChunkCategoryId(DSGroupId groupId) {
		return static_cast<ChunkCategoryId>(groupId & CHUNK_CATEGORY_MASK);
	}

	static const uint32_t MAX_ONCE_SWAP_SIZE_BYTE_ = 64 * 1024 * 1024;  
	static const uint32_t LIMIT_CHUNK_NUM_LIST[12];

	static inline bool isLargeChunkMode(uint32_t chunkExpSize) {
		return (chunkExpSize > 20) ? true : false;
	}

	static inline uint64_t calcMaxChunkId(uint32_t blockSize) {
		const uint32_t chunkExpSize = util::nextPowerBitsOf2(blockSize);
		const bool isLargeChunkMode = ChunkManager::isLargeChunkMode(chunkExpSize);
		return isLargeChunkMode ? ((static_cast<int32_t>(1) << 26) - 2): INT32_MAX - 1;
	}

	ChunkManager(
			ConfigTable& configTable, LogManager<MutexLocker> &logmanager,
			ChunkBuffer &chunkBuffer, AffinityManager &affinitymanager,
			int32_t chunkSize, PartitionId pId,
			ChunkManagerStats &stats);
	virtual ~ChunkManager();

	int32_t getChunkSize();

	void redo(const Log* log);

	void reinit();

	void startCheckpoint(int64_t logVersion);

	int64_t getLogVersion() { return logversion_; }
	void setLogVersion(int64_t logVersion) { logversion_ = logVersion; }

	MeshedChunkTable::StableChunkCursor createStableChunkCursor();

	MeshedChunkTable::CsectionChunkCursor createCsectionChunkCursor();

	void freeSpace(const Log* log);

	bool hasBlock(DSGroupId groupId);
	bool hasBlock();
	ChunkId getHeadChunkId(DSGroupId groupId);

	void adjustStoreMemory(uint64_t newLimit);

	uint64_t getCurrentLimit();

	void validatePinCount();

	Config& getConfig() {
		return config_;
	}

	PartitionId getPId() {
		return pId_;
	}
	int32_t getStatus() {
		return status_;
	}

	void flushBuffer(int64_t offset);

	void dumpBuffer(int64_t offset);

	MeshedChunkTable::Group* putGroup(DSGroupId groupId);

	MeshedChunkTable::Group* checkGroup(DSGroupId groupId);

	void allocateChunk(MeshedChunkTable::Group &group, ChunkAccessor &ca);

	void getChunk(DSGroupId groupId, int64_t chunkId, ChunkAccessor &ca);
	void getChunk(MeshedChunkTable::Group& group, int64_t chunkId, ChunkAccessor &ca);

	void update(MeshedChunkTable::Group &group, ChunkAccessor &ca);

	int64_t getRefCount(DSGroupId groupId, int64_t chunkId);

	void removeChunk(MeshedChunkTable::Group &group, int64_t chunkId);

	bool removeGroup(DSGroupId groupId);

	void fin();

	int64_t copyChunk(ChunkCopyContext* cxt, int64_t offset, bool keepSrcOffset);

	void restoreChunk(uint8_t* buff, size_t buffSize, int64_t offset);
	void restoreChunk(
			int64_t chunkId, int64_t groupId, int64_t offset, uint8_t vacancy,
			const void* blockImage, int64_t size);

	void resetIncrementalDirtyFlags(bool resetAll);

	const ChunkGroupStats::Table& groupStats(DSGroupId groupId);

	const ChunkManagerStats& getChunkManagerStats() { 
		return chunkManagerStats_;
	}

	ChunkBuffer& getChunkBuffer() { 
		return chunkBuffer_;
	}

	int64_t getFileNum() {
		return freespace_->getMaxOffset();
	}

	int64_t releaseUnusedFileBlocks();

public:
	void setSwapOutCounter(int64_t counter);

	int64_t getSwapOutCounter();

	void setStoreMemoryAgingSwapRate(double rate);
	double getStoreMemoryAgingSwapRate();
	void updateStoreMemoryAgingParams();

	void updateStoreObjectUseStats();

	int64_t getMaxChunkId() {
		return MAX_CHUNK_ID;
	}

	ChunkManagerStats& stats() {
		return chunkManagerStats_;
	}

	std::string toString() const;
	std::string getSignature();

private:
	void appendUndo(int64_t chunkId, DSGroupId groupId, int64_t offset, int8_t vacancy);

	uint64_t getAtomicUseChunkCount() const;
	uint64_t getCheckpointChunkNum() const;
	uint64_t getBatchFreeCount() const;

	void extentChecker();

private:
	const ConfigTable& configTable_;
	LogManager<MutexLocker> &defaultLogManager_;
	ChunkBuffer &chunkBuffer_;
	AffinityManager &affinityManager_;
	int32_t chunkSize_;

	MeshedChunkTable* chunkTable_;
	NoLocker* freespaceLocker_;
	FreeSpace* freespace_;
	int64_t logversion_;
	int32_t status_;
	PartitionId pId_;  
	Config config_;
	ChunkManagerStats &chunkManagerStats_;
	const int64_t MAX_CHUNK_ID;
	int64_t blockCount_;
};

struct ChunkManager::Coders {
	static const util::NameCoderEntry<ChunkCompressionTypes::Mode> MODE_LIST[];
	static const util::NameCoder<
			ChunkCompressionTypes::Mode,
			ChunkCompressionTypes::MODE_END> MODE_CODER;
};

/*!
	@brief グループの作成・取得
*/
inline MeshedChunkTable::Group* MeshedChunkTable::putGroup(DSGroupId groupId) {
	auto itr = groupMap_.find(groupId);
	if (itr != groupMap_.end()) {
		return itr->second;
	}
	Group* group = UTIL_NEW Group(groupId, *this);
	groupMap_[groupId] = group;
	return group;
}

/*!
	@brief グループ存在チェック(テスト用)
*/
inline MeshedChunkTable::Group* MeshedChunkTable::checkGroup(DSGroupId groupId) {
	auto itr = groupMap_.find(groupId);
	if (itr != groupMap_.end()) {
		return itr->second;
	}
	assert(false);
	GS_THROW_SYSTEM_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "GroupId is not exist. 0x" << std::hex << groupId << std::dec);
	return NULL;
}

/*!
	@brief グループ削除
*/
inline void MeshedChunkTable::removeGroupFromMap(DSGroupId groupId) {
	auto itr = groupMap_.find(groupId);
	if (itr != groupMap_.end()) {
		groupMap_.erase(itr);
	}
}

inline int64_t MeshedChunkTable::getChunkId(int32_t index, int16_t pos) {
	int64_t chunkId = ((int64_t)index << extentPower_) + (int64_t)pos;
	return chunkId;
}

inline int32_t MeshedChunkTable::getIndex(int64_t chunkId) {
	return (int32_t)(chunkId >> extentPower_);
}

inline int16_t MeshedChunkTable::getPos(int64_t chunkId) {
	return (int16_t)(chunkId & extentPosMask_);
}

/*!
	@brief グループ横断チャンク走査カーソル
*/
inline MeshedChunkTable::StableChunkCursor MeshedChunkTable::createStableChunkCursor() {
	return StableChunkCursor(*this);
}

/*!
	@brief 断面性の高いチャンクカーソル
*/
inline MeshedChunkTable::CsectionChunkCursor MeshedChunkTable::createCsectionChunkCursor() {
	return CsectionChunkCursor(*this);
}

inline MeshedChunkTable::Group::StableGroupChunkCursor
MeshedChunkTable::Group::createStableGroupChunkCursor() {
	return StableGroupChunkCursor(*this);
}

inline void MeshedChunkTable::Group::addExtentChain(int32_t index) {
	chain_->add(index);
}

inline void MeshedChunkTable::Group::addExtentFree(int32_t index) {
	free_->add(index);
}

/*!
	@brief チャンク取得
*/
inline RawChunkInfo MeshedChunkTable::Group::get(int64_t chunkId) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];

	RawChunkInfo rawInfo;
	rawInfo.setChunkId(chunkId)
			.setGroupId(groupId_)
			.setOffset(extent.getOffset(pos))
			.setVacancy(extent.getVacancy(pos))
			.setCumulativeDirty(extent.isCumulativeDirty(pos))
			.setDifferentialDirty(extent.isDifferentialDirty(pos))
			.setLogVersion(extent.getLogVersion(pos));
	return rawInfo;
}

/*!
	@brief チャンクエクステント取得
*/
inline ChunkExtent* MeshedChunkTable::Group::get(int64_t chunkId, int16_t &pos) {
	int32_t index = mct_.getIndex(chunkId);
	pos = mct_.getPos(chunkId);
	return mct_.extents_[index];
}

/*!
	@brief チャンク情報一括セット
*/
inline void MeshedChunkTable::Group::set(
		int64_t chunkId, int64_t offset, int64_t logversion, bool isDirty) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];
	if (offset >= 0) {
		extent.setOffset(pos, offset);
	}
	extent.setLogVersion(pos, logversion);
	extent.setCumulativeDirty(pos, isDirty);
	extent.setDifferentialDirty(pos, isDirty);
}

inline void MeshedChunkTable::Group::setDirtyFlags(int64_t chunkId, bool isDirty) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];
	extent.setCumulativeDirty(pos, isDirty);
	extent.setDifferentialDirty(pos, isDirty);
}


/*!
	@brief 空きサイズ設定
*/
inline void MeshedChunkTable::Group::setVacancy(int64_t chunkId, int8_t vacancy) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];
	assert(extent.getOffset(pos) >= 0);
	extent.setVacancy(pos, vacancy);
}

/*!
	@brief 空きサイズ取得
*/
inline int8_t MeshedChunkTable::Group::getVacancy(int64_t chunkId) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];
	assert(extent.getOffset(pos) >= 0);
	return extent.getVacancy(pos);
}

/*!
	@brief オフセット取得
*/
inline int64_t MeshedChunkTable::Group::getOffset(int64_t chunkId) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];
	assert(extent.getOffset(pos) >= 0);
	return extent.getOffset(pos);
}


inline void MeshedChunkTable::Group::StableGroupChunkCursor::set(int64_t chunkId) {
	index_ = mct_.getIndex(chunkId);
	pos_ = mct_.getPos(chunkId);
}
/*!
	@brief 繰り返しで取得する場合
*/
inline bool MeshedChunkTable::Group::StableGroupChunkCursor::current(RawChunkInfo& info) {
	return next(false, info);
}

inline bool MeshedChunkTable::Group::StableGroupChunkCursor::advance(RawChunkInfo& info) {
	return next(true, info);
}


inline bool MeshedChunkTable::CsectionChunkCursor::hasNext() {
	if (index_ >= maxindex_) {
		return false;
	}
	return true;
}


inline Chunk* ChunkAccessor::getChunk() {
	assert(!isEmpty());
	return chunk_;
}

inline PartitionId ChunkAccessor::getPartitionId() {
	assert(!isEmpty());
	return pId_;
}

inline DSGroupId ChunkAccessor::getGroupId() {
	assert(!isEmpty());
	return groupId_;
}

inline int64_t ChunkAccessor::getChunkId() {
	assert(!isEmpty());
	return chunkId_;
}

inline ChunkBufferFrameRef ChunkAccessor::getFrameRef() {
	assert(!isEmpty());
	return frameRef_;
}

inline void ChunkAccessor::setVacancy(int8_t vacancy) {
	assert(!isEmpty());
	MeshedChunkTable::Group& group = *chunkTable_->putGroup(groupId_);
	assert(chunkId_ >= 0);
	group.setVacancy(chunkId_, vacancy);
}

inline int8_t ChunkAccessor::getVacancy() {
	assert(!isEmpty());
	MeshedChunkTable::Group& group = *chunkTable_->putGroup(groupId_);
	assert(chunkId_ >= 0);
	return group.getVacancy(chunkId_);
}

/*!
	@brief チャンクサイズ取得
*/
inline int32_t ChunkManager::getChunkSize() {
	return chunkSize_;
}

/*!
	@brief グループ横断チャンク走査

	@note 主にフルチェックポイントで使われる
*/
inline MeshedChunkTable::StableChunkCursor ChunkManager::createStableChunkCursor() {
	return chunkTable_->createStableChunkCursor();
}

/*!
	@brief 主にパーシャルチェックポイントで使われる。耐停止性、終わりがある。
*/
inline MeshedChunkTable::CsectionChunkCursor ChunkManager::createCsectionChunkCursor() {
	return chunkTable_->createCsectionChunkCursor();
}

/*!
	@brief グループ作成取得
*/
inline MeshedChunkTable::Group* ChunkManager::putGroup(DSGroupId groupId) {
	return chunkTable_->putGroup(groupId);
}

inline MeshedChunkTable::Group* ChunkManager::checkGroup(DSGroupId groupId) {
	return chunkTable_->checkGroup(groupId);
}

/*!
	@brief 指定グループのチャンクが存在すればtrue
*/
inline bool ChunkManager::hasBlock(DSGroupId groupId) {
	MeshedChunkTable::Group& group = *chunkTable_->putGroup(groupId);
	return group.getBlockCount() > 0;
}

/*!
	@brief パーティションに1つでもチャンクがあればtrue
*/
inline bool ChunkManager::hasBlock() {
	return blockCount_ > 0;
}

inline ChunkId ChunkManager::getHeadChunkId(DSGroupId groupId) {
	MeshedChunkTable::Group& group = *chunkTable_->putGroup(groupId);
	MeshedChunkTable::Group::StableGroupChunkCursor cursor(group);
	int64_t chunkId = -1;
	int16_t pos = -1;
	ChunkExtent* extent = cursor.next(chunkId, pos);
	assert(chunkId == 0);
	return chunkId;
}


/*!
	@brief エクステントサイズ取得
*/
inline int16_t ChunkExtent::getExtentSize() const {
	return extentSize_;
}

/*!
	@brief エクステント使用
*/
inline ChunkExtent& ChunkExtent::use(DSGroupId groupId) {
	groupId_ = groupId;
	use_ = true;
	return *this;
}

/*!
	@brief エクステント不使用
*/
inline ChunkExtent& ChunkExtent::unuse() {
	groupId_ = -1;
	use_ = false;
	return *this;
}

/*!
	@brief エクステント使用中か否か
*/
inline bool ChunkExtent::isUsed() const {
	return use_;
}

inline void ChunkExtent::put0(int16_t pos, int64_t offset, int8_t vacancy) {
	use(pos);
	setOffset(pos, offset);
	setVacancy(pos, vacancy);
}

inline void ChunkExtent::free0(int16_t pos) {
	unuse(pos);
	setOffset(pos, -1);
	setVacancy(pos, (int8_t)-1);
}

/*!
	@brief 空きチェーンを作成
*/
inline ChunkExtent& ChunkExtent::initFreeChain() {
	free_ = -1;
	numUsed_ = 0;

	for (int16_t pos = (int16_t)(extentSize_ - 1); pos >= 0; pos--) {
		if (!isUsed(pos)) {
			setNext(pos, free_);
			free_ = pos;
		} else {
			numUsed_++;
		}
	}
	return *this;
}

inline int16_t ChunkExtent::peek() const {
	return free_;
}

inline int16_t ChunkExtent::allocate() {
	int16_t pos = free_;
	if (pos < 0) {
		return -1;
	}
	free_ = getNext(pos);
	assert(isUsed(pos) == false);
	use(pos);
	numUsed_++;
	return pos;
}

inline void ChunkExtent::tryAllocate(int16_t pos) {
	assert(isUsed(pos) == false);
	use(pos);
	initFreeChain();
}

inline void ChunkExtent::free(int16_t pos) {
	unuse(pos);
	numUsed_--;
	setNext(pos, free_);
	free_ = pos;
}

inline int16_t ChunkExtent::getNumUsed() const {
	return numUsed_;
}

inline int64_t ChunkExtent::getGroupId() const {
	return groupId_;
}

inline int64_t ChunkExtent::getOffset(int16_t pos) const {
	assert(groupId_ >= 0);
	assert(isUsed(pos));
	int64_t offset = offsets_->get(pos);
	assert(offset >= 0);
	return offset;
}

inline void ChunkExtent::setOffset(int16_t targetpos, int64_t offset) {
	offsets_->set(targetpos, offset);
}

inline int16_t ChunkExtent::getNext(int16_t pos) const {
	assert(!isUsed(pos));
	return (int16_t)offsets_->get(pos);
}

inline void ChunkExtent::setNext(int16_t pos, int16_t next) {
	assert(!isUsed(pos));
	offsets_->set(pos, next);
}

inline void ChunkExtent::use(int16_t pos) {
	vacancies_[pos] = (int8_t)0;
}

inline 
void ChunkExtent::unuse(int16_t pos) {
	vacancies_[pos] = (int8_t)-1;
}

inline 
bool ChunkExtent::isUsed(int16_t pos) const {
	return (vacancies_[pos] >= 0);
}

inline 
int8_t ChunkExtent::getVacancy(int16_t pos) const {
	return vacancies_[pos];
}

inline 
void ChunkExtent::setVacancy(int16_t pos, int8_t vacancy) {
	vacancies_[pos] = vacancy;
}

inline int64_t ChunkExtent::getLogVersion(int16_t pos) const {
	return baseLogVersion_ + deltaLogVersions_[pos];
}

inline bool ChunkExtent::isDifferentialDirty(int16_t pos) const {
	return ((flags_[pos] & CUMULATIVE_DIRTY_FLAG_BIT) != 0);
}

inline void ChunkExtent::setDifferentialDirty(int16_t pos, bool flag) {
	if (flag) {
		flags_[pos] |= CUMULATIVE_DIRTY_FLAG_BIT;
	} else {
		flags_[pos] &= ~CUMULATIVE_DIRTY_FLAG_BIT;
	}
}

inline bool ChunkExtent::isCumulativeDirty(int16_t pos) const {
	return ((flags_[pos] & DIFFERENTIAL_DIRTY_FLAG_BIT) != 0);
}

inline void ChunkExtent::setCumulativeDirty(int16_t pos, bool flag) {
	if (flag) {
		flags_[pos] |= DIFFERENTIAL_DIRTY_FLAG_BIT;
	} else {
		flags_[pos] &= ~DIFFERENTIAL_DIRTY_FLAG_BIT;
	}
}

inline uint8_t ChunkExtent::getFlags(int16_t pos) const {
	return flags_[pos];
}


/*!
	@brief RowChunkInfo一括取得
*/
inline bool MeshedChunkTable::Group::StableGroupChunkCursor::next(RawChunkInfo &rawInfo) {
	while (true) {
		if (index_ >= 0) {
			ChunkExtent& extent = *mct_.extents_[index_];
			if (pos_ < extent.getExtentSize()) {
				int16_t pos = pos_;
				pos_++;
				if (extent.isUsed(pos)) {
					assert(extent.getGroupId() == group_.groupId_);
					rawInfo
							.setChunkId(mct_.getChunkId(index_, pos))
							.setGroupId(group_.groupId_)
							.setOffset(extent.getOffset(pos))
							.setVacancy(extent.getVacancy(pos))
							.setCumulativeDirty(extent.isCumulativeDirty(pos))
							.setDifferentialDirty(extent.isDifferentialDirty(pos))
							.setLogVersion(extent.getLogVersion(pos));
					return true;
				}
			} else {
				index_ = group_.chain_->next(index_);
				pos_ = 0;
			}
		} else {
			return false;
		}
	}
}

/*!
	@brief chunkId, posのみ取得
*/
inline ChunkExtent* MeshedChunkTable::Group::StableGroupChunkCursor::next(int64_t &chunkId, int16_t &pos) {
	while (true) {
		if (index_ >= 0) {
			ChunkExtent& extent = *mct_.extents_[index_];
			assert(extent.getGroupId() == group_.groupId_);
			if (pos_ < extent.getExtentSize()) {
				pos = pos_;
				pos_++;
				if (extent.isUsed(pos)) {
					chunkId = mct_.getChunkId(index_, pos);
					return &extent;
				}
			} else {
				index_ = group_.chain_->next(index_);
				pos_ = 0;
			}
		} else {
			chunkId = -1;
			pos = -1;
			return nullptr;
		}
	}
}

inline MeshedChunkTable::StableChunkCursor::StableChunkCursor(MeshedChunkTable& mct)
: mct_(mct), index_(0), pos_(0) {
}

inline bool MeshedChunkTable::StableChunkCursor::next(RawChunkInfo& rawInfo) {
	while (true) {
		if (index_ >= mct_.extents_.size()) {
			return false;
		}
		ChunkExtent& extent = *mct_.extents_[index_];
		if ((extent.isUsed() == false) ||
				(pos_ >= extent.getExtentSize())) {
			index_++;
			pos_ = 0;
			continue;
		}
		int16_t pos = pos_;
		pos_++;
		if (extent.isUsed(pos)) {
			int64_t chunkId = mct_.getChunkId(index_, pos);
			rawInfo
					.setChunkId(chunkId)
					.setGroupId(extent.getGroupId())
					.setOffset(extent.getOffset(pos))
					.setVacancy(extent.getVacancy(pos))
					.setCumulativeDirty(extent.isCumulativeDirty(pos))
					.setDifferentialDirty(extent.isDifferentialDirty(pos))
					.setLogVersion(extent.getLogVersion(pos));
			return true;
		}
	}
}

inline ChunkExtent* MeshedChunkTable::StableChunkCursor::next(int64_t &chunkId, int16_t &pos) {
	while (true) {
		if (index_ >= mct_.extents_.size()) {
			return nullptr;
		}
		ChunkExtent& extent = *mct_.extents_[index_];
		if ((extent.isUsed() == false) ||
				(pos_ >= extent.getExtentSize())) {
			index_++;
			pos_ = 0;
			continue;
		}
		pos = pos_;
		pos_++;
		if (extent.isUsed(pos)) {
			chunkId = mct_.getChunkId(index_, pos);
			return &extent;
		}
	}
}

inline MeshedChunkTable::CsectionChunkCursor::CsectionChunkCursor(MeshedChunkTable& mct)
: mct_(mct), index_(0), pos_(0), maxindex_(0) {
	maxindex_ = mct.extents_.size();
}

inline bool MeshedChunkTable::CsectionChunkCursor::next(RawChunkInfo& rawInfo) {
	while (true) {
		if (index_ >= maxindex_) {
			return false;
		}
		ChunkExtent& extent = *mct_.extents_[index_];
		if (pos_ >= extent.getExtentSize()) {
			index_++;
			pos_ = 0;
			continue;
		}
		int16_t pos = pos_;
		pos_++;
		if (extent.isUsed(pos)) {
			int64_t chunkId = mct_.getChunkId(index_, pos);
			rawInfo
					.setChunkId(chunkId)
					.setGroupId(extent.getGroupId())
					.setOffset(extent.getOffset(pos))
					.setVacancy(extent.getVacancy(pos))
					.setCumulativeDirty(extent.isCumulativeDirty(pos))
					.setDifferentialDirty(extent.isDifferentialDirty(pos))
					.setLogVersion(extent.getLogVersion(pos));
			return true;
		} else {
			int64_t chunkId = mct_.getChunkId(index_, pos);
			rawInfo
					.setChunkId(chunkId)
					.setGroupId(-1)
					.setOffset(-1)
					.setVacancy((int8_t)-1)
					.setCumulativeDirty(false)
					.setDifferentialDirty(false)
					.setLogVersion(extent.getLogVersion(pos));
			return true;
		}
	}
}

inline ChunkExtent* MeshedChunkTable::CsectionChunkCursor::next(int64_t &chunkId, int16_t &pos) {
	while (true) {
		if (index_ >= maxindex_) {
			return nullptr;
		}
		ChunkExtent& extent = *mct_.extents_[index_];
		if (pos_ >= extent.getExtentSize()) {
			index_++;
			pos_ = 0;
			continue;
		}
		pos = pos_;
		pos_++;
		chunkId = mct_.getChunkId(index_, pos);
		if (extent.isUsed(pos)) {
			return &extent;
		} else {
			pos = -1;
			return &extent;
		}
	}
}

inline ChunkAccessor::~ChunkAccessor() {
	assert(isEmpty());
}

inline bool ChunkAccessor::isEmpty() {
	return (chunkBuffer_ == NULL);
}

inline void ChunkAccessor::reset() {
	assert(isEmpty());
	forceReset();
}

inline void ChunkAccessor::forceReset() {
	assign(NULL, NULL, -1, -1, -1, NULL, ChunkBufferFrameRef());
}

inline void ChunkAccessor::assign(
		MeshedChunkTable *chunkTable, ChunkBuffer *chunkBuffer,
		PartitionId pId, DSGroupId groupId, int64_t chunkId,
		Chunk *chunk, const ChunkBufferFrameRef &frameRef) {
	chunkTable_ = chunkTable;
	chunkBuffer_ = chunkBuffer;
	pId_ = pId;
	groupId_ = groupId;
	chunkId_ = chunkId;
	chunk_ = chunk;
	frameRef_ = frameRef;
}

inline void FreeSpace::init(std::deque<int64_t> &frees, int64_t maxoffset) {
	frees_.insert(frees.begin(), frees.end());
	maxoffset_ = maxoffset;
	chunkManagerStats_.table_(ChunkManagerStats::CHUNK_STAT_CP_FILE_USE_COUNT)
			.set(maxoffset_ - frees_.size());
	chunkManagerStats_.table_(ChunkManagerStats::CHUNK_STAT_CP_FILE_SIZE)
			.set(static_cast<uint64_t>(maxoffset_));
}

inline void FreeSpace::free(int64_t offset) {
	frees_.insert(offset);
	chunkManagerStats_.table_(ChunkManagerStats::CHUNK_STAT_CP_FILE_USE_COUNT)
			.decrement();
}

inline int64_t FreeSpace::allocate() {
	chunkManagerStats_.table_(ChunkManagerStats::CHUNK_STAT_CP_FILE_USE_COUNT)
			.increment();
	if (frees_.size() > 0) {
		int64_t offset = *frees_.begin();
		frees_.erase(offset);
		if (maxoffset_ < offset + 1) {
			maxoffset_ = offset + 1;
			chunkManagerStats_.table_(
					ChunkManagerStats::CHUNK_STAT_CP_FILE_SIZE)
					.set(static_cast<uint64_t>(maxoffset_));
		}
		return offset;
	} else {
		int64_t maxoffset = maxoffset_;
		maxoffset_++;
		chunkManagerStats_.table_(
				ChunkManagerStats::CHUNK_STAT_CP_FILE_SIZE).increment();
		return maxoffset;
	}
}


inline const std::set<int64_t>& FreeSpace::getFrees() {
	return frees_;
}


inline PartitionGroupId ChunkManager::Config::getPartitionGroupId(PartitionId pId) const {
	assert(pId < partitionNum_);
	return pgIdList_[pId];
}
inline PartitionGroupId ChunkManager::Config::getPartitionGroupNum() const {
	return partitionGroupNum_;
}  
inline PartitionId ChunkManager::Config::getPartitionNum() const {
	return partitionNum_;
}
inline ChunkCategoryId ChunkManager::Config::getChunkCategoryNum() const {
	return categoryNum_;
}
inline uint8_t ChunkManager::Config::getChunkExpSize() const {
	return chunkExpSize_;
}
inline uint32_t ChunkManager::Config::getChunkSize() const {
	return chunkSize_;
}

inline uint64_t ChunkManager::Config::getAtomicStoreMemoryLimit() {
	return atomicStorePoolLimit_;
}
inline uint64_t ChunkManager::Config::getAtomicStoreMemoryLimitNum() {
	return atomicStorePoolLimitNum_;
}

inline uint64_t ChunkManager::Config::getAtomicCheckpointMemoryLimit() {
	return atomicCheckpointPoolLimit_;
}
inline uint64_t ChunkManager::Config::getAtomicCheckpointMemoryLimitNum() {
	return atomicCheckpointPoolLimitNum_;
}

inline double ChunkManager::Config::getAtomicStoreMemoryColdRate() {
	return static_cast<double>(atomicStoreMemoryColdRate_) / 1.0E6;
}
inline uint64_t ChunkManager::Config::getAtomicCpFileFlushSize() {
	return atomicCpFileFlushSize_;
}
inline uint64_t ChunkManager::Config::getAtomicCpFileFlushInterval() {
	return atomicCpFileFlushInterval_;
}

inline bool ChunkManager::Config::getAtomicCpFileAutoClearCache() {
	return atomicCpFileAutoClearCache_;
}
inline double ChunkManager::Config::getBufferHashTableSizeRate() {
	return bufferHashTableSizeRate_;
}
inline uint32_t ChunkManager::Config::getCpFileSplitCount() {
	return ChunkManager::Config::cpFileSplitCount_;
}
inline uint32_t ChunkManager::Config::getCpFileSplitStripeSize() {
	return cpFileSplitStripeSize_;
}
inline const std::vector<std::string>& ChunkManager::Config::getCpFileDirList() {
	return cpFileDirList_;
}
inline uint32_t ChunkManager::Config::getAffinitySize() {
	return atomicAffinitySize_;
}

#endif 
