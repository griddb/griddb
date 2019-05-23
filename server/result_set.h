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
	@brief Definition of ResultSet
*/
#ifndef RESULT_SET_H_
#define RESULT_SET_H_

#include "data_store.h"  
#include "data_type.h"


class BaseContainer;
class ContainerRowScanner;


class ResultSetOption {
public:
	enum PartialPositionType {
		ROW_ID = 0,
		FILTERED_COUNT = 1,
		SWAP_OUT_COUNT = 2,
	};
private:
	bool isPartial_;
	bool isDistribute_;
	RowId minRowId_;		
	RowId maxRowId_;		
	int64_t filteredNum_;
	int64_t swapOutNum_;
	int64_t distLimit_;
	int64_t distOffset_;
	int32_t fetchByteSize_;
public:
	ResultSetOption() : isPartial_(false), isDistribute_(false),
		minRowId_(INITIAL_ROWID), maxRowId_(MAX_ROWID), 
		filteredNum_(0),
		swapOutNum_(0),
		distLimit_(MAX_RESULT_SIZE),
		distOffset_(0) {}
	static const ResultSize PARTIAL_SCAN_LIMIT = 100000;

	typedef util::Map<int8_t, util::XArray<uint8_t> *> PartialQueryOption;	 
	void set(int32_t fetchByteSize, bool isDistribute,
		bool isPartial, const PartialQueryOption &partialQueryOption) {
		setFetchByteSize(fetchByteSize);
		setDistribute(isDistribute);
		setPartial(isPartial);
		decodePartialQueryOption(partialQueryOption);
	}

	RowId getMinRowId() const {
		return minRowId_;
	}
	RowId getMaxRowId() const {
		return maxRowId_;
	}
	void setMinRowId(RowId id) {
		minRowId_ = id;
	}
	void setMaxRowId(RowId id) {
		maxRowId_ = id;
	}

	void decodePartialQueryOption(
		const PartialQueryOption &partialQueryOption) {
		PartialQueryOption::const_iterator itr;
		for (itr = partialQueryOption.begin(); itr != partialQueryOption.end(); itr++) {
			switch (itr->first) {
			case ROW_ID: {
				RowId *rowIdPair = reinterpret_cast<RowId *>(itr->second->data());
				minRowId_ = *rowIdPair;
				rowIdPair++;
				maxRowId_ = *rowIdPair;
			} break;
			case FILTERED_COUNT: {
				int64_t *filteredNum = reinterpret_cast<int64_t *>(itr->second->data());
				filteredNum_ = *filteredNum;
			} break;
			case SWAP_OUT_COUNT: {
				int64_t *swapOutNum = reinterpret_cast<int64_t *>(itr->second->data());
				swapOutNum_ = *swapOutNum;
			} break;
			default:
				break;
			}
		}
	}

	bool isDistribute() const {
		if (isDistribute_) {
			return true;
		} else {
			return false;
		}
	}
	void setDistribute(bool isDistribute) {
		isDistribute_ = isDistribute;
	}

	bool existLimitOffset() const {
		if (static_cast<ResultSize>(getDistLimit()) != MAX_RESULT_SIZE || getDistOffset() != 0) {
			return true;
		} else {
			return false;
		}
	}
	int64_t getFilteredNum() const {
		return filteredNum_;
	}
	void setFilteredNum(int64_t num) {
		filteredNum_ = num;
	}
	int64_t getSwapOutNum() const {
		return swapOutNum_;
	}
	void setSwapOutNum(int64_t num) {
		swapOutNum_ = num;
	}
	int32_t getFetchByteSize() const {
		return fetchByteSize_;
	}
	void setFetchByteSize(int32_t fetchByteSize) {
		fetchByteSize_ = fetchByteSize;
	}
	int64_t getDistLimit() const {
		return distLimit_;
	}
	int64_t getDistOffset() const {
		return distOffset_;
	}
	void setDistLimit(int64_t limit) {
		distLimit_ = limit;
	}
	void setDistOffset(int64_t offset) {
		distOffset_ = offset;
	}
	void setPartial(bool isPartial) {
		isPartial_  = isPartial;
	}
	bool isPartial() const {
		return isPartial_ ;
	}
	void encodePartialQueryOption(util::StackAllocator &alloc, PartialQueryOption &partialQueryOption) const {
		RowId minRowId = getMinRowId();
		RowId maxRowId = getMaxRowId();

		if (!isPartial() || 
			(minRowId == MAX_ROWID && maxRowId == MAX_ROWID)) {
			return;
		}
		{
			int32_t entryType = ROW_ID;
			util::XArray<uint8_t> *entryBody = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			entryBody->push_back(
				reinterpret_cast<uint8_t *>(&minRowId),
				sizeof(RowId));
			entryBody->push_back(
				reinterpret_cast<uint8_t *>(&maxRowId),
				sizeof(RowId));
			partialQueryOption.insert(std::make_pair(entryType, entryBody));
		}
		{
			int32_t entryType = FILTERED_COUNT;
			util::XArray<uint8_t> *entryBody = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			int64_t filteredNum = getFilteredNum();
			entryBody->push_back(
				reinterpret_cast<uint8_t *>(&filteredNum),
				sizeof(int64_t));
			partialQueryOption.insert(std::make_pair(entryType, entryBody));
		}
		{
			int32_t entryType = SWAP_OUT_COUNT;
			util::XArray<uint8_t> *entryBody = ALLOC_NEW(alloc) util::XArray<uint8_t>(alloc);
			int64_t swapOutNum = getSwapOutNum();
			entryBody->push_back(
				reinterpret_cast<uint8_t *>(&swapOutNum),
				sizeof(int64_t));
			partialQueryOption.insert(std::make_pair(entryType, entryBody));
		}
	}
};

/*!
	@brief ResultSet
*/
class ResultSet {
public:			  
	class UpdateRowIdHandler;

	ResultSet();  
	~ResultSet();

	void resetSerializedData();
	void clear();  

	inline void setId(ResultSetId id) {
		rsetId_ = id;
	}
	inline void setPartitionId(PartitionId id) {
		pId_ = id;
	}
	inline void setContainerId(ContainerId id) {
		containerId_ = id;
	}
	inline void setSchemaVersionId(SchemaVersionId id) {
		schemaVersionId_ = id;
	}  
	inline void setStartLsn(LogSequentialNumber startLsn) {
		startLsn_ = startLsn;
	}

	inline void setRSAllocator(util::StackAllocator *alloc) {
		rsAlloc_ = alloc;
	}
	inline void setTxnAllocator(util::StackAllocator *alloc) {
		txnAlloc_ = alloc;
	}
	inline void setTimeoutTime(Timestamp time) {
		timeoutTimestamp_ = time;
	}  
	inline void setSkipCount(uint64_t count) {
		skipCount_ = count;
	}


	inline void setFetchNum(ResultSize fetchNum) {
		fetchNum_ = fetchNum;
	}
	inline void setResultNum(ResultSize resultNum) {
		resultNum_ = resultNum;
	}
	inline void setMemoryLimit(size_t memoryLimit) {
		memoryLimit_ = memoryLimit;
	}

	inline void setResultType(ResultType resultType) {
		resultType_ = resultType;
		if (resultType_ == RESULT_ROW_ID_SET) {
			isId_ = true;
		}
	}
	inline void setResultType(ResultType resultType, ResultSize resultNum,
		bool isRowIdInclude = false) {
		resultNum_ = resultNum;
		setResultType(resultType);
		isRowIdInclude_ = isRowIdInclude;
	}

	void releaseTxnAllocator();  


	void setDataPos(uint64_t fixedStartPos, uint64_t fixedOffsetSize,
		uint64_t varStartPos, uint64_t varOffsetSize);
	inline void setRowIdInclude(bool isRowIdInclude) {
		isRowIdInclude_ = isRowIdInclude;
	}
	inline void setUseRSRowImage(bool flag) {
		useRSRowImage_ = flag;
	}


	const util::XArray<OId> *getOIdList() const;
	util::XArray<OId> *getOIdList();

	const util::XArray<RowId> *getRowIdList() const;
	util::XArray<RowId> *getRowIdList();

	const util::XArray<uint8_t> *getRowDataFixedPartBuffer() const;
	util::XArray<uint8_t> *getRowDataFixedPartBuffer();

	const util::XArray<uint8_t> *getRowDataVarPartBuffer() const;
	util::XArray<uint8_t> *getRowDataVarPartBuffer();

	const util::XArray<uint8_t> *getRSRowDataFixedPartBuffer() const;
	util::XArray<uint8_t> *getRSRowDataFixedPartBuffer();

	const util::XArray<uint8_t> *getRSRowDataVarPartBuffer() const;
	util::XArray<uint8_t> *getRSRowDataVarPartBuffer();

	inline ResultType getResultType() const {
		return resultType_;
	}
	inline ResultSize getResultNum() const {
		return resultNum_;
	}
	inline ResultSize getFetchNum() const {
		return fetchNum_;
	}
	inline size_t getMemoryLimit() const {
		return memoryLimit_;
	}

	inline uint64_t getSkipCount() const {
		return skipCount_;
	}


	inline bool getRowExist() const {
		return resultNum_ > 0;
	}  

	inline uint64_t getFixedOffsetSize() const {
		return fixedOffsetSize_;
	}
	inline uint64_t getVarOffsetSize() const {
		return varOffsetSize_;
	}
	inline uint64_t getFixedStartPos() const {
		return fixedStartPos_;
	}
	inline uint64_t getVarStartPos() const {
		return varStartPos_;
	}

	/*!
		@brief Get pointer of fixed data area
	*/
	inline const uint8_t *getFixedStartData() const {
		if (useRSRowImage_) {
			if (rsSerializedRowList_) {
				return rsSerializedRowList_->data() + fixedStartPos_;
			}
			else {
				return NULL;
			}
		}
		else {
			if (serializedRowList_) {
				return serializedRowList_->data() + fixedStartPos_;
			}
			else {
				return NULL;
			}
		}
	}
	/*!
		@brief Get pointer of variable data area
	*/
	inline const uint8_t *getVarStartData() const {
		if (useRSRowImage_) {
			if (rsSerializedVarDataList_) {
				return rsSerializedVarDataList_->data() + varStartPos_;
			}
			else {
				return NULL;
			}
		}
		else {
			if (serializedVarDataList_) {
				return serializedVarDataList_->data() + varStartPos_;
			}
			else {
				return NULL;
			}
		}
	}

	inline ResultSetId getId() const {
		return rsetId_;
	}
	inline LogSequentialNumber getStartLsn() const {
		return startLsn_;
	}

	inline PartitionId getPartitionId() const {
		return pId_;
	}
	inline ContainerId getContainerId() const {
		return containerId_;
	}
	inline SchemaVersionId getSchemaVersionId() const {
		return schemaVersionId_;
	}  
	inline TransactionId getTransactionId() const {
		return txnId_;
	}
	inline util::StackAllocator *getRSAllocator() {
		return rsAlloc_;
	}
	inline util::StackAllocator *getTxnAllocator() {
		return txnAlloc_;
	}

	inline bool hasRowId() const {
		return isId_;
	}

	inline bool isRowIdInclude() const {
		return isRowIdInclude_;
	}
	inline bool useRSRowImage() const {
		return useRSRowImage_;
	}
	inline Timestamp getTimeoutTime() const {
		return timeoutTimestamp_;
	}

	const util::Vector<int64_t>* getDistributedTarget() const;
	util::Vector<int64_t>* getDistributedTarget();

	void getDistributedTargetStatus(bool &uncovered, bool &reduced) const;
	void setDistributedTargetStatus(bool uncovered, bool reduced);


	bool isRelease() const;

	ResultSetOption &getQueryOption() {
		return queryOption_;
	}

	const ResultSetOption &getQueryOption() const {
		return queryOption_;
	}

	typedef util::Map<int8_t, util::XArray<uint8_t> *> PartialQueryOption;	 
	void setQueryOption(ResultSetOption *option) {
		if (option != NULL) {
			queryOption_ = *option;
		}
	}


	int32_t getSerializedSize() {
		if (getResultNum() > 0 && serializedRowList_ != NULL && serializedVarDataList_ != NULL) {
			size_t usedSize = serializedRowList_->size() + 
				serializedVarDataList_->size();
			return static_cast<int32_t>(usedSize);
		} else {
			return 0;
		}
	}

	enum PartialPositionType {
		ROW_ID = 0,
		FILTERED_COUNT = 1,
		SWAP_OUT_COUNT = 2,
	};
	ResultSetOption queryOption_;

private:							 
	util::StackAllocator *rsAlloc_;  
	util::StackAllocator
		*txnAlloc_;  

	util::XArray<OId> *oIdList_;					
	util::XArray<uint8_t> *serializedRowList_;		
	util::XArray<uint8_t> *serializedVarDataList_;  

	util::XArray<RowId> *rowIdList_;				  
	util::XArray<uint8_t> *rsSerializedRowList_;	  
	util::XArray<uint8_t> *rsSerializedVarDataList_;  

	BaseContainer *container_;  

	ResultSetId rsetId_;			
	TransactionId txnId_;			
	Timestamp timeoutTimestamp_;	
	LogSequentialNumber startLsn_;  
	PartitionId pId_;
	ContainerId containerId_;
	SchemaVersionId schemaVersionId_;  

	ResultSize resultNum_;
	ResultSize fetchNum_;
	size_t memoryLimit_;

	uint64_t fixedStartPos_;
	uint64_t fixedOffsetSize_;
	uint64_t varStartPos_;
	uint64_t varOffsetSize_;
	uint64_t
		skipCount_;  


	ResultType resultType_;

	bool isId_;
	bool isRowIdInclude_;  
	bool
		useRSRowImage_;  

	util::Vector<int64_t> *distributedTarget_;
	bool distributedTargetUncovered_;
	bool distributedTargetReduced_;

};


/*!
	@brief Pre/postprocess before/after ResultSet Operation
*/
class ResultSetGuard {
public:
	ResultSetGuard(TransactionContext &txn, DataStore &dataStore, ResultSet &rs);
	~ResultSetGuard();

private:
	DataStore &dataStore_;
	const ResultSetId rsId_;
	const PartitionId pId_;

	util::StackAllocator &txnAlloc_;
	const size_t txnMemoryLimit_;
};

class ResultSetHolderManager {
public:
	ResultSetHolderManager();
	~ResultSetHolderManager();

	void initialize(const PartitionGroupConfig &config);

	void closeAll(TransactionContext &txn, DataStore &dataStore);
	void closeAll(
			util::StackAllocator &alloc, PartitionGroupId pgId,
			DataStore &dataStore);

	void add(PartitionId pId, ResultSetId rsId);
	void setCloseable(PartitionId pId, ResultSetId rsId);
	void release(PartitionId pId, ResultSetId rsId);

private:
	struct Entry;
	struct Group;

	typedef util::VariableSizeAllocator<> Allocator;
	typedef std::pair<PartitionId, ResultSetId> EntryKey;
	typedef std::pair<const EntryKey, Entry*> MapValue;
	typedef util::Map<
			EntryKey, Entry*, std::less<EntryKey>,
			util::StdAllocator<MapValue, void> > Map;
	typedef util::Vector< Group*, util::StdAllocator<Group*, void> > GroupList;

	struct Entry {
		explicit Entry(const EntryKey &key);

		EntryKey key_;
		Entry *next_;
	};

	struct Group {
		explicit Group(Allocator &alloc);

		Map map_;
		util::Atomic<int64_t> version_;
		int64_t checkedVersion_;
		Entry *closeableEntry_;
	};

	ResultSetHolderManager(const ResultSetHolderManager &another);
	ResultSetHolderManager& operator=(const ResultSetHolderManager &another);

	void clear();
	void clearEntries(Group &group, bool withMap);

	void pullCloseable(
			PartitionGroupId pgId, util::Vector<EntryKey> &keyList);

	Group& getGroup(PartitionGroupId pgId);
	PartitionGroupId getPartitionGroupId(PartitionId pId) const;

	util::Mutex mutex_;
	util::VariableSizeAllocator<> alloc_;
	util::AllocUniquePtr<PartitionGroupConfig> config_;
	GroupList groupList_;
};

class ResultSetHolder {
public:
	ResultSetHolder();
	~ResultSetHolder();

	bool isEmpty() const throw();
	void assign(
			ResultSetHolderManager &manager, PartitionId pId,
			ResultSetId rsId);

	void reset() throw();
	void release() throw();

private:
	ResultSetHolder(const ResultSetHolder &another);
	ResultSetHolder& operator=(const ResultSetHolder &another);

	void clear() throw();

	ResultSetHolderManager *manager_;
	PartitionId pId_;
	ResultSetId rsId_;
};


inline ResultSetGuard::ResultSetGuard(TransactionContext &txn, DataStore &dataStore, ResultSet &rs)
	: dataStore_(dataStore),
	  rsId_(rs.getId()),
	  pId_(rs.getPartitionId()),
	  txnAlloc_(*rs.getTxnAllocator()),
	  txnMemoryLimit_(txnAlloc_.getTotalSizeLimit()) {
	txnAlloc_.setTotalSizeLimit(rs.getMemoryLimit());
	ObjectManager &objectManager = *(dataStore_.getObjectManager());
	if (objectManager.existPartition(pId_)) {
		if (txn.isStoreMemoryAgingSwapRateEnabled()) {
			objectManager.setStoreMemoryAgingSwapRate(pId_, txn.getStoreMemoryAgingSwapRate());
		} else {
			objectManager.setStoreMemoryAgingSwapRate(pId_, dataStore_.getConfig().getStoreMemoryAgingSwapRate());
		}
	}
}

inline ResultSetGuard::~ResultSetGuard() {
	if (pId_ != UNDEF_PARTITIONID) {
		dataStore_.closeOrClearResultSet(pId_, rsId_);
	}
	txnAlloc_.setTotalSizeLimit(txnMemoryLimit_);
}

inline ResultSetHolder::ResultSetHolder() :
		manager_(NULL),
		pId_(UNDEF_PARTITIONID),
		rsId_(UNDEF_RESULTSETID) {
}

inline ResultSetHolder::~ResultSetHolder() {
	reset();
}

inline bool ResultSetHolder::isEmpty() const throw() {
	return (manager_ == NULL);
}

inline void ResultSetHolder::assign(
		ResultSetHolderManager &manager, PartitionId pId,
		ResultSetId rsId) {
	reset();
	manager.add(pId, rsId);

	manager_ = &manager;
	pId_ = pId;
	rsId_ = rsId;
}

inline void ResultSetHolder::reset() throw() {
	if (!isEmpty()) {
		try {
			manager_->setCloseable(pId_, rsId_);
		}
		catch (...) {
		}
		clear();
	}
}

inline void ResultSetHolder::release() throw() {
	if (!isEmpty()) {
		try {
			manager_->release(pId_, rsId_);
		}
		catch (...) {
		}
		clear();
	}
}

inline void ResultSetHolder::clear() throw() {
	manager_ = NULL;
	pId_ = UNDEF_PARTITIONID;
	rsId_ = UNDEF_RESULTSETID;
}

#endif
