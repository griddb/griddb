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
	@brief LocalTempStoreクラスの実装
*/

#include "gs_error.h"
#include "sql_common.h" 
#include "sql_temp_store.h"
#include <iostream>
#include <iomanip> 
#ifndef _WIN32
#include <signal.h>  
#include <fcntl.h> 
#endif

#ifndef WIN32 
#include <iostream>
#include <execinfo.h>
#include <unistd.h>
#endif

UTIL_TRACER_DECLARE(IO_MONITOR);  




const uint64_t LocalTempStore::SWAP_FILE_IO_MAX_RETRY_COUNT = 10;

const uint32_t LocalTempStore::DEFAULT_IO_WARNING_THRESHOLD_MILLIS = 5000;

const char8_t* const LocalTempStore::SWAP_FILE_BASE_NAME = "swap_";
const char8_t* const LocalTempStore::SWAP_FILE_EXTENSION = ".dat";
const char8_t LocalTempStore::SWAP_FILE_SEPARATOR = '_';

const uint32_t LocalTempStore::DEFAULT_BLOCK_SIZE = 512 * 1024; 

const uint32_t LocalTempStore::MAX_BLOCK_EXP_SIZE = 24; 
const uint32_t LocalTempStore::MIN_BLOCK_EXP_SIZE = 14; 

const int32_t LocalTempStore::DEFAULT_STORE_MEMORY_LIMIT_MB = 1024;
#ifndef WIN32
const int32_t LocalTempStore::DEFAULT_STORE_SWAP_FILE_SIZE_LIMIT_MB = INT32_MAX; 
#else
const int32_t LocalTempStore::DEFAULT_STORE_SWAP_FILE_SIZE_LIMIT_MB = 2048;
#endif

const std::string LocalTempStore::DEFAULT_SWAP_FILES_TOP_DIR = "swap";
const int32_t LocalTempStore::DEFAULT_STORE_SWAP_SYNC_SIZE_MB = 1024;
const int32_t LocalTempStore::DEFAULT_STORE_SWAP_SYNC_INTERVAL = 0;

const size_t LocalTempStore::BLOCK_INFO_POOL_FREE_LIMIT = 1000;
const size_t LocalTempStore::MIN_INITIAL_BUCKETS = 10; 

const uint8_t LocalTempStore::Block::FLAG_FULL_BLOCK = 0;      
const uint8_t LocalTempStore::Block::FLAG_PARTIAL_BLOCK_1 = 1; 
const uint8_t LocalTempStore::Block::FLAG_PARTIAL_BLOCK_2 = 2; 

const LocalTempStore::GroupId LocalTempStore::UNDEF_GROUPID = UINT64_MAX;
const LocalTempStore::ResourceId LocalTempStore::UNDEF_RESOURCEID = UINT64_MAX;
const LocalTempStore::BlockId LocalTempStore::UNDEF_BLOCKID = UINT64_MAX;
const uint64_t LocalTempStore::UNDEF_FILEBLOCKID = UINT64_MAX;

/*!
	@brief コンストラクタ
	@param [in] config
	@return なし
*/
LocalTempStore::LocalTempStore(
		const Config &config, LTSVariableSizeAllocator &varAllocator,
		bool autoUseDefault)
:
varAlloc_(&varAllocator)
, bufferManager_(NULL), resourceInfoManager_(NULL)
, swapFilesTopDir_(config.swapFilesTopDir_)
, groupStatsMap_(GroupStatsMap::key_compare(), varAllocator)
, defaultBlockExpSize_(util::ilog2(util::nextPowerOf2(config.blockSize_))) {
	static_cast<void>(autoUseDefault);
	Config bufferManagerConfig = config;
	if (config.blockSize_ == 0) {
		bufferManagerConfig.blockSize_ = DEFAULT_BLOCK_SIZE;
		defaultBlockExpSize_ = util::ilog2(util::nextPowerOf2(DEFAULT_BLOCK_SIZE));
	}
	else if (config.blockSize_ < (1 << LocalTempStore::MIN_BLOCK_EXP_SIZE)) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_LTS_INVALID_PARAMETER,
				"BlockSize (" << config.blockSize_ << ") is too small.");
	}
	else if (config.blockSize_ > (1 << LocalTempStore::MAX_BLOCK_EXP_SIZE)) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_LTS_INVALID_PARAMETER,
				"BlockSize (" << config.blockSize_ << ") is too large.");
	}
	if (config.blockSize_ == 0) {
		bufferManagerConfig.blockSize_ = DEFAULT_BLOCK_SIZE;
		defaultBlockExpSize_ = util::ilog2(util::nextPowerOf2(DEFAULT_BLOCK_SIZE));
	}
	if (swapFilesTopDir_.empty()) {
		GS_THROW_SYSTEM_ERROR(GS_ERROR_LTS_INVALID_PARAMETER,
				"SwapFilesTopDir must not be empty.");
	}
	else {
		try {
			if (util::FileSystem::exists(swapFilesTopDir_.c_str())) {
				if (!util::FileSystem::isDirectory(swapFilesTopDir_.c_str())) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_LTS_INVALID_PARAMETER,
							"SwapFilesTopDir (" << swapFilesTopDir_.c_str() <<
							") is not directory");
				}
				util::Directory dir(swapFilesTopDir_.c_str());
				if (dir.isParentOrSelfChecked()) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_LTS_INVALID_PARAMETER,
							"SwapFilesTopDir must not be \".\" or \"..\".");
				}
			}
			else {
				util::FileSystem::createDirectoryTree(swapFilesTopDir_.c_str());
				util::Directory dir(swapFilesTopDir_.c_str());
				if (dir.isParentOrSelfChecked()) {
					GS_THROW_SYSTEM_ERROR(GS_ERROR_LTS_INVALID_PARAMETER,
							"SwapFilesTopDir must not be \".\" or \"..\".");
				}
			}
		}
		catch (std::exception &e) {
			GS_RETHROW_SYSTEM_ERROR(
					e, GS_EXCEPTION_MERGE_MESSAGE(e,
							"SwapFilesTopDir open failed"));
		}
	}
	resourceInfoManager_ = ALLOC_VAR_SIZE_NEW(*varAlloc_) ResourceInfoManager(*this);

	bufferManager_ = ALLOC_VAR_SIZE_NEW(*varAlloc_) BufferManager(*varAlloc_, config, *this);
}

/*!
	@brief デストラクタ
	@return なし
*/
LocalTempStore::~LocalTempStore() try {
	ALLOC_VAR_SIZE_DELETE(*varAlloc_, bufferManager_);
	ALLOC_VAR_SIZE_DELETE(*varAlloc_, resourceInfoManager_);
}
catch (...) {
}

/*!
	@brief 使用するブロックサイズの登録
	@param [in] blockSize 使用するブロックサイズ
	@return なし
*/
void LocalTempStore::useBlockSize(uint32_t blockSize) {
	static_cast<void>(blockSize);
}

/*!
	@brief 使用メモリ上限の設定
	@param [in] size 使用メモリ上限
	@return なし
*/
void LocalTempStore::setSwapFileSizeLimit(uint64_t size) {
	bufferManager_->setSwapFileSizeLimit(size);
}

/*!
	@brief 常時確保メモリ上限の設定
	@param [in] size 常時確保メモリ上限
	@return なし
	@note この値までは必要に応じてメモリを確保する。使用中メモリがこの値を超えた時は、メモリ解放時にこの値までメモリを解放する。
*/
void LocalTempStore::setStableMemoryLimit(uint64_t size) {
	bufferManager_->setStableMemoryLimit(size);
}


/*!
	@brief リソースグループIDの割当
	@return 割り当てたリソースグループID
	@note 同一リソースグループは同一ファイルにスワップアウトされる。また小ブロックの場合は同一大ブロックからメモリ確保される。
*/
LocalTempStore::GroupId LocalTempStore::allocateGroup() {
	return resourceInfoManager_->allocateGroup();
}

/*!
	@brief リソースグループIDの割当解除
	@param [in] groupId 解除するリソースグループID
	@return なし
	@note これを呼び出した後、指定したgroupIdでallocateResourceするとエラーになる。リソース自体は破棄されない。(リソースが破棄されるのはリソースの参照カウンタが0になった時)
*/
void LocalTempStore::deallocateGroup(LocalTempStore::GroupId groupId) {
	resourceInfoManager_->freeGroup(groupId);
}

/*!
	@brief IDで紐づけられたリソースを割り当て
	@param [in] type リソース種別: TUPLE_LISTかJSON_LIST
	@param [in] groupId リソースグループID: TUPLE_LISTでは必要。JSON_LISTでは不要(省略可)
	@return 割り当てたリソースID
*/
LocalTempStore::ResourceId LocalTempStore::allocateResource(
	LocalTempStore::ResourceType type, LocalTempStore::GroupId groupId) {

	return resourceInfoManager_->allocateResource(type, groupId);
}

/*!
	@brief IDとリソースとの紐づけを解消
	@param [in] resourceId 割当を解除するリソースID
	@return なし
	@note 以後指定IDによるリソースへのアクセスはできなくなるが、
		リソースの実体がすべて破棄されるのは関連するインスタンス
		(TupleListの場合はTupleListオブジェクト)が破棄された時点となる
*/
void LocalTempStore::deallocateResource(LocalTempStore::ResourceId resourceId) {
	return resourceInfoManager_->freeResource(resourceId);
}



/*!
	@brief コンストラクタ
	@return なし
*/
LocalTempStore::BufferManager::BufferManager(
	LocalTempStore::LTSVariableSizeAllocator &varAlloc,
	const LocalTempStore::Config &config,
	LocalTempStore &store):
varAlloc_(varAlloc), store_(store),
swapFileSizeLimit_(config.swapFileSizeLimit_),
file_(NULL),
blockInfoTable_(varAlloc, store, store.defaultBlockExpSize_,
		config.stableMemoryLimit_, config.swapFileSizeLimit_),
maxBlockNth_(0), inuseMaxBlockNth_(0), wroteMaxBlockNth_(0),
trimSwapFileThreshold_(0),
blockExpSize_(store.defaultBlockExpSize_),
blockSize_(1 << store.defaultBlockExpSize_),
stableMemoryLimit_(config.stableMemoryLimit_),
currentTotalMemory_(0), peakTotalMemory_(0),
activeBlockCount_(0)
{
	file_ = ALLOC_VAR_SIZE_NEW(varAlloc_) File(
			varAlloc_, store, config, store.defaultBlockExpSize_, config.swapFilesTopDir_);
	file_->open();
	BlockId trimUnitBlockCount = SWAP_FILE_TRIM_UNIT_SIZE / blockSize_;
	trimSwapFileThreshold_ = trimUnitBlockCount > 10 ? trimUnitBlockCount : 10;
}


/*!
	@brief デストラクタ
	@param [in] groupId リソースグループID
	@param [in] resourceId リソースID
	@param [in] resourceBlockSize ブロックサイズ
	@return なし
*/
LocalTempStore::BufferManager::~BufferManager() {
	if (file_) {
		file_->close();
		ALLOC_VAR_SIZE_DELETE(varAlloc_, file_);
		file_ = NULL;
	}
}

/*!
	@brief スワップファイルサイズ合計の上限の設定
	@param [in] size スワップファイルサイズ合計の上限
	@return なし
*/
void LocalTempStore::BufferManager::setSwapFileSizeLimit(uint64_t size) {
	util::LockGuard<util::Mutex> guard(mutex_);
	swapFileSizeLimit_ = size;
}


void LocalTempStore::BufferManager::allocateMemoryMain(
		BlockId id, BlockInfo* &blockInfo) {
	try {
		blockInfoTable_.insert(id, blockInfo); 
		blockInfo->data_ = static_cast<uint8_t*>(blockInfoTable_.fixedAlloc_->allocate());
		assert(blockInfo->data_);
		LocalTempStore::Block::Header::resetHeader(blockInfo->data_, blockExpSize_);
	}
	catch (std::exception &e) {
		blockInfoTable_.remove(id);
		decrementActiveBlockCount();
		blockInfo->clear();
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"Memory allocation failed"));
	}
}

bool LocalTempStore::BufferManager::allocateMemory(BlockId id, BlockInfo* &blockInfo) {
	blockInfo = NULL;
	bool needSwap = false;
	if (getAllocatedMemory() < stableMemoryLimit_) {
		allocateMemoryMain(id, blockInfo);
		addCurrentMemory();
	}
	else if (checkTotalMemory()) {
		allocateMemoryMain(id, blockInfo);
		addCurrentMemory();
	}
	else {
		BlockInfo* swapOutTarget = blockInfoTable_.getNextTarget();
		if (swapOutTarget) {
			blockInfo = swapOutTarget;
#ifndef NDEBUG
			BlockId headerBlockId = LocalTempStore::Block::Header::getBlockId(blockInfo->data_);
			assert(headerBlockId == blockInfo->blockId_);
#endif
			needSwap = true;
		}
		else {
			allocateMemoryMain(id, blockInfo);
			addCurrentMemory();
		}
	}
	assert(blockInfo);
	assert(blockInfo->baseBlockInfo_);
	return needSwap;
}

/*!
	@brief ブロック割り当て
	@return なし
*/
void LocalTempStore::BufferManager::allocate(
		BlockInfo* &blockInfo, uint64_t affinity, bool force) {
	bool swapped = false;
	static_cast<void>(affinity);
	BlockId id = UNDEF_BLOCKID;
	try {
		LockGuardVariant<util::Mutex> guard(mutex_);
		id = LocalTempStore::makeBlockId(
				blockInfoTable_.allocateBlockNth(force), blockExpSize_);
		incrementActiveBlockCount();
		bool needSwap = allocateMemory(id, blockInfo);
		blockInfoTable_.update(blockInfo, id);
		blockInfo->addReference();
		if (needSwap) {
			blockInfo->swapOut(store_, id);
		}
		guard.release();
		try {
			blockInfo->lock();
			blockInfo->setup(store_, id, true);
			blockInfo->unlock();
		}
		catch(std::exception &e) {
			blockInfo->unlock();
			GS_RETHROW_USER_ERROR(e, "");
		}
		guard.acquire();
#ifndef NDEBUG
		BlockInfo* blockInfo2 = blockInfoTable_.lookup(id);
		assert(blockInfo == blockInfo2);
#endif
		blockInfo->baseBlockInfo_ = lookupBaseInfo(id);
		assert(LocalTempStore::Block::Header::getBlockId(blockInfo->data_) == blockInfo->blockId_);
		assert(id == blockInfo->blockId_);

		LocalTempStore::Block::Header::resetHeader(blockInfo->data_, blockExpSize_);
		LocalTempStore::Block::Header::setBlockId(blockInfo->data_, id);
		blockInfo->baseBlockInfo_->assignmentCount_ &= (~MASK_ALREADY_SWAPPED);
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"Failed to create block."));
	}
}

/*!
	@brief ブロック取得
	@param [in] block
	@return なし
*/
void LocalTempStore::BufferManager::get(BlockId id, BlockInfo* &blockInfo) {
	try {
		LockGuardVariant<util::Mutex> guard(mutex_);
		blockInfo = blockInfoTable_.lookup(id);
		if (blockInfo) {
			blockInfo->addReference();
			guard.release();
			try {
				blockInfo->lock();
				blockInfo->setup(store_, id, false);
				blockInfo->unlock();
				assert(blockInfo->blockId_ == id);
			}
			catch(std::exception &e) {
				blockInfo->unlock();
				GS_RETHROW_USER_ERROR(e, "");
			}
			guard.acquire();
			blockInfo->baseBlockInfo_ = lookupBaseInfo(id);

			assert(LocalTempStore::Block::Header::getBlockId(blockInfo->data_) == blockInfo->blockId_);
			assert(id == blockInfo->blockId_);
			return;
		}
		else {
			LocalTempStore::BaseBlockInfo* baseInfo = blockInfoTable_.lookupBaseInfo(id);
			if (baseInfo->assignmentCount_ != 0) {
				allocateMemory(id, blockInfo);
				blockInfoTable_.update(blockInfo, id);
				blockInfo->addReference();
				blockInfo->swapOut(store_, id);
				guard.release();
				try {
					blockInfo->lock();
					blockInfo->setup(store_, id, false); 
					blockInfo->unlock();
				}
				catch(std::exception &e) {
					blockInfo->unlock();
					GS_RETHROW_USER_ERROR(e, "");
				}
				guard.acquire();
#ifndef NDEBUG
				BlockInfo* blockInfo2 = blockInfoTable_.lookup(id);
				assert(blockInfo == blockInfo2);
#endif
				blockInfo->baseBlockInfo_ = lookupBaseInfo(id);
				assert(LocalTempStore::Block::Header::getBlockId(blockInfo->data_) == blockInfo->blockId_);
				assert(id == blockInfo->blockId_);
				return;
			}
			else {	
				blockInfo = NULL;
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}	
		}
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"Failed to get block: (blockId=" << id << ")"));
	}
}

/*!
	@brief ブロック解放
	@param [in] groupId リソースグループID
	@param [in] resourceId リソースID
	@param [in] resourceBlockSize ブロックサイズ
	@return なし
	@note 使用中メモリを解放し、blockIdを再利用可能にする
	A=0かつR=0になった場合に、removeReferenceまたはremoveAssignmentから呼ばれる
*/
void LocalTempStore::BufferManager::release(BlockInfo &blockInfo, uint64_t affinity) {
	static_cast<void>(affinity);
	blockInfoTable_.fixedAlloc_->deallocate(blockInfo.data_);
	subCurrentMemory();
	BlockId blockId = blockInfo.getBlockId();
	if (inuseMaxBlockNth_ > blockId) {
		inuseMaxBlockNth_ = blockInfoTable_.getCurrentMaxInuseBlockNth(blockId);
	}
	blockInfo.clear();
	blockInfoTable_.remove(blockId);
	decrementActiveBlockCount();
}

void LocalTempStore::BufferManager::decrementActiveBlockCount() {
	assert(activeBlockCount_ > 0);
	--activeBlockCount_;
	if (activeBlockCount_ == 0) {
		if (file_->getFileSize() > 0) {
			try {
				file_->trim(0);
				UTIL_TRACE_INFO(SQL_TEMP_STORE,
						"Truncate swap file. (maxActiveBlockCount=" <<
						getMaxActiveBlockCount() << ")");
			} catch(std::exception &e) {
				UTIL_TRACE_WARNING(SQL_TEMP_STORE,
						"Failed to truncate swap file. (reason=" <<
						GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}
	}
}

/*!
	@brief 目標メモリ量設定
	@param [in] size 目標メモリ量
	@return なし
*/
void LocalTempStore::BufferManager::setStableMemoryLimit(uint64_t size) {
	util::LockGuard<util::Mutex> guard(mutex_);
	stableMemoryLimit_ = size;
	blockInfoTable_.fixedAlloc_->setLimit(
			util::AllocatorStats::STAT_STABLE_LIMIT, static_cast<size_t>(size));
}


/*!
	@brief 使用中メモリサイズの取得
*/
size_t LocalTempStore::BufferManager::getAllocatedMemory() {
	return blockInfoTable_.fixedAlloc_->getElementSize()
	  * (blockInfoTable_.fixedAlloc_->getTotalElementCount()
		 - blockInfoTable_.fixedAlloc_->getFreeElementCount());
}

/*!
	@brief 指定したブロックをスワップインする
	@param [in/out] blockInfo
	@return なし
*/
void LocalTempStore::BufferManager::swapIn(BlockId blockId, BlockInfo &blockInfo) {
	BlockId blockNth = LocalTempStore::getBlockNth(blockId);
	assert(blockInfo.baseBlockInfo_);
	store_.incrementSwapInCount(blockInfo.baseBlockInfo_->groupId_);
	try {
		file_->readBlock(blockNth, 1, blockInfo.data());
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
				"Swap file read failed: (blockId=" << blockId << ")"));
	}

	LocalTempStore::Block::Header::validateHeader(blockInfo.data_, blockExpSize_, false);
}

/*!
	@brief 指定したブロックをスワップアウトする
	@param [in] blockInfo
	@return なし
*/
void LocalTempStore::BufferManager::swapOut(BlockInfo &blockInfo) {
	assert(blockInfo.baseBlockInfo_);
	bool isSwapped =
			((MASK_ALREADY_SWAPPED & blockInfo.baseBlockInfo_->assignmentCount_) != 0);
	if (!isSwapped && blockInfo.baseBlockInfo_->assignmentCount_ > 0) {
		store_.incrementSwapOutCount(blockInfo.baseBlockInfo_->groupId_);
		BlockId blockNth = LocalTempStore::getBlockNth(blockInfo.getBlockId());
		if (blockInfo.data() == NULL) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_SWAP_OUT_BLOCK_FAILED,
				"blockInfo.data_ is NULL. blockId=" << blockInfo.getBlockId() <<
				", blockNth=" << blockNth <<
				", assignmentCount=" << blockInfo.baseBlockInfo_->assignmentCount_);
		}
		if (!file_) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_SWAP_OUT_BLOCK_FAILED,
					"file_ is NULL.");
		}
		assert(blockInfo.data() != NULL);
		LocalTempStore::Block::Header::validateHeader(blockInfo.data(), blockExpSize_, false);
		file_->writeBlock(blockNth, 1, blockInfo.data());
		blockInfo.baseBlockInfo_->assignmentCount_ |= MASK_ALREADY_SWAPPED;
	}
}

/*!
	@brief 空きブロックのBlockNthをダンプする
	@param [in] ostr
	@return なし
*/
void LocalTempStore::BufferManager::dumpActiveBlockId(std::ostream &ostr) {
	blockInfoTable_.dumpActiveBlockId(ostr, blockExpSize_);
}

/*!
	@brief スワップファイルサイズ取得
	@return スワップファイルサイズ
*/
uint64_t LocalTempStore::BufferManager::getSwapFileSize() {
	return file_->getFileSize();
}

/*!
	@brief 書き込みブロック総数の取得
	@return 書き込みブロック数
*/
uint64_t LocalTempStore::BufferManager::getWriteBlockCount() {
	return file_->getWriteBlockCount();
}

uint64_t LocalTempStore::BufferManager::getWriteOperation() {
	return file_->getWriteOperation();
}

uint64_t LocalTempStore::BufferManager::getWriteSize() {
	return file_->getWriteSize();
}

uint64_t LocalTempStore::BufferManager::getWriteTime() {
	return file_->getWriteTime();
}

/*!
	@brief 読み込みブロック総数の取得
	@return 読み込みブロック数
*/
uint64_t LocalTempStore::BufferManager::getReadBlockCount() {
	return file_->getReadBlockCount();
}

uint64_t LocalTempStore::BufferManager::getReadOperation() {
	return file_->getReadOperation();
}

uint64_t LocalTempStore::BufferManager::getReadSize() {
	return file_->getReadSize();
}

uint64_t LocalTempStore::BufferManager::getReadTime() {
	return file_->getReadTime();
}


/*!
	@brief コンストラクタ
	@return なし
*/
LocalTempStore::BlockInfoTable::BlockInfoTable(
		LTSVariableSizeAllocator &varAlloc, LocalTempStore &store,
		uint32_t blockExpSize, uint64_t stableMemoryLimit
		, uint64_t swapFileSizeLimit
	) :
varAlloc_(varAlloc), fixedAlloc_(NULL), store_(store),
blockInfoList_(varAlloc),
#if UTIL_CXX11_SUPPORTED
blockInfoMap_(LocalTempStore::MIN_INITIAL_BUCKETS, BlockInfoMap::hasher(), BlockInfoMap::key_equal(), varAlloc),
#else
blockInfoMap_(BlockInfoMap::key_compare(), varAlloc),
#endif
freeBlockIdSet_(BlockIdSet::key_compare(), varAlloc),
baseBlockInfoArray_(varAlloc),
blockExpSize_(blockExpSize), stableMemoryLimit_(stableMemoryLimit)
, blockIdMaxLimit_(0)
{
	blockInfoPool_ = ALLOC_VAR_SIZE_NEW(varAlloc_) util::ObjectPool<LocalTempStore::BlockInfo>(
			util::AllocatorInfo(ALLOCATOR_GROUP_SQL_LTS, "blockInfoPool"));
	blockInfoPool_->setFreeElementLimit(LocalTempStore::BLOCK_INFO_POOL_FREE_LIMIT);
	fixedAlloc_ = ALLOC_VAR_SIZE_NEW(varAlloc_) LocalTempStore::LTSFixedSizeAllocator(util::AllocatorInfo(ALLOCATOR_GROUP_SQL_LTS, "ltsBufMgrFixed"), 1 << blockExpSize_);

	uint64_t maxCount = stableMemoryLimit_ / static_cast<uint32_t>(1UL << blockExpSize_) + 1;
	fixedAlloc_->setFreeElementLimit(static_cast<size_t>(maxCount));
	blockIdMaxLimit_ = swapFileSizeLimit / static_cast<uint64_t>(1UL << blockExpSize_) + 1;
}

/*!
	@brief デストラクタ
	@return なし
*/
LocalTempStore::BlockInfoTable::~BlockInfoTable() {
	BlockInfoMap::iterator itr = blockInfoMap_.begin();
	for (; itr != blockInfoMap_.end(); ++itr) {
		LocalTempStore::BlockInfo* blockInfo = *(itr->second);
		if (blockInfo->data_) {
			fixedAlloc_->deallocate(blockInfo->data_);
			store_.getBufferManager().subCurrentMemory();
			blockInfo->data_ = NULL;
		}
		UTIL_OBJECT_POOL_DELETE(*blockInfoPool_, blockInfo);
	}
	ALLOC_VAR_SIZE_DELETE(varAlloc_, fixedAlloc_);
	fixedAlloc_ = NULL;
	ALLOC_VAR_SIZE_DELETE(varAlloc_, blockInfoPool_);
	blockInfoPool_ = NULL;
}


/*!
	@brief コンストラクタ
	@param [in] groupId リソースグループID
	@param [in] resourceId リソースID
	@param [in] resourceBlockSize ブロックサイズ
	@return なし
*/
LocalTempStore::BufferManager::File::File(
	LTSVariableSizeAllocator &varAlloc, LocalTempStore &store,
	const LocalTempStore::Config &config,
	uint32_t blockExpSize, const std::string &topDir)
: varAlloc_(varAlloc), store_(store), swapFilesTopDir_(topDir)
, file_(NULL), blockExpSize_(blockExpSize)
, blockSize_(1 << blockExpSize)
, blockNum_(0), readBlockCount_(0), readOperation_(0), readSize_(0), readTime_(0)
, writeBlockCount_(0), writeOperation_(0), writeSize_(0), writeTime_(0)
, swapSyncInterval_(config.swapSyncInterval_), swapSyncCount_(0)
, swapSyncSize_(config.swapSyncSize_)
, swapReleaseInterval_(config.swapReleaseInterval_)
, ioWarningThresholdMillis_(
		LocalTempStore::DEFAULT_IO_WARNING_THRESHOLD_MILLIS) {
	if (config.swapSyncSize_ > 0) {
		uint32_t interval = static_cast<uint32_t>((config.swapSyncSize_ - 1) / blockSize_ + 1);
		if (config.swapSyncInterval_ > 0) {
			swapSyncInterval_ = static_cast<uint32_t>(std::min(config.swapSyncInterval_, interval));
		}
		else {
			swapSyncInterval_ = interval;
		}
	}
	UTIL_TRACE_INFO(SQL_TEMP_STORE,
			"File init: fileName," << fileName_ <<
			", swapSyncInterval_=" << swapSyncInterval_ <<
			", swapSyncSize_=" << swapSyncSize_);
}

/*!
	@brief デストラクタ
	@return なし
*/
LocalTempStore::BufferManager::File::~File() try {
	if (file_) {
		UTIL_TRACE_DEBUG(SQL_TEMP_STORE, "~File(): fileName=" << fileName_.c_str()
						<< ", readCount=" << readBlockCount_
						<< ", writeCount=" << writeBlockCount_);
	}
	ALLOC_VAR_SIZE_DELETE(varAlloc_, file_);
	file_ = NULL;
}
catch (...) {
}

/*!
	@brief 書き込みブロック総数の取得
	@return 書き込みブロック数
*/
uint64_t LocalTempStore::BufferManager::File::getWriteBlockCount() {
	return writeBlockCount_;
}

uint64_t LocalTempStore::BufferManager::File::getWriteOperation() {
	return writeOperation_;
}

uint64_t LocalTempStore::BufferManager::File::getWriteSize() {
	return writeSize_;
}

uint64_t LocalTempStore::BufferManager::File::getWriteTime() {
	return writeTime_;
}

/*!
	@brief 読み込みブロック総数の取得
	@return 読み込みブロック数
*/
uint64_t LocalTempStore::BufferManager::File::getReadBlockCount() {
	return readBlockCount_;
}

uint64_t LocalTempStore::BufferManager::File::getReadOperation() {
	return readOperation_;
}

uint64_t LocalTempStore::BufferManager::File::getReadSize() {
	return readSize_;
}

uint64_t LocalTempStore::BufferManager::File::getReadTime() {
	return readTime_;
}

/*!
	@brief 書き込みブロック総数のリセット
	@return なし
*/
void LocalTempStore::BufferManager::File::resetWriteBlockCount() {
	writeBlockCount_ = 0;
}

/*!
	@brief 書き込みブロック総数のリセット
	@return なし
*/
void LocalTempStore::BufferManager::File::resetReadBlockCount() {
	readBlockCount_ = 0;
}

void LocalTempStore::BufferManager::File::countFileBlock(
	uint64_t &totalCount, uint64_t &fileSize) {
	totalCount = blockNum_;
	fileSize = getFileSize();
}

void LocalTempStore::BlockInfoTable::dumpActiveBlockId(
	std::ostream &ostr, uint32_t blockExpSize) {
	size_t blockNum = baseBlockInfoArray_.size();
	bool found = false;
	ostr << "blockNum=" << blockNum << ", freeBlockNthList=(";
	for (uint64_t blockNth = 0; blockNth < blockNum; ++blockNth) {
		if (freeBlockIdSet_.find(LocalTempStore::makeBlockId(blockNth, blockExpSize))
			== freeBlockIdSet_.end()) {
			ostr << blockNth << ", ";
			found = true;
		}
	}
	if (found) {
		ostr << ")" << std::endl;
	}
	else {
		ostr << "empty)" << std::endl;
	}
}

/*!
	@brief 物理ファイルからのブロック読み込み
	@param [in] buffer 読み込みブロックメモリアドレス
	@param [in] count 読み込みブロック数
	@param [in] blockNo 読み込みファイルブロック番号
	@return 読み込みブロック数
*/
size_t LocalTempStore::BufferManager::File::readBlock(
		FileBlockId fileBlockId, size_t count, void *buffer) {
	try {
		assert(buffer);
		assert(file_);  
		if (blockNum_ > count + fileBlockId - 1) {
			ssize_t remain = static_cast<ssize_t>(count * blockSize_);
			uint8_t* addr = static_cast<uint8_t*>(buffer);
			off_t offset = static_cast<off_t>(fileBlockId * blockSize_);

			const uint64_t readStartClock = util::Stopwatch::currentClock();
			uint64_t retryCount = 0;
			GS_FILE_READ_ALL(
					IO_MONITOR, (*file_), addr, remain, offset,
					ioWarningThresholdMillis_, retryCount);
			const uint32_t readTime = util::Stopwatch::clockToMillis(
					util::Stopwatch::currentClock() - readStartClock);
			readBlockCount_ += count;
			++readOperation_;
			readSize_ += static_cast<uint64_t>(count * blockSize_);
			readTime_ += readTime;

			uint32_t currentCheckSum =
					LocalTempStore::Block::Header::calcBlockCheckSum(buffer);
			uint32_t headerCheckSum =
					LocalTempStore::Block::Header::getHeaderCheckSum(buffer);
			if (headerCheckSum != currentCheckSum) {
				GS_THROW_USER_ERROR(GS_ERROR_LTS_SWAP_IN_BLOCK_FAILED,
						"Checksum error occured.");
			}
			return count;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_SWAP_IN_BLOCK_FAILED,
					"Target block is not existed: fileName=" << fileName_ <<
					",blockNo," << fileBlockId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
						"Swap file read failed: fileName=" << fileName_ <<
						",blockNo," << fileBlockId));
	}
}


/*!
	@brief ファイルへのブロック書き込み
	@param [in] buffer 書き込むバッファのアドレス
	@param [in] count 書き込むブロック数
	@param [in] blockNo ファイル書き込み先のブロック番号
	@return 書き込みブロック数
*/
size_t LocalTempStore::BufferManager::File::writeBlock(
		FileBlockId fileBlockId, size_t count, void* buffer) {
	LocalTempStore::Block::Header::updateHeaderCheckSum(buffer);

	assert(buffer);
	try {
		assert(file_);
		uint64_t writeOffset = fileBlockId;

		ssize_t remain = static_cast<ssize_t>(count * blockSize_);
		const uint8_t* addr = static_cast<const uint8_t*>(buffer);
		off_t offset = static_cast<off_t>(fileBlockId * blockSize_);
		const uint64_t writeStartClock = util::Stopwatch::currentClock();
		uint64_t retryCount = 0;
		GS_FILE_WRITE_ALL(
				IO_MONITOR, (*file_), addr, remain, offset,
				ioWarningThresholdMillis_, retryCount);
		const uint32_t writeTime = util::Stopwatch::clockToMillis(
				util::Stopwatch::currentClock() - writeStartClock);
		writeBlockCount_ += count;
		++writeOperation_;
		writeSize_ += static_cast<uint64_t>(count * blockSize_);
		writeTime_ += writeTime;

		if (blockNum_ < (count + writeOffset)) {
			blockNum_ = (count + writeOffset);
		}
		if (swapSyncInterval_ > 0 &&
				(writeBlockCount_ % swapSyncInterval_ == 0)) {
			file_->sync();
			UTIL_TRACE_INFO(SQL_TEMP_STORE,
					"sync swap file: fileName," << fileName_ << 
					",writeBlockCount," << writeBlockCount_);
#ifndef WIN32
			advise(POSIX_FADV_DONTNEED);
#endif
		}
		return count;
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
						"Swap file write failed: fileName=" << fileName_ <<
						",blockNo," << fileBlockId));
	}
}


/*!
	@brief 物理ファイルをオープンする
	@return 成功ならtrue
	@note 起動時に1回だけ呼び出される前提。ファイルが既存の場合、truncateする
*/
bool LocalTempStore::BufferManager::File::open() {
	try {
		if (!swapFilesTopDir_.empty()) {
			util::NormalOStringStream name;
			name << LocalTempStore::SWAP_FILE_BASE_NAME << blockExpSize_ <<
					LocalTempStore::SWAP_FILE_EXTENSION;
			util::FileSystem::createPath(
					swapFilesTopDir_.c_str(), name.str().c_str(), fileName_);
		}
		if (util::FileSystem::exists(fileName_.c_str())) {
			try {
				util::FileSystem::remove(fileName_.c_str());
			}
			catch (std::exception &e) {
				UTIL_TRACE_WARNING(SQL_TEMP_STORE, "Swap file remove failed. (reason="
						<< GS_EXCEPTION_MESSAGE(e) << ")");
			}
			file_ = ALLOC_VAR_SIZE_NEW(varAlloc_) util::NamedFile();
			file_->open(fileName_.c_str(),
						util::FileFlag::TYPE_CREATE
						| util::FileFlag::TYPE_TRUNCATE
						| util::FileFlag::TYPE_READ_WRITE);
			file_->lock();
			blockNum_ = 0;
			return false;
		}
		else {
			u8string dirName;
			util::FileSystem::getDirectoryName(fileName_.c_str(), dirName);
			util::FileSystem::createDirectoryTree(dirName.c_str());
			file_ = ALLOC_VAR_SIZE_NEW(varAlloc_) util::NamedFile();
			file_->open(fileName_.c_str(),
						util::FileFlag::TYPE_CREATE |
						util::FileFlag::TYPE_READ_WRITE);
			file_->lock();
			blockNum_ = 0;
			return true;
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
						"Failed to create swap file"));
	}
}

/*!
	@brief 物理ファイルをクローズする
	@return なし
	@note 終了時にのみ呼び出される
*/
void LocalTempStore::BufferManager::File::close() {
	if (file_) {
		file_->unlock();
#ifndef WIN32
		file_->sync();
		advise(POSIX_FADV_DONTNEED);
#endif
		file_->close();
		try {
			util::FileSystem::remove(fileName_.c_str());
		}
		catch (std::exception &e) {
			UTIL_TRACE_WARNING(SQL_TEMP_STORE,
					"Swap file remove failed. (reason=" <<
					GS_EXCEPTION_MESSAGE(e) << ")");
		}
		ALLOC_VAR_SIZE_DELETE(varAlloc_, file_);
	}
	file_ = NULL;
}

/*!
	@brief 物理ファイルをフラッシュする
	@return なし
*/
void LocalTempStore::BufferManager::File::flush() {
	if (file_) {
		const uint64_t startClock = util::Stopwatch::currentClock(); 
		file_->sync();
		const uint32_t lap = util::Stopwatch::clockToMillis(util::Stopwatch::currentClock() - startClock); 
		if (lap > ioWarningThresholdMillis_) { 
			UTIL_TRACE_WARNING(IO_MONITOR,
					"[LONG I/O] sync time," << lap <<
					",fileName," << fileName_);
		}
	}
}

/*!
	@brief 物理ファイルをトリミングする
	@return なし
*/
void LocalTempStore::BufferManager::File::trim(uint64_t size) {
	if (file_) {
		file_->setSize(size);
	}
}

/*!
	@brief ファイルのアクセスパターン宣言
	@return なし
*/
void LocalTempStore::BufferManager::File::advise(int32_t advise) {
#ifndef WIN32
	if (!file_->isClosed()) {
		int32_t ret = posix_fadvise(file_->getHandle(), 0, 0, advise);
		if (ret > 0) {
			UTIL_TRACE_WARNING(SQL_TEMP_STORE,
					"fadvise failed. :" <<
						"fileName," << fileName_ <<
						",advise," << advise <<
						",returnCode," << ret);
		}
		UTIL_TRACE_INFO(SQL_TEMP_STORE,
				"advise(POSIX_FADV_DONTNEED) : fileName," << fileName_);
	}
#endif
}

/*!
	@brief 物理ファイルのファイルサイズを取得する
	@return ファイルサイズ
*/
uint64_t LocalTempStore::BufferManager::File::getFileSize() {
	uint64_t fileSize = 0;
	try {
		if (file_) {
			util::FileStatus fileStatus;
			file_->getStatus(&fileStatus);
			fileSize = fileStatus.getSize();
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
						"Failed to get file status"));
	}
	return fileSize;
}

LocalTempStore::Block::Block() : blockInfo_(NULL) {
}

/*!
	@brief コンストラクタ
	@param [in] store LocalTempStore
	@param [in] resourceId 対象のリソースID
	@param [in] resourceBlockId 対象のリソースブロックID
	@note このインスタンスが存在する限り、対応するブロックはオンメモリで維持される
*/

LocalTempStore::Block::Block(LocalTempStore &store, LocalTempStore::BlockId blockId)
: blockInfo_(NULL) {
	store.getBufferManager().get(blockId, blockInfo_);

}

LocalTempStore::Block::Block(LocalTempStore &store, size_t blockSize, uint64_t affinity) {
	static_cast<void>(blockSize);
	store.getBufferManager().allocate(blockInfo_, affinity);
	setSwapped(false);
}

LocalTempStore::Block::Block(LocalTempStore &store, const void *data, size_t size) {
	store.getBufferManager().allocate(blockInfo_);
	uint32_t blockExpSize = LocalTempStore::Block::Header::getBlockExpSize(blockInfo_->data());

	assert(size == static_cast<size_t>(1 << blockExpSize));
	if (size != static_cast<size_t>(1 << blockExpSize)) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
				"Invalid block size (blockSize=" << (1 << blockExpSize) <<
				", imageSize=" << size << ")");
	}
	uint8_t* dest = static_cast<uint8_t*>(blockInfo_->data())
	  + LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
	const uint8_t* src = static_cast<const uint8_t*>(data)
	  + LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
	memcpy(dest, src, size - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET);
	setSwapped(false);
	LocalTempStore::Block::Header::validateHeader(blockInfo_->data(), blockExpSize, false);
}

LocalTempStore::Block::Block(LocalTempStore::BlockInfo &blockInfo)
: blockInfo_(&blockInfo) {

}

void LocalTempStore::Block::setGroupId(LocalTempStore::GroupId groupId) {
	assert(blockInfo_);
	assert(blockInfo_->baseBlockInfo_);
	if (blockInfo_->baseBlockInfo_->groupId_ != groupId) {
		assert(blockInfo_->store_);
		blockInfo_->store_->decrementLatchBlockCount(blockInfo_->baseBlockInfo_->groupId_);
		blockInfo_->store_->incrementLatchBlockCount(groupId);
	}
	blockInfo_->baseBlockInfo_->groupId_ = groupId;
}

LocalTempStore::GroupId LocalTempStore::Block::getGroupId() {
	assert(blockInfo_);
	assert(blockInfo_->baseBlockInfo_);
	return blockInfo_->baseBlockInfo_->groupId_;
}

void LocalTempStore::Block::dumpContents(std::ostream &ostr) const {
	assert(blockInfo_);
	ostr << std::endl;
}

void LocalTempStore::Block::Header::validateHeader(
		void *block, uint32_t blockExpSize, bool verifyCheckSum) {
	uint16_t magic = LocalTempStore::Block::Header::getMagic(block);
	uint16_t version = LocalTempStore::Block::Header::getVersion(block);
	uint32_t expSize = LocalTempStore::Block::Header::getBlockExpSize(block);

	assert(magic == LocalTempStore::Block::Header::MAGIC_NUMBER);
	if (magic != LocalTempStore::Block::Header::MAGIC_NUMBER) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
				"Invalid magic number (value=" <<
				LocalTempStore::Block::Header::MAGIC_NUMBER << ")");
	}
	assert(version == LocalTempStore::Block::Header::VERSION_NUMBER);
	if (version != LocalTempStore::Block::Header::VERSION_NUMBER) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
				"Invalid version number (value=" <<
				LocalTempStore::Block::Header::MAGIC_NUMBER << ")");
	}
	assert(expSize == blockExpSize);
	if (blockExpSize != 0 && expSize != blockExpSize) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
				"Invalid block size (expected=" << blockExpSize <<
				", value=" <<
				LocalTempStore::Block::Header::MAGIC_NUMBER << ")");
	}
	if (verifyCheckSum) {
		uint32_t headerCheckSum = LocalTempStore::Block::Header::getHeaderCheckSum(block);
		uint32_t blockCheckSum = LocalTempStore::Block::Header::calcBlockCheckSum(block);
		assert(headerCheckSum != blockCheckSum);
		if (headerCheckSum != blockCheckSum) {
			GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
					"Checksum error (header=" << headerCheckSum <<
					", actual=" << blockCheckSum <<")");
		}
	}
}


void LocalTempStore::BlockInfo::dumpContents(std::ostream &ostr) const {
	assert(false);
	ostr << std::endl;
}

bool LocalTempStore::BlockInfo::checkLocked(bool getLock) {
	bool isLocked = !mutex_.tryLock();
	if (!getLock) {
		mutex_.unlock();
	}
	return isLocked;
};

LocalTempStore::BlockInfo::BlockInfo(LocalTempStore &store, BlockId blockId)
: data_(NULL), baseBlockInfo_(NULL), store_(&store)
, blockId_(0)   
, refCount_(0) {
}

void LocalTempStore::BlockInfo::initialize(LocalTempStore &store, BlockId id) {
	store_ = &store;
	blockId_ = id;
	refCount_ = 0;
}

void LocalTempStore::BlockInfo::swapOut(LocalTempStore &store, BlockId id) {
	assert(data_);
	bool isSwapped =
			((MASK_ALREADY_SWAPPED & baseBlockInfo_->assignmentCount_) != 0);
	uint64_t assignmentCount = (~MASK_ALREADY_SWAPPED) & baseBlockInfo_->assignmentCount_;
	if (!isSwapped && assignmentCount > 0 && blockId_ != id) {
#ifndef NDEBUG
		BlockId headerBlockId = LocalTempStore::Block::Header::getBlockId(data_);
		assert(headerBlockId == blockId_);
#endif
		store.getBufferManager().swapOut(*this);
	}
}

void LocalTempStore::BlockInfo::setup(LocalTempStore &store, BlockId id, bool isNew) {
	static const uint32_t blockExpSize = store.getDefaultBlockExpSize();
	assert(data_);
	BlockId headerBlockId = LocalTempStore::Block::Header::getBlockId(data_);
	if (!isNew && (id != blockId_ || id != headerBlockId)) {
		store.getBufferManager().swapIn(id, *this);
		assert(LocalTempStore::Block::Header::getBlockId(data_) == id);

	}
	if (isNew) {
		LocalTempStore::Block::Header::resetHeader(data_, blockExpSize);
		LocalTempStore::Block::Header::setBlockId(data_, id);
	}
	store_ = &store;
	blockId_ = id;
#ifndef NDEBUG
	headerBlockId = LocalTempStore::Block::Header::getBlockId(data_);
	assert(headerBlockId == id);
	assert(blockId_ == id);
#endif
}

void LocalTempStore::BlockInfo::setGroupId(LocalTempStore::GroupId groupId) {
	assert(baseBlockInfo_);
	if (baseBlockInfo_->groupId_ != groupId) {
		assert(store_);
		store_->decrementLatchBlockCount(baseBlockInfo_->groupId_);
		store_->incrementLatchBlockCount(groupId);
	}
	baseBlockInfo_->groupId_ = groupId;
}

void LocalTempStore::Block::assign(const void *data, size_t size, bool isPartial) {
	assert(blockInfo_);
	assert(data);
	blockInfo_->assign(data, size, isPartial);
}

void LocalTempStore::BlockInfo::assign(const void *data, size_t size, bool isPartial) {
	assert(data_);
	assert(store_);

	const size_t blockSize = static_cast<size_t>(
			1 << LocalTempStore::Block::Header::getBlockExpSize(data_));
	assert(isPartial ? true : (size == blockSize));

	if (size > blockSize) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
				"Block size is too small (blockSize=" << blockSize <<
				", imageSize=" << size << ")");
	}
	uint8_t* dest = data_ + LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;
	const uint8_t* src =
			static_cast<const uint8_t*>(data) +
			LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET;

	memcpy(dest, src, size - LocalTempStore::Block::Header::CONTIGUOUS_BLOCK_NUM_OFFSET);

}


LocalTempStore::Config::Config(const ConfigTable &config)
: blockSize_(DEFAULT_BLOCK_SIZE)
, swapFileSizeLimit_(ConfigTable::megaBytesToBytes(
		config.getUInt32(CONFIG_TABLE_SQL_STORE_SWAP_FILE_SIZE_LIMIT)))
, stableMemoryLimit_(ConfigTable::megaBytesToBytes(
		config.getUInt32(CONFIG_TABLE_SQL_STORE_MEMORY_LIMIT)))
, swapFilesTopDir_(
		config.get<const char8_t *>(CONFIG_TABLE_SQL_STORE_SWAP_FILE_PATH))
, swapSyncInterval_(config.getUInt32(CONFIG_TABLE_SQL_STORE_SWAP_SYNC_INTERVAL))
, swapSyncSize_(ConfigTable::megaBytesToBytes(
		config.getUInt32(CONFIG_TABLE_SQL_STORE_SWAP_SYNC_SIZE)))
, swapReleaseInterval_(config.getUInt32(CONFIG_TABLE_SQL_STORE_SWAP_RELEASE_INTERVAL))
{
}

LocalTempStore::Config::Config()
: blockSize_(DEFAULT_BLOCK_SIZE)
, swapFileSizeLimit_(ConfigTable::megaBytesToBytes(
		static_cast<size_t>(DEFAULT_STORE_SWAP_FILE_SIZE_LIMIT_MB)))
, stableMemoryLimit_(ConfigTable::megaBytesToBytes(
		static_cast<size_t>(DEFAULT_STORE_MEMORY_LIMIT_MB)))
, swapFilesTopDir_(DEFAULT_SWAP_FILES_TOP_DIR)
, swapSyncInterval_(0)
, swapSyncSize_(0)
, swapReleaseInterval_(0)
{
}


/*!
	@brief コンストラクタ
	@param [in] store LocalTempStore
	@return なし
	@note 内部でallocateGroupを行う(RAIIでグループID管理を行う場合に使用する)
*/
LocalTempStore::Group::Group(LocalTempStore &store): store_(store), groupId_(UNDEF_GROUPID) {
	groupId_ = store_.allocateGroup();
}

/*!
	@brief デストラクタ
	@return なし
	@note 内部でdeallocateGroupを行う(RAIIでグループID管理を行う場合に使用する)
*/
LocalTempStore::Group::~Group() try {
	store_.deallocateGroup(groupId_);
	store_.clearGroupStats(groupId_);
}
catch (...) {
}

/*!
	@brief LocalTempStoreの取得
	@return LocalTempStore
*/
LocalTempStore& LocalTempStore::Group::getStore() const {
	return store_;
}

/*!
	@brief GroupIdの取得
	@return 割り当てられたgroupId
*/
LocalTempStore::GroupId LocalTempStore::Group::getId() const {
	return groupId_;
}

LocalTempStore::ResourceInfo& LocalTempStore::getResourceInfo(ResourceId resourceId) {
	return resourceInfoManager_->getResourceInfo(resourceId);
}

void LocalTempStore::incrementGroupResourceCount(GroupId groupId) {
	resourceInfoManager_->incrementGroupResourceCount(groupId);
}

void LocalTempStore::decrementGroupResourceCount(GroupId groupId) {
	resourceInfoManager_->decrementGroupResourceCount(groupId);
}

int64_t LocalTempStore::getGroupResourceCount(GroupId groupId) {
	return resourceInfoManager_->getGroupResourceCount(groupId);
}


/*!
	@brief 使用中メモリサイズの取得
*/
size_t LocalTempStore::getVariableAllocatorStat(
	size_t &totalSize, size_t &freeSize, size_t &hugeCount, size_t &hugeSize) {
	totalSize = varAlloc_->getTotalElementSize();
	freeSize = varAlloc_->getFreeElementSize();
	hugeCount = varAlloc_->getHugeElementCount();
	hugeSize = varAlloc_->getHugeElementSize();
	return (totalSize - freeSize) + hugeSize;
}

std::string LocalTempStore::getVariableAllocatorStat2() {
	util::NormalOStringStream oss;
	util::AllocatorStats stats;
	varAlloc_->getStats(stats);
	oss << "{";
	for (size_t pos = 0; pos < util::AllocatorStats::STAT_TYPE_END; ++pos) {
		oss << stats.values_[pos] << ",";
	}
	oss << "}";
	return oss.str();
}

void LocalTempStore::countBlockInfo(
	uint64_t &freeCount, uint64_t &latchedCount, uint64_t &unlatchedCount, uint64_t &noneCount) {
	bufferManager_->countBlockInfo(freeCount, latchedCount, unlatchedCount, noneCount);
}

void LocalTempStore::countFileBlock(uint64_t &totalCount, uint64_t &freeCount, uint64_t &fileSize) {
	totalCount = 0;
	freeCount = 0;
	fileSize = 0;
	bufferManager_->countFileBlock(totalCount, fileSize);
}

void LocalTempStore::BufferManager::countFileBlock(uint64_t &totalCount, uint64_t &fileSize) {
	file_->countFileBlock(totalCount, fileSize);
}

void LocalTempStore::dumpBlockInfo() {
	bufferManager_->dumpBlockInfo();
}

void LocalTempStore::dumpBlockInfoContents() {
	bufferManager_->dumpBlockInfoContents();
}

/*!
	@brief ブロック数の情報を取得する
	@param [out] freeCount
	@param [out] latchedCount
	@param [out] unlatchedCount
	@param [out] noneCount
	@return なし
	@note テストでの確認用
*/
void LocalTempStore::BufferManager::countBlockInfo(
	uint64_t &freeCount, uint64_t &latchedCount,
	uint64_t &unlatchedCount, uint64_t &noneCount) {
	util::LockGuard<util::Mutex> guard(mutex_);
	blockInfoTable_.countBlockInfo(
		blockExpSize_, freeCount, latchedCount, unlatchedCount, noneCount);
}

void LocalTempStore::BlockInfoTable::countBlockInfo(
	uint32_t blockExpSize, uint64_t &freeCount, uint64_t &latchedCount,
	uint64_t &unlatchedCount, uint64_t &noneCount) {
	freeCount = 0;
	latchedCount = 0;
	unlatchedCount = 0;
	noneCount = 0;

	size_t blockNum = baseBlockInfoArray_.size();
	for (uint64_t blockNth = 0; blockNth < blockNum; ++blockNth) {
		BlockId id = LocalTempStore::makeBlockId(blockNth, blockExpSize);
		BaseBlockInfo* baseBlockInfo = lookupBaseInfo(id);
		BlockInfo* blockInfo = lookup(id);
		if (blockInfo) {
			if (blockInfo->getReferenceCount() > 0) {
				++latchedCount;
			}
			else {
				if (0 == ((~MASK_ALREADY_SWAPPED) & baseBlockInfo->assignmentCount_)) {
					++freeCount;
				}
				else {
					++unlatchedCount;
				}
			}
		}
		else {
			if (0 == ((~MASK_ALREADY_SWAPPED) & baseBlockInfo->assignmentCount_)) {
				++freeCount;
			}
			else {
				++unlatchedCount;
			}
		}
	}
}

void LocalTempStore::BufferManager::dumpBlockInfo() {
	std::cerr << "LocalTempStore::BufferManager::dumpMemory" << std::endl;
	std::cerr << std::endl;
}

void LocalTempStore::BufferManager::dumpBlockInfoContents() {
	std::cerr << "LocalTempStore::BufferManager::dumpMemoryContents" << std::endl;
	std::cerr << std::endl;
}

void LocalTempStore::dumpMemory(std::ostream &ostr, const void* top, size_t size) {
	int64_t count = 0;

	const uint8_t* startAddr = static_cast<const uint8_t*>(top);
	const uint8_t* addr = startAddr;
	for (; addr < startAddr + size; ++addr) {
		if (count % 16 == 0) {
			ostr << std::setw(8) << std::setfill('0') << std::hex
			  << std::nouppercase << (uintptr_t)(addr - startAddr) << " ";
		}
		ostr << std::setw(2) << std::setfill('0') << std::hex
		  << std::nouppercase << (uint32_t)(*addr) << " ";

		if (count % 16 == 15) {
			ostr << std::endl;
		}
		++count;
	}
	ostr << std::endl;
}

void LocalTempStore::dumpBackTrace() {
#ifndef WIN32
	int nptrs;
	const int FRAME_DEPTH = 50;
	void *buffer[FRAME_DEPTH];
	nptrs = backtrace(buffer, FRAME_DEPTH);
	backtrace_symbols_fd(buffer, nptrs, STDOUT_FILENO);
#endif
}




LocalTempStore::ResourceInfoManager::ResourceInfoManager(LocalTempStore &store) :
store_(store),
#if UTIL_CXX11_SUPPORTED
groupInfoMap_(LocalTempStore::MIN_INITIAL_BUCKETS, GroupInfoMap::hasher(),
		GroupInfoMap::key_equal(), store.getVarAllocator()),
resourceInfoMap_(LocalTempStore::MIN_INITIAL_BUCKETS, ResourceInfoMap::hasher(),
		ResourceInfoMap::key_equal(), store.getVarAllocator()),
#else
groupInfoMap_(GroupInfoMap::key_compare(), store.getVarAllocator()),
resourceInfoMap_(ResourceInfoMap::key_compare(), store.getVarAllocator()),
#endif
maxGroupId_(0),
maxResourceId_(0) {
}

LocalTempStore::ResourceInfoManager::~ResourceInfoManager() {
	util::LockGuard<util::Mutex> guard(mutex_);
	groupInfoMap_.clear();
	resourceInfoMap_.clear();
}

LocalTempStore::GroupId LocalTempStore::ResourceInfoManager::allocateGroup() {
	util::LockGuard<util::Mutex> guard(mutex_);

	GroupId id = ++maxGroupId_;

	assert(groupInfoMap_.find(id) == groupInfoMap_.end());
	GroupInfo info;
	groupInfoMap_[id] = info;
	GroupInfo& groupInfo = groupInfoMap_[id];
	groupInfo.groupId_ = id;
	groupInfo.resourceCount_ = 0;
	groupInfo.status_ = GROUP_ACTIVE; 

	assert(groupInfo.groupId_ == id);
	return id;
}

LocalTempStore::GroupInfo& LocalTempStore::ResourceInfoManager::getGroupInfo(GroupId id) {
	util::LockGuard<util::Mutex> guard(mutex_);
	assert(id <= maxGroupId_);
	assert(groupInfoMap_.find(id) != groupInfoMap_.end());
	assert(groupInfoMap_[id].groupId_ == id);
	return groupInfoMap_[id];
}

void LocalTempStore::ResourceInfoManager::freeGroup(GroupId groupId) {
	util::LockGuard<util::Mutex> guard(mutex_);

	assert(groupId <= maxGroupId_);
	if (groupInfoMap_.find(groupId) != groupInfoMap_.end()) {
		GroupInfo &info = groupInfoMap_[groupId];
		info.status_ = GROUP_INACTIVE; 
		if (0 == info.resourceCount_) {
			groupInfoMap_.erase(groupId);
		}
	}
}

void LocalTempStore::ResourceInfoManager::incrementGroupResourceCount(
		GroupId groupId) {
	if (groupInfoMap_.find(groupId) == groupInfoMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_INVALID_GROUP_ID,
				"GroupId (" << groupId <<
				") is invalid. (Not allocated or already deallocated.)");
	}
	GroupInfo& groupInfo = groupInfoMap_[groupId];
	++groupInfo.resourceCount_;
}

void LocalTempStore::ResourceInfoManager::decrementGroupResourceCount(GroupId groupId) {
	if (groupInfoMap_.find(groupId) == groupInfoMap_.end()) {
		return;
	}
	GroupInfo& groupInfo = groupInfoMap_[groupId];
	--groupInfo.resourceCount_;
	assert(groupInfo.resourceCount_ >= 0);
	if (groupInfo.resourceCount_ == 0) {
		groupInfoMap_.erase(groupId);
	}
}

int64_t LocalTempStore::ResourceInfoManager::getGroupResourceCount(GroupId groupId) {
	assert(groupInfoMap_.find(groupId) != groupInfoMap_.end());
	GroupInfo &groupInfo = groupInfoMap_[groupId];
	return groupInfo.resourceCount_;
}


LocalTempStore::ResourceId LocalTempStore::ResourceInfoManager::allocateResource(
	LocalTempStore::ResourceType type, LocalTempStore::GroupId groupId) {

	util::LockGuard<util::Mutex> guard(mutex_);
	ResourceId id = ++maxResourceId_;

	ResourceInfo info;
	info.groupId_ = groupId;
	info.id_ = id;
	info.status_ = LocalTempStore::RESOURCE_INITIALIZED;
	info.resource_ = NULL;
	info.type_ = type;

	assert(resourceInfoMap_.find(id) == resourceInfoMap_.end());
	resourceInfoMap_[id] = info;
	if (UNDEF_GROUPID != groupId) {
		store_.incrementGroupResourceCount(groupId);
	}
	return id;
}

LocalTempStore::ResourceInfo& LocalTempStore::ResourceInfoManager::getResourceInfo(ResourceId id) {
	util::LockGuard<util::Mutex> guard(mutex_);
	assert(id <= maxResourceId_);
	if (resourceInfoMap_.find(id) != resourceInfoMap_.end()) {
		assert(resourceInfoMap_[id].id_ == id);
		return resourceInfoMap_[id];
	}
	else {
		GS_THROW_USER_ERROR(GS_ERROR_LTS_INVALID_RESOURCE_ID,
				"ResourceId(" << id << ") is not found");
	}
}

void LocalTempStore::ResourceInfoManager::freeResource(ResourceId resourceId) {
	util::LockGuard<util::Mutex> guard(mutex_);
	assert(resourceId <= maxResourceId_);

	if (resourceInfoMap_.find(resourceId) != resourceInfoMap_.end()) {
		ResourceInfo &resourceInfo = resourceInfoMap_[resourceId];
		assert(resourceInfo.status_ != RESOURCE_NONE);
		resourceInfo.status_ = RESOURCE_INACTIVE; 
		if (UNDEF_GROUPID != resourceInfo.groupId_) {
			store_.decrementGroupResourceCount(resourceInfo.groupId_);
		}
		resourceInfoMap_.erase(resourceId);
	}
}

void LocalTempStore::Block::encode(EventByteOutStream &out, uint32_t checkRatio) {
	try {
		if (blockInfo_ != NULL) {
			encodeMain(out, checkRatio);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(
				e, GS_EXCEPTION_MERGE_MESSAGE(e,
						"Encode binary failed"));

	}
}

void LocalTempStore::Block::decode(util::StackAllocator &alloc, EventByteInStream &in) {
	decodeMain(alloc, in);
}
void LocalTempStore::Block::encodeMain(EventByteOutStream &out, uint32_t checkRatio) {
	uint32_t usedRatio = 0;
	uint32_t usedSize = 0;
	assert((1U << Header::getBlockExpSize(data())) == DEFAULT_BLOCK_SIZE);
	static const uint32_t BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
	if (Header::getContiguousBlockNum(data()) >= 0) {
		usedSize = Header::getNextFixDataOffset(data())
		  + BLOCK_SIZE - Header::getNextVarDataOffset(data()) + 1;
		usedRatio = usedSize * 100 / BLOCK_SIZE;
	}
	else {
		usedSize = Header::getNextVarDataOffset(data());
		usedRatio = usedSize * 100 / BLOCK_SIZE;
	}
	if (usedRatio <= checkRatio) {
		if (Header::getContiguousBlockNum(data()) >= 0) {
			if (BLOCK_SIZE == Header::getNextVarDataOffset(data())) {
				out << FLAG_PARTIAL_BLOCK_1;
				uint32_t fixedPartSize = Header::getNextFixDataOffset(data());
				out << std::pair<const uint8_t*, size_t>(
						static_cast<const uint8_t *>(data()), fixedPartSize);
			}
			else {
				out << FLAG_PARTIAL_BLOCK_2;
				uint32_t fixedPartSize = Header::getNextFixDataOffset(data());
				out << fixedPartSize;
				out << std::pair<const uint8_t*, size_t>(
						static_cast<const uint8_t *>(data()), fixedPartSize);
				uint32_t varPartSize = BLOCK_SIZE - Header::getNextVarDataOffset(data());
				out << std::pair<const uint8_t*, size_t>(
						static_cast<const uint8_t *>(data())
						+ Header::getNextVarDataOffset(data()), varPartSize);
			}

		}
		else {
			out << FLAG_PARTIAL_BLOCK_1;
			out << std::pair<const uint8_t*, size_t>(
					static_cast<const uint8_t *>(data()),
					Header::getNextVarDataOffset(data()));
		}
	}
	else {
		out << FLAG_FULL_BLOCK;
		out << std::pair<const uint8_t*, size_t>(
			static_cast<const uint8_t *>(data()), BLOCK_SIZE);
	}
}

void LocalTempStore::Block::decodeMain(
		util::StackAllocator &alloc, util::ArrayByteInStream &in) {
	util::StackAllocator::Scope scope(alloc);
	size_t size = static_cast<size_t>(in.base().remaining());
	const uint8_t temp = 0;
	assert((1U << Header::getBlockExpSize(data())) == DEFAULT_BLOCK_SIZE);
	static const uint32_t BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
#ifndef NDEBUG
	uint64_t origBlockId = LocalTempStore::Block::Header::getBlockId(data());
#endif
	if (size != 0) {
		uint8_t partialFlag;
		in >> partialFlag;
		size = static_cast<size_t>(in.base().remaining());
		switch (partialFlag) {
		case FLAG_FULL_BLOCK:
			{
				assert(BLOCK_SIZE == size);
				util::XArray<uint8_t> binary(alloc);
				binary.resize(size);
				binary.assign(size, temp);
				in >> std::make_pair(binary.data(), size);
				if (size != BLOCK_SIZE) {
					GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
							"Invalid block size (blockSize=" << BLOCK_SIZE <<
							", imageSize=" << size << ")");
				}
				assign(binary.data(), size, false);
			}
			break;
		case FLAG_PARTIAL_BLOCK_1:
			{
				util::XArray<uint8_t> binary(alloc);
				binary.resize(size);
				binary.assign(size, temp);
				in >> std::make_pair(binary.data(), size);
				if ((1U << Header::getBlockExpSize(binary.data())) != BLOCK_SIZE) {
					GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
							"Invalid block size (blockSize=" << BLOCK_SIZE <<
							", imageBlockSize=" << (1U << Header::getBlockExpSize(binary.data())) << ")");
				}
				assign(binary.data(), size, true);
			}
			break;
		case FLAG_PARTIAL_BLOCK_2:
			{
				uint32_t firstPartSize;
				in >> firstPartSize;
				assert(firstPartSize <= BLOCK_SIZE);
				util::XArray<uint8_t> binary(alloc);
				binary.resize(firstPartSize);
				binary.assign(firstPartSize, temp);
				in >> std::make_pair(binary.data(), firstPartSize);
				if ((1U << Header::getBlockExpSize(binary.data())) != BLOCK_SIZE) {
					GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
							"Invalid block size (blockSize=" << BLOCK_SIZE <<
							", imageBlockSize=" << (1U << Header::getBlockExpSize(binary.data())) << ")");
				}
				assign(binary.data(), firstPartSize, true);

				uint32_t secondPartSize = static_cast<uint32_t>(in.base().remaining());
				assert(secondPartSize <= BLOCK_SIZE);
				assert((firstPartSize + secondPartSize) <= BLOCK_SIZE);
				if ((firstPartSize + secondPartSize) > BLOCK_SIZE) {
					GS_THROW_USER_ERROR(GS_ERROR_LTS_BLOCK_VALIDATION_FAILED,
							"Block size is too small (blockSize=" << BLOCK_SIZE <<
							", totalImageSize=" << (firstPartSize + secondPartSize) << ")");
				}
				uint8_t *addr = static_cast<uint8_t*>(data()) + (BLOCK_SIZE - secondPartSize);
				in >> std::make_pair(addr, secondPartSize);
			}
			break;
		default:
			assert(false);
			break;
		}
		assert(LocalTempStore::Block::Header::getBlockId(data()) == origBlockId);
		LocalTempStore::Block::Header::validateHeader(
				data(), Header::getBlockExpSize(data()), false);
	}
}



LTSEventBufferManager::LTSEventBufferManager(LocalTempStore &store) :
		store_(store),
		group_(store_) {
}

LTSEventBufferManager::~LTSEventBufferManager() {
}

size_t LTSEventBufferManager::getUnitSize() {
	const size_t blockSize = store_.getDefaultBlockSize();

	assert(blockSize > LocalTempStore::Block::Header::BLOCK_HEADER_SIZE);

	return blockSize - LocalTempStore::Block::Header::BLOCK_HEADER_SIZE;
}

std::pair<LTSEventBufferManager::BufferId, void*>
LTSEventBufferManager::allocate() {
	LocalTempStore::BlockInfo *blockInfo;

	store_.getBufferManager().allocate(blockInfo, group_.getId(), true);
	store_.getBufferManager().addAssignment(blockInfo->getBlockId());

	assert(blockInfo);
	blockInfo->setGroupId(group_.getId());
	store_.incrementActiveBlockCount(group_.getId());

	return std::make_pair(
			BufferId(blockInfo->getBlockId(), 0), getBlockBody(*blockInfo));
}

void LTSEventBufferManager::deallocate(const BufferId &id) {
	store_.getBufferManager().removeAssignment(id.first);
	store_.decrementActiveBlockCount(group_.getId());
}

void* LTSEventBufferManager::latch(const BufferId &id) {
	LocalTempStore::BlockInfo *blockInfo;
	store_.getBufferManager().get(id.first, blockInfo);
	return getBlockBody(*blockInfo);
}

void LTSEventBufferManager::unlatch(const BufferId &id) {
	LocalTempStore::BlockInfo *blockInfo;
	store_.getBufferManager().get(id.first, blockInfo);
	for (size_t i = 0; i < 2; i++) {
		store_.getBufferManager().removeReference(*blockInfo);
	}
}

void* LTSEventBufferManager::getBlockBody(
		LocalTempStore::BlockInfo &blockInfo) {
	return static_cast<uint8_t*>(blockInfo.data()) +
			LocalTempStore::Block::Header::BLOCK_HEADER_SIZE;
}

