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
	@brief Implementation of ObjectManager
*/
#include "gs_error.h"
#include "object_manager_v4.h"
#include "chunk_manager.h"

void ObjectManagerV4::checkDirtyFlag() {}
/*!
	@brief コンストラクタ
*/
ObjectManagerV4::ObjectManagerV4(
		const ConfigTable& configTable, ChunkManager* chunkManager,
		ObjectManagerStats &stats) :
		configTable_(configTable), chunkManager_(chunkManager),
		OBJECT_MAX_CHUNK_ID_(calcMaxChunkId(
				configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE))),
		PARTITION_NUM_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
		CHUNK_EXP_SIZE_(util::nextPowerBitsOf2(
				configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE))),
		maxObjectSize_(
				(1 << (CHUNK_EXP_SIZE_ - 1)) - OBJECT_HEADER_SIZE),
		halfOfMaxObjectSize_(
				(1 << (CHUNK_EXP_SIZE_ - 2)) - OBJECT_HEADER_SIZE),
		recommendLimitObjectSize_(
				(1 << (CHUNK_EXP_SIZE_ - 4)) - OBJECT_HEADER_SIZE),
		LARGE_CHUNK_MODE_(OIdBitUtils::isLargeChunkMode(CHUNK_EXP_SIZE_)),
		UNIT_CHUNK_SHIFT_BIT(calcUnitChunkShiftBit(LARGE_CHUNK_MODE_)),
		MAX_UNIT_OFFSET_BIT(calcMaxUnitOffsetBit(LARGE_CHUNK_MODE_)),
		MASK_UNIT_OFFSET(
				(((static_cast<uint64_t>(1) << MAX_UNIT_OFFSET_BIT) - 1) <<
						UNIT_OFFSET_SHIFT_BIT) | MASK_MAGIC | MASK_USER),
		chunkHeaderSize_(ChunkBuddy::CHUNK_HEADER_FULL_SIZE),
		minPower_(util::nextPowerBitsOf2(sizeof(HeaderBlock))),
		maxPower_(util::nextPowerBitsOf2(1 << CHUNK_EXP_SIZE_)),
		tableSize_(sizeof(uint32_t) * (maxPower_ + 1)),
		freeListOffset_(ChunkBuddy::CHUNK_HEADER_FULL_SIZE - tableSize_),
		maxV4GroupId_(3),
		swapOutCounter_(0),
		stats_(stats) {
	assert(chunkManager_);
	ChunkBuddy::resetParameters(1 << CHUNK_EXP_SIZE_);
	v5StartChunkIdList_.assign(maxV4GroupId_ + 1, 0);
}

ObjectManagerV4::~ObjectManagerV4() {
}

/*!
	@brief 指定groupId, OIdのObjectを解放する
	@note 例外発生時は一度catchすること
*/
void ObjectManagerV4::free(ChunkAccessor &chunkaccessor, DSGroupId groupId, OId oId) {
	try {
		int64_t chunkId = getChunkId(oId);
		if (isV4OId(oId)) {
			chunkId = getV5ChunkId(groupId, chunkId);
		}
		MeshedChunkTable::Group& group = *chunkManager_->putGroup(groupId);
		chunkManager_->getChunk(group, chunkId, chunkaccessor);
		chunkManager_->update(group, chunkaccessor);
		ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkaccessor.getChunk());

		int32_t offset = getOffset(oId);

		uint32_t objectSize = chunk->getObjectSize(offset, OBJECT_HEADER_SIZE);
		chunk->removeObject(offset);

		chunkaccessor.setVacancy(chunk->getMaxFreeExpSize());
		chunkaccessor.unpin(true);
		stats_.table_(ObjectManagerStats::OBJECT_STAT_ALLOCATE_SIZE)
				.subtract(objectSize);
		if (chunk->getOccupiedSize() == 0) {
			chunkManager_->removeChunk(group, chunkId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "groupId," << groupId << ",oId," << oId <<
					  GS_EXCEPTION_MESSAGE(e));

	}
}

/*!
	@brief オブジェクト不正発生時の例外出力
*/
void ObjectManagerV4::errorOId(OId oId, DSGroupId groupId) const {
	const ChunkId chunkId = getChunkId(oId);
	const DSObjectOffset offset = getOffset(oId);
	GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_OBJECT,
			"object is Invalid. " << ",oId," << oId <<
			",groupId," << groupId << ",chunkId," << chunkId <<
			",offset," << offset << ", " << util::StackTraceUtils::getStackTrace);
}

/*!
	@brief Validates RefCounter of fixing and unfixing.
*/
void ObjectManagerV4::validateRefCounter() {
	MeshedChunkTable::CsectionChunkCursor cursor =
			chunkManager_->createCsectionChunkCursor();
	RawChunkInfo chunkInfo;
	while (cursor.hasNext()) {
		cursor.next(chunkInfo);
		if (chunkInfo.getVacancy() < 0) {
			continue;
		}
		if (chunkInfo.getGroupId() != -1) {
			int64_t refCount = chunkManager_->getRefCount(chunkInfo.getGroupId(), chunkInfo.getChunkId());
			assert(refCount == 0);
			if (refCount != 0) {
				GS_THROW_SYSTEM_ERROR(GS_ERROR_OM_INVALID_OID,
						"refCount_ is not zero. "
						<< "groupId," << chunkInfo.getGroupId()
						<< ",chunkId," << chunkInfo.getChunkId()
						<< ", ref = " << refCount);
			}
		}
	}
}

/*!
	@brief パーティションのdrop
*/
void ObjectManagerV4::drop() {
	MeshedChunkTable::CsectionChunkCursor cursor =
			chunkManager_->createCsectionChunkCursor();
	MeshedChunkTable::Group* group = NULL;
	RawChunkInfo chunkInfo;
	while (cursor.hasNext()) {
		cursor.next(chunkInfo);
		if (chunkInfo.getGroupId() != -1) {
			if (!group || group->getId() != chunkInfo.getGroupId()) {
				group = chunkManager_->putGroup(chunkInfo.getGroupId());
			}
			chunkManager_->removeChunk(*group, chunkInfo.getChunkId());
		}
	}
	chunkManager_->fin();
	chunkManager_->reinit();
}

/*!
	@brief V4ChunkIDをV5ChunkIDに変換するテーブルの初期化
	@attention 
*/
void ObjectManagerV4::initializeV5StartChunkIdList(
		int64_t maxV4GroupId, const std::deque<int64_t>& chunkIdList) {
	if (chunkIdList.size() > 0) {
		assert(chunkIdList.size() == maxV4GroupId + 1);
		v5StartChunkIdList_.assign(chunkIdList.begin(), chunkIdList.end());
	}
}

/*!
	@brief 指定されたサイズ以上の空きを持つチャンクを検索する
	@return 指定されたサイズ以上の空きを持つチャンクのメタチャンク
	@retval 非NULL 指定されたサイズの空きをもつチャンクがあった場合
	@retval NULL 指定されたサイズの空きをもつチャンクがなかった場合
	@attention BATCH_FREE_MODEチャンクの場合は一定以上の空きがある場合にかぎる
*/
int64_t ObjectManagerV4::searchChunk(
		MeshedChunkTable::Group& group, uint8_t powerSize, ChunkAccessor &chunkAccessor) {



	const DSGroupId groupId = group.getId();
	auto result = free_.find(groupId);
	if (result == free_.end()) {
		free_[groupId] = UTIL_NEW MeshedChunkTable::Group::StableGroupChunkCursor(group);
	}
	int32_t neededSize = powerSize;
	int64_t chunkId = -1;
	{
		MeshedChunkTable::Group::StableGroupChunkCursor* cursor = free_[groupId];
		RawChunkInfo chunkInfo;
		bool exist = cursor->current(chunkInfo);
		int32_t trial;
		bool nullp = false;
		for (trial = 0; trial < RANGE; trial++) {
			if (!exist) {
				if (nullp) {
					break;
				}
				delete free_[groupId];
				free_[groupId] = UTIL_NEW MeshedChunkTable::Group::StableGroupChunkCursor(group);
				cursor = free_[groupId];
				exist = cursor->current(chunkInfo);
				nullp = true;
				continue;
			}
			int32_t availablespace = chunkInfo.getVacancy();
			if (availablespace >= neededSize) {
				chunkId = chunkInfo.getChunkId();
				chunkManager_->getChunk(group, chunkId, chunkAccessor);
				chunkManager_->update(group, chunkAccessor);
				break;
			}
			exist = cursor->advance(chunkInfo);
		}
		if (chunkId < 0) {
			chunkManager_->allocateChunk(group, chunkAccessor);
			ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkAccessor.getChunk());

			chunk->init();
			chunkId = chunkAccessor.getChunkId();
			initV4ChunkHeader(*chunk, groupId, chunkId);
			cursor->set(chunkId);
		}
	}
	return chunkId;
}

void ObjectManagerV4::resetRefCounter() {
}

void ObjectManagerV4::freeLastLatchPhaseMemory() {
}

/*!
	@brief テーブルスキャン用バッファ制御
	@note 判定に使用するSwapOutのカウンターを設定(通常0、SQL等で部分実行する際は前回のカウンター値を設定)
*/
void ObjectManagerV4::setSwapOutCounter(int64_t counter) {
	chunkManager_->setSwapOutCounter(static_cast<uint64_t>(counter));
	swapOutCounter_ = counter;
}

int64_t ObjectManagerV4::getSwapOutCounter() {
	return chunkManager_->getSwapOutCounter();
}

void ObjectManagerV4::setStoreMemoryAgingSwapRate(double ratio) {
	chunkManager_->setStoreMemoryAgingSwapRate(ratio);
}
double ObjectManagerV4::getStoreMemoryAgingSwapRate() {
	return chunkManager_->getStoreMemoryAgingSwapRate();
}


/*!
	@brief 新規オブジェクト割り当て
*/
OId GroupObjectAccessor::allocate(DSObjectSize requestSize, ObjectType objectType) {

	uint32_t powerSize = objMgr_->getObjectExpSize(requestSize);
	int64_t chunkId = objMgr_->searchChunk(*group_, powerSize, chunkAccessor_);

	ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkAccessor_.getChunk());
	int32_t offset = chunk->allocateObject(nullptr, powerSize, objectType);

	chunkAccessor_.setVacancy(chunk->getMaxFreeExpSize());
	chunkAccessor_.unpin(true);
	objMgr_->stats_.table_(ObjectManagerStats::OBJECT_STAT_ALLOCATE_SIZE).add(
			(UINT32_C(1) << powerSize) - ObjectManagerV4::OBJECT_HEADER_SIZE);

	OId oId = objMgr_->getOId(group_->getId(), chunkId, offset);
	return oId;
}

/*!
	@brief 可能な限り指定オブジェクトと同一チャンクに新規オブジェクト割り当て
*/
OId GroupObjectAccessor::allocateNeighbor(
		DSObjectSize requestSize, OId neighborOId, ObjectType objectType) {

	uint32_t powerSize = objMgr_->getObjectExpSize(requestSize);
	int64_t chunkId = objMgr_->getChunkId(neighborOId);
	if (objMgr_->isV4OId(neighborOId)) {
		chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
	}
	int32_t availablespace = group_->getVacancy(chunkId);
	if (availablespace < powerSize) {
		return allocate(requestSize, objectType);
	}
	objMgr_->chunkManager_->getChunk(*group_, chunkId, chunkAccessor_);
	objMgr_->chunkManager_->update(*group_, chunkAccessor_);
	ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkAccessor_.getChunk());
	int32_t offset = chunk->allocateObject(nullptr, powerSize, objectType);

	chunkAccessor_.setVacancy(chunk->getMaxFreeExpSize());
	chunkAccessor_.unpin(true);
	objMgr_->stats_.table_(ObjectManagerStats::OBJECT_STAT_ALLOCATE_SIZE).add(
			(UINT32_C(1) << powerSize) - ObjectManagerV4::OBJECT_HEADER_SIZE);

	OId oId = objMgr_->getOId(group_->getId(), chunkId, offset);
	return oId;
}

/*!
	@brief 指定groupId, OIdのObjectを解放する
	@note 例外発生時は一度catchすること
*/
void GroupObjectAccessor::free(OId oId) {
	try {
		int64_t chunkId = objMgr_->getChunkId(oId);
		if (objMgr_->isV4OId(oId)) {
			chunkId = objMgr_->getV5ChunkId(group_->getId(), chunkId);
		}
		objMgr_->chunkManager_->getChunk(*group_, chunkId, chunkAccessor_);
		objMgr_->chunkManager_->update(*group_, chunkAccessor_);
		ChunkBuddy* chunk = reinterpret_cast<ChunkBuddy*>(chunkAccessor_.getChunk());

		int32_t offset = objMgr_->getOffset(oId);

		uint32_t objectSize = chunk->getObjectSize(offset, ObjectManagerV4::OBJECT_HEADER_SIZE);
		chunk->removeObject(offset);

		chunkAccessor_.setVacancy(chunk->getMaxFreeExpSize());
		chunkAccessor_.unpin(true);
		objMgr_->stats_.table_(ObjectManagerStats::OBJECT_STAT_ALLOCATE_SIZE)
				.subtract(objectSize);
		if (chunk->getOccupiedSize() == 0) {
			objMgr_->chunkManager_->removeChunk(*group_, chunkId);
		}
	}
	catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, "groupId," << group_->getId() << ",oId," << oId <<
					  GS_EXCEPTION_MESSAGE(e));

	}
}

ObjectManagerStats::ObjectManagerStats(const Mapper *mapper) :
		table_(mapper) {
}

void ObjectManagerStats::setUpMapper(Mapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);
	mapper.addFilter(STAT_TABLE_DISPLAY_OPTIONAL_DS);

	mapper.bind(OBJECT_STAT_ALLOCATE_SIZE, STAT_TABLE_PERF_DS_ALLOCATE_DATA);
}
