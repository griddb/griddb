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
	@brief Implementation of ChunkManager
*/
#include "chunk_manager.h"
#include "chunk_buffer.h"
#include "partition_file.h"
#include "log_manager.h"
#include "affinity_manager.h"
#include "util/allocator.h"
#include "util/file.h"
#include "util/trace.h"
#include "gs_error.h"
#include <numeric>
#include <iostream>
#include <iomanip>
#include <algorithm>
#include <memory>
#include <numeric>

UTIL_TRACER_DECLARE(CHUNK_MANAGER);  
UTIL_TRACER_DECLARE(SIZE_MONITOR);
UTIL_TRACER_DECLARE(IO_MONITOR);

const uint32_t ChunkManager::LIMIT_CHUNK_NUM_LIST[12] = {
	64 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024,
	128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024,
	128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024,
	128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024 };

std::string RawChunkInfo::toString() const {
	util::NormalOStringStream oss;
	oss << "<" << "RawChunkInfo" << ",C" << chunkId_ <<
	  ",G" << groupId_  <<",O" <<  offset_ << ",V" << (int32_t)vacancy_ << ",L" << logversion_;
	return oss.str();
}


DLTable::DLTable() {
}

std::string DLTable::toString() const {
	util::NormalOStringStream oss;
	for (const auto& e : table_) {
		oss << e.toString() << ",";
	}
	return oss.str();
}



ChunkExtent::ChunkExtent(int16_t extentsize) :
extentSize_(extentsize), use_(false), groupId_(-1), baseLogVersion_(0),
offsets_(NULL), free_(0), numUsed_(0)
{
	deltaLogVersions_.assign(extentSize_, -1);
	vacancies_.assign(extentSize_, -1);
	flags_.assign(extentSize_, 0);
	offsets_ = UTIL_NEW MimicLongArray(extentSize_);
	for (int16_t pos = 0; pos < extentSize_; pos++) {
		offsets_->set(pos, -1);
	}
	initFreeChain();
}

ChunkExtent::~ChunkExtent() {
	delete offsets_;
}

void ChunkExtent::setLogVersion(int16_t targetpos, int64_t logversion) {
	if ((logversion - baseLogVersion_) >= INT8_MAX) {
		for (int16_t pos = 0; pos < extentSize_; pos++) {
			if ((logversion - (baseLogVersion_ + deltaLogVersions_[pos])) >= 2) {
				deltaLogVersions_[pos] = (int8_t)0;
			} else if ((logversion - (baseLogVersion_ + deltaLogVersions_[pos])) == 1) {
				deltaLogVersions_[pos] = (int8_t)1;
			} else {
				deltaLogVersions_[pos] = (int8_t)2;
			}
		}
		baseLogVersion_ = logversion - 2;
	}
	deltaLogVersions_[targetpos] = (int8_t)(logversion - baseLogVersion_);
}

std::string ChunkExtent::toString() const {
	util::NormalOStringStream oss;
	oss << "<" << "ChunkExtent" << ",G" << groupId_ << ",BL" << baseLogVersion_;
	for (int16_t pos = 0; pos < extentSize_; pos++) {
		oss << "<Chunk,O" << (isUsed(pos) ? getOffset(pos) : getNext(pos)) <<
				",V" << (int32_t)getVacancy(pos) << ",DL" << getLogVersion(pos) <<
				",N" << (isUsed(pos) ? getOffset(pos) : getNext(pos)) <<
				",F" << (int32_t)getFlags(pos) << ">,";
	}
	oss << ">";
	return oss.str();
}


ChunkGroupStats::ChunkGroupStats(const Mapper *mapper) :
		table_(mapper) {
}

MeshedChunkTable::MeshedChunkTable()
: extentPower_(0), extentPosMask_(0), extentSize_(0),
  chainTable_(NULL), freeTable_(NULL), chain0_(NULL)
{
	init(DEFAULT_EXTENT_SIZE);
}
MeshedChunkTable::MeshedChunkTable(int16_t extentsize)
: extentPower_(0), extentPosMask_(0), extentSize_(extentsize),
  chainTable_(NULL), freeTable_(NULL), chain0_(NULL)
{
	init(extentsize);
}

MeshedChunkTable::~MeshedChunkTable() {
	delete chain0_;
	for (auto& itr : groupMap_) {
		if (itr.second) {
			delete itr.second;
			itr.second = NULL;
		}
	}
	delete freeTable_;
	delete chainTable_;
	for (auto& itr : extents_) {
		delete itr;
	}
}

void MeshedChunkTable::init(int16_t extentsize) {
	extentPower_ = util::nextPowerBitsOf2(extentsize);
	extentSize_ = INT16_C(1) << extentPower_;
	extentPosMask_ = extentSize_ - 1;


	chainTable_ = UTIL_NEW DLTable();
	freeTable_ = UTIL_NEW DLTable();

	chain0_ = chainTable_->createChain();

}

void MeshedChunkTable::redo(int64_t chunkId, DSGroupId groupId, int64_t offset, int8_t vacancy) {
	int32_t targetindex = getIndex(chunkId);
	if (offset >= 0) {
		assert(vacancy >= 0);
		{
			for (int32_t index = extents_.size(); index <= targetindex; index++) {
				extents_.push_back(UTIL_NEW ChunkExtent(extentSize_));
				chainTable_->resize(extents_.size());
				freeTable_->resize(extents_.size());
			}
		}
		{
			ChunkExtent* extent = extents_[targetindex];
			extent->use(groupId);
			int16_t pos = getPos(chunkId);
			extent->put0(pos, offset, vacancy);
		}
	} else {
		assert(vacancy < 0);
		if (targetindex < extents_.size()) {
			ChunkExtent* extent = extents_[targetindex];
			int16_t pos = getPos(chunkId);
			extent->free0(pos);
		}
	}
}

int64_t MeshedChunkTable::reinit() {
	int64_t blockCount = 0;
	for (int32_t index = 0; index < extents_.size(); index++) {
		ChunkExtent& extent = *extents_[index];
		extent.initFreeChain();
		if (extent.getNumUsed() == extent.getExtentSize()) {
			DSGroupId groupId = extent.getGroupId();
			Group* group = putGroup(groupId);
			group->addExtentChain(index);
			extent.use(groupId);
			group->stats()(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT)
					.add(extent.getNumUsed());
			blockCount += extent.getNumUsed();
		} else if (extent.getNumUsed() > 0) {
			DSGroupId groupId = extent.getGroupId();
			Group* group = putGroup(groupId);
			group->addExtentChain(index);
			group->addExtentFree(index);
			extent.use(groupId);
			group->stats()(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT)
					.add(extent.getNumUsed());
			blockCount += extent.getNumUsed();
		} else {
			chain0_->add(index);
			extent.unuse();
		}
	}
	return blockCount;
}

int64_t MeshedChunkTable::getFreeSpace(std::deque<int64_t>& frees) {
	UtilBitmap bitmap(true);
	StableChunkCursor chunkcursor = createStableChunkCursor();
	RawChunkInfo chunkinfo;
	while (chunkcursor.next(chunkinfo)) {
		bitmap.set(chunkinfo.getOffset(), false);
	}
	for (int64_t i = 0; i < (bitmap.length() - 1); i++) {
		if (bitmap.get(i)) {
			frees.push_back(i);
		}
	}
	return bitmap.length();
}

int32_t MeshedChunkTable::allocateExtent(DSGroupId groupId) {
	size_t index;
	if (chain0_->peek() < 0) {
		index = extents_.size();
		extents_.push_back(UTIL_NEW ChunkExtent(extentSize_));
		chainTable_->resize(extents_.size());
		freeTable_->resize(extents_.size());
	} else {
		index = chain0_->peek();
		chain0_->remove(index);
	}
	extents_[index]->initFreeChain();
	extents_[index]->use(groupId);
	return index;
}

bool MeshedChunkTable::tryAllocateExtent(DSGroupId groupId, int32_t index) {
	if (index >= extents_.size()) {
		for (int32_t i = extents_.size(); i <= index; i++) {
			extents_.push_back(UTIL_NEW ChunkExtent(extentSize_));
			chainTable_->resize(extents_.size());
			freeTable_->resize(extents_.size());
			chain0_->add(index);
		}
	}
	if (extents_[index]->isUsed()) {
		assert(groupId == extents_[index]->getGroupId());
		assert(extents_[index]->isUsed());
		return false;
	}
	chain0_->remove(index);

	extents_[index]->initFreeChain();
	extents_[index]->use(groupId);
	return true;
}

void MeshedChunkTable::resetIncrementalDirtyFlags(bool resetAll) {
	for (int32_t index = 0; index < extents_.size(); index++) {
		ChunkExtent* extent = extents_[index];
		const int16_t extentSize = extent->getExtentSize();
		if (extent && extent->isUsed()) {
			for (int16_t pos = 0; pos < extentSize; ++pos) {
				extent->setDifferentialDirty(pos, false);
				if (resetAll) {
					extent->setCumulativeDirty(pos, false);
				}
			}
		}
	}
}

std::string MeshedChunkTable::toString()  {
	util::NormalOStringStream oss;
	std::vector<RawChunkInfo> chunkinfos(dump());
	uint64_t tablesignature = 0;
	util::NormalOStringStream oss2;
	for (const auto& chunkinfo : chunkinfos) {
		tablesignature += chunkinfo.getSignature();
		oss2 << chunkinfo.toString() << ";";
	}
	oss << "MeshedChunkTable,#" << tablesignature << "," << oss2.str().c_str() << ",";
	return oss.str();
}

std::vector<RawChunkInfo> MeshedChunkTable::dump() {
	std::vector<RawChunkInfo> chunkinfos;
	for (const auto& itr : groupMap_) {
		MeshedChunkTable::Group& group = *(groupMap_[itr.first]);
		group.dump(chunkinfos);
	}
	std::sort(chunkinfos.begin(), chunkinfos.end(),
		[](const RawChunkInfo& lhs, const RawChunkInfo& rhs) {
			return lhs.getChunkId() < rhs.getChunkId();
		});
	return chunkinfos;
}

void MeshedChunkTable::extentChecker() {
	for (auto& itr : groupMap_) {
		Group& group = *groupMap_[itr.first];
		group.extentChecker();
	}
}


MeshedChunkTable::Group::Group(DSGroupId groupId, MeshedChunkTable& mct) :
		groupId_(groupId), mct_(mct), stats_(NULL) {

	chain_ = mct_.chainTable_->createChain();
	free_ = mct_.freeTable_->createChain();
}

MeshedChunkTable::Group::~Group() {
	delete free_;
	delete chain_;
}

void MeshedChunkTable::Group::allocate(RawChunkInfo &rawInfo) {
	int32_t index = free_->peek();
	if (index < 0) {
		index = mct_.allocateExtent(groupId_);
		chain_->add(index);
		free_->add(index);
	}

	ChunkExtent& extent = *(mct_.extents_[index]);
	int16_t pos = extent.allocate();
	assert(pos >= 0);
	stats()(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT).increment();

	if (extent.getNumUsed() == mct_.extentSize_) {
		assert(extent.peek() < 0);
		free_->remove(index);
	}
	int64_t chunkId = mct_.getChunkId(index, pos);

	rawInfo.setChunkId(chunkId)
			.setGroupId(groupId_)
			.setOffset(-1)
			.setVacancy((int8_t)0)
			.setCumulativeDirty(true)
			.setDifferentialDirty(true)
			.setLogVersion(extent.getLogVersion(pos));
}

bool MeshedChunkTable::Group::isAllocated(int64_t chunkId) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	if (index < mct_.extents_.size()) {
		ChunkExtent& extent = *(mct_.extents_[index]);
		if (extent.isUsed()) {
			assert(extent.getGroupId() == groupId_);
		}
		return extent.isUsed(pos);
	}
	return false;
}

void MeshedChunkTable::Group::tryAllocate(int64_t chunkId, RawChunkInfo &rawInfo) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);

	if (mct_.tryAllocateExtent(groupId_, index)) {
		chain_->add(index);
		free_->add(index);
	}
	ChunkExtent& extent = *mct_.extents_[index];
	extent.tryAllocate(pos);
	assert(extent.isUsed());
	stats()(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT).increment();

	if (extent.getNumUsed() == mct_.extentSize_) {
		assert(extent.peek() < 0);
		free_->remove(index);
	}

	rawInfo.setChunkId(chunkId)
		.setGroupId(groupId_)
		.setOffset(-1)
		.setVacancy((int8_t)0)
		.setCumulativeDirty(true)
		.setDifferentialDirty(true)
		.setLogVersion(extent.getLogVersion(pos));
}

void MeshedChunkTable::Group::remove(int64_t chunkId, int64_t logversion) {
	int32_t index = mct_.getIndex(chunkId);
	int16_t pos = mct_.getPos(chunkId);
	ChunkExtent& extent = *mct_.extents_[index];
	extent.free(pos);
	stats()(ChunkGroupStats::GROUP_STAT_USE_BLOCK_COUNT).decrement();
	if (extent.getNumUsed() == (mct_.extentSize_ - 1)) {
		free_->add(index);
	}
	if (extent.getNumUsed() <= 0) {
		chain_->remove(index);
		free_->remove(index);
		mct_.chain0_->add(index);
		extent.initFreeChain().unuse();
	}
	extent.setLogVersion(pos, logversion);
}

void MeshedChunkTable::Group::dump(std::vector<RawChunkInfo>& chunkinfos) {
	StableGroupChunkCursor cursor(*this);
	RawChunkInfo chunkinfo;
	while (cursor.next(chunkinfo)) {
		chunkinfos.push_back(std::move(chunkinfo));
	}
}

std::string MeshedChunkTable::Group::toString() {
	std::vector<RawChunkInfo> chunkinfos;
	dump(chunkinfos);
	util::NormalOStringStream oss("[");
	for (const auto& chunkinfo : chunkinfos) {
		oss << chunkinfo.toString() << ",";
	}
	oss << "]";
	return oss.str();
}
void MeshedChunkTable::Group::extentChecker() {
	int32_t index;
	for (index = 0; index < mct_.extents_.size(); index++) {
		ChunkExtent& extent = *mct_.extents_[index];
		std::cout << index <<  " ***** " << extent.toString() <<
				" " << extent.getNumUsed() << " " << extent.getExtentSize() <<
				std::endl;
	}
	index = free_->peek();
	std::cout << index << " " << mct_.freeTable_->toString() << std::endl;
	while (index >= 0) {
		ChunkExtent& extent = *mct_.extents_[index];
		std::cout << index << " NOT FULL " << extent.toString() <<
		  " " << extent.getNumUsed() << " " << extent.getExtentSize() << std::endl;
		assert(extent.getNumUsed() < extent.getExtentSize());

		index = free_->next(index);
	}
	index = chain_->peek();
	std::cout << index << " " << mct_.chainTable_->toString() << std::endl;
	while (index >= 0) {
		ChunkExtent& extent = *mct_.extents_[index];
		std::cout << index << " BELONG " << extent.toString() <<
		  " " << extent.getNumUsed() << " " << extent.getExtentSize() << std::endl;

		assert(extent.getNumUsed() >= 0);
		index = chain_->next(index);
	}
	index = mct_.chain0_->peek();
	while (index >= 0) {
		ChunkExtent& extent = *mct_.extents_[index];
		std::cout << index << " EMPTY " << extent.toString() <<
		  " " << extent.getNumUsed() << " " << extent.getExtentSize() << std::endl;

		assert(extent.getNumUsed() == 0);
		index = mct_.chain0_->next(index);
	}
}


void MeshedChunkTable::Group::StableGroupChunkCursor::setRawInfo(ChunkExtent* extent, int16_t pos, RawChunkInfo& rawInfo) {
	rawInfo
		.setChunkId(mct_.getChunkId(index_, pos))
		.setGroupId(group_.groupId_)
		.setOffset(extent->getOffset(pos))
		.setVacancy(extent->getVacancy(pos))
		.setCumulativeDirty(extent->isCumulativeDirty(pos))
		.setDifferentialDirty(extent->isDifferentialDirty(pos))
		.setLogVersion(extent->getLogVersion(pos));
}
std::string MeshedChunkTable::Group::StableGroupChunkCursor::toString() const {
	util::NormalOStringStream oss;
	oss << "<" << "StableGroupChunkCursor" << ",G" << group_.getId() <<
	  ",I" << index_  <<",P" << pos_ << ",C" << counter_;
	return oss.str();
}
bool MeshedChunkTable::Group::StableGroupChunkCursor::next(bool advance, RawChunkInfo& rawInfo) {
	if (index_ >= 0) {
		ChunkExtent* extent = mct_.extents_[index_];
		assert(extent);
		if (extent->getGroupId() == group_.groupId_) {
			if (pos_ < extent->getExtentSize()) {
				int16_t pos = pos_;
				if (advance) {
					pos_++;
				}
				if (extent->isUsed(pos)) {
					setRawInfo(extent, pos, rawInfo);
					return true;
				} else {
					pos_ = 0;
					while((index_ = group_.chain_->next(index_)) >= 0) {
						extent = mct_.extents_[index_];
						if (extent->getGroupId() == group_.groupId_ && extent->isUsed(pos_)) {
							setRawInfo(extent, pos_, rawInfo);
							return true;
						}
					}
				}
			} else {
				pos_ = 0;
				while((index_ = group_.chain_->next(index_)) >= 0) {
					extent = mct_.extents_[index_];
					if (extent->getGroupId() == group_.groupId_ && extent->isUsed(pos_)) {
						setRawInfo(extent, pos_, rawInfo);
						return true;
					}
				}
			}
		} else {
			counter_++;
			if (counter_ % 2 == 0) {
				index_ = group_.chain_->peek();
				pos_ = 0;
			} else {
				for (; index_ < mct_.extents_.size(); index_++) {
					extent = mct_.extents_[index_];
					assert(extent);
					if (extent->getGroupId() == group_.groupId_) {
						pos_ = 0;
						break;
					}
				}
				if (index_ >= mct_.extents_.size()) {
					return false;
				}
			}
		}
	} else {
		index_ = group_.chain_->peek();
		pos_ = 0;
		counter_ = 0;
	}
	return false;
}


void ChunkAccessor::unpin() {
	const bool dirty = false;
	unpin(dirty);
}

void ChunkAccessor::unpin(bool dirty) {
	chunkBuffer_->unpinChunk(frameRef_, dirty);
	forceReset();
}

int64_t ChunkAccessor::getPinCount(int64_t offset) {
	return chunkBuffer_->getPinCount(pId_, offset);
}

inline void ChunkAccessor::set(
		MeshedChunkTable *chunkTable, ChunkBuffer *chunkBuffer,
		PartitionId pId, DSGroupId groupId, int64_t chunkId,
		const ChunkBufferFrameRef &frameRef) {
	assert(isEmpty());
	Chunk *chunk = ChunkBuffer::getPinnedChunk(frameRef);
	assign(chunkTable, chunkBuffer, pId, groupId, chunkId, chunk, frameRef);
	assert(!isEmpty());
}

FreeSpace::FreeSpace(Locker& locker, ChunkManagerStats& chunkManagerStats) :
	maxoffset_(-1), locker_(locker), chunkManagerStats_(chunkManagerStats) {
	;
}

std::string FreeSpace::toString() const {
	util::NormalOStringStream oss;
	oss << "FreeSpace" << "," << frees_.size();
	for (int64_t offset : frees_){
		oss << "<O" << offset << ">";
	}
	oss << "<MO" << maxoffset_ << ">";
	return oss.str();
}



ChunkManagerStats::ChunkManagerStats(const Mapper *mapper) :
		table_(mapper),
		timeTable_(table_) {
}

void ChunkManagerStats::setUpMapper(Mapper &mapper, uint32_t chunkSize) {
	{
		Mapper sub(mapper.getSource());
		setUpBasicMapper(sub, chunkSize);
		mapper.addSub(sub);
	}

	{
		Mapper sub(mapper.getSource());
		setUpCPMapper(sub);
		mapper.addSub(sub);
	}
}

void ChunkManagerStats::setUpBasicMapper(Mapper &mapper, uint32_t chunkSize) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_PERF);

	mapper.bind(CHUNK_STAT_USE_CHUNK_COUNT, STAT_TABLE_PERF_DS_STORE_TOTAL_USE)
			.setConversionUnit(chunkSize, false);


	const ConfigTable::ValueUnit timeUnit = ConfigTable::VALUE_UNIT_DURATION_MS;

	mapper.bind(
			CHUNK_STAT_CP_FILE_FLUSH_COUNT,
			STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_COUNT);
	mapper.bind(
			CHUNK_STAT_CP_FILE_FLUSH_TIME,
			STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_TIME)
			.setTimeUnit(timeUnit);
}

void ChunkManagerStats::setUpCPMapper(Mapper &mapper) {
	mapper.addFilter(STAT_TABLE_DISPLAY_SELECT_CP);

	{
		const double defaultRate = 1.0;
		mapper.bind(
				CHUNK_STAT_CP_FILE_USE_COUNT,
				STAT_TABLE_PERF_DATA_FILE_USAGE_RATE)
				.setValueType(ParamValue::PARAM_TYPE_DOUBLE)
				.setDefaultValue(ParamValue(defaultRate));
		mapper.setFraction(
				CHUNK_STAT_CP_FILE_USE_COUNT, CHUNK_STAT_CP_FILE_SIZE);
	}
}


ChunkManager::ChunkManager(
		ConfigTable& configTable,
		LogManager<MutexLocker> &logmanager, ChunkBuffer &chunkBuffer,
		AffinityManager &affinitymanager, int32_t chunkSize, PartitionId pId,
		ChunkManagerStats &stats) :
		configTable_(configTable),
		defaultLogManager_(logmanager), chunkBuffer_(chunkBuffer),
		affinityManager_(affinitymanager), chunkSize_(chunkSize),
		chunkTable_(NULL), freespaceLocker_(NULL), freespace_(NULL),
		logversion_(-1), status_(0), pId_(pId),
		config_(
				configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY),
				configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM),
				CHUNK_CATEGORY_NUM,
				configTable.get<int32_t>(CONFIG_TABLE_DS_STORE_BLOCK_SIZE),
				configTable.get<int32_t>(CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE)
				, configTable.getUInt32(CONFIG_TABLE_DS_STORE_MEMORY_LIMIT)
				, configTable
				, configTable.get<double>(
						CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE)
				, configTable.getUInt64(CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE)
				, configTable.get<bool>(CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE)
				, configTable.get<double>(
						CONFIG_TABLE_DS_STORE_BUFFER_TABLE_SIZE_RATE)   
				, configTable.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT)
				, configTable.getUInt32(CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE)
				),
		chunkManagerStats_(stats), blockCount_(0),
		MAX_CHUNK_ID(calcMaxChunkId(
				configTable.getUInt32(CONFIG_TABLE_DS_STORE_BLOCK_SIZE)))
{
	chunkBuffer_.use();
	affinityManager_.use();

	uint32_t blockExtentExpSize = util::nextPowerBitsOf2(configTable.get<int32_t>(
		CONFIG_TABLE_DS_STORE_BLOCK_EXTENT_SIZE));
	int16_t blockExtentSize = static_cast<int16_t>(INT16_C(1) << blockExtentExpSize);

	chunkTable_ = UTIL_NEW MeshedChunkTable(blockExtentSize);
	freespaceLocker_ = UTIL_NEW NoLocker();
	freespace_ = UTIL_NEW FreeSpace(*freespaceLocker_, chunkManagerStats_);
}

ChunkManager::~ChunkManager() {
	delete freespace_;
	freespace_ = NULL;
	delete freespaceLocker_;
	freespaceLocker_ = NULL;
	delete chunkTable_;
	chunkTable_ = NULL;
}

void ChunkManager::redo(const Log* log) {
	int32_t op = log->getType();
	switch (op) {
	case LogType::CPLog:
	case LogType::CPULog:
		chunkTable_->redo(log->getChunkId(), log->getGroupId(), log->getOffset(), log->getVacancy());
		break;
	case LogType::PutChunkStartLog:
		break;
	case LogType::PutChunkDataLog:
	 	restoreChunk(log->getChunkId(), log->getGroupId(), log->getOffset(),
					 log->getVacancy(), log->getData(), log->getDataSize());
		break;
	case LogType::PutChunkEndLog:
		break;
	default:
		assert(false);
		break;
	}
}

void ChunkManager::reinit() {
	blockCount_ = chunkTable_->reinit();
	stats().table_(ChunkManagerStats::CHUNK_STAT_USE_CHUNK_COUNT)
			.set(blockCount_);
	std::deque<int64_t> frees;
	int64_t maxoffset = chunkTable_->getFreeSpace(frees);
	freespace_->init(frees, maxoffset);
	status_ = 1;
}

void ChunkManager::startCheckpoint(int64_t logversion) {
	setLogVersion(logversion);
}

void ChunkManager::freeSpace(const Log* log) {
	try {
		if (log->getType() == LogType::CPULog && log->getOffset() >= 0) {
			chunkBuffer_.purgeChunk(pId_, log->getOffset(), true);
			freespace_->free(log->getOffset());
		}
	} catch (std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "freeSpace, logOffset, " << log->getOffset()));
	}
}

void ChunkManager::appendUndo(
		int64_t chunkId, DSGroupId groupId, int64_t offset, int8_t vacancy) {
	flushBuffer(offset);
	Log log(LogType::CPULog, defaultLogManager_.getLogFormatVersion());
	log.setChunkId(chunkId)
			.setGroupId(groupId)
			.setOffset(offset)
			.setVacancy(vacancy)
			.setLsn(defaultLogManager_.getLSN())
			.setCheckSum();

	defaultLogManager_.appendCpLog(log);

}

void ChunkManager::flushBuffer(int64_t offset) {
	try {
		chunkBuffer_.flushChunk(pId_, offset);
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "flushBuffer, offset, " << offset));
	}
}

void ChunkManager::dumpBuffer(int64_t offset) {
	chunkBuffer_.dumpChunk(pId_, offset);
}


void ChunkManager::allocateChunk(MeshedChunkTable::Group &group, ChunkAccessor &ca) {
	const DSGroupId groupId = group.getId();
	try {
		RawChunkInfo chunkinfo;
		group.allocate(chunkinfo);
		if (static_cast<int64_t>(MAX_CHUNK_ID) < chunkinfo.getChunkId()) {
			GS_THROW_SYSTEM_ERROR(GS_ERROR_CHM_CID_LIMIT_OVER,
					"pId," << pId_ << ",chunkId," << chunkinfo.getChunkId());
		}
		int64_t offset = freespace_->allocate();
		++blockCount_;
		if (chunkinfo.getLogVersion() < logversion_) {
			appendUndo(chunkinfo.getChunkId(), groupId, -1, (int8_t)-1);
		}
		group.set(chunkinfo.getChunkId(), offset, logversion_, true);
		ChunkBufferFrameRef frameRef;
		chunkBuffer_.pinChunk(pId_, offset, true, true, groupId, frameRef);
		ca.set(
				chunkTable_, &chunkBuffer_, pId_, groupId, chunkinfo.getChunkId(),
				frameRef);
		stats().table_(ChunkManagerStats::CHUNK_STAT_USE_CHUNK_COUNT)
				.increment();
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "allocateChunk, groupId, " << groupId));
	}
}


void ChunkManager::update(MeshedChunkTable::Group &group, ChunkAccessor &ca) {
	const DSGroupId groupId = group.getId();
	const int64_t chunkId = ca.getChunkId();
	assert(groupId > -1 && chunkId > -1);
	try {
		RawChunkInfo chunkinfo = group.get(chunkId);
		group.setDirtyFlags(chunkId, true);
		int64_t offset1 = chunkinfo.getOffset();
		if (chunkinfo.getLogVersion() < logversion_) {
			const int64_t offset2 = freespace_->allocate();
			group.set(chunkId, offset2, logversion_, true);

			ChunkBufferFrameRef frameRef1 = ca.getFrameRef();
			Chunk* chunkPtr1 = ca.getChunk();

			ChunkBufferFrameRef frameRef2;
			chunkBuffer_.pinChunk(pId_, offset2, true, true, groupId, frameRef2);
			Chunk* chunkPtr2 = ChunkBuffer::getPinnedChunk(frameRef2);

			chunkPtr2->copy(*chunkPtr1);

			assert(chunkBuffer_.isDirty(frameRef2));
			chunkBuffer_.swapOffset(pId_, offset1, offset2);
			assert(chunkBuffer_.isDirty(frameRef1));

			chunkBuffer_.purgeChunk(pId_, offset1, false);
			appendUndo(chunkId, groupId, offset1, chunkinfo.getVacancy());

			offset1 = offset2;
		}
		else {
			chunkBuffer_.setDirty(ca.getFrameRef());
		}

	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(
			e, GS_EXCEPTION_MERGE_MESSAGE(e, "updateChunk, groupId," << groupId << ",chunkId," << chunkId));
	}
}

int64_t ChunkManager::getRefCount(DSGroupId groupId, int64_t chunkId) {
	try {
		MeshedChunkTable::Group &group = *chunkTable_->putGroup(groupId);
		RawChunkInfo chunkinfo = group.get(chunkId);
		int64_t offset = chunkinfo.getOffset();
		if (offset >= 0) {
			return chunkBuffer_.getPinCount(pId_, offset);
		}
		else {
			return 0;
		}
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
				GS_EXCEPTION_MERGE_MESSAGE(e, "getRefCount, groupId," << groupId << ",chunkId," << chunkId));
	}
}

void ChunkManager::removeChunk(MeshedChunkTable::Group &group, int64_t chunkId) {
	try {
		if (chunkId < 0) {
			return;
		}
		RawChunkInfo chunkinfo = group.get(chunkId);
		int64_t offset0 = chunkinfo.getOffset();

		if (chunkinfo.getLogVersion() >= logversion_) {
			freespace_->free(offset0);
		} else {
			appendUndo(chunkId, group.getId(), offset0, chunkinfo.getVacancy());
		}
		group.remove(chunkId, logversion_);
		chunkBuffer_.purgeChunk(pId_, offset0, false);
		const ChunkCategoryId categoryId = getChunkCategoryId(group.getId());
		stats().table_(ChunkManagerStats::CHUNK_STAT_USE_CHUNK_COUNT)
				.decrement();
		--blockCount_;
	}
	catch(std::exception &e) {
		GS_RETHROW_SYSTEM_ERROR(e,
				GS_EXCEPTION_MERGE_MESSAGE(e, "removeChunk, groupId," << group.getId() << ",chunkId," << chunkId));
	}
}

bool ChunkManager::removeGroup(DSGroupId groupId) {
	MeshedChunkTable::Group &group = *chunkTable_->putGroup(groupId);
	MeshedChunkTable::Group::StableGroupChunkCursor chunkcursor(group);
	RawChunkInfo chunkinfo;
	bool removed = false;
	while (chunkcursor.next(chunkinfo)) {
		removeChunk(group, chunkinfo.getChunkId());
		removed = true;
	}
	chunkTable_->removeGroupFromMap(groupId);
	return removed;
}

void ChunkManager::fin() {
	affinityManager_.unuse();
	status_ = 0;
}

int64_t ChunkManager::copyChunk(
		ChunkCopyContext* cxt, int64_t offset, bool keepSrcOffset) {
	int64_t destOffset = chunkBuffer_.copyChunk(cxt, pId_, offset, keepSrcOffset);
	if (cxt->logManager_) {
		Chunk* chunk0 = cxt->chunk_;
		ChunkBuddy chunk(*chunk0);

		DSGroupId groupId = chunk.getGroupId();
		ChunkId chunkId = chunk.getChunkId();
		uint8_t vacancy = chunk.getMaxFreeExpSize();

		Log log(LogType::PutChunkDataLog, cxt->logManager_->getLogFormatVersion());
		log.setChunkId(chunkId)
			.setGroupId(groupId)
			.setOffset(offset)
			.setVacancy(vacancy)
			.setLsn(defaultLogManager_.getLSN())
			.setData(chunk.getData())
			.setDataSize(chunk.getDataSize())
			.setCheckSum();

		cxt->logManager_->appendCpLog(log);


	}
	return destOffset;
}

void ChunkManager::resetIncrementalDirtyFlags(bool resetAll) {
	chunkTable_->resetIncrementalDirtyFlags(resetAll);
}

std::string ChunkManager::toString() const {
	return chunkTable_->toString() + freespace_->toString();
}

std::string ChunkManager::getSignature() {
	int64_t chunkSig = 0;
	int64_t chunkCount = 0;
	int64_t chunkId = 0;
	util::NormalOStringStream chunkSig0;
	const int32_t HEADER_SIZE = Chunk::getHeaderSize();
	const int32_t bodySize = getChunkSize() - HEADER_SIZE;
	try {
		MeshedChunkTable::StableChunkCursor cursor = createStableChunkCursor();
		RawChunkInfo chunkInfo;
		ChunkAccessor ca;
		while (cursor.next(chunkInfo)) {
			if (chunkInfo.getVacancy() < 0) {
				continue;
			}
			getChunk(chunkInfo.getGroupId(), chunkInfo.getChunkId(), ca);
			Chunk* chunk = ca.getChunk();
			chunkId = ca.getChunkId();
			uint64_t sig = UtilBytes::getSignature(
					chunk->getData() + HEADER_SIZE, bodySize);
			ca.unpin();
			chunkSig0 << "<" << chunkId << "," << sig << ">";
			chunkSig += (chunkId + 1) * sig;
			++chunkCount;
		}
	} catch (std::exception& e) {
		std::cerr << "ChunkManager::getSignature: exception: " << e.what() << std::endl;
	}
	util::NormalOStringStream oss;
	oss << "#" << std::abs(chunkSig) << "_" << chunkCount <<
			"_" << chunkId << " " << chunkSig0.str().c_str();
	return oss.str();
}

void ChunkManager::extentChecker() {
	chunkTable_->extentChecker();
}

void ChunkManager::updateStoreObjectUseStats() {
}

uint64_t ChunkManager::getCurrentLimit() {
	return chunkBuffer_.getCurrentLimit();
}

void ChunkManager::adjustStoreMemory(uint64_t newLimit) {
	chunkBuffer_.resize(newLimit);

	updateStoreMemoryAgingParams();
	chunkBuffer_.rebalance();
}

void ChunkManager::setSwapOutCounter(int64_t counter) {
	chunkBuffer_.setSwapOutCounter(counter);
}
int64_t ChunkManager::getSwapOutCounter() {
	return chunkBuffer_.getSwapOutCounter();
}

void ChunkManager::setStoreMemoryAgingSwapRate(double rate) {
	chunkBuffer_.setStoreMemoryAgingSwapRate(
			rate, config_.getAtomicStoreMemoryLimitNum(),
			config_.getPartitionGroupNum());
}
double ChunkManager::getStoreMemoryAgingSwapRate() {
	return chunkBuffer_.getStoreMemoryAgingSwapRate();
}
void ChunkManager::updateStoreMemoryAgingParams() {
	chunkBuffer_.updateStoreMemoryAgingParams(
			config_.getAtomicStoreMemoryLimitNum(),
			config_.getPartitionGroupNum());
}

void ChunkManager::validatePinCount() {
	chunkBuffer_.validatePinCount();
}

/*!
	@brief 長期同期のチャンク復元
*/
void ChunkManager::restoreChunk(uint8_t *blockImage, size_t size, int64_t offset) {
	Chunk srcChunk0;
	ChunkBuddy srcChunk(srcChunk0);
	srcChunk.setData(blockImage, size);
	DSGroupId groupId = srcChunk.getGroupId();
	ChunkId chunkId = srcChunk.getChunkId();
	uint8_t vacancy = srcChunk.getMaxFreeExpSize();

	chunkTable_->redo(chunkId, groupId, offset, vacancy);
	chunkBuffer_.restoreChunk(pId_, offset, blockImage, size);
}

/*!
	@brief PutChunkDataLogからのチャンク復元
*/
void ChunkManager::restoreChunk(
		int64_t chunkId, int64_t groupId, int64_t offset,
		uint8_t vacancy, const void *data, int64_t dataSize) {

	chunkTable_->redo(chunkId, groupId, offset, vacancy);
	const uint8_t* image = static_cast<const uint8_t*>(data);
	uint8_t* blockImage = const_cast<uint8_t*>(image);
	chunkBuffer_.restoreChunk(pId_, offset, blockImage, dataSize);
}


/*!
	@brief チャンク取得
	@note ピン状態でリターン
*/
void ChunkManager::getChunk(DSGroupId groupId, int64_t chunkId, ChunkAccessor &ca) {
	try {
		MeshedChunkTable::Group& group = *chunkTable_->checkGroup(groupId);
		int64_t offset = group.getOffset(chunkId);
		ChunkBufferFrameRef frameRef;
		chunkBuffer_.pinChunk(pId_, offset, false, false, groupId, frameRef);
		ca.set(chunkTable_, &chunkBuffer_, pId_, groupId, chunkId, frameRef);
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "getChunk,pId," << pId_ << ",groupId," << groupId << ",chunkId," << chunkId));
	}
}

/*!
	@brief チャンク取得(グループ事前取得)
	@note ピン状態でリターン
*/
void ChunkManager::getChunk(MeshedChunkTable::Group& group, int64_t chunkId, ChunkAccessor &ca) {
	try {
		const DSGroupId groupId = group.getId();
		int64_t offset = group.getOffset(chunkId);
		ChunkBufferFrameRef frameRef;
		chunkBuffer_.pinChunk(pId_, offset, false, false, groupId, frameRef);
		ca.set(chunkTable_, &chunkBuffer_, pId_, groupId, chunkId, frameRef);
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(e, "getChunk,pId," << pId_ << ",groupId," << group.getId() << ",chunkId," << chunkId));
	}
}

const ChunkGroupStats::Table& ChunkManager::groupStats(DSGroupId groupId) {
	return putGroup(groupId)->stats();
}

/*!
	@brief 空きチャンク圧縮(領域解放)
*/
int64_t ChunkManager::releaseUnusedFileBlocks() {
	int64_t count = 0;
	try {
		const std::set<int64_t>& frees = freespace_->getFrees();
		std::set<int64_t>::const_iterator itr = frees.begin();
		for (; itr != frees.end(); ++itr) {
			chunkBuffer_.punchHoleChunk(pId_, *itr);
			++count;
		}
		return count;
	}
	catch(std::exception &e) {
		GS_RETHROW_USER_ERROR(e, GS_EXCEPTION_MERGE_MESSAGE(
				e, "Failed to punch hole: count," << count));
	}
}


bool ChunkManager::Config::isValid() {
	assert(pgIdList_.size() == static_cast<uint64_t>(getPartitionNum()));

	bool isValidChunkSize = ((1ULL << getChunkExpSize()) == getChunkSize());
	if (!isValidChunkSize) {
		GS_THROW_SYSTEM_ERROR(
			GS_ERROR_CHM_INVALID_CHUNK_SIZE, "chunk size must be 2^n.");
	}
	return true;
}

void ChunkManager::Config::operator()(
	ConfigTable::ParamId id, const ParamValue& value) {
	switch (id) {
	case CONFIG_TABLE_DS_STORE_MEMORY_LIMIT:
		setAtomicStoreMemoryLimit(ConfigTable::megaBytesToBytes(
			static_cast<uint32_t>(value.get<int32_t>())));
		break;
	case CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE:
		setAtomicStoreMemoryColdRate(value.get<double>());
		break;
	case CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE:
		setAtomicCpFileFlushSize(value.get<int64_t>());
		break;
	case CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE:
		setAtomicCpFileAutoClearCache(value.get<bool>());
		break;
	}
}

ChunkManager::Config::Config(
		PartitionGroupId partitionGroupNum, PartitionId partitionNum,
		ChunkCategoryId chunkCategoryNum, int32_t chunkSize,
		int32_t affinitySize, uint64_t storeMemoryLimit
		, ConfigTable &configTable
		, double coldRate
		, uint64_t cpFileFlushSize
		, bool cpFileAutoClearCache
		, double bufferHashTableSizeRate   
		, uint32_t cpFileSplitCount   
		, uint32_t cpFileSplitStripeSize
	)
	: partitionGroupNum_(partitionGroupNum),
	  partitionNum_(partitionNum),
	  categoryNum_(chunkCategoryNum),
	  chunkExpSize_(static_cast<uint8_t>(util::nextPowerBitsOf2(static_cast<uint32_t>(chunkSize)))),
	  chunkSize_(chunkSize),
	  atomicAffinitySize_(affinitySize),
	  isWarmStart_(false),
	  maxOnceSwapNum_(ChunkManager::MAX_ONCE_SWAP_SIZE_BYTE_ / chunkSize)
	  , bufferHashTableSizeRate_(bufferHashTableSizeRate)  
	  , cpFileSplitCount_(cpFileSplitCount)   
	  , cpFileSplitStripeSize_(cpFileSplitStripeSize)   
{
	cpFileDirList_ = ChunkManager::Config::parseCpFileDirList(configTable);
	setAtomicStoreMemoryLimit(storeMemoryLimit * 1024 * 1024);
	setAtomicStoreMemoryColdRate(coldRate);
	setAtomicCpFileFlushSize(cpFileFlushSize);
	setAtomicCpFileAutoClearCache(cpFileAutoClearCache);
}
ChunkManager::Config::~Config() {}
std::string ChunkManager::Config::dump() {
	util::NormalOStringStream stream;
	stream << "Config"
		   << ",allocatedCount," << getPartitionGroupNum()
		   << ",partitionNum," << getPartitionNum()
		   << ",storePoolLimitNum," << getAtomicStoreMemoryLimitNum()
		   << ",chunkExpSize," << (uint32_t)getChunkExpSize()
		   << ",storeMemoryLimit," << getAtomicStoreMemoryLimit()
		   << ",affinitySize," << getAffinitySize();
	stream << ",storeMemoryColdRate," << getAtomicStoreMemoryColdRate();
	stream << ",cpFileFlushSize," << getAtomicCpFileFlushSize();
	stream << ",cpFileAutoClearCache," << (getAtomicCpFileAutoClearCache() ? "true" : "false");
	return stream.str();
}

bool ChunkManager::Config::setAtomicStoreMemoryLimit(uint64_t memoryLimitByte) {
	assert((memoryLimitByte >> getChunkExpSize()) <= UINT32_MAX);

	int32_t nth = getChunkExpSize() - MIN_CHUNK_EXP_SIZE_;
	uint64_t limitMegaByte = memoryLimitByte / 1024 / 1024;

	if (limitMegaByte > LIMIT_CHUNK_NUM_LIST[nth]) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large value (source=gs_node.json, "
				<< "name=dataStore.storeMemoryLimit, "
				<< "value=" << limitMegaByte << "MB, "
				<< "max=" << LIMIT_CHUNK_NUM_LIST[nth] << "MB)");
	}
	atomicStorePoolLimit_ = memoryLimitByte;
	atomicStorePoolLimitNum_
			= static_cast<uint32_t>(memoryLimitByte >> getChunkExpSize());
	return true;
}

bool ChunkManager::Config::setAtomicCheckpointMemoryLimit(
		uint64_t memoryLimitByte) {
	assert((memoryLimitByte >> getChunkExpSize()) <= UINT32_MAX);

	int32_t nth = getChunkExpSize() - MIN_CHUNK_EXP_SIZE_;
	uint64_t limitMegaByte = memoryLimitByte / 1024 / 1024;

	if (limitMegaByte > LIMIT_CHUNK_NUM_LIST[nth]) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large value (source=gs_node.json, "
				<< "name=dataStore.storeMemoryLimit, "
				<< "value=" << limitMegaByte << "MB, "
				<< "max=" << LIMIT_CHUNK_NUM_LIST[nth] << "MB)");
	}
	atomicCheckpointPoolLimit_ = memoryLimitByte;
	atomicCheckpointPoolLimitNum_
			= static_cast<uint32_t>(memoryLimitByte >> getChunkExpSize());
	return true;
}

ChunkCompressionTypes::Mode ChunkManager::Config::getCompressionMode(
		const ConfigTable& configTable) {
	const int32_t mode = configTable.get<int32_t>(
			CONFIG_TABLE_DS_STORE_COMPRESSION_MODE);
	switch (mode) {
	case ChunkCompressionTypes::NO_BLOCK_COMPRESSION:
	case ChunkCompressionTypes::BLOCK_COMPRESSION:
		break;
	default:
		assert(false);
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
	return static_cast<ChunkCompressionTypes::Mode>(mode);
}

bool ChunkManager::Config::setAtomicStoreMemoryColdRate(double rate) {
	if (rate > 1.0) {
		rate = 1.0;
	}
	atomicStoreMemoryColdRate_ = static_cast<uint64_t>(rate * 1.0E6);
	return true;
}

bool ChunkManager::Config::setAtomicCpFileFlushSize(uint64_t size) {
	atomicCpFileFlushSize_ = size;
	atomicCpFileFlushInterval_ = size / chunkSize_;
	GS_TRACE_INFO(
			SIZE_MONITOR, GS_TRACE_CHM_CP_FILE_FLUSH_SIZE,
			"setAtomicCpFileFlushSize: size," << atomicCpFileFlushSize_ <<
			",interval," << atomicCpFileFlushInterval_);
	return true;
}
bool ChunkManager::Config::setAtomicCpFileAutoClearCache(bool flag) {
	atomicCpFileAutoClearCache_ = flag;
	return true;
}

std::vector<std::string> ChunkManager::Config::parseCpFileDirList(
		ConfigTable &configTable) {
	std::vector<std::string> dirList;
	return dirList;
}

void ChunkManager::Config::setCpFileDirList(const char* jsonStr) {
	cpFileDirList_.clear();
}


const util::NameCoderEntry<ChunkCompressionTypes::Mode>
		ChunkManager::Coders::MODE_LIST[] = {
	UTIL_NAME_CODER_ENTRY(ChunkCompressionTypes::NO_BLOCK_COMPRESSION),
	UTIL_NAME_CODER_ENTRY(ChunkCompressionTypes::BLOCK_COMPRESSION)
};

const util::NameCoder<
		ChunkCompressionTypes::Mode, ChunkCompressionTypes::MODE_END>
		ChunkManager::Coders::MODE_CODER(MODE_LIST, 0);

