﻿/*
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

#include "util/allocator.h"

#include <algorithm>

#if UTIL_ALLOCATOR_REPORTER_ENABLED
#include <iostream>
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
#include <vector>
#include <map>
#include <iostream>
#include <cstring>
#endif

#ifndef UTIL_ALLOCATOR_DIFF_REPORTER_FULL_TRACE
#define UTIL_ALLOCATOR_DIFF_REPORTER_FULL_TRACE 0
#endif

#if UTIL_ALLOCATOR_REPORTER_ENABLED3
	int64_t g_limitCount = 1000;
	int64_t g_limitSize = 1024*1024;
	 int64_t g_limitCountCount = 0;
	int64_t g_limitSizeCount = 0;
#endif

namespace util {

#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
struct AllocatorDiffReporter::Body {
	typedef std::vector<util::AllocatorGroupId> IdList;
	typedef std::vector<util::AllocatorStats> StatsList;

	typedef std::pair<std::string, uint64_t> BaseKey;
	typedef std::pair<AllocatorGroupId, BaseKey> Key;
	typedef std::map<Key, int64_t> ValueMap;

	Body();

	void getStatsList(StatsList &statsList) const;

	static void statsListToMap(const StatsList &src, ValueMap &dest);
	static void compare(
			const ValueMap &prev, const ValueMap &cur,
			const AllocatorInfo &startInfo, std::ostream &out);

	AllocatorGroupId groupId_;
	std::string namePrefix_;
	AllocatorInfo startInfo_;
	ValueMap valueMap_;
};

struct AllocatorDiffReporter::ActivationState {
	typedef AllocatorInfo::ManagerInside ManagerInside;

	typedef std::map<void*, std::string> TraceMap;
	typedef std::map<uint64_t, TraceMap> SnapshotMap;

	static void compare(
			const TraceMap &prev, const TraceMap &cur,
			const AllocatorInfo &info, std::ostream &out,
			const ManagerInside *inside);
	static void compare(
			const TraceMap &map1, const TraceMap &map2,
			const AllocatorInfo &info, std::ostream &out,
			bool reversed, bool &found, DateTime &foundTime,
			const ManagerInside *inside);

	SnapshotMap snapshotMap_;
	TraceMap traceMap_;
};
#endif 


AllocationErrorHandler::~AllocationErrorHandler() {
}


#if UTIL_ALLOCATOR_REPORTER_ENABLED
namespace detail {

util::Atomic<uint64_t> AllocatorReporter::reporterCount_;

AllocatorReporter::AllocatorReporter() : id_(++reporterCount_), repeat_(0) {
	try {
		UTIL_THROW_UTIL_ERROR(CODE_DEFAULT, "");
	}
	catch (util::Exception &e) {
		NormalOStringStream oss;
		e.format(oss);
		reporterLocation_ = oss.str();
	}
}

AllocatorReporter::~AllocatorReporter() {
}

void AllocatorReporter::reportMissHit(
		size_t elementSize, size_t totalSize, size_t freeSize) {
	if (++repeat_ <= 1) {
		return;
	}

	try {
		NormalOStringStream oss;

		oss << "Allocator miss hit occurred (id=" << id_ <<
				", repeat=" << repeat_ <<
				", elementSize=" << elementSize <<
				", totalSize=" << totalSize << ", freeSize=" << freeSize <<
				", allocatorLocation=" << reporterLocation_ <<
				", missHitLocation=";
		try {
			UTIL_THROW_UTIL_ERROR(CODE_DEFAULT, "");
		}
		catch (util::Exception &e) {
			e.format(oss);
		}
		oss << ")";

		std::string str = oss.str();
		for (const char8_t *ch = "\r\n"; *ch != '\0'; ch++) {
			std::string::size_type pos;
			while ((pos = str.find(*ch)) != std::string::npos) {
				str[pos] = ' ';
			}
		}

		std::cerr << str << std::endl;
	}
	catch (std::exception &e) {
		UTIL_RETHROW_UTIL_ERROR(CODE_DEFAULT, e, "");
	}
}

}	
#endif	


AllocatorInfo::AllocatorInfo(
		AllocatorGroupId groupId, const char8_t *nameLiteral,
		AllocatorManager *manager) :
		groupId_(groupId), nameLiteral_(nameLiteral), manager_(manager),
		unitSize_(0) {
}

AllocatorInfo::AllocatorInfo() :
		groupId_(AllocatorManager::GROUP_ID_ROOT),  nameLiteral_(NULL),
		manager_(NULL), unitSize_(0) {
}

AllocatorGroupId AllocatorInfo::getGroupId() const {
	return groupId_;
}

const char8_t* AllocatorInfo::getName() const {
	return nameLiteral_;
}

AllocatorManager& AllocatorInfo::resolveManager() const {
	return (manager_ == NULL ?
			AllocatorManager::getDefaultInstance() : *manager_);
}

size_t AllocatorInfo::getUnitSize() const {
	return unitSize_;
}

void AllocatorInfo::setUnitSize(size_t size) {
	unitSize_ = size;
}

void AllocatorInfo::format(
		std::ostream &stream, bool partial, bool nameOnly,
		bool withUnit, const ManagerInside *inside) const {
	AllocatorManager &manager = resolveManager();

	if (nameLiteral_ != NULL) {
		if (!nameOnly) {
			AllocatorInfo(groupId_, NULL, &manager).format(
					stream, false, false, true, inside);
			stream << ".";
		}
		stream << nameLiteral_;

		if (withUnit && unitSize_ > 0) {
			stream << "[";
			formatUnitSize(stream, unitSize_, true);
			stream << "]";
		}

		return;
	}

	AllocatorGroupId parentId = groupId_;
	if (manager.getParentId(parentId, inside)) {
		if (!nameOnly) {
			AllocatorInfo(parentId, nameLiteral_, &manager).format(
					stream, true, false, true, inside);
			if (manager.getParentId(parentId, inside)) {
				stream << ".";
			}
		}
	}
	else {
		if (!partial) {
			stream << "(root)";
		}
		return;
	}

	const char8_t *name = manager.getName(groupId_, inside);
	if (name == NULL) {
		stream << "(unknownGroup:" << groupId_ << ")";
	}
	else {
		stream << name;
	}
}

void AllocatorInfo::formatUnitSize(
		std::ostream &stream, int64_t size, bool exact) {
	const char8_t *unit = "";
	int64_t value = size;
	if ((exact && value % 1024 == 0) || (!exact && value >= 10000)) {
		value /= 1024;
		if ((exact && value % 1024 == 0) || (!exact && value >= 10000)) {
			value /= 1024;
			if ((exact && value % 1024 == 0) || (!exact && value >= 10000)) {
				value /= 1024;
				if (value != 0) {
					unit = "G";
				}
			}
			else {
				unit = "M";
			}
		}
		else {
			unit = "K";
		}
	}
	stream << value << unit;
}

std::ostream& operator<<(std::ostream &stream, const AllocatorInfo &info) {
	info.format(stream);
	return stream;
}


AllocatorStats::AllocatorStats(const AllocatorInfo &info) : info_(info) {
	std::fill(values_, values_ + STAT_TYPE_END, 0);
}

void AllocatorStats::merge(const util::AllocatorStats &stats) {
	assert(this != &stats);

	info_ = stats.info_;

	for (int32_t i = 0; i < STAT_TYPE_END; i++) {
		if (STAT_GROUP_TOTAL_LIMIT <= i && i < STAT_GROUP_TOTAL_LIMIT +
				AllocatorManager::LIMIT_TYPE_END) {
			continue;
		}
		values_[i] += stats.values_[i];
	}
}

int64_t AllocatorStats::asStatValue(size_t value) {
	if (value >= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
		return std::numeric_limits<int64_t>::max();
	}

	return static_cast<uint64_t>(value);
}



AllocatorDiffReporter::AllocatorDiffReporter() : body_(NULL) {
}

AllocatorDiffReporter::~AllocatorDiffReporter() {
#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
	clear();
#endif
}

void AllocatorDiffReporter::setGroup(AllocatorGroupId groupId) {
#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
	Body &body = prepare();
	body.groupId_ = groupId;
#else
	static_cast<void>(groupId);
#endif
}

void AllocatorDiffReporter::setNamePrefix(const char8_t *prefix) {
#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
	Body &body = prepare();
	body.namePrefix_ = prefix;
#else
	static_cast<void>(prefix);
#endif
}

void AllocatorDiffReporter::start(const AllocatorInfo &startInfo) {
#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
	Body &body = prepare();

	Body::StatsList statsList;
	body.getStatsList(statsList);

	body.valueMap_.clear();
	Body::statsListToMap(statsList, body.valueMap_);

	body.startInfo_ = startInfo;

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorManager &allocMgr = AllocatorManager::getDefaultInstance();
	Body::IdList idList;
	idList.push_back(body.groupId_);
	allocMgr.listSubGroup(body.groupId_, std::back_inserter(idList), true);
	allocMgr.saveReporterSnapshots(
			&idList[0], idList.size(), body.namePrefix_.c_str(), this);
#endif
#else
	static_cast<void>(startInfo);
#endif 
}

void AllocatorDiffReporter::finish(std::ostream *out) {
#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
	if (body_ == NULL) {
		assert(false);
		return;
	}
	Body &body = *body_;

	Body::StatsList statsList;
	body.getStatsList(statsList);

	Body::ValueMap curMap;
	Body::statsListToMap(statsList, curMap);

	Body::compare(body.valueMap_, curMap, body.startInfo_, resolveOutput(out));

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	AllocatorManager &allocMgr = AllocatorManager::getDefaultInstance();
	Body::IdList idList;
	idList.push_back(body.groupId_);
	allocMgr.listSubGroup(body.groupId_, std::back_inserter(idList), true);
	allocMgr.compareReporterSnapshots(
			&idList[0], idList.size(), body.namePrefix_.c_str(), this, out);
	allocMgr.removeReporterSnapshots(
			&idList[0], idList.size(), body.namePrefix_.c_str(), this);
#endif
#else
	static_cast<void>(out);
#endif 
}

#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
AllocatorDiffReporter::Body& AllocatorDiffReporter::prepare() {
	if (body_ == NULL) {
		body_ = UTIL_NEW Body();
	}
	return *body_;
}
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
void AllocatorDiffReporter::clear() {
	delete body_;
	body_ = NULL;
}
#endif

#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
std::ostream& AllocatorDiffReporter::resolveOutput(std::ostream *out) {
	return (out == NULL ? std::cout : *out);
}
#endif

AllocatorDiffReporter::Scope::Scope(
		AllocatorDiffReporter &base, const AllocatorInfo &startInfo) :
		base_(base) {
	base_.start(startInfo);
}

AllocatorDiffReporter::Scope::~Scope() try {
	base_.finish();
}
catch (...) {
}

#if UTIL_ALLOCATOR_DIFF_REPORTER_ENABLED
AllocatorDiffReporter::Body::Body() : groupId_(AllocatorGroupId()) {
}

void AllocatorDiffReporter::Body::getStatsList(StatsList &statsList) const {
	AllocatorManager &allocMgr = AllocatorManager::getDefaultInstance();

	IdList idList;
	idList.push_back(groupId_);
	allocMgr.listSubGroup(groupId_, std::back_inserter(idList), true);

	statsList.clear();
	allocMgr.getAllocatorStats(
			&idList[0], idList.size(), std::back_inserter(statsList));

	const char8_t *prefix = namePrefix_.c_str();
	for (StatsList::iterator it = statsList.begin(); it != statsList.end();) {
		const char8_t *name = it->info_.getName();
		if (strstr(name, prefix) == name) {
			++it;
		}
		else {
			it = statsList.erase(it);
		}
	}
}

void AllocatorDiffReporter::Body::statsListToMap(
		const StatsList &src, ValueMap &dest) {
	dest.clear();
	for (StatsList::const_iterator it = src.begin(); it != src.end(); ++it) {
		const AllocatorInfo &info = it->info_;
		const Key key(
				info.getGroupId(),
				BaseKey(info.getName(), info.getUnitSize()));
		const int64_t usage = it->values_[AllocatorStats::STAT_TOTAL_SIZE] -
				it->values_[AllocatorStats::STAT_CACHE_SIZE];
		dest[key] += usage;
	}
}

void AllocatorDiffReporter::Body::compare(
		const ValueMap &prev, const ValueMap &cur,
		const AllocatorInfo &startInfo, std::ostream &out) {
	bool found = false;
	DateTime foundTime;

	for (ValueMap::const_iterator it = cur.begin(); it != cur.end(); ++it) {
		ValueMap::const_iterator prevIt = prev.find(it->first);
		if (prevIt != prev.end() && prevIt->second == it->second) {
			continue;
		}

		if (!found) {
			found = true;
			foundTime = DateTime::now(false);
			out << "=== ";
			out <<"[ALLOCATOR_DIFF_REPORT] " << foundTime << " " <<
					startInfo << std::endl;
		}

		const Key &key = it->first;
		AllocatorInfo info(key.first, key.second.first.c_str());
		info.setUnitSize(static_cast<size_t>(key.second.second));

		out << "===== ";
		if (prevIt == prev.end()) {
			out << "Extra allocator: " << info <<
					", usage=" << it->second << std::endl;
		}
		else {
			out << "Usage diff: " << info;
			out << ", prev=";
			AllocatorInfo::formatUnitSize(out, prevIt->second, true);
			out << ", cur=";
			AllocatorInfo::formatUnitSize(out, it->second, true);
			out << std::endl;
		}
	}
}

AllocatorDiffReporter::ActivationSaver::ActivationSaver() : state_(NULL) {
}

AllocatorDiffReporter::ActivationSaver::~ActivationSaver() {
	clear();
}

void AllocatorDiffReporter::ActivationSaver::onAllocated(void *ptr) {
#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
#if !UTIL_ALLOCATOR_DIFF_REPORTER_FULL_TRACE
	if (state_ == NULL || state_->snapshotMap_.empty()) {
		return;
	}
#endif
	ActivationState &state = prepare();

	NormalOStringStream oss;
	oss << StackTraceUtils::getStackTrace;

	state.traceMap_.insert(
			ActivationState::TraceMap::value_type(ptr, oss.str()));
#else
	static_cast<void>(ptr);
#endif 
}

void AllocatorDiffReporter::ActivationSaver::onDeallocated(void *ptr) {
	if (state_ == NULL) {
		return;
	}
	ActivationState &state = *state_;

	state.traceMap_.erase(ptr);
}

void AllocatorDiffReporter::ActivationSaver::saveSnapshot(size_t id) {
	ActivationState &state = prepare();
	try {
		state.snapshotMap_[id] = state.traceMap_;
	}
	catch (...) {
		state.snapshotMap_.erase(id);
		throw;
	}
}

void AllocatorDiffReporter::ActivationSaver::removeSnapshot(size_t id) {
	ActivationState &state = prepare();
	state.snapshotMap_.erase(id);
}

void AllocatorDiffReporter::ActivationSaver::compareSnapshot(
		size_t id, const AllocatorInfo &info, std::ostream *out,
		const ManagerInside *inside) {
	ActivationState &state = prepare();

	ActivationState::SnapshotMap::const_iterator it =
			state.snapshotMap_.find(id);
	if (it == state.snapshotMap_.end()) {
		return;
	}

	ActivationState::compare(
			it->second, state.traceMap_, info, resolveOutput(out), inside);
}

AllocatorDiffReporter::ActivationState&
AllocatorDiffReporter::ActivationSaver::prepare() {
	if (state_ == NULL) {
		state_ = UTIL_NEW ActivationState();
	}
	return *state_;
}

void AllocatorDiffReporter::ActivationSaver::clear() {
	delete state_;
	state_ = NULL;
}

void AllocatorDiffReporter::ActivationState::compare(
		const TraceMap &prev, const TraceMap &cur, const AllocatorInfo &info,
		std::ostream &out, const ManagerInside *inside) {
	bool found = false;
	DateTime foundTime;

	compare(prev, cur, info, out, false, found, foundTime, inside);

#if UTIL_ALLOCATOR_DIFF_REPORTER_FULL_TRACE
	compare(cur, prev, info, out, true, found, foundTime, inside);
#endif
}

void AllocatorDiffReporter::ActivationState::compare(
		const TraceMap &map1, const TraceMap &map2, const AllocatorInfo &info,
		std::ostream &out, bool reversed, bool &found, DateTime &foundTime,
		const ManagerInside *inside) {
	for (TraceMap::const_iterator it2 = map2.begin();
			it2 != map2.end(); ++it2) {
		TraceMap::const_iterator it1= map1.find(it2->first);

		if (it1 != map1.end()) {
			continue;
		}

		if (!found) {
			found = true;
			foundTime = DateTime::now(false);
			out << "=== ";
			out <<"[ALLOCATOR_DIFF_REPORT_TRACE] " << foundTime << " ";
			info.format(out, false, false, true, inside);
			out << std::endl;
		}

		out << "===== ";
		out << (reversed ? "Deallocated" : "Allocated") << ": " <<
				it2->second << std::endl;
	}
}
#endif 


AllocationErrorHandler *StackAllocator::defaultErrorHandler_ = NULL;

#if !UTIL_ALLOCATOR_FORCE_ALLOCATOR_INFO
StackAllocator::StackAllocator(BaseAllocator &base) :
		restSize_(0),
		end_(NULL),
		base_(base),
		topBlock_(NULL),
		freeBlock_(NULL),
		totalSizeLimit_(std::numeric_limits<size_t>::max()),
		freeSizeLimit_(std::numeric_limits<size_t>::max()),
		totalSize_(0),
		freeSize_(0),
		hugeCount_(0),
		hugeSize_(0),
		errorHandler_(NULL),
		stats_(AllocatorInfo()),
		limitter_(NULL) {
	assert(base.getElementSize() >= detail::AlignedSizeOf<BlockHead>::VALUE);

	util::AllocatorManager::addAllocator(stats_.info_, *this);
}
#endif

StackAllocator::StackAllocator(const AllocatorInfo &info, BaseAllocator *base) :
		restSize_(0),
		end_(NULL),
		base_(*base),
		topBlock_(NULL),
		freeBlock_(NULL),
		totalSizeLimit_(std::numeric_limits<size_t>::max()),
		freeSizeLimit_(std::numeric_limits<size_t>::max()),
		totalSize_(0),
		freeSize_(0),
		hugeCount_(0),
		hugeSize_(0),
		errorHandler_(NULL),
		stats_(info),
		limitter_(NULL) {
	assert(base != NULL);
	assert(base->getElementSize() >= detail::AlignedSizeOf<BlockHead>::VALUE);

	util::AllocatorManager::addAllocator(stats_.info_, *this);
}

StackAllocator::~StackAllocator() {
#if UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR
	while (topBlock_ != NULL) {
		BlockHead *prev = topBlock_->prev_;
		UTIL_FREE(topBlock_);
		topBlock_ = prev;
	}
#else
	BlockHead **blockList[] = { &topBlock_, &freeBlock_, NULL };
	for (BlockHead ***cur = blockList; *cur != NULL; ++cur) {
		for (BlockHead *&block = **cur; block != NULL;) {
			BlockHead *prev = block->prev_;

			if (block->blockSize_ == base_.getElementSize()) {
				base_.deallocate(block);
			}
			else {
				UTIL_FREE(block);
			}

			block = prev;
		}
	}

#ifndef NDEBUG
	restSize_ = 0;
	end_ = NULL;
	totalSize_ = 0;
	freeSize_ = 0;
#endif

#endif

	util::AllocatorManager::removeAllocator(stats_.info_, *this);
}

void StackAllocator::trim() {
	bool adjusted = false;
	for (bool hugeOnly = freeSize_ > base_.getElementSize();;) {
		if (!hugeOnly && freeSize_ <= freeSizeLimit_) {
			break;
		}

		for (BlockHead **cur = &freeBlock_; *cur != NULL;) {
			BlockHead *block = *cur;

			if ((!hugeOnly && freeSize_ > freeSizeLimit_) ||
					block->blockSize_ != base_.getElementSize()) {
				adjusted = true;

				assert(freeSize_ >= block->blockSize_);
				assert(totalSize_ >= block->blockSize_);

				freeSize_ -= block->blockSize_;
				totalSize_ -= block->blockSize_;

				BlockHead *prev = block->prev_;

				if (block->blockSize_ == base_.getElementSize()) {
					base_.deallocate(block);
				}
				else {
					hugeCount_--;
					hugeSize_ -= block->blockSize_;

					stats_.values_[AllocatorStats::STAT_CACHE_SIZE] -=
							AllocatorStats::asStatValue(block->blockSize_);

					UTIL_FREE(block);
				}

				*cur = prev;
			}
			else {
				cur = &block->prev_;
			}
		}

		if (!hugeOnly) {
			break;
		}
		hugeOnly = false;
	}

	if (adjusted) {
		stats_.values_[AllocatorStats::STAT_CACHE_ADJUST_COUNT]++;
		stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] =
				AllocatorStats::asStatValue(hugeSize_);
	}
}

void* StackAllocator::allocateOverBlock(size_t size) {
#if UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR
	const size_t blockSize = detail::AlignedSizeOf<BlockHead>::VALUE + size;
	BlockHead *newBlock = static_cast<BlockHead*>(UTIL_MALLOC(blockSize));
	try {
		if (newBlock == NULL) {
			UTIL_THROW_UTIL_ERROR_CODED(CODE_NO_MEMORY);
		}
	}
	catch (Exception &e) {
		if (errorHandler_ != NULL) {
			(*errorHandler_)(e);
		}
		throw;
	}
	newBlock->prev_ = topBlock_;
	newBlock->blockSize_ = blockSize;
	topBlock_ = newBlock;

	hugeCount_++;
	hugeSize_ += blockSize;

	stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] =
			AllocatorStats::asStatValue(hugeSize_);
	stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE] = std::max(
			stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE],
			stats_.values_[AllocatorStats::STAT_TOTAL_SIZE]);
#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
	activationSaver_.onAllocated(newBlock->body());
#endif

	return newBlock->body();
#else
	assert(size > restSize_);

	for (BlockHead **cur = &freeBlock_; *cur != NULL; cur = &(*cur)->prev_) {
		BlockHead *block = *cur;
		if (size <= block->bodySize()) {
			*cur = block->prev_;

			block->prev_ = topBlock_;
			topBlock_ = block;

			end_ = block->end();
			restSize_ = block->bodySize() - size;

			assert(freeSize_ >= block->blockSize_);
			freeSize_ -= block->blockSize_;

			if (block->blockSize_ != base_.getElementSize()) {
				stats_.values_[AllocatorStats::STAT_CACHE_SIZE] -=
						AllocatorStats::asStatValue(block->blockSize_);
			}

			return block->body();
		}
	}

	const size_t minSize = detail::AlignedSizeOf<BlockHead>::VALUE + size;
	const size_t baseSize = base_.getElementSize();

	UTIL_DETAIL_ALLOCATOR_REPORT_MISS_HIT(
			std::max(minSize, baseSize), getTotalSize(), getFreeSize());

	stats_.values_[AllocatorStats::STAT_CACHE_MISS_COUNT]++;

	BlockHead *newBlock;
	try {
		if (totalSize_ + minSize > totalSizeLimit_) {
			UTIL_THROW_UTIL_ERROR(CODE_MEMORY_LIMIT_EXCEEDED,
					"Memory limit exceeded ("
					"name=" << stats_.info_ <<
					", requestedSize=" << minSize <<
					", totalSizeLimit=" << totalSizeLimit_ <<
					", freeSizeLimit=" << freeSizeLimit_ <<
					", totalSize=" << totalSize_ <<
					", freeSize=" << freeSize_ << ")");
		}

		if (minSize <= baseSize) {
			newBlock = static_cast<BlockHead*>(base_.allocate());
			newBlock->blockSize_ = baseSize;
		}
		else {
			const size_t desiredSize = std::max(
					(minSize + (baseSize - 1)) / baseSize * baseSize, minSize);

			size_t blockSize;
			if (limitter_ == NULL) {
				blockSize = desiredSize;
			}
			else {
				blockSize = limitter_->acquire(minSize, desiredSize);
			}

			newBlock = static_cast<BlockHead*>(UTIL_MALLOC(blockSize));
			if (newBlock == NULL) {
				UTIL_THROW_UTIL_ERROR(CODE_NO_MEMORY,
						"Memory allocation failed ("
						"requestedBlockSize=" << blockSize <<
						", baseBlockSize=" << baseSize <<
						", totalSize=" << totalSize_ <<
						", freeSize=" << freeSize_ << ")");
			}
			newBlock->blockSize_ = blockSize;

			hugeCount_++;
			hugeSize_ += blockSize;

			stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] =
					AllocatorStats::asStatValue(hugeSize_);
			stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE] = std::max(
					stats_.values_[AllocatorStats::STAT_PEAK_TOTAL_SIZE],
					stats_.values_[AllocatorStats::STAT_TOTAL_SIZE]);
		}
	}
	catch (Exception &e) {
		if (errorHandler_ != NULL) {
			(*errorHandler_)(e);
		}
		handleAllocationError(e);
		throw;
	}

	newBlock->prev_ = topBlock_;
	topBlock_ = newBlock;

	end_ = newBlock->end();
	restSize_ = newBlock->bodySize() - size;

	totalSize_ += newBlock->blockSize_;

	return newBlock->body();
#endif
}

void StackAllocator::pop(BlockHead *lastBlock, size_t lastRestSize) {
#if UTIL_ALLOCATOR_SUBSTITUTE_STACK_ALLOCATOR
	static_cast<void>(lastRestSize);
	while (topBlock_ != NULL && topBlock_ != lastBlock) {
		hugeCount_--;
		hugeSize_ -= topBlock_->blockSize_;
		stats_.values_[AllocatorStats::STAT_TOTAL_SIZE] -=
				AllocatorStats::asStatValue(topBlock_->blockSize_);

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
		activationSaver_.onDeallocated(topBlock_->body());
#endif

		BlockHead *prev = topBlock_->prev_;
		UTIL_FREE(topBlock_);
		topBlock_ = prev;
	}
#else
	while (topBlock_ != lastBlock) {
		BlockHead *prev = topBlock_->prev_;

		freeSize_ += topBlock_->blockSize_;
		assert(freeSize_ <= totalSize_);

		if (topBlock_->blockSize_ != base_.getElementSize()) {
			stats_.values_[AllocatorStats::STAT_CACHE_SIZE] +=
					AllocatorStats::asStatValue(topBlock_->blockSize_);
		}

		topBlock_->prev_ = freeBlock_;
		freeBlock_ = topBlock_;

		topBlock_ = prev;
	}

	if (lastBlock == NULL) {
		assert(lastRestSize == 0);

		restSize_ = 0;
		end_ = NULL;
	}
	else {
		restSize_ = lastRestSize;
		end_ = lastBlock->end();
	}
#endif
}

void StackAllocator::handleAllocationError(util::Exception &e) {
	if (defaultErrorHandler_ != NULL) {
		(*defaultErrorHandler_)(e);
	}
	throw;
}

void StackAllocator::getStats(AllocatorStats &stats) {
	stats.merge(stats_);
}

void StackAllocator::setLimit(AllocatorStats::Type type, size_t value) {
	switch (type) {
	case AllocatorStats::STAT_CACHE_LIMIT:
		freeSizeLimit_ = value;
		break;
	default:
		break;
	}
}

void StackAllocator::setLimit(AllocatorStats::Type type, AllocatorLimitter *limitter) {
	switch (type) {
	case AllocatorStats::STAT_GROUP_TOTAL_LIMIT:
		limitter_ = AllocatorLimitter::moveSize(limitter, limitter_, hugeSize_);
		break;
	default:
		break;
	}
}


void StackAllocator::Tool::forceReset(StackAllocator &alloc) {
	alloc.pop(NULL, 0);
}

size_t StackAllocator::Tool::getRestSize(StackAllocator &alloc) {
	return alloc.restSize_;
}

size_t StackAllocator::Tool::getRestSizeAligned(StackAllocator &alloc) {
	const size_t restSize = alloc.restSize_;

	size_t alignedSize = detail::AllocatorUtils::getAlignedSize(restSize);
	if (alignedSize > restSize) {
		const size_t unit = detail::AllocatorUtils::getAlignedSize(1);
		if (alignedSize >= unit) {
			alignedSize -= unit;
		}
		else {
			alignedSize = 0;
		}
	}

	return alignedSize;
}


AllocatorManager *AllocatorManager::defaultInstance_ = NULL;

AllocatorManager::~AllocatorManager() {
}

AllocatorManager& AllocatorManager::getDefaultInstance() {
	assert(defaultInstance_ != NULL);
	return *defaultInstance_;
}

bool AllocatorManager::addGroup(
		GroupId parentId, GroupId id, const char8_t *nameLiteral) {
	assert(id >= 0);
	assert (nameLiteral != NULL);

	util::LockGuard<util::Mutex> guard(mutex_);

	while (id >= groupList_.end_ - groupList_.begin_) {
		groupList_.add(GroupEntry());
	}

	GroupEntry &entry = groupList_.begin_[id];
	const bool added = (entry.nameLiteral_ == NULL);

	if (entry.parentId_ != parentId) {
		if (entry.parentId_ != GROUP_ID_ROOT || id == GROUP_ID_ROOT) {
			assert(false);
			return false;
		}
		entry.parentId_ = parentId;
	}

	entry.nameLiteral_ = nameLiteral;

	return added;
}

bool AllocatorManager::getParentId(
		GroupId &id, const ManagerInside *inside) {
	util::DynamicLockGuard<util::Mutex> guard(inside == NULL ? &mutex_ : NULL);

	return (getParentEntry(id) != NULL);
}

const char8_t* AllocatorManager::getName(
		GroupId id, const ManagerInside *inside) {
	util::DynamicLockGuard<util::Mutex> guard(inside == NULL ? &mutex_ : NULL);

	if (id >= groupList_.end_ - groupList_.begin_) {
		return NULL;
	}

	return groupList_.begin_[id].nameLiteral_;
}

void AllocatorManager::getGroupStats(
		const GroupId *idList, size_t idCount, AllocatorStats *statsList) {
	util::LockGuard<util::Mutex> guard(mutex_);

	for (size_t i = 0; i < idCount; i++) {
		statsList[i] = AllocatorStats();
	}

	for (GroupEntry *groupIt = groupList_.begin_;
			groupIt != groupList_.end_; ++groupIt) {
		AllocatorStats groupStats;

		for (AllocatorEntry *it = groupIt->allocatorList_.begin_;
				it != groupIt->allocatorList_.end_; ++it) {
			AllocatorStats stats;
			AccessorParams params;
			params.stats_ = &stats;
			(*it->accessor_)(it->allocator_, COMMAND_GET_STAT, params);

			groupStats.merge(stats);
		}

		GroupId id = static_cast<GroupId>(groupIt - groupList_.begin_);
		const GroupId *idListEnd = idList + idCount;
		for (;;) {
			const GroupId *it = std::find(idList, idListEnd, id);
			if (it != idListEnd) {
				statsList[id].merge(groupStats);
			}

			GroupEntry &entry = groupList_.begin_[id];
			for (int32_t i = 0; i < LIMIT_TYPE_END; i++) {
				statsList[id].values_[
						AllocatorStats::STAT_GROUP_TOTAL_LIMIT + i] =
						AllocatorStats::asStatValue(entry.limitList_[i]);
			}

			if (getParentEntry(id) == NULL) {
				break;
			}
		}
	}
}

void AllocatorManager::setLimit(
		GroupId id, LimitType limitType, size_t limit) {
	assert(id < groupList_.end_ - groupList_.begin_);

	util::LockGuard<util::Mutex> guard(mutex_);

	GroupEntry &entry = groupList_.begin_[id];
	bool updating = true;
	if (limitType == LIMIT_GROUP_TOTAL_SIZE) {
		if (entry.totalLimitter_ == NULL) {
			entry.totalLimitter_ =
					UTIL_NEW AllocatorLimitter(AllocatorInfo(id, NULL, this));
		}
		else {
			updating = false;
		}
		entry.totalLimitter_->setLimit(limit);
	}

	for (GroupEntry *groupIt = groupList_.begin_;
			groupIt != groupList_.end_; ++groupIt) {

		const GroupId subId = static_cast<GroupId>(groupIt - groupList_.begin_);
		if (!isDescendantOrSelf(id, subId)) {
			continue;
		}

		if (limitType != LIMIT_GROUP_TOTAL_SIZE || groupIt != &entry) {
			groupIt->limitList_[limitType] = limit;
		}

		if (!updating) {
			continue;
		}

		TinyList<AllocatorEntry> &list = groupIt->allocatorList_;
		for (AllocatorEntry *it = list.begin_; it != list.end_; ++it) {
			applyAllocatorLimit(*it, limitType, *groupIt);
		}
	}
}

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
void AllocatorManager::saveReporterSnapshots(
		const GroupId *idList, size_t idCount, const char8_t *prefix,
		void *requester) {
	operateReporterSnapshots(
			idList, idCount, prefix, requester,
			COMMAND_SAVE_REPORTER_SNAPSHOT, NULL);
}

void AllocatorManager::removeReporterSnapshots(
		const GroupId *idList, size_t idCount, const char8_t *prefix,
		void *requester) {
	operateReporterSnapshots(
			idList, idCount, prefix, requester,
			COMMAND_REMOVE_REPORTER_SNAPSHOT, NULL);
}

void AllocatorManager::compareReporterSnapshots(
		const GroupId *idList, size_t idCount, const char8_t *prefix,
		void *requester, std::ostream *out) {
	operateReporterSnapshots(
			idList, idCount, prefix, requester,
			COMMAND_COMPARE_REPORTER_SNAPSHOT, out);
}
#endif 

AllocatorManager::AllocatorManager() {
}

void AllocatorManager::applyAllocatorLimit(
		AllocatorEntry &entry, LimitType limitType, GroupEntry &groupEntry) {

	Command command;
	AllocatorStats::Type type;

	switch (limitType) {
	case LIMIT_GROUP_TOTAL_SIZE:
		command = COMMAND_SET_TOTAL_LIMITTER;
		type = AllocatorStats::STAT_TYPE_END;
		if (groupEntry.totalLimitter_ == NULL) {
			return;
		}
		break;
	case LIMIT_EACH_CACHE_SIZE:
		command = COMMAND_SET_EACH_LIMIT;
		type = AllocatorStats::STAT_CACHE_LIMIT;
		break;
	case LIMIT_EACH_STABLE_SIZE:
		command = COMMAND_SET_EACH_LIMIT;
		type = AllocatorStats::STAT_STABLE_LIMIT;
		break;
	default:
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "");
	}

	AccessorParams params;
	params.size_ = groupEntry.limitList_[limitType];
	params.limitter_ = groupEntry.totalLimitter_;
	params.type_ = type;

	(*entry.accessor_)(entry.allocator_, command, params);
}

AllocatorManager::GroupEntry* AllocatorManager::getParentEntry(GroupId &id) {
	if (id == GROUP_ID_ROOT) {
		return NULL;
	}

	id = groupList_.begin_[id].parentId_;
	return &groupList_.begin_[id];
}

bool AllocatorManager::isDescendantOrSelf(GroupId id, GroupId subId) {
	for (GroupId curId = subId;;) {
		if (id == curId) {
			return true;
		}
		if (getParentEntry(curId) == NULL) {
			return false;
		}
	}
}

AllocatorManager::AllocatorEntry* AllocatorManager::findAllocatorEntry(
		void *alloc, GroupEntry &groupEntry) {
	for (AllocatorEntry *it = groupEntry.allocatorList_.begin_;
			it != groupEntry.allocatorList_.end_; ++it) {
		if (it->allocator_ == alloc) {
			return it;
		}
	}

	return NULL;
}

#if UTIL_ALLOCATOR_DIFF_REPORTER_TRACE_ENABLED
void AllocatorManager::operateReporterSnapshots(
		const GroupId *idList, size_t idCount, const char8_t *prefix,
		void *requester, Command command, std::ostream *out) {
	const size_t snapshotId = reinterpret_cast<uintptr_t>(requester);

	util::LockGuard<util::Mutex> guard(mutex_);

	for (size_t i = 0; i < idCount; i++) {
		const GroupEntry &entry = groupList_.begin_[idList[i]];

		for (AllocatorEntry *entryIt = entry.allocatorList_.begin_;
				entryIt != entry.allocatorList_.end_; ++entryIt) {
			AllocatorStats stats;
			{
				AccessorParams params;
				params.stats_ = &stats;
				(*entryIt->accessor_)(
						entryIt->allocator_, COMMAND_GET_STAT, params);
			}

			const char8_t *name = stats.info_.getName();
			if (strstr(name, prefix) != name) {
				continue;
			}

			{
				AccessorParams params;
				params.size_ = snapshotId;
				params.stats_ = &stats;
				params.out_ = out;
				(*entryIt->accessor_)(entryIt->allocator_, command, params);
			}
		}
	}
}
#endif 

template<typename T> AllocatorManager::TinyList<T>::TinyList() :
		begin_(NULL), end_(NULL), storageEnd_(NULL) {
}

template<typename T> AllocatorManager::TinyList<T>::~TinyList() {
	delete[] begin_;
}

template<typename T>
AllocatorManager::TinyList<T>::TinyList(const TinyList &another) :
		begin_(NULL), end_(NULL), storageEnd_(NULL) {
	(*this) = another;
}

template<typename T> AllocatorManager::TinyList<T>&
AllocatorManager::TinyList<T>::operator=(const TinyList &another) {
	if (this != &another) {
		delete[] begin_;
		begin_ = NULL;
		end_ = NULL;
		storageEnd_ = NULL;

		for (T *it = another.begin_; it != another.end_; ++it) {
			add(*it);
		}
	}
	return *this;
}

template<typename T>
void AllocatorManager::TinyList<T>::add(const T &value) {
	if (end_ >= storageEnd_) {
		const size_t lastSize = static_cast<size_t>(end_ - begin_);
		const size_t lastCapacity = static_cast<size_t>(storageEnd_ - begin_);

		const size_t newCapacity = std::max<size_t>(
				std::max<size_t>(16, lastCapacity * 2), lastSize + 1);

		T *newStorage = UTIL_NEW T[newCapacity];
		for (T *it = begin_; it != end_; ++it) {
			newStorage[it - begin_] = *it;
		}
		delete[] begin_;

		begin_ = newStorage;
		end_ = newStorage + lastSize;
		storageEnd_ = newStorage + newCapacity;
	}

	*end_ = value;
	++end_;
}

template<typename T>
void AllocatorManager::TinyList<T>::remove(T *it) {
	assert(begin_ <= it && it <= end_);

	if (it < end_) {
		for (T *curIt = it; ++curIt < end_;) {
			*(curIt  - 1) = *curIt;
		}
		--end_;
	}
}

template struct AllocatorManager::TinyList<AllocatorManager::AllocatorEntry>;
template struct AllocatorManager::TinyList<AllocatorManager::GroupEntry>;

size_t AllocatorManager::Initializer::counter_ = 0;

AllocatorManager::Initializer::Initializer() {
	if (counter_++ == 0) {
		assert(AllocatorManager::defaultInstance_ == NULL);
		AllocatorManager::defaultInstance_ = UTIL_NEW AllocatorManager();
	}
}

AllocatorManager::Initializer::~Initializer() {
	if (--counter_ == 0) {
		delete AllocatorManager::defaultInstance_;
	}
}

AllocatorManager::AccessorParams::AccessorParams() :
		size_(0),
		stats_(NULL),
		limitter_(NULL),
		type_(AllocatorStats::STAT_TYPE_END),
		out_(NULL) {
}

AllocatorManager::AllocatorEntry::AllocatorEntry() :
		allocator_(NULL),
		accessor_(NULL) {
}

AllocatorManager::GroupEntry::GroupEntry() :
		parentId_(GROUP_ID_ROOT),
		nameLiteral_(NULL),
		totalLimitter_(NULL) {

	std::fill(limitList_, limitList_ + LIMIT_TYPE_END,
			std::numeric_limits<size_t>::max());
}

AllocatorManager::GroupEntry::~GroupEntry() {
	delete totalLimitter_;
}

AllocatorLimitter::AllocatorLimitter(const AllocatorInfo &info) :
		info_(info), limit_(std::numeric_limits<size_t>::max()), acquired_(0) {
}

void AllocatorLimitter::setLimit(size_t size) {
	util::LockGuard<util::Mutex> guard(mutex_);

	if (size < acquired_) {
		UTIL_THROW_UTIL_ERROR(CODE_MEMORY_LIMIT_EXCEEDED,
				"Memory limit exceeded ("
				", group=" << info_ <<
				", acquired=" << acquired_ <<
				", newLimit=" << size << ")");
	}

	limit_ = size;
}

size_t AllocatorLimitter::acquire(size_t minimum, size_t desired) {
	util::LockGuard<util::Mutex> guard(mutex_);

	if (minimum > desired) {
		UTIL_THROW_UTIL_ERROR(CODE_ILLEGAL_ARGUMENT, "");
	}

	const size_t rest = limit_ - acquired_;
	if (rest < minimum) {
		UTIL_THROW_UTIL_ERROR(CODE_MEMORY_LIMIT_EXCEEDED,
				"Memory limit exceeded ("
				", group=" << info_ <<
				", required=" << minimum <<
				", acquired=" << acquired_ <<
				", limit=" << limit_ << ")");
	}

	const size_t size = std::min(rest, desired);
	acquired_ += size;

	return size;
}

void AllocatorLimitter::release(size_t size) {
	util::LockGuard<util::Mutex> guard(mutex_);
	if (size > acquired_) {
		assert(false);
		return;
	}
	acquired_ -= size;
}

AllocatorLimitter* AllocatorLimitter::moveSize(
		AllocatorLimitter *dest, AllocatorLimitter *src, size_t size) {
	if (dest != src) {
		if (dest != NULL) {
			dest->acquire(size, size);
		}

		if (src != NULL) {
			src->release(size);
		}
	}

	return dest;
}

}	
