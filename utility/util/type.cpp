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
/*
    Copyright (c) 2008, Yubin Lim(purewell@gmail.com).
    All rights reserved.

    Redistribution and use in source and binary forms, with or without 
    modification, are permitted provided that the following conditions 
    are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the 
      documentation and/or other materials provided with the distribution.
    * Neither the name of the Purewell nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS 
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT 
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR 
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT 
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, 
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT 
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, 
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY 
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#ifdef NDEBUG
#if (defined(_SECURE_SCL) && _SECURE_SCL) || \
	(defined(_HAS_ITERATOR_DEBUGGING) && _HAS_ITERATOR_DEBUGGING)
#error Performance problems will arise
#endif
#endif

#include "util/type.h"
#include "util/allocator.h"
#include "util/code.h"
#include "util/os.h"
#include <stdexcept>
#include <cassert>
#include <climits>

#ifdef UTIL_STACK_TRACE_ENABLED
#include "util/thread.h"

#ifdef _WIN32
#include <windows.h>
#include <dbghelp.h>
#include <string>
#include <map>
#include <iostream>
#include <sstream>

#pragma comment(lib, "dbghelp.lib")
#else
#include <execinfo.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <pthread.h>
#include <cxxabi.h>
#endif

#endif	

#ifndef UTIL_MEMORY_MANAGE_LARGE
#define UTIL_MEMORY_MANAGE_LARGE 1
#endif

#ifndef UTIL_MEMORY_SIZE_HISTOGRAM_ENABLED
#define UTIL_MEMORY_SIZE_HISTOGRAM_ENABLED 0
#endif

namespace util {

int stricmp(const char *x, const char *y) {
#if defined(UTIL_HAVE_STRCASECMP)
	return strcasecmp(x, y);
#elif defined(UTIL_HAVE_STRICMP)
#ifdef _MSC_VER
	return _stricmp(x, y);
#else
	return stricmp(x, y);
#endif
#else
#error 0
#endif 
}

namespace detail {


void* DirectMemoryUtils::mallocDirect(size_t size) UTIL_NOEXCEPT {
#if UTIL_FAILURE_SIMULATION_ENABLED
	if (util::AllocationFailureSimulator::checkOperationDirect(
			util::AllocationFailureSimulator::TARGET_NEW, size)) {
		return NULL;
	}
#endif

	void *ptr = malloc(size);
#if UTIL_MEMORY_ALLOCATE_INITIALIZE
	if (ptr != NULL) {
		memset(ptr, 0, size);
	}
#endif
	return ptr;
}

void DirectMemoryUtils::freeDirect(void *ptr) UTIL_NOEXCEPT {
	free(ptr);
}


class MemoryManager {
public:
	enum StatType {
		STAT_TOTAL_SIZE,
		STAT_CACHE_SIZE,
		STAT_MONITORING_SIZE,
		STAT_TOTAL_COUNT,
		STAT_CACHE_COUNT,
		STAT_DEALLOC_COUNT,

		END_STAT
	};

	typedef int64_t (UnitStats)[END_STAT];

	class Initializer;

	static MemoryManager* findInstance() UTIL_NOEXCEPT;

	static void* allocate(
			MemoryManager *manager, size_t size,
			bool monitoring) UTIL_NOEXCEPT;
	static void deallocate(
			MemoryManager *manager, void *ptr, bool monitoring) UTIL_NOEXCEPT;

	static FreeLink* allocateBulk(
			MemoryManager *manager, size_t size, bool monitoring, bool aligned,
			size_t &count) UTIL_NOEXCEPT;
	static void deallocateBulk(
			MemoryManager *manager, FreeLink *link, size_t size,
			bool monitoring, bool aligned) UTIL_NOEXCEPT;

	static void* allocateDirect(
			MemoryManager *manager, size_t size, bool monitoring,
			bool aligned) UTIL_NOEXCEPT;
	static void deallocateDirect(
			MemoryManager *manager, void *ptr, size_t size, bool monitoring,
			bool aligned) UTIL_NOEXCEPT;

	static size_t adjustAllocationSize(
			size_t size, bool withHead, size_t *index) UTIL_NOEXCEPT;

	static size_t getUnitCount() UTIL_NOEXCEPT;
	static bool getUnitProfile(
			MemoryManager *manager, size_t index, int64_t &totalSize,
			int64_t &cacheSize, int64_t &monitoringSize, int64_t &totalCount,
			int64_t &cacheCount, int64_t &deallocCount) UTIL_NOEXCEPT;
	static bool getUnitProfile(
			MemoryManager *manager, size_t index,
			UnitStats &stats) UTIL_NOEXCEPT;

	static void dumpStats(
			MemoryManager *manager, std::ostream &os);
	static void dumpSizeHistogram(
			MemoryManager *manager, std::ostream &os);

private:
	friend class MemoryManagerInitializer;

	enum {
		NORMAL_HEAD_SIZE = AlignedSizeOf<size_t>::VALUE,

#if UTIL_MEMORY_MANAGE_LARGE
		ENTRY_COUNT = 31
#else
		ENTRY_COUNT = 21
#endif
	};

	typedef UnitStats (StatsList)[ENTRY_COUNT + 1];

	struct Shared;

	typedef std::pair<uint32_t, FreeLink*> SubLink;

	struct Entry {
		Entry() UTIL_NOEXCEPT;

		DirectMutex mutex_;
		FreeLink *freeLink_;
		SubLink *subList_;
		size_t subCount_;
	};

	typedef std::pair<uint64_t, uint64_t> HistogramEntry;
	struct SizeHistogram {
		enum {
			HISTOGRAM_CAPACITY = 1000
		};
		typedef HistogramEntry (HistogramEntryList)[HISTOGRAM_CAPACITY];

		SizeHistogram() UTIL_NOEXCEPT;
		void add(size_t size, size_t count) UTIL_NOEXCEPT;
		void dump(std::ostream &os) UTIL_NOEXCEPT;

		HistogramEntryList histogramEntries_;
		size_t histogramSize_;
	};

	explicit MemoryManager(StatsList &statsListRef) UTIL_NOEXCEPT;
	~MemoryManager();

	void unmanageAll() UTIL_NOEXCEPT;

	static void* allocateInternal(
			DynamicLockGuard<DirectMutex>&, size_t allocSize, Entry *entry,
			UnitStats &stats, bool monitoring) UTIL_NOEXCEPT;
	static void deallocateInternal(
			MemoryManager *manager, DynamicLockGuard<DirectMutex>&,
			void *ptr, size_t allocSize, Entry *entry, UnitStats &stats,
			bool monitoring) UTIL_NOEXCEPT;

	static size_t adjustAllocationSizeInternal(
			size_t size, bool aligned, size_t *index) UTIL_NOEXCEPT;
	static bool tryAddHeadSize(size_t srcSize, size_t &destSize) UTIL_NOEXCEPT;

	static Entry* findEntry(
			MemoryManager *manager, size_t index,
			DirectMutex *&mutex) UTIL_NOEXCEPT;
	static void updateStats(
			MemoryManager *manager, size_t size, size_t index,
			bool allocated) UTIL_NOEXCEPT;

	void adviceDeallocatedRange(void *ptr, size_t allocSize) UTIL_NOEXCEPT;
	static size_t resolvePageSize() UTIL_NOEXCEPT;

	static UnitStats& getUnitStats(
			MemoryManager *manager, size_t index) UTIL_NOEXCEPT;

	static void addSizeHistogram(
			MemoryManager *manager, size_t size, size_t count) UTIL_NOEXCEPT;

	Entry entryList_[ENTRY_COUNT];
	StatsList &statsListRef_;
	DirectMutex totalMutex_;
	size_t pageSize_;

#if UTIL_MEMORY_SIZE_HISTOGRAM_ENABLED
	SizeHistogram sizeHistogram_;
#endif
};

struct MemoryManager::Shared {
	static size_t initializerCounter_;
	static MemoryManager *instance_;
	static uint8_t storage_[sizeof(MemoryManager)];
	static StatsList statsList_;
};

MemoryManager* MemoryManager::findInstance() UTIL_NOEXCEPT {
	return Shared::instance_;
}

void* MemoryManager::allocate(
		MemoryManager *manager, size_t size, bool monitoring) UTIL_NOEXCEPT {
	size_t totalSize;
	if (!tryAddHeadSize(size, totalSize)) {
		return NULL;
	}

	const bool aligned = true;
	void *headPtr = allocateDirect(manager, totalSize, monitoring, aligned);
	if (headPtr == NULL) {
		return NULL;
	}

	UTIL_STATIC_ASSERT(sizeof(size_t) <= NORMAL_HEAD_SIZE);
	const size_t offset = NORMAL_HEAD_SIZE;

	*static_cast<size_t*>(headPtr) = totalSize;
	return static_cast<uint8_t*>(headPtr) + offset;
}

void MemoryManager::deallocate(
		MemoryManager *manager, void *ptr, bool monitoring) UTIL_NOEXCEPT {
	if (ptr == NULL) {
		return;
	}

	UTIL_STATIC_ASSERT(sizeof(size_t) <= NORMAL_HEAD_SIZE);
	const size_t offset = NORMAL_HEAD_SIZE;

	void *headPtr = static_cast<uint8_t*>(ptr) - offset;
	const size_t totalSize = *static_cast<size_t*>(headPtr);

	const bool aligned = true;
	deallocateDirect(manager, headPtr, totalSize, monitoring, aligned);
}

FreeLink* MemoryManager::allocateBulk(
		MemoryManager *manager, size_t size, bool monitoring, bool aligned,
		size_t &count) UTIL_NOEXCEPT {
	addSizeHistogram(manager, size, count);

	size_t index;
	const size_t allocSize =
			adjustAllocationSizeInternal(size, aligned, &index);
	UnitStats &stats = getUnitStats(manager, index);

	DirectMutex *mutex;
	Entry *entry = findEntry(manager, index, mutex);
	DynamicLockGuard<DirectMutex> guard(mutex);

	size_t allocCount = 0;
	FreeLink *link = NULL;
	for (size_t i = count; i > 0; i--) {
		FreeLink *sub = static_cast<FreeLink*>(allocateInternal(
				guard, allocSize, entry, stats, monitoring));
		if (sub == NULL) {
			break;
		}
		sub->next_ = link;
		link = sub;
		allocCount++;
	}

	count = allocCount;
	return link;
}

void MemoryManager::deallocateBulk(
		MemoryManager *manager, FreeLink *link, size_t size,
		bool monitoring, bool aligned) UTIL_NOEXCEPT {
	size_t index;
	const size_t allocSize =
			adjustAllocationSizeInternal(size, aligned, &index);
	UnitStats &stats = getUnitStats(manager, index);

	DirectMutex *mutex;
	Entry *entry = findEntry(manager, index, mutex);
	DynamicLockGuard<DirectMutex> guard(mutex);

	for (FreeLink *sub = link; sub != NULL;) {
		FreeLink *next = sub->next_;
		deallocateInternal(
				manager, guard, sub, allocSize, entry, stats, monitoring);
		sub = next;
	}
}

void* MemoryManager::allocateDirect(
		MemoryManager *manager, size_t size, bool monitoring,
		bool aligned) UTIL_NOEXCEPT {
	addSizeHistogram(manager, size, 1);

	size_t index;
	const size_t allocSize =
			adjustAllocationSizeInternal(size, aligned, &index);
	UnitStats &stats = getUnitStats(manager, index);

	DirectMutex *mutex;
	Entry *entry = findEntry(manager, index, mutex);
	DynamicLockGuard<DirectMutex> guard(mutex);

	return allocateInternal(guard, allocSize, entry, stats, monitoring);
}

void MemoryManager::deallocateDirect(
		MemoryManager *manager, void *ptr, size_t size, bool monitoring,
		bool aligned) UTIL_NOEXCEPT {
	assert(ptr != NULL);
	size_t index;
	const size_t allocSize =
			adjustAllocationSizeInternal(size, aligned, &index);
	UnitStats &stats = getUnitStats(manager, index);

	DirectMutex *mutex;
	Entry *entry = findEntry(manager, index, mutex);
	DynamicLockGuard<DirectMutex> guard(mutex);

	deallocateInternal(
			manager, guard, ptr, allocSize, entry, stats, monitoring);
}

size_t MemoryManager::adjustAllocationSize(
		size_t size, bool withHead, size_t *index) UTIL_NOEXCEPT {
	size_t totalSize = size;
	if (withHead && !tryAddHeadSize(size, totalSize)) {
		if (index != NULL) {
			*index = ENTRY_COUNT;
		}
		return size;
	}

	const bool aligned = true;
	const size_t alignedSize =
			adjustAllocationSizeInternal(totalSize, aligned, index);

	assert(alignedSize >= NORMAL_HEAD_SIZE);
	return alignedSize - NORMAL_HEAD_SIZE;
}

size_t MemoryManager::getUnitCount() UTIL_NOEXCEPT {
	return ENTRY_COUNT;
}

bool MemoryManager::getUnitProfile(
		MemoryManager *manager, size_t index, int64_t &totalSize,
		int64_t &cacheSize, int64_t &monitoringSize, int64_t &totalCount,
		int64_t &cacheCount, int64_t &deallocCount) UTIL_NOEXCEPT {
	int64_t *values[END_STAT] = {
			&totalSize, &cacheSize, &monitoringSize, &totalCount, &cacheCount,
			&deallocCount
	};

	UnitStats stats;
	const bool found = getUnitProfile(manager, index, stats);

	for (size_t i = 0; i < END_STAT; i++) {
		*(values[i]) = stats[i];
	}

	return found;
}

bool MemoryManager::getUnitProfile(
		MemoryManager *manager, size_t index, UnitStats &stats) UTIL_NOEXCEPT {
	if (index > ENTRY_COUNT) {
		for (size_t i = 0; i < END_STAT; i++) {
			stats[i] = 0;
		}
		return false;
	}

	bool found = false;
	const UnitStats &srcStats = getUnitStats(manager, index);
	for (size_t i = 0; i < END_STAT; i++) {
		if (srcStats[i] != 0) {
			found = true;
		}
		stats[i] = srcStats[i];
	}
	return found;
}

void MemoryManager::dumpStats(
		MemoryManager *manager, std::ostream &os) {
	const size_t count = getUnitCount();

	int64_t totalSize = 0;
	int64_t totalCount = 0;
	for (size_t i = 0; i < count; i++) {
		UnitStats stats;
		getUnitProfile(manager, i, stats);
		totalSize += stats[STAT_TOTAL_SIZE];
		totalCount += stats[STAT_TOTAL_COUNT];
	}

	os << totalSize << " [";
	for (size_t i = 0; i < count; i++) {
		UnitStats stats;
		getUnitProfile(manager, i, stats);
		int64_t size = stats[STAT_TOTAL_SIZE];
		if (size == 0) {
			continue;
		}
		os << " " << i << ":" << size;
	}
	os << " ] " << totalCount << std::endl;;
}

void MemoryManager::dumpSizeHistogram(
		MemoryManager *manager, std::ostream &os) {
#if UTIL_MEMORY_SIZE_HISTOGRAM_ENABLED
	if (manager == NULL) {
		return;
	}
	LockGuard<DirectMutex> guard(manager->totalMutex_);
	manager->sizeHistogram_.dump(os);
#else
	static_cast<void>(manager);
	static_cast<void>(os);
#endif
}

MemoryManager::MemoryManager(StatsList &statsListRef) UTIL_NOEXCEPT :
		statsListRef_(statsListRef),
		pageSize_(resolvePageSize()) {
}

MemoryManager::~MemoryManager() {
	unmanageAll();
}

void MemoryManager::unmanageAll() UTIL_NOEXCEPT {
	for (uint32_t i = 0; i < ENTRY_COUNT; i++) {
		Entry &entry = entryList_[i];

		for (FreeLink *&link = entry.freeLink_; link != NULL;) {
			FreeLink *next = link->next_;

			const size_t allocSize = (static_cast<size_t>(1) << i);
			DirectMemoryUtils::freeDirect(link);

			UnitStats &stats = getUnitStats(this, i);
			stats[STAT_TOTAL_SIZE] -= static_cast<int64_t>(allocSize);
			stats[STAT_TOTAL_COUNT]--;
			stats[STAT_CACHE_SIZE] -= static_cast<int64_t>(allocSize);
			stats[STAT_CACHE_COUNT]--;
			stats[STAT_DEALLOC_COUNT]++;

			link = next;
		}
	}
}

void* MemoryManager::allocateInternal(
		DynamicLockGuard<DirectMutex>&, size_t allocSize, Entry *entry,
		UnitStats &stats, bool monitoring) UTIL_NOEXCEPT {
	void *ptr = NULL;
	do {
		if (entry == NULL) {
			break;
		}

		FreeLink *&link = entry->freeLink_;
		if (link == NULL) {
			break;
		}

		stats[STAT_CACHE_SIZE] -= static_cast<int64_t>(allocSize);
		stats[STAT_CACHE_COUNT]--;

		ptr = link;
		link = link->next_;
	}
	while (false);

	if (ptr == NULL) {
		ptr = DirectMemoryUtils::mallocDirect(allocSize);
		if (ptr == NULL) {
			return NULL;
		}
		stats[STAT_TOTAL_SIZE] += static_cast<int64_t>(allocSize);
		stats[STAT_TOTAL_COUNT]++;
	}

	if (monitoring) {
		stats[STAT_MONITORING_SIZE] += static_cast<int64_t>(allocSize);
	}

	return ptr;
}

void MemoryManager::deallocateInternal(
		MemoryManager *manager, DynamicLockGuard<DirectMutex>&,
		void *ptr, size_t allocSize, Entry *entry, UnitStats &stats,
		bool monitoring) UTIL_NOEXCEPT {
	stats[STAT_DEALLOC_COUNT]++;
	if (monitoring) {
		stats[STAT_MONITORING_SIZE] -= static_cast<int64_t>(allocSize);
	}

	if (entry == NULL) {
		DirectMemoryUtils::freeDirect(ptr);
		stats[STAT_TOTAL_SIZE] -= static_cast<int64_t>(allocSize);
		stats[STAT_TOTAL_COUNT]--;
		return;
	}

	FreeLink *&link = entry->freeLink_;
	FreeLink *next = link;

	link = static_cast<FreeLink*>(ptr);
	link->next_ = next;

	if (next != NULL && manager != NULL && allocSize > manager->pageSize_) {
		manager->adviceDeallocatedRange(next, allocSize);
	}

	stats[STAT_CACHE_SIZE] += static_cast<int64_t>(allocSize);
	stats[STAT_CACHE_COUNT]++;
}

size_t MemoryManager::adjustAllocationSizeInternal(
		size_t size, bool aligned, size_t *index) UTIL_NOEXCEPT {
	size_t indexBase;
	size_t &indexRef = (index == NULL ? indexBase : *index);
	indexRef = ENTRY_COUNT;

	if (size > (static_cast<size_t>(1) << (ENTRY_COUNT - 1))) {
		return size;
	}

	const uint32_t baseSize =
			static_cast<uint32_t>(std::max<uint64_t>(size, sizeof(FreeLink)));
	const uint32_t bits = static_cast<uint32_t>(sizeof(uint32_t) * CHAR_BIT) -
			util::nlz(static_cast<uint32_t>(baseSize - 1));
	assert((static_cast<size_t>(1) << (bits - 1)) < baseSize &&
			baseSize <= (static_cast<size_t>(1) << bits));

	indexRef = bits;
	return (aligned ? (static_cast<size_t>(1) << bits) : size);
}

inline bool MemoryManager::tryAddHeadSize(
		size_t srcSize, size_t &destSize) UTIL_NOEXCEPT {
	if (srcSize > std::numeric_limits<size_t>::max() - NORMAL_HEAD_SIZE) {
		destSize = srcSize;
		return false;
	}
	destSize = srcSize + NORMAL_HEAD_SIZE;
	return true;
}

MemoryManager::Entry* MemoryManager::findEntry(
		MemoryManager *manager, size_t index,
		DirectMutex *&mutex) UTIL_NOEXCEPT {
	mutex = (manager == NULL ? NULL : &manager->totalMutex_);

	if (manager == NULL || index >= ENTRY_COUNT) {
		return NULL;
	}

	Entry &entry = manager->entryList_[index];
	mutex = &entry.mutex_;
	return &entry;
}

void MemoryManager::adviceDeallocatedRange(
		void *ptr, size_t allocSize) UTIL_NOEXCEPT {
#ifdef _WIN32
	static_cast<void>(ptr);
	static_cast<void>(allocSize);
#else
	const uintptr_t alignedBegin = (reinterpret_cast<uintptr_t>(ptr) +
			sizeof(FreeLink*) + pageSize_ - 1) / pageSize_ * pageSize_;

	const uintptr_t ptrEnd = reinterpret_cast<uintptr_t>(ptr) + allocSize;
	const uintptr_t alignedEndBase =
			(ptrEnd + pageSize_ - 1) / pageSize_ * pageSize_;
	const uintptr_t alignedEnd =
			alignedEndBase - (ptrEnd < alignedEndBase ? pageSize_ : 0);

	if (alignedBegin < alignedEnd) {
		madvise(
				reinterpret_cast<void*>(alignedBegin),
				static_cast<size_t>(alignedEnd - alignedBegin),
				MADV_DONTNEED);
	}
#endif
}

size_t MemoryManager::resolvePageSize() UTIL_NOEXCEPT {
#ifndef _WIN32
	const long size = sysconf(_SC_PAGESIZE);
	if (size > 0) {
		return static_cast<size_t>(size);
	}
#endif
	return std::numeric_limits<size_t>::max();
}

MemoryManager::UnitStats& MemoryManager::getUnitStats(
		MemoryManager *manager, size_t index) UTIL_NOEXCEPT {
	assert(index <= ENTRY_COUNT);

	UnitStats &statValue = (manager == NULL ?
			Shared::statsList_ : manager->statsListRef_)[index];

	return statValue;
}

inline void MemoryManager::addSizeHistogram(
		MemoryManager *manager, size_t size, size_t count) UTIL_NOEXCEPT {
#if UTIL_MEMORY_SIZE_HISTOGRAM_ENABLED
	if (manager == NULL) {
		return;
	}
	LockGuard<DirectMutex> guard(manager->totalMutex_);
	manager->sizeHistogram_.add(size, count);
#else
	static_cast<void>(manager);
	static_cast<void>(size);
	static_cast<void>(count);
#endif
}

size_t MemoryManager::Shared::initializerCounter_ = 0;
MemoryManager *MemoryManager::Shared::instance_ = NULL;
uint8_t MemoryManager::Shared::storage_[sizeof(MemoryManager)];
MemoryManager::StatsList MemoryManager::Shared::statsList_ = { { 0 } };

MemoryManager::Entry::Entry() UTIL_NOEXCEPT :
		freeLink_(NULL),
		subList_(NULL),
		subCount_(0) {
}

MemoryManager::SizeHistogram::SizeHistogram() UTIL_NOEXCEPT :
		histogramSize_(0) {
}

void MemoryManager::SizeHistogram::add(
		size_t size, size_t count) UTIL_NOEXCEPT {
	const HistogramEntry value(size, 0);
	HistogramEntry *begin = histogramEntries_;
	HistogramEntry *end = begin + histogramSize_;
	HistogramEntry *it = std::lower_bound(begin, end, value);
	if (it == end || it->first != value.first) {
		if (histogramSize_ >= HISTOGRAM_CAPACITY) {
			return;
		}
		histogramSize_++;

		for (HistogramEntry *sub = end; sub != it; --sub) {
			*sub = *(sub - 1);
		}
		*it = value;
	}
	it->second += count;
}

void MemoryManager::SizeHistogram::dump(std::ostream &os) UTIL_NOEXCEPT {
	HistogramEntry *begin = histogramEntries_;
	HistogramEntry *end = begin + histogramSize_;
	uint64_t base = 0;
	bool lineStarted = false;
	for (HistogramEntry *it = begin;; ++it) {
		const bool done = (it == end);
		bool baseUpdated = false;
		const uint64_t size = (done ? 0 : it->first);
		while (size >= (static_cast<uint64_t>(1) << (base + 1))) {
			if (base > sizeof(uint64_t) * CHAR_BIT) {
				break;
			}
			base++;
			baseUpdated = true;
		}
		if (lineStarted && (done || baseUpdated)) {
			os << " ]" << std::endl;
		}
		if (done) {
			break;
		}
		if (!lineStarted || baseUpdated) {
			os << "2^" << base << ": [";
			lineStarted = true;
		}
		os << " +" << (size - (static_cast<size_t>(1) << base));
		os << ":" << it->second;
	}
}


MemoryManagerInitializer::MemoryManagerInitializer() UTIL_NOEXCEPT {
	typedef MemoryManager::Shared Shared;
	if (++Shared::initializerCounter_ == 1) {
		assert(Shared::instance_ == NULL);
		Shared::instance_ =
				new (Shared::storage_) MemoryManager(Shared::statsList_);
	}
}

MemoryManagerInitializer::~MemoryManagerInitializer() {
	typedef MemoryManager::Shared Shared;
	if (--Shared::initializerCounter_ == 0) {
		assert(Shared::instance_ != NULL);
		Shared::instance_->~MemoryManager();
		Shared::instance_ = NULL;
	}
}

} 


bool Exception::whatEnabled_ = false;

const Exception::DuplicatedLiteralFlags Exception::LITERAL_NORMAL = 0;

const Exception::DuplicatedLiteralFlags Exception::LITERAL_ALL_DUPLICATED =
		(1U << FIELD_ERROR_CODE_NAME) |
		(1U << FIELD_FILE_NAME) |
		(1U << FIELD_FUNCTION_NAME) |
		(1U << FIELD_TYPE_NAME);

Exception::Exception(
		const NamedErrorCode &namedErrorCode,
		const char8_t *message,
		const SourceSymbolChar *fileNameLiteral,
		const SourceSymbolChar *functionNameLiteral,
		int32_t lineNumber,
		const std::exception *causeInHandling,
		const char8_t *typeNameLiteral,
		StackTraceMode stackTraceMode,
		DuplicatedLiteralFlags literalFlags) throw() :
		bufferOffset_(0),
		subEntries_(NULL),
		maxDepth_(0),
		topEntry_(),
		what_(NULL) {

	do {
#ifdef UTIL_STACK_TRACE_ENABLED
		if (causeInHandling == NULL && stackTraceMode == STACK_TRACE_TOP) {
			try {
				char8_t buf[512];
				detail::LocalString str(buf, sizeof(buf));

				detail::StackTraceStringHandler handler(str);
				StackTraceUtils::getStackTrace(handler);

				setEntry(topEntry_,
						namedErrorCode, message, str.tryGet(), typeNameLiteral,
						fileNameLiteral, functionNameLiteral, lineNumber,
						literalFlags);
				break;
			}
			catch (...) {
			}
		}
#endif
		setEntry(topEntry_,
				namedErrorCode, message, NULL, typeNameLiteral,
				fileNameLiteral, functionNameLiteral, lineNumber,
				literalFlags);
	}
	while (false);

	if (causeInHandling != NULL) {
		Exception *fullEx;
		std::exception *stdEx;
		const char8_t *causeType = resolveException(&fullEx, &stdEx);

		size_t subEntryCount = (fullEx == NULL ? 1 : fullEx->maxDepth_ + 1);
		subEntries_ =
				static_cast<Entry*>(allocate(sizeof(Entry) * subEntryCount));
		if (subEntries_ == NULL) {
			subEntryCount = 0;
		}
		maxDepth_ = subEntryCount;

		if (fullEx == NULL) {
			if (subEntryCount > 0) {
				const char8_t *causeMessage = NULL;
				if (stdEx != NULL) {
					try {
						causeMessage = stdEx->what();
					}
					catch (...) {
					}
				}

				setEntry(subEntries_[0],
						NamedErrorCode(), causeMessage, NULL, causeType,
						NULL, NULL, -1, LITERAL_NORMAL);
			}
		}
		else {
			for (size_t i = 0; i < subEntryCount; i++) {
				Entry &dest = subEntries_[i];
				const Entry *src = fullEx->getEntryAt(i);

				setEntry(dest, *src);
			}
		}
	}

	fillWhat();
}

Exception::~Exception() throw() {
	clear();
}

Exception::Exception(const Exception &another) throw() :
		bufferOffset_(0),
		subEntries_(NULL),
		maxDepth_(0),
		topEntry_(),
		what_(NULL) {
	append(another, 0);
}

Exception& Exception::operator=(const Exception &another) throw() {
	assign(another, 0);
	return *this;
}

void Exception::assign(const Exception &another, size_t startDepth) throw() {
	if (this == &another) {
		if (startDepth > 0) {
			Exception base(*this);
			clear();
			append(base, startDepth);
		}
		return;
	}
	clear();
	append(another, startDepth);
}

void Exception::append(const Exception &another, size_t startDepth) throw() {
	Exception base;

	if (this == &another) {
		base.append(another, startDepth);
		append(base, 0);
		return;
	}

	size_t baseEntryCount = 0;
	if (!isEmpty()) {
		assert(base.isEmpty());

		base.append(*this, 0);
		baseEntryCount = 1 + base.maxDepth_;

		clear();
	}

	const size_t anotherDepth = another.maxDepth_;
	const size_t anotherStartDepth =
			static_cast<size_t>(std::min<uint64_t>(anotherDepth, startDepth));

	size_t subEntryCount = baseEntryCount + (anotherDepth - anotherStartDepth);
	if (subEntryCount > 0) {
		subEntries_ =
				static_cast<Entry*>(allocate(sizeof(Entry) * subEntryCount));
		if (subEntries_ == NULL) {
			subEntryCount = 0;
		}
	}
	maxDepth_ = subEntryCount;

	for (size_t i = 0; i <= subEntryCount; i++) {
		Entry &dest = (i > 0 ? subEntries_[i - 1] : topEntry_);

		const Entry *src;
		if (i < baseEntryCount) {
			src = base.getEntryAt(i);
		}
		else {
			src = another.getEntryAt(anotherStartDepth + (i - baseEntryCount));
		}

		setEntry(dest, *src);
	}

	fillWhat();
}

bool Exception::isEmpty() const throw() {
	return (maxDepth_ == 0 && topEntry_.isEmpty(true));
}

size_t Exception::getMaxDepth() const throw() {
	return maxDepth_;
}

void Exception::format(std::ostream &stream) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	for (size_t i = 0; i <= maxDepth_; i++) {
		if (i > 0) {
			stream << "    by ";
		}
		formatEntry(stream, i);
		stream << std::endl;
	}
}

void Exception::formatField(
		std::ostream &stream, FieldType fieldType, size_t depth) const {

	switch (fieldType) {
	case FIELD_ERROR_CODE:
		{
			LocaleUtils::CLocaleScope localeScope(stream);
			stream << getErrorCode(depth);
		}
		break;
	case FIELD_ERROR_CODE_NAME:
		formatErrorCodeName(stream, depth);
		break;
	case FIELD_MESSAGE:
		formatMessage(stream, depth);
		break;
	case FIELD_FILE_NAME:
		formatFileName(stream, depth);
		break;
	case FIELD_FUNCTION_NAME:
		formatFunctionName(stream, depth);
		break;
	case FIELD_LINE_NUMBER:
		{
			LocaleUtils::CLocaleScope localeScope(stream);
			stream << getLineNumber(depth);
		}
		break;
	case FIELD_STACK_TRACE:
		formatStackTrace(stream, depth);
		break;
	case FIELD_TYPE_NAME:
		formatTypeName(stream, depth);
		break;
	default:
		assert(false);
		break;
	}
}

Exception::Field Exception::getField(
		FieldType fieldType, size_t depth) const throw() {
	return Field(*this, fieldType, depth);
}

int32_t Exception::getErrorCode(size_t depth) const throw() {
	const Entry *entry = getEntryAt(depth);
	if (entry == NULL) {
		return 0;
	}

	return entry->namedErrorCode_.getCode();
}

Exception::NamedErrorCode Exception::getNamedErrorCode(
		size_t depth) const throw() {
	const Entry *entry = getEntryAt(depth);
	if (entry == NULL) {
		return NamedErrorCode();
	}

	return entry->namedErrorCode_;
}

void Exception::formatEntry(std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	if (depth > maxDepth_) {
		return;
	}

	if (hasTypeName(depth)) {
		formatTypeName(stream, depth);
	}
	else {
		stream << "(Unknown exception)";
	}

	if (hasFileName(depth)) {
		stream << " ";
		formatFileName(stream, depth);
	}

	if (hasFunctionName(depth)) {
		stream << " ";
		formatFunctionName(stream, depth);
	}

	if (hasLineNumber(depth)) {
		stream << " line=" << getLineNumber(depth);
	}

	if (hasErrorCode(depth)) {
		stream << " [";

		if (!hasErrorCodeName(depth)) {
			stream << "Code:";
		}

		stream << getErrorCode(depth);

		if (hasErrorCodeName(depth)) {
			stream << ":";
			formatErrorCodeName(stream, depth);
		}

		stream << "]";
	}

	if (hasMessage(depth)) {
		stream << " " ;
		formatMessage(stream, depth);
	}
#ifdef UTIL_STACK_TRACE_ENABLED

	if (hasStackTrace(depth)) {
		stream << " : " ;
		formatStackTrace(stream, depth);
	}
#endif
}

void Exception::formatErrorCodeName(std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	const Entry *entry = getEntryAt(depth);
	if (entry == NULL || entry->namedErrorCode_.getName() == NULL) {
		return;
	}

	stream << entry->namedErrorCode_.getName();
}

void Exception::formatMessage(std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	const Entry *entry = getEntryAt(depth);
	if (entry == NULL || entry->message_ == NULL) {
		return;
	}

	stream << entry->message_;
}

#ifdef UTIL_STACK_TRACE_ENABLED
void Exception::formatStackTrace(std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	const Entry *entry = getEntryAt(depth);
	if (entry == NULL || entry->stackTrace_ == NULL) {
		return;
	}

	stream << entry->stackTrace_;
}
#endif

void Exception::formatTypeName(std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	const Entry *entry = getEntryAt(depth);
	if (entry == NULL || entry->typeName_ == NULL) {
		return;
	}

	stream << entry->typeName_;
}

void Exception::formatFileName(std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	const Entry *entry = getEntryAt(depth);
	if (entry == NULL || entry->fileName_ == NULL) {
		return;
	}

	stream << entry->fileName_;
}

void Exception::formatFunctionName(
		std::ostream &stream, size_t depth) const {
	LocaleUtils::CLocaleScope localeScope(stream);

	const Entry *entry = getEntryAt(depth);
	if (entry == NULL || entry->functionName_ == NULL) {
		return;
	}

	stream << entry->functionName_;
}

int32_t Exception::getLineNumber(size_t depth) const throw() {
	const Entry *entry = getEntryAt(depth);
	if (entry == NULL) {
		return -1;
	}

	return entry->lineNumber_;
}

bool Exception::hasErrorCode(size_t depth) const throw() {
	return (depth <= maxDepth_ &&
			!getEntryAt(depth)->namedErrorCode_.isEmpty());
}

bool Exception::hasErrorCodeName(size_t depth) const throw() {
	return (depth <= maxDepth_ &&
			getEntryAt(depth)->namedErrorCode_.getName() != NULL);
}

bool Exception::hasMessage(size_t depth) const throw() {
	return (depth <= maxDepth_ && getEntryAt(depth)->message_ != NULL);
}

#ifdef UTIL_STACK_TRACE_ENABLED
bool Exception::hasStackTrace(size_t depth) const throw() {
	return (depth <= maxDepth_ && getEntryAt(depth)->stackTrace_ != NULL);
}
#endif

bool Exception::hasTypeName(size_t depth) const throw() {
	return (depth <= maxDepth_ && getEntryAt(depth)->typeName_ != NULL);
}

bool Exception::hasFileName(size_t depth) const throw() {
	return (depth <= maxDepth_ && getEntryAt(depth)->fileName_ != NULL);
}

bool Exception::hasFunctionName(size_t depth) const throw() {
	return (depth <= maxDepth_ && getEntryAt(depth)->functionName_ != NULL);
}

bool Exception::hasLineNumber(size_t depth) const throw() {
	return (depth <= maxDepth_ && getEntryAt(depth)->lineNumber_ > 0);
}

Exception::DuplicatedLiteralFlags Exception::inheritLiteralFlags(
		DuplicatedLiteralFlags baseFlags, size_t depth) const throw() {
	if (depth <= maxDepth_) {
		return (getEntryAt(depth)->literalFlags_ & baseFlags);
	}
	else {
		return LITERAL_NORMAL;
	}
}

void* Exception::allocate(size_t size) throw() {
	if (size == 0) {
		return NULL;
	}
	else if (bufferOffset_ + size <= sizeof(buffer_)) {
		void *ptr = buffer_ + bufferOffset_;
		bufferOffset_ += size;
		return ptr;
	}
	else {
		return UTIL_MALLOC(size);
	}
}

void Exception::deallocate(void *ptr) throw() {
	uint8_t *bytePtr = static_cast<uint8_t*>(ptr);
	if (bytePtr < buffer_ || buffer_ + sizeof(buffer_) <= bytePtr) {
		UTIL_FREE(bytePtr);
	}
}

void Exception::deallocateLiteral(const char8_t *literalStr,
		FieldType fieldType, DuplicatedLiteralFlags literalFlags) throw() {
	if ((literalFlags & (1U << fieldType)) == 0) {
		return;
	}
	deallocate(const_cast<char8_t*>(literalStr));
}

char8_t* Exception::tryCopyString(const char8_t *str) throw() {
	if (str == NULL) {
		return NULL;
	}

	const size_t size = strlen(str);
	if (size == 0) {
		return NULL;
	}

	char8_t *destStr = static_cast<char8_t*>(allocate(size + 1));
	if (destStr == NULL) {
		return NULL;
	}

	memcpy(destStr, str, size + 1);
	return destStr;
}

const char8_t* Exception::tryCopyLiteral(const char8_t *literalStr,
		FieldType fieldType, DuplicatedLiteralFlags literalFlags) throw() {

	if ((literalFlags & (1U << fieldType)) == 0) {
		return literalStr;
	}

	return tryCopyString(literalStr);
}

Exception::NamedErrorCode Exception::tryCopyNamedErrorCode(
		const NamedErrorCode &src,
		DuplicatedLiteralFlags literalFlags) throw() {
	const SourceSymbolChar *name = tryCopyLiteral(
			src.getName(), FIELD_ERROR_CODE_NAME, literalFlags);

	return NamedErrorCode(src.getCode(), name);
}

void Exception::clear() throw() {
	for (size_t i = 0; i < maxDepth_; i++) {
		clearEntry(subEntries_[i]);
	}
	deallocate(subEntries_);
	subEntries_ = NULL;
	maxDepth_ = 0;

	clearEntry(topEntry_);

	deallocate(what_);
	what_ = NULL;

	bufferOffset_ = 0;
}

const char* Exception::what() const throw() {
	if (what_ != NULL) {
		return what_;
	}
	else if (topEntry_.typeName_ != NULL) {
		return topEntry_.typeName_;
	}

	return std::exception::what();
}

void Exception::enableWhat(bool enabled) throw() {
	whatEnabled_ = enabled;
}

Exception::NamedErrorCode Exception::makeNamedErrorCode(
		int32_t code,
		const SourceSymbolChar *nameLiteral,
		const SourceSymbolChar *nameLiteralPrefix) throw() {
	assert(nameLiteral != NULL);
	assert(nameLiteralPrefix != NULL);

	if (code == 0) {
		return NamedErrorCode();
	}
	else if (strstr(nameLiteral, nameLiteralPrefix) == nameLiteral) {
		return NamedErrorCode(code, nameLiteral + strlen(nameLiteralPrefix));
	}
	else {
		return NamedErrorCode(code, nameLiteral);
	}
}

const Exception::Entry* Exception::getEntryAt(size_t depth) const throw() {
	if (depth == 0) {
		return &topEntry_;
	}
	else if (depth <= maxDepth_) {
		return &subEntries_[depth - 1];
	}
	else {
		return NULL;
	}
}

const char8_t* Exception::resolveException(
		Exception **fullEx, std::exception **stdEx) throw() {
	*fullEx = NULL;
	*stdEx = NULL;

	try {
		throw;
	}
	catch (Exception &e) {
		*fullEx = &e;
		return e.topEntry_.typeName_;
	}
	catch (std::exception &e) {
		*stdEx = &e;
		try {
			throw;
		}
		catch (std::domain_error&) {
			return "std::domain_error";
		}
		catch (std::invalid_argument&) {
			return "std::invalid_argument";
		}
		catch (std::length_error&) {
			return "std::length_error";
		}
		catch (std::out_of_range&) {
			return "std::out_of_range";
		}
		catch (std::range_error&) {
			return "std::range_error";
		}
		catch (std::overflow_error&) {
			return "std::overflow_error";
		}
		catch (std::underflow_error&) {
			return "std::underflow_error";
		}
		catch (std::logic_error&) {
			return "std::logic_error";
		}
		catch (std::runtime_error&) {
			return "std::runtime_error";
		}
		catch (std::bad_alloc&) {
			return "std::bad_alloc";
		}
		catch (...) {
		}
		return "std::exception";
	}
	catch (...) {
	}
	return NULL;
}

const char8_t* Exception::resolveTypeName(const char8_t *customName) throw() {
	if (customName == NULL) {
		return "util::Exception";
	}
	else {
		return customName;
	}
}

void Exception::fillWhat() throw() {
	if (!whatEnabled_ || what_ != NULL) {
		return;
	}

	try {
		util::NormalOStringStream oss;
		format(oss);
		what_ = tryCopyString(oss.str().c_str());
	}
	catch (...) {
	}
}

void Exception::setEntry(Entry &dest, const Entry &src) throw() {
	setEntry(
			dest,
			src.namedErrorCode_,
			src.message_,
			src.stackTrace_,
			src.typeName_,
			src.fileName_,
			src.functionName_,
			src.lineNumber_,
			src.literalFlags_);
}

void Exception::setEntry(Entry &entry,
		const NamedErrorCode &namedErrorCode,
		const char8_t *message,
		const char8_t *stackTrace,
		const char8_t *typeNameLiteral,
		const SourceSymbolChar *fileNameLiteral,
		const SourceSymbolChar *functionNameLiteral,
		int32_t lineNumber,
		DuplicatedLiteralFlags literalFlags) throw() {

	entry.namedErrorCode_ =
			tryCopyNamedErrorCode(namedErrorCode, literalFlags);
	entry.message_ = tryCopyString(message);
#ifdef UTIL_STACK_TRACE_ENABLED
	entry.stackTrace_ = tryCopyString(stackTrace);
#endif
	entry.typeName_ = tryCopyLiteral(
			typeNameLiteral, FIELD_TYPE_NAME, literalFlags);
	entry.fileName_ = tryCopyLiteral(
			fileNameLiteral, FIELD_FILE_NAME, literalFlags);
	entry.functionName_ = tryCopyLiteral(
			functionNameLiteral, FIELD_FUNCTION_NAME, literalFlags);
	entry.lineNumber_ = lineNumber;

	entry.literalFlags_ = literalFlags;
}

void Exception::clearEntry(Entry &entry) throw() {
	deallocate(entry.message_);
#ifdef UTIL_STACK_TRACE_ENABLED
	deallocate(entry.stackTrace_);
#endif

	deallocateLiteral(entry.namedErrorCode_.getName(),
			FIELD_ERROR_CODE_NAME, entry.literalFlags_);
	deallocateLiteral(entry.typeName_,
			FIELD_TYPE_NAME, entry.literalFlags_);
	deallocateLiteral(entry.fileName_,
			FIELD_FILE_NAME, entry.literalFlags_);
	deallocateLiteral(entry.functionName_,
			FIELD_FUNCTION_NAME, entry.literalFlags_);

	entry = Entry();
}


Exception::NamedErrorCode::NamedErrorCode(
		int32_t code, const SourceSymbolChar *nameLiteral) throw() :
		code_(code), name_(code == 0 ? NULL : nameLiteral) {
}


Exception::Entry::Entry() throw() :
		message_(NULL),
#ifdef UTIL_STACK_TRACE_ENABLED
		stackTrace_(NULL),
#endif
		typeName_(NULL),
		fileName_(NULL),
		functionName_(NULL),
		lineNumber_(0),
		literalFlags_(LITERAL_NORMAL) {
}

bool Exception::Entry::isEmpty(bool typeNameIgnorable) const throw() {
	return (namedErrorCode_.isEmpty() &&
			message_ == NULL &&
#ifdef UTIL_STACK_TRACE_ENABLED
			stackTrace_ == NULL &&
#endif
			(typeNameIgnorable || typeName_ == NULL) &&
			fileName_ == NULL &&
			functionName_ == NULL &&
			lineNumber_ == 0 &&
			literalFlags_ == LITERAL_NORMAL);
}


template<typename Alloc>
Exception::NoThrowString<Alloc>::NoThrowString(
		Stream &stream) throw() : str_(NULL) {
	try {
		str_ = new(static_cast<void*>(&storage_)) String(stream.str());
	}
	catch (...) {
	}
}

template<typename Alloc>
Exception::NoThrowString<Alloc>::~NoThrowString() throw() {
	if (str_ != NULL) {
		str_->~basic_string();
	}
}

template<typename Alloc>
const char8_t* Exception::NoThrowString<Alloc>::get() const throw() {
	return (str_ == NULL ? NULL : str_->c_str());
}

template struct Exception::NoThrowString<util::NormalOStringStream::allocator_type>;


Exception::Field::Field(
		const Exception &exception, FieldType fieldType, size_t depth) throw() :
		exception_(exception), fieldType_(fieldType), depth_(depth) {
}

void Exception::Field::format(std::ostream &s) const {
	exception_.formatField(s, fieldType_, depth_);
}

std::ostream& operator<<(std::ostream &s, const Exception::Field &field) {
	field.format(s);
	return s;
}

Exception::Field::Field(const Field &field) :
		exception_(field.exception_),
		fieldType_(field.fieldType_),
		depth_(field.depth_) {
}


PlatformException::PlatformException(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_LIST) throw() :
		Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {
}

PlatformException::~PlatformException() throw() {
}


UtilityException::UtilityException(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_LIST) throw() :
		Exception(UTIL_EXCEPTION_CONSTRUCTOR_ARGS_SET) {
}

UtilityException::~UtilityException() throw() {
}

UtilityException UtilityException::inherit(
		UTIL_EXCEPTION_CONSTRUCTOR_ARGS_LIST) throw() {

	Exception cause;
	Exception::NamedErrorCode modErrorCode;

	try {
		throw;
	}
	catch (Exception &e) {
		modErrorCode = e.getNamedErrorCode();
		cause = e;
	}
	catch (std::bad_alloc&) {
		modErrorCode = UTIL_EXCEPTION_UTIL_NAMED_CODE(CODE_NO_MEMORY);
	}
	catch (...) {
		modErrorCode = UTIL_EXCEPTION_UTIL_NAMED_CODE(CODE_ILLEGAL_OPERATION);
	}

	return UtilityException(
			(namedErrorCode.isEmpty() ? modErrorCode : namedErrorCode),
			(message == NULL || strlen(message) == 0) ?
					UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(
							cause.getField(Exception::FIELD_MESSAGE)) :
					message,
			fileNameLiteral, functionNameLiteral, lineNumber,
			causeInHandling, typeNameLiteral, stackTraceMode, literalFlags);
}

} 


namespace util {
namespace detail {
const char8_t* RawNumberFormatter::operator()(uint64_t value) {
#ifdef _MSC_VER
	 _snprintf_s(result_, sizeof(result_), _TRUNCATE, "%llu",
			static_cast<unsigned long long int>(value));
#else
	 snprintf(result_, sizeof(result_), "%llu",
			static_cast<unsigned long long int>(value));
#endif
	 return result_;
}

const char8_t* RawNumberFormatter::operator()(int64_t value) {
#ifdef _MSC_VER
	 _snprintf_s(result_, sizeof(result_), _TRUNCATE, "%lld",
			static_cast<long long int>(value));
#else
	 snprintf(result_, sizeof(result_), "%lld",
			static_cast<long long int>(value));
#endif
	 return result_;
}

const char8_t* RawNumberFormatter::operator()(uint32_t value) {
	return (*this)(static_cast<uint64_t>(value));
}

const char8_t* RawNumberFormatter::operator()(int32_t value) {
	return (*this)(static_cast<int64_t>(value));
}
}	
}	


namespace util {
namespace detail {
LocalString::LocalString(char8_t *localBuf, size_t capacity) throw() :
		size_(0),
		capacity_(capacity),
		localBuf_(localBuf),
		dynamicBuf_(NULL) {

	if (!tryAppend("")) {
		capacity_ = 0;
		localBuf_ = NULL;
	}
}

LocalString::~LocalString() {
	UTIL_FREE(dynamicBuf_);
}

bool LocalString::tryAppend(const char8_t *value) throw() {
	return tryAppend(value, value + strlen(value));
}

bool LocalString::tryAppend(const char8_t *begin, const char8_t *end) throw() {
	assert(begin <= end);
	const size_t len = static_cast<size_t>(end - begin);

	char8_t *buf = (localBuf_ == NULL ? dynamicBuf_ : localBuf_);
	if (size_ + len >= capacity_ || size_ + len < len) {
		const size_t newCapacity = std::max<size_t>(
				std::max<size_t>(capacity_ * 2, 64), size_ + len + 1);
		if (newCapacity <= size_ || newCapacity <= len) {
			return false;
		}

		char8_t *newBuf = static_cast<char8_t*>(UTIL_MALLOC(newCapacity));
		if (newBuf == NULL) {
			return false;
		}

		memcpy(newBuf, buf, size_);
		UTIL_FREE(dynamicBuf_);

		capacity_ = newCapacity;
		localBuf_ = NULL;
		buf = dynamicBuf_ = newBuf;
	}

	memcpy(buf + size_, begin, len);
	size_ += len;
	buf[size_] = '\0';

	return true;
}

const char8_t* LocalString::tryGet(const char8_t *alternative) throw() {
	char8_t *buf = (localBuf_ == NULL ? dynamicBuf_ : localBuf_);
	return (buf == NULL ? alternative : buf);
}
}	
}	

#ifdef UTIL_STACK_TRACE_ENABLED


namespace util {
struct StackTraceUtils::Impl {
	Impl();
	~Impl();

#ifdef _WIN32
	void getSymbolName(StackTraceHandler &handler, void *address);
#else
	void getSymbolName(StackTraceHandler &handler, char8_t *rawSymbol);
#endif

	std::ios_base::Init iosInit_;

	util::Mutex mutex_;

#ifdef _WIN32
	typedef USHORT (WINAPI *CaptureStackBackTraceType)(
			__in ULONG, __in ULONG, __out PVOID*, __out_opt PULONG);

	bool symInitializeSucceeded_;
	CaptureStackBackTraceType captureStackBackTraceFunc_;
#endif
};
}	


namespace util {
StackTraceUtils::Impl *StackTraceUtils::impl_ = NULL;

void StackTraceUtils::getStackTrace(StackTraceHandler &handler) {
	if (impl_ == NULL) {
		return;
	}

	const size_t maxCallstackDepth = 30;
	void *stack[maxCallstackDepth];

	DynamicLockGuard<util::Mutex> guard(
			impl_ == NULL ? NULL : &impl_->mutex_);

#ifdef _WIN32
	if (impl_->captureStackBackTraceFunc_ == NULL) {
		return;
	}

	const int skipSize = 3;
	int callStackSize = impl_->captureStackBackTraceFunc_(
			skipSize, maxCallstackDepth, stack, NULL);

	for(int i = 0; i < callStackSize; ++i) {
		impl_->getSymbolName(handler, stack[i]);
	}
#else
	int callStackSize = backtrace(stack, maxCallstackDepth);
	char **symbols = backtrace_symbols(stack, callStackSize);
	if (symbols == NULL) {
		return;
	}

	try {
		for (int i = 0; i < callStackSize; i++) {
			impl_->getSymbolName(handler, symbols[i]);
		}
	}
	catch (...) {
		detail::DirectMemoryUtils::freeDirect(symbols);
		throw;
	}
	detail::DirectMemoryUtils::freeDirect(symbols);
#endif
}

std::ostream& StackTraceUtils::getStackTrace(std::ostream &stream) {
	LocaleUtils::CLocaleScope localeScope(stream);

	char8_t buf[512];
	detail::LocalString str(buf, sizeof(buf));

	detail::StackTraceStringHandler handler(str);
	getStackTrace(handler);

	stream << str.tryGet();

	return stream;
}
}	


namespace util {
const std::locale *LocaleUtils::cLocale_ = NULL;

void LocaleUtils::CLocaleScope::set(std::ios_base &baseStream) {
	assert(orgLocale_ == NULL);

	orgLocale_ = new (static_cast<void*>(&orgLocaleStorage_)) std::locale(
			baseStream.imbue(getCLocale()));
	baseStream_ = &baseStream;
}

void LocaleUtils::CLocaleScope::unset() throw() {
	assert(baseStream_ != NULL);
	assert(orgLocale_ != NULL);

	try {
		baseStream_->imbue(*orgLocale_);
	}
	catch (...) {
	}
	orgLocale_->~locale();
	orgLocale_ = NULL;
}

LocaleUtils::Initializer::Initializer() {
	cLocale_ = &std::locale::classic();
}

LocaleUtils::Initializer::~Initializer() {
}
}	


namespace util {
NormalOStringStream::~NormalOStringStream() {
}
}	


namespace util {
NormalIStringStream::~NormalIStringStream() {
}
}	


namespace util {
StackTraceUtils::Impl::Impl()
#ifdef _WIN32
		: symInitializeSucceeded_(false),
		captureStackBackTraceFunc_(NULL)
#endif
{
#ifdef _WIN32
	if (::SymInitialize(::GetCurrentProcess(), NULL, TRUE)) {
		symInitializeSucceeded_ = true;
		::SymSetOptions(SYMOPT_DEFERRED_LOADS | SYMOPT_LOAD_LINES);
	}

	HINSTANCE kernel32 = LoadLibraryW(L"Kernel32.dll");
	if (!kernel32) {
		return;
	}

	CaptureStackBackTraceType func =
			reinterpret_cast<CaptureStackBackTraceType>(
			GetProcAddress(kernel32, "RtlCaptureStackBackTrace"));
	if (!func) {
		return;
	}

	captureStackBackTraceFunc_ = func;
#endif
}

StackTraceUtils::Impl::~Impl() {
#ifdef _WIN32
	if (symInitializeSucceeded_) {
		::SymCleanup(::GetCurrentProcess());
	}
#endif
}

#ifdef _WIN32
void StackTraceUtils::Impl::getSymbolName(
		StackTraceHandler &handler, void *address) {
#ifdef _WIN64
	typedef DWORD64 DWORDX;
#else
	typedef DWORD DWORDX;
#endif

	HANDLE process = ::GetCurrentProcess();
	if (!process) {
		handler("", -1);
		return;
	}

	IMAGEHLP_MODULE imageModule = { sizeof(IMAGEHLP_MODULE) };
	IMAGEHLP_LINE line ={ sizeof(IMAGEHLP_LINE) };
	DWORDX dispSym = 0;
	DWORD dispLine = 0;

	char symbolBuffer[sizeof(IMAGEHLP_SYMBOL) + MAX_PATH] = {0};
	IMAGEHLP_SYMBOL * imageSymbol = (IMAGEHLP_SYMBOL*) symbolBuffer;
	imageSymbol->SizeOfStruct = sizeof(IMAGEHLP_SYMBOL);
	imageSymbol->MaxNameLength = MAX_PATH;

	const DWORDX intAddress =
			static_cast<DWORDX>(reinterpret_cast<uintptr_t>(address));
	if(!SymGetModuleInfo(process, intAddress, &imageModule)) {
		util::detail::RawNumberFormatter formatter;
		handler(formatter(static_cast<uint64_t>(intAddress)), -1);
	}
	else if(!SymGetSymFromAddr(process, intAddress, &dispSym, imageSymbol)) {
		util::detail::RawNumberFormatter formatter;
		handler(formatter(static_cast<uint64_t>(intAddress)), -1);
	}
	else if(!SymGetLineFromAddr(process, intAddress, &dispLine, &line)) {
		handler(imageSymbol->Name, -1);
	}
	else {
		handler(imageSymbol->Name, line.LineNumber);
	}
}
#else
void StackTraceUtils::Impl::getSymbolName(
		StackTraceHandler &handler, char8_t *rawSymbol) {

	do {
		char8_t *left = strchr(rawSymbol, '(');
		if (left == NULL) {
			break;
		}
		left++;

		char8_t *right = strchr(left, '+');
		if (right == NULL) {
			break;
		}

		char8_t mangledBuf[128];
		detail::LocalString mangled(mangledBuf, sizeof(mangledBuf));
		mangled.tryAppend(left, right);

		int status = 0;
		char8_t *demangled =
				abi::__cxa_demangle(mangled.tryGet(), 0, 0, &status);

		if (demangled == NULL) {
			handler(mangled.tryGet(), -1);
			return;
		}

		try {
			handler(demangled, -1);
		}
		catch (...) {
			detail::DirectMemoryUtils::freeDirect(demangled);
			throw;
		}

		detail::DirectMemoryUtils::freeDirect(demangled);
		return;
	}
	while (false);

	handler("", -1);
}
#endif	
}	


namespace util {
size_t StackTraceUtils::Initializer::counter_ = 0;

StackTraceUtils::Initializer::Initializer() {
	if (counter_++ == 0) {
		try {
			StackTraceUtils::impl_ = new StackTraceUtils::Impl;
		}
		catch (...) {
		}
	}
}

StackTraceUtils::Initializer::~Initializer() {
	if (--counter_ == 0) {
		StackTraceUtils::Impl *impl = StackTraceUtils::impl_;

		StackTraceUtils::impl_ = NULL;
		delete impl;
	}
}
}	


namespace util {
namespace detail {
StackTraceStringHandler::StackTraceStringHandler(
		detail::LocalString &str,
		size_t maxDepth,
		bool ignoreLibs) :
		str_(str),
		maxDepth_(maxDepth),
		ignoreLibs_(ignoreLibs),
		lastDepth_(0) {
}

StackTraceStringHandler::~StackTraceStringHandler() {
}

void StackTraceStringHandler::operator()(const char8_t *name, int32_t line) {
	if (ignoreLibs_ && (
			strlen(name) == 0 ||
			strstr(name, "util::StackTraceUtils::getStackTrace(") == name ||
			strstr(name, "std::") == name ||
			strstr(name, "operator ") == name)) {
		return;
	}

	if (maxDepth_ > 0 && lastDepth_ >= maxDepth_) {
		return;
	}
	lastDepth_++;

	str_.tryAppend("[");
	str_.tryAppend(name);

	if (line >= 0) {
		str_.tryAppend(":");

		detail::RawNumberFormatter formatter;
		str_.tryAppend(formatter(line));
	}

	str_.tryAppend("]");
}
}	
}	

#endif	


namespace util {

bool DebugUtils::isDebuggerAttached() {
#ifdef _WIN32
	return !!IsDebuggerPresent();
#else
	return false;
#endif
}

void DebugUtils::interrupt() {
#ifdef _WIN32
	DebugBreak();
#endif
}

} 


namespace util {
namespace detail {

void* DirectAllocationUtils::allocate(
		size_t size, bool monitoring) UTIL_NOEXCEPT {
#if UTIL_MEMORY_POOL_AGGRESSIVE
	return MemoryManager::allocate(MemoryManager::findInstance(), size, monitoring);
#else
	static_cast<void>(monitoring);
	return DirectMemoryUtils::mallocDirect(size);
#endif
}

void DirectAllocationUtils::deallocate(
		void *ptr, bool monitoring) UTIL_NOEXCEPT {
#if UTIL_MEMORY_POOL_AGGRESSIVE
	MemoryManager::deallocate(MemoryManager::findInstance(), ptr, monitoring);
#else
	static_cast<void>(monitoring);
	DirectMemoryUtils::freeDirect(ptr);
#endif
}

#if UTIL_MEMORY_POOL_AGGRESSIVE
FreeLink* DirectAllocationUtils::allocateBulk(
		size_t size, bool monitoring, bool aligned,
		size_t &count) UTIL_NOEXCEPT {
	return MemoryManager::allocateBulk(
			MemoryManager::findInstance(), size, monitoring, aligned, count);
}

void DirectAllocationUtils::deallocateBulk(
		FreeLink *link, size_t size, bool monitoring,
		bool aligned) UTIL_NOEXCEPT {
	return MemoryManager::deallocateBulk(
			MemoryManager::findInstance(), link, size, monitoring, aligned);
}
#endif 

void* DirectAllocationUtils::allocateDirect(
		size_t size, bool monitoring, bool aligned) UTIL_NOEXCEPT {
#if UTIL_MEMORY_POOL_AGGRESSIVE
	return MemoryManager::allocateDirect(
			MemoryManager::findInstance(), size, monitoring, aligned);
#else
	static_cast<void>(monitoring);
	static_cast<void>(aligned);
	return DirectMemoryUtils::mallocDirect(size);
#endif
}

void DirectAllocationUtils::deallocateDirect(
		void *ptr, size_t size, bool monitoring, bool aligned) UTIL_NOEXCEPT {
#if UTIL_MEMORY_POOL_AGGRESSIVE
	MemoryManager::deallocateDirect(
			MemoryManager::findInstance(), ptr, size, monitoring, aligned);
#else
	static_cast<void>(size);
	static_cast<void>(monitoring);
	static_cast<void>(aligned);
	DirectMemoryUtils::freeDirect(ptr);
#endif
}

size_t DirectAllocationUtils::adjustAllocationSize(
		size_t size, bool withHead, size_t *index) UTIL_NOEXCEPT {
	return MemoryManager::adjustAllocationSize(size, withHead, index);
}

size_t DirectAllocationUtils::getUnitCount() UTIL_NOEXCEPT {
	return MemoryManager::getUnitCount();
}

bool DirectAllocationUtils::getUnitProfile(
		size_t index, int64_t &totalSize, int64_t &cacheSize,
		int64_t &monitoringSize, int64_t &totalCount, int64_t &cacheCount,
		int64_t &deallocCount) UTIL_NOEXCEPT {
	return MemoryManager::getUnitProfile(
			MemoryManager::findInstance(), index, totalSize, cacheSize,
			monitoringSize, totalCount, cacheCount, deallocCount);
}

void DirectAllocationUtils::dumpStats(std::ostream &os) {
	MemoryManager::dumpStats(MemoryManager::findInstance(), os);
}

void DirectAllocationUtils::dumpSizeHistogram(std::ostream &os) {
	MemoryManager::dumpSizeHistogram(MemoryManager::findInstance(), os);
}

} 
} 


#if UTIL_FAILURE_SIMULATION_ENABLED
namespace util {

volatile bool AllocationFailureSimulator::enabled_ = false;
volatile int32_t AllocationFailureSimulator::targetType_ = TARGET_NEW;
volatile uint64_t AllocationFailureSimulator::startCount_ = 0;
volatile uint64_t AllocationFailureSimulator::endCount_ = 0;
volatile uint64_t AllocationFailureSimulator::lastOperationCount_ = 0;

void AllocationFailureSimulator::set(
		int32_t targetType, uint64_t startCount, uint64_t endCount) {
	if (startCount < endCount) {
		enabled_ = false;
		targetType_ = targetType;
		startCount_ = startCount;
		endCount_ = endCount;
		lastOperationCount_ = 0;
		enabled_ = true;
	}
	else {
		enabled_ = false;
		targetType_ = TARGET_NEW;
		startCount_ = 0;
		endCount_ = 0;
	}
}

void AllocationFailureSimulator::checkOperation(int32_t targetType, size_t size) {
	if (!checkOperationDirect(targetType, size)) {
		return;
	}

	switch (targetType) {
	case TARGET_NEW:
		UTIL_THROW_UTIL_ERROR(CODE_NO_MEMORY,
				"Allocation failed");
	case TARGET_STACK_ALLOCATION:
		try {
			UTIL_THROW_UTIL_ERROR(CODE_NO_MEMORY,
					"Allocation failed");
		}
		catch (util::Exception &e) {
			StackAllocator::handleAllocationError(e);
		}
	}
}

bool AllocationFailureSimulator::checkOperationDirect(
		int32_t targetType, size_t size) UTIL_NOEXCEPT {
	static_cast<void>(size);

	if (enabled_ && targetType == targetType_) {
		const uint64_t count = lastOperationCount_;
		lastOperationCount_++;

		if (startCount_ <= count && count < endCount_) {
			return true;
		}
	}

	return false;
}

} 
#else	

#if UTIL_MEMORY_POOL_PLACEMENT_NEW && !UTIL_MEMORY_POOL_AGGRESSIVE
#error 0
#endif

#ifdef UTIL_PLACEMENT_NEW_ENABLED
void* operator new(size_t size) UTIL_PLACEMENT_NEW_SPECIFIER {
	void *p = UTIL_MALLOC(size);

	if (p == NULL) {
		throw std::bad_alloc();
	}

	return p;
}

void* operator new[](size_t size) UTIL_PLACEMENT_NEW_SPECIFIER {
	return operator new(size);
}

void operator delete(void *p) UTIL_NOEXCEPT {
#ifdef UTIL_DUMP_OPERATOR_DELETE
	if (p != NULL) {
		char8_t buf[512];
		util::detail::LocalString str(buf, sizeof(buf));

		util::detail::StackTraceStringHandler handler(str, 3, true);
		util::StackTraceUtils::getStackTrace(handler);
		str.tryAppend("\n");

		std::cerr << str.tryGet();
	}
#endif 

	UTIL_FREE(p);
}

void operator delete[](void *p) UTIL_NOEXCEPT {
	operator delete(p);
}
#endif	

#endif	
