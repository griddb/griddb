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
	@brief  C Utility
*/

#ifndef NEWSQL_C_UTILITY_CPP_H_
#define NEWSQL_C_UTILITY_CPP_H_

#include "util/allocator.h"
#include "c_utility.h"

typedef util::VariableSizeAllocatorTraits<128, 1024 * 4, 1024 * 1024>
  SQLVariableSizeGlobalAllocatorTraits;
typedef util::VariableSizeAllocator<util::Mutex, SQLVariableSizeGlobalAllocatorTraits>
  SQLVariableSizeGlobalAllocator;

typedef util::VariableSizeAllocator<> SQLVariableSizeLocalAllocator;

struct CUtilTool {
public:
	static GSResult saveCurrentException(CUtilException *exception) throw();

private:
	CUtilTool();
};

template<typename Wrapped, typename Raw>
struct CUtilWrapper {
public:
	inline static Wrapped* wrap(Raw *src) throw() {
		return reinterpret_cast<Wrapped*>(src);
	}

	inline static Raw* unwrap(Wrapped *src) throw() {
		return reinterpret_cast<Raw*>(src);
	}

private:
	CUtilWrapper();
};

struct DynamicVariableSizeAllocatorTraits {
public:
	static const size_t FIXED_ALLOCATOR_COUNT = 3;

	DynamicVariableSizeAllocatorTraits(
			size_t fixedAllocatorCount, const size_t *fixedSizeList);

	size_t getFixedSize(size_t index) const;
	size_t selectFixedAllocator(size_t size) const;

private:
	size_t sizeList_[FIXED_ALLOCATOR_COUNT];
};

struct CUtilStackAllocatorScopeTag {
public:
	explicit CUtilStackAllocatorScopeTag(CUtilStackAllocator &alloc) throw();
	~CUtilStackAllocatorScopeTag();

	inline CUtilStackAllocator& getAllocator() { return alloc_; }

private:
	CUtilStackAllocator &alloc_;
	util::StackAllocator::Scope base_;
};

struct CUtilStackAllocatorTag {
public:
	explicit CUtilStackAllocatorTag(util::StackAllocator &base);
	~CUtilStackAllocatorTag();

	CUtilStackAllocatorScope* enterScope() throw();
	void leaveScope(CUtilStackAllocatorScope *scope) throw();

	inline util::StackAllocator& getBase() { return base_; }

private:
	union ScopeStorage {
		util::detail::AlignmentUnit unit_;
		uint8_t body_[sizeof(CUtilStackAllocatorScope)];
	};

	static const size_t ALLOCATOR_MAX_SCOPE;

	util::StackAllocator &base_;
	ScopeStorage *scopeList_;
	size_t scopeIndex_;
};

typedef CUtilWrapper<CUtilException, util::Exception> CUtilExceptionWrapper;
typedef CUtilWrapper<CUtilFixedSizeAllocator, util::FixedSizeAllocator<> >
		CUtilFixedSizeAllocatorWrapper;

typedef SQLVariableSizeLocalAllocator CUtilRawVariableSizeAllocator;

typedef CUtilWrapper<
		CUtilVariableSizeAllocator, CUtilRawVariableSizeAllocator>
		CUtilVariableSizeAllocatorWrapper;

inline size_t DynamicVariableSizeAllocatorTraits::getFixedSize(
		size_t index) const {
	if (index >= FIXED_ALLOCATOR_COUNT) {
		assert(false);
		return std::numeric_limits<size_t>::max();
	}

	return sizeList_[index];
}

inline size_t DynamicVariableSizeAllocatorTraits::selectFixedAllocator(
		size_t size) const {
	if (size > sizeList_[0]) {
		if (size > sizeList_[1]) {
			if (size > sizeList_[2]) {
				return 3;
			}
			return 2;
		}
		return 1;
	}
	return 0;
}

struct LocalException {
public:

	LocalException() throw();

	~LocalException() throw();

	util::Exception* check() throw();

	static void set(const util::Exception &exception) throw();
	static const util::Exception *get() throw();

private:
	friend struct CUtilTool;

	static UTIL_THREAD_LOCAL util::Exception *localRef_;
	util::Exception storage_;
};

#endif 
