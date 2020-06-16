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
#ifndef VIRTUAL_MAP_H_
#define VIRTUAL_MAP_H_

#include "util/allocator.h"
#include "gs_error.h"

class VirtualMap {
public:
	struct SearchContext;
};

struct VirtualMap::SearchContext {
public:
	explicit SearchContext(util::StackAllocator &alloc) :
			alloc_(alloc), valueList_(alloc), typeList_(alloc) {
	}

	void setParameter(size_t index, const Value &value, ColumnType type) {
		while (index >= valueList_.size()) {
			valueList_.push_back(Value());
		}
		valueList_[index] = value;

		while (index >= typeList_.size()) {
			typeList_.push_back(ColumnType());
		}
		typeList_[index] =type;
	}

	const Value &getParameter(size_t index) {
		if (index >= valueList_.size()) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
		return valueList_[index];
	}

	ColumnType getParameterType(size_t index) {
		if (index >= typeList_.size()) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
		return typeList_[index];
	}

	util::StackAllocator &alloc_;
	util::Vector<Value> valueList_;
	util::Vector<ColumnType> typeList_;
};
#endif
