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
#ifndef SQL_TUPLE_VAR_H_
#define SQL_TUPLE_VAR_H_

#include "util/allocator.h"

class BaseObject;
class BlobCursor;

struct TupleValueVarUtils {
	enum VarDataType {
		VAR_DATA_TEMP_STORE,
		VAR_DATA_CONTAINER,
		VAR_DATA_STACK_ALLOCATOR,
		VAR_DATA_HEAD,
		VAR_DATA_RELATED
	};

	typedef util::VariableSizeAllocator<> VarSizeAllocator;
	typedef util::StackAllocator StackAllocator;

	struct VarData;
	class BaseVarContext;
	struct ContainerVarArray;
	struct ContainerVarArrayReader;
};

struct TupleValueVarUtils::VarData {
public:
	VarData();

	VarData* getNext();
	VarData* getRelated();
	VarDataType getType() const;

	void* getBody();
	const void* getBody() const;

	static size_t getHeadSize();
	size_t getBodySize(BaseVarContext &cxt) const;

private:
	friend class BaseVarContext;

	VarData *next_;
	VarData *prev_;
	VarData *related_;
	VarDataType type_;

	union BodyPart {
		void *ptrBody;
		uint64_t longBody;
	} bodyPart_;
};

class TupleValueVarUtils::BaseVarContext {
public:
	class Scope;

	BaseVarContext();
	~BaseVarContext();

	void setVarAllocator(VarSizeAllocator *varAlloc);
	VarSizeAllocator* getVarAllocator() const;

	void setStackAllocator(StackAllocator *stackAlloc);
	StackAllocator* getStackAllocator() const;

	VarData* allocate(
			VarDataType type, size_t minBodySize, VarData *base = NULL);
	VarData* allocateRelated(size_t minBodySize, VarData *base);

	VarData* getHead();
	bool isEmpty() const;
	void clear();
	void clearElement(VarData *data);

	void moveToParent(size_t depth, VarData *data);

private:
	friend class Scope;

	BaseVarContext(const BaseVarContext&);
	BaseVarContext& operator=(const BaseVarContext&);

	void clearRelated(VarData *base);

	VarData* pushScope(VarData *nextHead);
	void popScope(VarData *prevHead);
	static void move(VarData *newHead, VarData *data);

	static void insertIntoLink(VarData *link, VarData *data);
	static void removeFromLink(VarData *data);

	VarData topHead_;
	VarData *head_;
	VarSizeAllocator *varAlloc_;
	StackAllocator *stackAlloc_;
};

class TupleValueVarUtils::BaseVarContext::Scope {
public:
	explicit Scope(BaseVarContext &cxt);
	~Scope();

	void move(VarData *data);

private:
	Scope(const Scope&);
	Scope& operator=(const Scope&);

	BaseVarContext &cxt_;
	VarData head_;
	VarData *prevHead_;
};

struct TupleValueVarUtils::ContainerVarArray {
public:
	void initialize(const BaseObject &fieldObject, const void *data);

private:
	friend struct ContainerVarArrayReader;

	const BaseObject *fieldObject_;
	const uint64_t *body_;
};

struct TupleValueVarUtils::ContainerVarArrayReader {
public:
	void initialize(const ContainerVarArray &varArray);
	void destroy();
	size_t length() const;
	bool next(const void *&data, size_t &size);
	void reset();

private:
	typedef std::pair<util::StackAllocator*, BlobCursor> BlobCursorStorage;

	enum {
		CURSOR_STORAGE_CAPACITY = 776
	};

	BaseObject* getBaseObject(size_t index);

	util::StackAllocator*& getBlobCursorAllocator();
	BlobCursor* getBlobCursor();

	BlobCursorStorage& getCursorStorage();

	template<size_t Required, size_t Capacity>
	static void checkStorageCapacity();

	const BaseObject *fieldObject_;
	const uint64_t *it_;
	const uint64_t *end_;
	uint32_t length_;

	union {
		int64_t asInt64_;
		uint8_t asBytes_[CURSOR_STORAGE_CAPACITY];
	} cursorStorage_;
};

inline TupleValueVarUtils::VarData*
TupleValueVarUtils::BaseVarContext::pushScope(VarData *nextHead) {
	assert(nextHead->type_ == VAR_DATA_HEAD);

	VarData *prevHead = head_;
	head_ = nextHead;
	head_->related_ = prevHead;
	return prevHead;
}

inline void TupleValueVarUtils::BaseVarContext::popScope(VarData *prevHead) {
	assert(prevHead->type_ == VAR_DATA_HEAD);

	if (head_->next_ != NULL) {
		clear();
	}
	head_ = prevHead;
}

inline bool TupleValueVarUtils::BaseVarContext::isEmpty() const {
	return (head_->next_ == NULL);
}

inline TupleValueVarUtils::BaseVarContext::Scope::Scope(BaseVarContext &cxt) :
		cxt_(cxt),
		prevHead_(cxt.pushScope(&head_)) {
}

inline TupleValueVarUtils::BaseVarContext::Scope::~Scope() {
	cxt_.popScope(prevHead_);
}

#endif
