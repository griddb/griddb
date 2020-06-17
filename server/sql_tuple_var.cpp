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
#include "sql_tuple_var.h"
#include "sql_common.h"

#include "transaction_context.h"
#include "base_object.h"
#include "value_processor.h"

TupleValueVarUtils::VarData::VarData() :
		next_(NULL),
		prev_(NULL),
		related_(NULL),
		type_(VAR_DATA_HEAD) {
}

TupleValueVarUtils::VarData* TupleValueVarUtils::VarData::getNext() {
	return next_;
}

TupleValueVarUtils::VarData* TupleValueVarUtils::VarData::getRelated() {
	return related_;
}

TupleValueVarUtils::VarDataType TupleValueVarUtils::VarData::getType() const {
	return type_;
}

void* TupleValueVarUtils::VarData::getBody() {
	return &bodyPart_;
}

const void* TupleValueVarUtils::VarData::getBody() const {
	return &bodyPart_;
}

size_t TupleValueVarUtils::VarData::getHeadSize() {
	static VarData data;
	return static_cast<uint8_t*>(data.getBody()) -
			reinterpret_cast<uint8_t*>(&data);
}

size_t TupleValueVarUtils::VarData::getBodySize(BaseVarContext &cxt) const {
	assert(type_ != VAR_DATA_HEAD);

	VarSizeAllocator *varAlloc = cxt.getVarAllocator();
	if (varAlloc) {
		const size_t headSize = getHeadSize();
		const size_t elemSize = varAlloc->getElementCapacity(this);

		assert(elemSize >= headSize);
		return elemSize - headSize;
	}
	else {
		StackAllocator *stackAlloc = cxt.getStackAllocator();
		const size_t headSize = getHeadSize();
		const size_t elemSize = stackAlloc->base().getElementSize();

		assert(elemSize >= headSize);
		return elemSize - headSize;
	}
}

TupleValueVarUtils::BaseVarContext::BaseVarContext() :
		head_(&topHead_),
		varAlloc_(NULL),
		stackAlloc_(NULL) {
}

TupleValueVarUtils::BaseVarContext::~BaseVarContext() {
	try {
		clear();
	}
	catch (...) {
		assert(false);
	}
}

void TupleValueVarUtils::BaseVarContext::setVarAllocator(
		VarSizeAllocator *varAlloc) {
	varAlloc_ = varAlloc;
}

TupleValueVarUtils::VarSizeAllocator*
TupleValueVarUtils::BaseVarContext::getVarAllocator() const {
	return varAlloc_;
}

void TupleValueVarUtils::BaseVarContext::setStackAllocator(
		StackAllocator *stackAlloc) {
	stackAlloc_ = stackAlloc;
}

TupleValueVarUtils::StackAllocator*
TupleValueVarUtils::BaseVarContext::getStackAllocator() const {
	return stackAlloc_;
}

TupleValueVarUtils::VarData* TupleValueVarUtils::BaseVarContext::allocate(
		VarDataType type, size_t minBodySize, VarData *base) {
	assert(varAlloc_ != NULL || stackAlloc_ != NULL);
	assert(base == NULL || base->type_ != VAR_DATA_RELATED);

	const size_t size = VarData::getHeadSize() + minBodySize;
	VarData *data = NULL;
	if (varAlloc_) {
		data = new(varAlloc_->allocate(size)) VarData();
	}
	else {
		data = new(stackAlloc_->allocate(size)) VarData();
	}
	data->type_ = type;

	if (base == NULL) {
		insertIntoLink(head_, data);
	}
	else {
		VarData *related = base->related_;

		data->related_ = related;
		base->related_ = data;
	}

	return data;
}

TupleValueVarUtils::VarData* TupleValueVarUtils::BaseVarContext::allocateRelated(
		size_t minBodySize, VarData *base) {
	return allocate(VAR_DATA_RELATED, minBodySize, base);
}

TupleValueVarUtils::VarData* TupleValueVarUtils::BaseVarContext::getHead() {
	return head_;
}

void TupleValueVarUtils::BaseVarContext::clear() {
	assert(varAlloc_ != NULL || stackAlloc_ != NULL);
	assert(head_ != NULL);

	for (;;) {
		VarData *data = head_->next_;
		if (data == NULL) {
			break;
		}

		clearRelated(data);

		VarData *next = data->next_;
		if (varAlloc_) {
			varAlloc_->deallocate(data);
		}
		else {
			assert(stackAlloc_ != NULL);
			stackAlloc_->deallocate(data);
		}
		head_->next_ = next;
	}
}

void TupleValueVarUtils::BaseVarContext::clearElement(VarData *data) {
	if (data->type_ != TupleValueVarUtils::VAR_DATA_TEMP_STORE &&
			data->type_ != TupleValueVarUtils::VAR_DATA_CONTAINER) {
		assert(false);
		return;
	}

	clearRelated(data);
	removeFromLink(data);

	assert(varAlloc_ != NULL);
	varAlloc_->deallocate(data);
}

void TupleValueVarUtils::BaseVarContext::moveToParent(
		size_t depth, VarData *data) {
	VarData *newHead = head_;

	for (size_t i = 0; i < depth && newHead != NULL; i++) {
		newHead = newHead->related_;
	}

	if (newHead == NULL) {
		assert(false);
		return;
	}

	move(newHead, data);
}

void TupleValueVarUtils::BaseVarContext::clearRelated(VarData *base) {
	assert(varAlloc_ != NULL || stackAlloc_ != NULL);
	assert(base != NULL);

	for (;;) {
		VarData *data = base->related_;
		if (data == NULL) {
			break;
		}

		assert(data->next_ == NULL);
		assert(data->prev_ == NULL);

		VarData *related = data->related_;
		if (varAlloc_) {
			varAlloc_->deallocate(data);
		}
		else {
			assert(stackAlloc_ != NULL);
			stackAlloc_->deallocate(data);
		}
		base->related_ = related;
	}
}

void TupleValueVarUtils::BaseVarContext::move(
		VarData *newHead, VarData *data) {
	assert(newHead->type_ == VAR_DATA_HEAD);
	assert(data->type_ != VAR_DATA_HEAD);

	removeFromLink(data);
	insertIntoLink(newHead, data);
}

void TupleValueVarUtils::BaseVarContext::insertIntoLink(
		VarData *link, VarData *data) {
	assert(link != NULL);
	assert(data != NULL);

	assert(data->next_ == NULL);
	assert(data->prev_ == NULL);

	VarData *next = link->next_;
	if (next != NULL) {
		data->next_ = next;
		next->prev_ = data;
	}
	link->next_ = data;
	data->prev_ = link;
}

void TupleValueVarUtils::BaseVarContext::removeFromLink(VarData *data) {
	assert(data != NULL);

	VarData *next = data->next_;
	VarData *prev = data->prev_;

	if (next != NULL) {
		next->prev_ = prev;
		data->next_ = NULL;
	}

	if (prev != NULL) {
		prev->next_ = next;
		data->prev_ = NULL;
	}
}

void TupleValueVarUtils::BaseVarContext::Scope::move(VarData *data) {
	BaseVarContext::move(prevHead_, data);
}

class ContainerVarArrayAllocPool {
public:
	ContainerVarArrayAllocPool();
	~ContainerVarArrayAllocPool();

	static ContainerVarArrayAllocPool& getInstance();

	util::StackAllocator* allocate();
	void release(util::StackAllocator *alloc);

private:
	static const size_t ALLOC_LIST_SIZE = 8;
	static ContainerVarArrayAllocPool instance_;

	util::FixedSizeAllocator<util::Mutex> fixedAlloc_;
	util::Atomic<util::StackAllocator*> allocList_[ALLOC_LIST_SIZE];
};

class StubTransactionContext {
public:
	StubTransactionContext(util::StackAllocator &alloc, PartitionId pId);
	TransactionContext& get();

	static ptrdiff_t zero() { return zero_; }

private:
	void* getStorage();

	struct Stub {
		TransactionManager *manager_;
		util::StackAllocator *alloc_;
		ClientId clientId_;
		PartitionId pId_;
	};

	static ptrdiff_t zero_;

	union {
		void *asPtr_;
		int64_t asInt64_;
		uint8_t asBytes_[sizeof(TransactionContext)];
	} storage_;
};

ContainerVarArrayAllocPool ContainerVarArrayAllocPool::instance_;

ContainerVarArrayAllocPool::ContainerVarArrayAllocPool() :
		fixedAlloc_(
				util::AllocatorInfo(
						ALLOCATOR_GROUP_SQL_WORK, "containerVarArrayFixed"),
				1024) {
}

ContainerVarArrayAllocPool::~ContainerVarArrayAllocPool() {
	typedef util::Atomic<util::StackAllocator*> *It;
	It end = allocList_ + ALLOC_LIST_SIZE;
	for (It it = allocList_; it != end; ++it) {
		delete it->load();
	}
}

ContainerVarArrayAllocPool& ContainerVarArrayAllocPool::getInstance() {
	return instance_;
}

util::StackAllocator* ContainerVarArrayAllocPool::allocate() {
	typedef util::Atomic<util::StackAllocator*> *It;
	It end = allocList_ + ALLOC_LIST_SIZE;
	const uint64_t off = util::Thread::getSelfId() % ALLOC_LIST_SIZE;
	for (It it = allocList_ + off + 1;; ++it) {
		if (it == end) {
			it = allocList_;
		}

		util::StackAllocator *alloc = it->load();
		if (alloc != NULL) {
			if (it->compareExchange(alloc, NULL)) {
				return alloc;
			}
		}

		if (static_cast<size_t>(it - allocList_) == off) {
			break;
		}
	}

	{
		util::StackAllocator *alloc = UTIL_NEW util::StackAllocator(
				util::AllocatorInfo(
						ALLOCATOR_GROUP_SQL_WORK, "containerVarArrayStack"),
				&fixedAlloc_);
		alloc->setFreeSizeLimit(0);
		return alloc;
	}
}

void ContainerVarArrayAllocPool::release(util::StackAllocator *alloc) {
	try {
		util::StackAllocator::Tool::forceReset(*alloc);
		alloc->trim();
	}
	catch (...) {
	}

	typedef util::Atomic<util::StackAllocator*> *It;
	It end = allocList_ + ALLOC_LIST_SIZE;
	const uint64_t off = util::Thread::getSelfId() % ALLOC_LIST_SIZE;
	for (It it = allocList_ + off + 1;; ++it) {
		if (it == end) {
			it = allocList_;
		}

		util::StackAllocator *expected = NULL;
		if (it->compareExchange(expected, alloc)) {
			return;
		}

		if (static_cast<size_t>(it - allocList_) == off) {
			break;
		}
	}

	delete alloc;
}

ptrdiff_t StubTransactionContext::zero_ = 0;

StubTransactionContext::StubTransactionContext(
		util::StackAllocator &alloc, PartitionId pId) {
	UTIL_STATIC_ASSERT(sizeof(Stub) <= sizeof(TransactionContext));

	void *addr = getStorage();
	Stub *stub = static_cast<Stub*>(addr);

	stub->alloc_ = &alloc;
	stub->pId_ = pId;

	assert(&get().getDefaultAllocator() == &alloc);
	assert(get().getPartitionId() == pId);
}

TransactionContext& StubTransactionContext::get() {
	void *addr = getStorage();
	return *static_cast<TransactionContext*>(addr);
}

void* StubTransactionContext::getStorage() {
	void *addr = &storage_;
	return static_cast<uint8_t*>(addr) + zero_;
}

void TupleValueVarUtils::ContainerVarArray::initialize(
		const BaseObject &fieldObject, const void *data) {
	fieldObject_ = &fieldObject;
	body_ = static_cast<const uint64_t*>(data);
}

void TupleValueVarUtils::ContainerVarArrayReader::initialize(
		const ContainerVarArray &varArray) {
	fieldObject_ = varArray.fieldObject_;
	it_ = varArray.body_;
	end_ = varArray.body_;
	length_ = 0;

	const PartitionId partitionId = fieldObject_->getPartitionId();
	ObjectManager &objectManager = *(fieldObject_->getObjectManager());

	util::StackAllocator *&alloc = getBlobCursorAllocator();
	alloc = ContainerVarArrayAllocPool::getInstance().allocate();
	try {
		StubTransactionContext txn(*alloc, partitionId);

		const void *addr = varArray.body_;
		BlobCursor *cursor = getBlobCursor();

		new (cursor) BlobCursor(
				partitionId, objectManager, static_cast<const uint8_t*>(addr));
	}
	catch (...) {
		ContainerVarArrayAllocPool::getInstance().release(alloc);
		throw;
	}
}

void TupleValueVarUtils::ContainerVarArrayReader::destroy() {
	getBlobCursor()->~BlobCursor();
	util::StackAllocator *&alloc = getBlobCursorAllocator();
	ContainerVarArrayAllocPool::getInstance().release(alloc);
}

size_t TupleValueVarUtils::ContainerVarArrayReader::length() const {
	ContainerVarArray varArray;
	varArray.initialize(*fieldObject_, it_);

	size_t result = 0;

	ContainerVarArrayReader reader;
	reader.initialize(varArray);
	try {
		const void *data;
		size_t size;
		while (reader.next(data, size)) {
			result++;
		}
	}
	catch (...) {
		reader.destroy();
		throw;
	}
	reader.destroy();

	return result;
}

bool TupleValueVarUtils::ContainerVarArrayReader::next(
		const void *&data, size_t &size) {
	BlobCursor *cursor = getBlobCursor();
	if (!cursor->hasNext()) {
		return false;
	}

	const PartitionId partitionId = fieldObject_->getPartitionId();
	StubTransactionContext txn(*getBlobCursorAllocator(), partitionId);

	const uint8_t *cursorData;
	uint32_t cursorSize;
	cursor->next();
	cursor->getCurrentBinary(cursorData, cursorSize);

	data = cursorData;
	size = cursorSize;

	return true;
}

void TupleValueVarUtils::ContainerVarArrayReader::reset() {
	getBlobCursor()->reset();
}

BaseObject* TupleValueVarUtils::ContainerVarArrayReader::getBaseObject(
		size_t index) {
	static_cast<void>(index);
	GS_THROW_USER_ERROR(GS_ERROR_LTS_UNSUPPORTED, "");
}

util::StackAllocator*&
TupleValueVarUtils::ContainerVarArrayReader::getBlobCursorAllocator() {
	typedef std::pair<util::StackAllocator*, BlobCursor> BlobCursorStorage;
	UTIL_STATIC_ASSERT(sizeof(BlobCursorStorage) <= sizeof(cursorStorage_));
	void *src = &cursorStorage_;
	void *addr = static_cast<uint8_t*>(src) + StubTransactionContext::zero();
	return static_cast<BlobCursorStorage*>(addr)->first;
}

BlobCursor* TupleValueVarUtils::ContainerVarArrayReader::getBlobCursor() {
	typedef std::pair<util::StackAllocator*, BlobCursor> BlobCursorStorage;
	UTIL_STATIC_ASSERT(sizeof(BlobCursorStorage) <= sizeof(cursorStorage_));
	void *src = &cursorStorage_;
	void *addr = static_cast<uint8_t*>(src) + StubTransactionContext::zero();
	return &static_cast<BlobCursorStorage*>(addr)->second;
}
