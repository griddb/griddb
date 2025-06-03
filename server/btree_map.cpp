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
	@brief Implementation of BtreeMap
*/

#ifndef GS_BTREE_MAP_DEFINE_SWITCHER_ONLY
#define GS_BTREE_MAP_DEFINE_SWITCHER_ONLY 0
#endif

#if !GS_BTREE_MAP_DEFINE_SWITCHER_ONLY

#include "btree_map.h"
#include "data_store_common.h"
#include "schema.h"
#endif 

template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject8> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject16> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject24> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject32> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject40> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject48> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject56> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject64> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject72> {
	typedef CompositeInfoObject TYPE;
};
template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject80> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject88> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject96> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject104> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject112> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject120> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject128> {
	typedef CompositeInfoObject TYPE;
};template<>
struct BtreeMap::MapKeyTraits<CompositeInfoObject136> {
	typedef CompositeInfoObject TYPE;
};

#if !GS_BTREE_MAP_DEFINE_SWITCHER_ONLY
int32_t BtreeMap::compositeInfoCmp(TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy,
						 const CompositeInfoObject *e1, const CompositeInfoObject *e2, BaseIndex::Setting &setting) {

	util::StackAllocator::Scope scope(txn.getDefaultAllocator());

	int32_t ret = 0;
	TreeFuncInfo *funcInfo = setting.getFuncInfo();
	assert(funcInfo != NULL);
	ColumnSchema *columnSchema = funcInfo->getColumnSchema();
	VariableArrayCursor *cursor1 = NULL, *cursor2 = NULL;
	for (uint32_t i = 0; i < setting.getCompareNum(); i++) {
		bool isNull1 = e1->isNull(i);
		bool isNull2 = e2->isNull(i);
		if (isNull1 || isNull2) {
			if (isNull1 && isNull2) {
				ret = 0;
			} else if (isNull2) {
				ret = -1;
			} else {
				ret = 1;
			}
		} else {
			ColumnInfo &columnInfo = columnSchema->getColumnInfo(i);
			uint8_t *val1 = e1->getField(txn, objectManager, strategy, columnInfo, cursor1);
			uint8_t *val2 = e2->getField(txn, objectManager, strategy, columnInfo, cursor2);
			ret = ValueProcessor::compare(txn, objectManager, strategy, columnInfo.getColumnType(), val1, val2);
		}
		if (ret != 0) {
			break;
		}
	}
	if (cursor1 != NULL) {
		cursor1->~VariableArrayCursor();
	}
	if (cursor2 != NULL) {
		cursor2->~VariableArrayCursor();
	}
	return ret;
}

template <>
bool BtreeMap::compositeInfoMatch(TransactionContext &txn, ObjectManagerV4 &objectManager,
						 const CompositeInfoObject *e, BaseIndex::Setting &setting) {

	util::StackAllocator::Scope scope(txn.getDefaultAllocator());

	bool isMatch = true;
	TreeFuncInfo *funcInfo = setting.getFuncInfo();
	VariableArrayCursor *cursor = NULL;
	util::Vector<TermCondition> *conds = setting.getFilterConds();
	if (conds != NULL) {
		ColumnSchema *columnSchema = funcInfo->getColumnSchema();
		for (uint32_t i = 0; i < conds->size(); i++) {
			TermCondition &cond = (*conds)[i];
			bool isNull = e->isNull(cond.columnId_);
			if (isNull || cond.opType_ == DSExpression::IS) {
				if (!(isNull && cond.opType_ == DSExpression::IS)) {
					isMatch = false;
					break;
				}
			} else if (cond.opType_ != DSExpression::ISNOT) {
				ColumnInfo &columnInfo = columnSchema->getColumnInfo(cond.columnId_);
				uint8_t *val1 = e->getField(txn, objectManager, allocateStrategy_, columnInfo, cursor);
				ColumnType columnType = columnInfo.getColumnType();
				if (columnType == COLUMN_TYPE_STRING) {
					StringCursor obj1(const_cast<uint8_t *>(val1));
					isMatch = cond.operator_(txn, obj1.str(), obj1.stringLength(), 
						static_cast<const uint8_t *>(cond.value_), cond.valueSize_);
				} else {
					assert(ValueProcessor::isSimple(columnType));
					isMatch = cond.operator_(txn, val1, 0,
						static_cast<const uint8_t *>(cond.value_), cond.valueSize_);
				}
				if (!isMatch) {
					break;
				}
			}
		}
	}
	if (cursor != NULL) {
		cursor->~VariableArrayCursor();
	}
	return isMatch;
}

template <typename K, typename V>
std::string BtreeMap::BNode<K, V>::dump(TransactionContext &txn, TreeFuncInfo *funcInfo) {
	UNUSED_VARIABLE(txn);
	UNUSED_VARIABLE(funcInfo);

	util::NormalOStringStream out;
	out << "(@@Node@@)";
	for (int32_t i = 0; i < numkeyValues(); ++i) {
		out << "(";
		out << "," << getKeyValue(i).value_ << "), ";
	}
	return out.str();
}

template <>
std::string BtreeMap::BNode<CompositeInfoObject24, OId>::dump(TransactionContext &txn, TreeFuncInfo *funcInfo) {
	util::NormalOStringStream out;
	out << "(@@Node@@)";
	for (int32_t i = 0; i < numkeyValues(); ++i) {
		out << "(";
		getKeyValue(i).key_.dump(txn, *getObjectManager(), allocateStrategy_, *funcInfo, out);
		out << "," << getKeyValue(i).value_ << "), ";
	}
	return out.str();
}


template <>
int32_t BtreeMap::getInitialItemSizeThreshold<TransactionId, MvccRowImage>() {
	return INITIAL_MVCC_ITEM_SIZE_THRESHOLD;
}

template<>
MvccRowImage BtreeMap::getMaxValue() {
	return MvccRowImage();
}

template<>
MvccRowImage BtreeMap::getMinValue() {
	return MvccRowImage();
}

template <>
void BaseIndex::SearchContext::setSuspendPoint(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, TreeFuncInfo *funcInfo, const StringKey &suspendKey, const OId &suspendValue) {
	UNUSED_VARIABLE(funcInfo);
	StringCursor obj(objectManager, strategy, suspendKey.oId_);
	suspendKey_ = ALLOC_NEW(txn.getDefaultAllocator())
		uint8_t[obj.stringLength()];
	memcpy(suspendKey_, obj.str(), obj.stringLength());
	suspendKeySize_ = obj.stringLength();

	uint32_t valueSize = sizeof(OId);
	suspendValue_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[valueSize];
	memcpy(suspendValue_, &suspendValue, valueSize);
	suspendValueSize_ = valueSize;
}

template <>
void BaseIndex::SearchContext::setSuspendPoint(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, TreeFuncInfo *funcInfo, const FullContainerKeyAddr &suspendKey, const OId &suspendValue) {
	UNUSED_VARIABLE(funcInfo);
	FullContainerKeyCursor obj(objectManager, strategy, suspendKey.oId_);
	FullContainerKey containerKey = obj.getKey();
	const void *keyData;
	size_t keySize;
	containerKey.toBinary(keyData, keySize);

	suspendKey_ = ALLOC_NEW(txn.getDefaultAllocator())
		uint8_t[keySize];
	memcpy(suspendKey_, keyData, keySize);
	suspendKeySize_ = static_cast<uint32_t>(keySize);

	uint32_t valueSize = sizeof(OId);
	suspendValue_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[valueSize];
	memcpy(suspendValue_, &suspendValue, valueSize);
	suspendValueSize_ = valueSize;
}

template <>
void BaseIndex::SearchContext::setSuspendPoint(TransactionContext& txn,
	ObjectManagerV4& objectManager, AllocateStrategy& strategy, TreeFuncInfo* funcInfo, const FullContainerKeyAddr& suspendKey, const KeyDataStoreValue& suspendValue) {
	UNUSED_VARIABLE(funcInfo);
	FullContainerKeyCursor obj(objectManager, strategy, suspendKey.oId_);
	FullContainerKey containerKey = obj.getKey();
	const void* keyData;
	size_t keySize;
	containerKey.toBinary(keyData, keySize);

	suspendKey_ = ALLOC_NEW(txn.getDefaultAllocator())
		uint8_t[keySize];
	memcpy(suspendKey_, keyData, keySize);
	suspendKeySize_ = static_cast<uint32_t>(keySize);

	uint32_t valueSize = sizeof(KeyDataStoreValue);
	suspendValue_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[valueSize];
	memcpy(suspendValue_, &suspendValue, valueSize);
	suspendValueSize_ = valueSize;
}

template <>
void BaseIndex::SearchContext::setSuspendPoint(TransactionContext &txn,
	ObjectManagerV4 &objectManager, AllocateStrategy &strategy, TreeFuncInfo *funcInfo, const CompositeInfoObject &suspendKey, const OId &suspendValue) {

	assert(funcInfo != NULL);
	suspendKey.serialize(txn, objectManager, strategy, *funcInfo, suspendKey_, suspendKeySize_);

	uint32_t valueSize = sizeof(OId);
	suspendValue_ = ALLOC_NEW(txn.getDefaultAllocator()) uint8_t[valueSize];
	memcpy(suspendValue_, &suspendValue, valueSize);
	suspendValueSize_ = valueSize;
}
#endif 

template<typename Action>
void BtreeMap::switchToBasicType(ColumnType type, Action &action) {
	switch (type) {
	case COLUMN_TYPE_COMPOSITE: {
		switch (elemSize_ - sizeof(OId)) {
		case 8:
			action.template execute<CompositeInfoObject8, CompositeInfoObject8, OId, OId>();
			break;
		case 16:
			action.template execute<CompositeInfoObject16, CompositeInfoObject16, OId, OId>();
			break;
		case 24:
			action.template execute<CompositeInfoObject24, CompositeInfoObject24, OId, OId>();
			break;
		case 32:
			action.template execute<CompositeInfoObject32, CompositeInfoObject32, OId, OId>();
			break;
		case 40:
			action.template execute<CompositeInfoObject40, CompositeInfoObject40, OId, OId>();
			break;
		case 48:
			action.template execute<CompositeInfoObject48, CompositeInfoObject48, OId, OId>();
			break;
		case 56:
			action.template execute<CompositeInfoObject56, CompositeInfoObject56, OId, OId>();
			break;
		case 64:
			action.template execute<CompositeInfoObject64, CompositeInfoObject64, OId, OId>();
			break;
		case 72:
			action.template execute<CompositeInfoObject72, CompositeInfoObject72, OId, OId>();
			break;
		case 80:
			action.template execute<CompositeInfoObject80, CompositeInfoObject80, OId, OId>();
			break;
		case 88:
			action.template execute<CompositeInfoObject88, CompositeInfoObject88, OId, OId>();
			break;
		case 96:
			action.template execute<CompositeInfoObject96, CompositeInfoObject96, OId, OId>();
			break;
		case 104:
			action.template execute<CompositeInfoObject104, CompositeInfoObject104, OId, OId>();
			break;
		case 112:
			action.template execute<CompositeInfoObject112, CompositeInfoObject112, OId, OId>();
			break;
		case 120:
			action.template execute<CompositeInfoObject120, CompositeInfoObject120, OId, OId>();
			break;
		case 128:
			action.template execute<CompositeInfoObject128, CompositeInfoObject128, OId, OId>();
			break;
		case 136:
			action.template execute<CompositeInfoObject136, CompositeInfoObject136, OId, OId>();
			break;
		default:
			GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED,
				"unknown elem size = " << elemSize_ - sizeof(OId));
			break;
		}
	} break;
	case COLUMN_TYPE_STRING:
		action.template execute<StringObject, StringKey, OId, OId>();
		break;
	case COLUMN_TYPE_BOOL:
		action.template execute<bool, bool, OId, OId>();
		break;
	case COLUMN_TYPE_BYTE:
	case COLUMN_TYPE_NULL:
		action.template execute<int8_t, int8_t, OId, OId>();
		break;
	case COLUMN_TYPE_SHORT:
		action.template execute<int16_t, int16_t, OId, OId>();
		break;
	case COLUMN_TYPE_INT:
		action.template execute<int32_t, int32_t, OId, OId>();
		break;
	case COLUMN_TYPE_LONG:
		action.template execute<int64_t, int64_t, OId, OId>();
		break;
	case COLUMN_TYPE_FLOAT:
		action.template execute<float32, float32, OId, OId>();
		break;
	case COLUMN_TYPE_DOUBLE:
		action.template execute<float64, float64, OId, OId>();
		break;
	case COLUMN_TYPE_TIMESTAMP:
		action.template execute<Timestamp, Timestamp, OId, OId>();
		break;
	case COLUMN_TYPE_MICRO_TIMESTAMP:
		action.template execute<MicroTimestampKey, MicroTimestampKey, OId, OId>();
		break;
	case COLUMN_TYPE_NANO_TIMESTAMP:
		action.template execute<NanoTimestampKey, NanoTimestampKey, OId, OId>();
		break;
	case COLUMN_TYPE_OID:
		action.template execute<OId, OId, OId, OId>();
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED, "");
		break;
	}
}

#if !GS_BTREE_MAP_DEFINE_SWITCHER_ONLY
int32_t BtreeMap::initialize(TransactionContext &txn, ColumnType columnType,
	bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize) {
	InitializeFunc initializeFunc(txn, columnType, isUnique, btreeMapType,
		elemSize, this);
	elemSize_ = static_cast<uint8_t>(elemSize);
	switchToBasicType(columnType, initializeFunc);
	return initializeFunc.ret_;
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::InitializeFunc::execute() {
	typedef typename MapKeyTraits<K>::TYPE S;
	ret_ = tree_->initialize<S, V>(txn_, columnType_,
		isUnique_, btreeMapType_, elemSize_);
}

template <>
int32_t BtreeMap::initialize<FullContainerKeyCursor, OId>(TransactionContext &txn, ColumnType columnType,
													bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize) {
	UNUSED_VARIABLE(elemSize);
	return initialize<FullContainerKeyAddr, OId>(txn, columnType, isUnique, btreeMapType);
}

template <>
int32_t BtreeMap::initialize<FullContainerKeyCursor, KeyDataStoreValue>(TransactionContext& txn, ColumnType columnType,
	bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize) {
	UNUSED_VARIABLE(elemSize);
	return initialize<FullContainerKeyAddr, KeyDataStoreValue>(txn, columnType, isUnique, btreeMapType);
}

template <>
int32_t BtreeMap::initialize<CompositeInfoObject, OId>(TransactionContext &txn, ColumnType columnType,
													   bool isUnique, BtreeMapType btreeMapType, uint32_t elemSize) {
	assert(elemSize != 0);
	elemSize_ = static_cast<uint8_t>(elemSize); 
	nodeMaxSize_ = calcNodeMaxSize(elemSize_);
	nodeMinSize_ = calcNodeMinSize(elemSize_);

	isUnique_ = isUnique;
	keyType_ = columnType;
	btreeMapType_ = btreeMapType;

	BaseObject::allocate<BNodeImage<CompositeInfoObject, OId> >(getInitialNodeSize<CompositeInfoObject, OId>(elemSize_),
		getBaseOId(), OBJECT_TYPE_BTREE_MAP);
	BNode<CompositeInfoObject, OId> rootNode(this, allocateStrategy_);
	rootNode.initialize(txn, getBaseOId(), true,
		getInitialItemSizeThreshold<CompositeInfoObject, OId>(), elemSize_);

	assert(rootNode.getNodeElemSize() != 0);

	rootNode.setRootNodeHeader(
		columnType, btreeMapType, static_cast<uint8_t>(isUnique), UNDEF_OID);

	return GS_SUCCESS;
}

bool BtreeMap::finalize(TransactionContext &txn) {
	setDirty();
	FinalizeFunc finalizeFunc(txn, this);
	switchToBasicType(getKeyType(), finalizeFunc);
	return finalizeFunc.ret_;
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::FinalizeFunc::execute() {
	ret_ = tree_->finalize<K, V>(txn_);
}

template <typename K, typename V>
bool BtreeMap::finalize(TransactionContext &txn) {
	uint64_t removeNum = NUM_PER_EXEC;
	OId rootOId = getRootOId();
	if (rootOId != UNDEF_OID) {
		BNode<K, V> dirtyRootNode(this, allocateStrategy_);
		dirtyRootNode.finalize(txn, removeNum);
		if (removeNum > 0) {
			BaseObject::finalize();
		}
	}
	return removeNum > 0;
}

int32_t BtreeMap::insert(
	TransactionContext &txn, const void *constKey, OId oId) {
	assert(oId != UNDEF_OID);
	InsertFunc insertFunc(txn, constKey, oId, this);
	switchToBasicType(getKeyType(), insertFunc);
	return insertFunc.ret_;
}

template<>
void BtreeMap::InsertFunc::execute<StringObject, StringKey, OId, OId>() {
	StringCursor stringCursor(reinterpret_cast<uint8_t *>(key_));
	StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCursor));
	bool isCaseSensitive = true;
	ret_ = tree_->insertInternal<StringObject, StringKey, OId>(txn_, convertKey, oId_, isCaseSensitive);
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::InsertFunc::execute() {
	bool isCaseSensitive = true;
	ret_ = tree_->insertInternal<P, K, V>(txn_, *reinterpret_cast<P *>(key_), oId_, isCaseSensitive);
}

int32_t BtreeMap::remove(
	TransactionContext &txn, const void *constKey, OId oId) {
	assert(oId != UNDEF_OID);
	RemoveFunc removeFunc(txn, constKey, oId, this);
	switchToBasicType(getKeyType(), removeFunc);
	return removeFunc.ret_;
}

template<>
void BtreeMap::RemoveFunc::execute<StringObject, StringKey, OId, OId>() {
	StringCursor stringCursor(reinterpret_cast<uint8_t *>(key_));
	StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCursor));
	bool isCaseSensitive = true;
	ret_ = tree_->removeInternal<StringObject, StringKey, OId>(txn_, convertKey, oId_, isCaseSensitive);
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::RemoveFunc::execute() {
	bool isCaseSensitive = true;
	ret_ = tree_->removeInternal<P, K, V>(txn_, *reinterpret_cast<P *>(key_), oId_, isCaseSensitive);
}

int32_t BtreeMap::update(
	TransactionContext &txn, const void *constKey, OId oId, OId newOId) {
	assert(oId != UNDEF_OID && newOId != UNDEF_OID);
	UpdateFunc updateFunc(txn, constKey, oId, newOId, this);
	switchToBasicType(getKeyType(), updateFunc);
	return updateFunc.ret_;
}

template<>
void BtreeMap::UpdateFunc::execute<StringObject, StringKey, OId, OId>() {
	StringCursor stringCursor(reinterpret_cast<uint8_t *>(key_));
	StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCursor));
	bool isCaseSensitive = true;
	ret_ = tree_->updateInternal<StringObject, StringKey, OId>(txn_, convertKey, oId_, newOId_, isCaseSensitive);
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::UpdateFunc::execute() {
	bool isCaseSensitive = true;
	ret_ = tree_->updateInternal<P, K, V>(txn_, *reinterpret_cast<P *>(key_), oId_, newOId_, isCaseSensitive);
}

int32_t BtreeMap::search(
	TransactionContext &txn, const void *constKey, OId &oId) {
	assert(constKey != NULL);
	if (isEmpty()) {
		oId = UNDEF_OID;
		return GS_FAIL;
	}
	Setting setting(getKeyType(), true, getFuncInfo());
	UniqueSearchFunc uniqueSearchFunc(txn, constKey, oId, setting, this);
	switchToBasicType(getKeyType(), uniqueSearchFunc);
	if (uniqueSearchFunc.ret_ != GS_SUCCESS) {
		oId = UNDEF_OID;
	}
	return uniqueSearchFunc.ret_;
}

template<>
void BtreeMap::UniqueSearchFunc::execute<StringObject, StringKey, OId, OId>() {
	StringCursor stringCursor(reinterpret_cast<uint8_t *>(key_));
	StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCursor));
	ret_ = tree_->find<StringObject, StringKey, OId, KEY_COMPONENT>(txn_, convertKey, oId_, setting_);
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::UniqueSearchFunc::execute() {
	ret_ = tree_->find<P, K, V, KEY_COMPONENT>(txn_, *reinterpret_cast<P *>(key_), oId_, setting_);
}

int32_t BtreeMap::search(
		TransactionContext &txn, SearchContext &sc, util::XArray<OId> &idList,
		OutputOrder outputOrder) {
	if (sc.getTermConditionUpdator() != NULL) {
		return searchBulk(txn, sc, idList, outputOrder);
	}

	if (isEmpty()) {
		return GS_FAIL;
	}
	if (sc.getLimit() == 0) {
		return GS_SUCCESS;
	}
	SearchFunc searchFunc(txn, sc, idList, outputOrder, this);
	switchToBasicType(getKeyType(), searchFunc);
	return searchFunc.ret_;
}

template<>
void BtreeMap::SearchFunc::execute<StringObject, StringKey, OId, OId>() {
	util::StackAllocator &alloc = txn_.getDefaultAllocator();
	TermCondition *startCond = sc_.getStartKeyCondition();
	TermCondition *endCond = sc_.getEndKeyCondition();
	const void *orgStartKey = NULL;
	const void *orgEndKey = NULL;
	if (startCond != NULL) {
		StringCursor *startStringCursor = ALLOC_NEW(alloc) 
			StringCursor(alloc, static_cast<const uint8_t *>(startCond->value_), startCond->valueSize_);
		StringObject *startConvertKey = ALLOC_NEW(alloc) 
			StringObject(reinterpret_cast<uint8_t *>(startStringCursor));
		orgStartKey = startCond->value_;
		startCond->value_ = startConvertKey;
	}
	if (endCond != NULL) {
		StringCursor *endStringCursor = ALLOC_NEW(alloc) 
			StringCursor(alloc, static_cast<const uint8_t *>(endCond->value_), endCond->valueSize_);
		StringObject *endConvertKey = ALLOC_NEW(alloc)
			StringObject(reinterpret_cast<uint8_t *>(endStringCursor));
		orgEndKey = endCond->value_;
		endCond->value_ = endConvertKey;
	}
	if (sc_.getSuspendValue() != NULL) {
		ret_ = tree_->find<StringObject, StringKey, OId, OId, KEY_VALUE_COMPONENT>(
			txn_, sc_, idList_, outputOrder_);
	} else {
		ret_ = tree_->find<StringObject, StringKey, OId, OId, KEY_COMPONENT>(
			txn_, sc_, idList_, outputOrder_);
	}
	if (startCond != NULL) {
		startCond->value_ = orgStartKey;
	}
	if (endCond != NULL) {
		endCond->value_ = orgEndKey;
	}
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::SearchFunc::execute() {
	if (sc_.getSuspendValue() != NULL) {
		ret_ = tree_->find<P, K, V, V, KEY_VALUE_COMPONENT>(txn_, sc_, idList_, outputOrder_);
	} else {
		ret_ = tree_->find<P, K, V, V, KEY_COMPONENT>(txn_, sc_, idList_, outputOrder_);
	}
}

int32_t BtreeMap::getAll(
	TransactionContext &txn, ResultSize limit, util::XArray<OId> &idList) {
	if (isEmpty()) {
		return GS_FAIL;
	}
	if (limit == 0) {
		return GS_SUCCESS;
	}
	GetAllFunc getAllFunc(txn, limit, idList, this);
	switchToBasicType(getKeyType(), getAllFunc);
	return getAllFunc.ret_;
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::GetAllFunc::execute() {
	ret_ = tree_->getAllByAscending<P, K, V, R>(txn_, limit_, idList_);
}

template <typename P, typename K, typename V, typename R>
int32_t BtreeMap::getAllByAscending(
	TransactionContext &txn, ResultSize limit, util::XArray<R> &result) {
	KeyValue<K, V> suspendKeyValue;
	Setting setting(getKeyType(), false, NULL);
	getAllByAscending<P, K, V, R>(
		txn, limit, result, MAX_RESULT_SIZE, suspendKeyValue, setting);
	return GS_SUCCESS;
}

int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<OId> &idList, BtreeMap::BtreeCursor &cursor) {
	if (isEmpty()) {
		return GS_SUCCESS;
	}
	if (limit == 0) {
		return GS_SUCCESS;
	}
	GetAllCursorFunc getAllCursorFunc(txn, limit, idList, cursor, this);
	switchToBasicType(getKeyType(), getAllCursorFunc);
	return getAllCursorFunc.ret_;
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::GetAllCursorFunc::execute() {
	ret_ = tree_->getAllByAscending<K, V, R>(txn_, limit_, idList_, cursor_);
}

template <typename K, typename V, typename R>
int32_t BtreeMap::getAllByAscending(TransactionContext &txn, ResultSize limit,
	util::XArray<R> &result, BtreeMap::BtreeCursor &cursor) {
	if (isEmpty()) {
		return GS_SUCCESS;
	}
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc = 0;
	if (cursor.nodeId_ == UNDEF_OID) {
		getHeadNode<K, V>(txn, node);
		loc = 0;
	}
	else {
		node.load(cursor.nodeId_, false);
		loc = cursor.loc_;
	}
	bool hasNext = false;
	while (true) {
		pushResultList<K, V, R>(node.getKeyValue(loc), result);
		hasNext = nextPos(txn, node, loc);

		if (!hasNext || result.size() >= limit) {
			break;
		}
	}
	if (hasNext) {
		cursor.nodeId_ = node.getSelfOId();
		cursor.loc_ = loc;
		return GS_FAIL;
	}
	else {
		return GS_SUCCESS;
	}
}


std::string BtreeMap::validate(TransactionContext &txn) {
	ValidateFunc validateFunc(txn, this);
	switchToBasicType(getKeyType(), validateFunc);
	return validateFunc.ret_;
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::ValidateFunc::execute() {
	ret_ = tree_->validateInternal<K, V>(txn_);
}

template <typename K, typename V>
std::string BtreeMap::validateInternal(TransactionContext &txn) {
	if (isEmpty()) {
		return "empty";
	}
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	for (OId nodeId = getRootOId(); nodeId != UNDEF_OID;) {
		node.load(nodeId, false);
		if (node.isLeaf()) {
			break;
		}
		nodeId = node.getChild(txn, 0);
	}

	CmpFunctor<K, K, V, KEY_VALUE_COMPONENT> cmp;

	Setting setting(getKeyType(), false, getFuncInfo());
	int32_t loc = 0;
	KeyValue<K, V> currentVal, nextVal;
	while (true) {
		currentVal = node.getKeyValue(loc);
		if (!nextPos(txn, node, loc)) {
			break;
		}
		nextVal = node.getKeyValue(loc);

		if (cmp(txn, *getObjectManager(), allocateStrategy_, currentVal, nextVal, setting) > 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_COLUMN_ID_INVALID, "invalid prevKey > nextKey");
		}
		if (isUnique() && currentVal.value_ == nextVal.value_) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_COLUMN_ID_INVALID, "invalid prevVal == nextVal");
		}
	}
	if (node.getSelfOId() != getTailNodeOId()) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_COLUMN_ID_INVALID, "tail node invalid");
	}

	util::NormalOStringStream strstrm;  
	return "o.k.";
}

std::string BtreeMap::dump(TransactionContext &txn, uint8_t mode) {
	util::NormalOStringStream strstrm;
	if (mode == SUMMARY_DUMP) {
		OId rootOId = getRootOId();
		int32_t nodeNum = 0;
		int32_t realKeyNum = 0;
		int32_t keySpaceNum = 0;
		int32_t maxDepth = 0;
		int32_t minDepth = INT32_MAX;
		int32_t maxKeyNode = 0;
		int32_t minKeyNode = INT32_MAX;
		int32_t depth = 0;

		DumpSummaryFunc dumpSummaryFunc(txn, rootOId, nodeNum, realKeyNum, keySpaceNum,
			maxDepth, minDepth, maxKeyNode, minKeyNode, depth, this);
		switchToBasicType(getKeyType(), dumpSummaryFunc);
		strstrm << "{BTreeStatus:{" << std::endl;
		strstrm << "\t nodeNum : " << nodeNum << "," << std::endl;
		strstrm << "\t realKeyNum : " << realKeyNum << "," << std::endl;
		strstrm << "\t keySpaceNum : " << keySpaceNum << "," << std::endl;
		strstrm << "\t maxDepth : " << maxDepth << "," << std::endl;
		strstrm << "\t minDepth : " << minDepth << "," << std::endl;
		strstrm << "\t maxKeyNode : " << maxKeyNode << "," << std::endl;
		strstrm << "\t minKeyNode : " << minKeyNode << "," << std::endl;
		strstrm << "}" << std::endl;
	} else {
		OId nodeId = getRootOId();
		int32_t depth = 1;
		TreeFuncInfo *funcInfo = getFuncInfo();
		DumpAllFunc dumpAllFunc(txn, strstrm, nodeId, depth,
			funcInfo, this);
		switchToBasicType(getKeyType(), dumpAllFunc);
	}
	return strstrm.str();
}

template <typename K, typename V>
std::string BtreeMap::dump(TransactionContext& txn) {
	util::NormalOStringStream out;
	print<K, V>(txn, out, getRootOId());
	return out.str();
}


template <typename P, typename K, typename V, typename R>
void BtreeMap::DumpSummaryFunc::execute() {
	tree_->getTreeStatus<K, V>(txn_, nodeId_, nodeNum_, realKeyNum_,
		keySpaceNum_, maxDepth_, minDepth_, maxKeyNode_, minKeyNode_, depth_);
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::DumpAllFunc::execute() {
	tree_->print<K, V>(txn_, out_, nodeId_, depth_, funcInfo_);
}

template <typename K, typename V>
void BtreeMap::getTreeStatus(TransactionContext &txn, OId nodeId,
	int32_t &nodeNum, int32_t &realKeyNum, int32_t &keySpaceNum,
	int32_t &maxDepth, int32_t &minDepth, int32_t &maxKeyNode,
	int32_t &minKeyNode, int32_t depth) {
	if (nodeId == UNDEF_OID) {
		return;
	}
	BNode<K, V> node(txn, *getObjectManager(), nodeId, allocateStrategy_);
	nodeNum++;
	realKeyNum += node.numkeyValues();
	keySpaceNum += nodeMaxSize_;
	if (maxKeyNode < node.numkeyValues()) {
		maxKeyNode = node.numkeyValues();
	}
	if (minKeyNode > node.numkeyValues()) {
		minKeyNode = node.numkeyValues();
	}
	if (node.isLeaf()) {
		if (maxDepth < depth) {
			maxDepth = depth;
		}
		if (minDepth > depth) {
			minDepth = depth;
		}
	}
	if (!node.isLeaf()) {
		for (int32_t i = 0; i < node.numkeyValues() + 1; ++i) {
			getTreeStatus<K, V>(txn, node.getChild(txn, i), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode,
				depth + 1);
		}
	}
}

template <typename K, typename V>
int32_t BtreeMap::print(TransactionContext &txn, std::ostream &out, OId nodeId,
	int32_t depth, TreeFuncInfo *funcInfo) {
	int32_t totalSize = 0;
	if (nodeId != UNDEF_OID) {
		BNode<K, V> node(
			txn, *getObjectManager(), nodeId, allocateStrategy_);
		int32_t i;
		int32_t size = node.numkeyValues();
		if (depth >= 1) {
			out << std::setw((depth)*5) << "|-->[ " << size << "] ";
		}
		out << node.dump(txn, funcInfo);
		out << "\n";
		if (!node.isLeaf()) {
			for (i = size; i >= 0; --i) {
				if (node.getChild(txn, i) != UNDEF_OID) {
					totalSize +=
						print<K, V>(txn, out, node.getChild(txn, i), depth + 1, funcInfo);
				}
			}
		}
		totalSize += size;
	}
	return totalSize;
}

uint32_t BtreeMap::SearchContext::getRangeConditionNum() {
	const size_t UNDEF_POS = SIZE_MAX;
	uint32_t rangeConditionNum = 0;
	for (size_t i = 0; i < columnIdList_.size(); i++) { 
		size_t eqPos = UNDEF_POS;
		size_t greaterPos = UNDEF_POS;
		size_t lessPos = UNDEF_POS;
		for (util::Vector<size_t>::iterator itr = keyList_.begin(); 
			itr != keyList_.end(); itr++) {
			if (conditionList_[*itr].columnId_ == columnIdList_[i]) {
				if (conditionList_[*itr].opType_ == DSExpression::EQ) {
					eqPos = *itr;
					break;
				} else if (conditionList_[*itr].isStartCondition()) {
					greaterPos = *itr;
				} else if (conditionList_[*itr].isEndCondition()) {
					lessPos = *itr;
				}
			}
		}
		if (eqPos != UNDEF_POS && (greaterPos != UNDEF_POS || lessPos  != UNDEF_POS)) {
			assert(false);
		}
		if (eqPos != UNDEF_POS) {
			rangeConditionNum++;
		} else {
			if (greaterPos != UNDEF_POS || lessPos  != UNDEF_POS) {
				rangeConditionNum++;
			}
			break;
		}
	}
	return rangeConditionNum;
}


#endif 
