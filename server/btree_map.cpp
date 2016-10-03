/*
	Copyright (c) 2012 TOSHIBA CORPORATION.

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
#include "btree_map.h"
#include "data_store_common.h"


template <>
int32_t BtreeMap::getInitialItemSizeThreshold<TransactionId, MvccRowImage>() {
	return INITIAL_MVCC_ITEM_SIZE_THRESHOLD;
}

template <typename K, typename V>
int32_t BtreeMap::getInitialItemSizeThreshold() {
	return INITIAL_DEFAULT_ITEM_SIZE_THRESHOLD;
}

/*!
	@brief Allocate root BtreeMap Object
*/
template int32_t BtreeMap::initialize<TransactionId, MvccRowImage>(
	TransactionContext &txn, ColumnType columnType, bool isUnique,
	BtreeMapType btreeMapType);
template <typename K, typename V>
int32_t BtreeMap::initialize(TransactionContext &txn, ColumnType columnType,
	bool isUnique, BtreeMapType btreeMapType) {
	BaseObject::allocate<BNodeImage<K, V> >(getInitialNodeSize<K, V>(),
		allocateStrategy_, getBaseOId(), OBJECT_TYPE_BTREE_MAP);
	BNode<K, V> rootNode(this, allocateStrategy_);
	rootNode.initialize(txn, getBaseOId(), true,
		getInitialItemSizeThreshold<K, V>(), nodeBlockType_);

	rootNode.setRootNodeHeader(
		columnType, btreeMapType, static_cast<uint8_t>(isUnique), UNDEF_OID);

	return GS_SUCCESS;
}

/*!
	@brief Allocate root BtreeMap Object
*/
int32_t BtreeMap::initialize(TransactionContext &txn, ColumnType columnType,
	bool isUnique, BtreeMapType btreeMapType) {
	int32_t ret = GS_SUCCESS;
	switch (columnType) {
	case COLUMN_TYPE_STRING:
		ret =
			initialize<StringKey, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_BOOL:
		ret = initialize<bool, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_BYTE:
		ret = initialize<int8_t, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_SHORT:
		ret = initialize<int16_t, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_INT:
		ret = initialize<int32_t, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_LONG:
		ret = initialize<int64_t, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = initialize<float32, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = initialize<float64, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret =
			initialize<Timestamp, OId>(txn, columnType, isUnique, btreeMapType);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED, "");
		break;
	}
	return ret;
}

/*!
	@brief Free Objects related to BtreeMap
*/
int32_t BtreeMap::finalize(TransactionContext &txn) {
	setDirty();
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING:
		finalizeInternal<StringKey, OId>(txn);
		break;
	case COLUMN_TYPE_BOOL:
		finalizeInternal<bool, OId>(txn);
		break;
	case COLUMN_TYPE_BYTE:
		finalizeInternal<int8_t, OId>(txn);
		break;
	case COLUMN_TYPE_SHORT:
		finalizeInternal<int16_t, OId>(txn);
		break;
	case COLUMN_TYPE_INT:
		finalizeInternal<int32_t, OId>(txn);
		break;
	case COLUMN_TYPE_LONG:
		finalizeInternal<int64_t, OId>(txn);
		break;
	case COLUMN_TYPE_FLOAT:
		finalizeInternal<float32, OId>(txn);
		break;
	case COLUMN_TYPE_DOUBLE:
		finalizeInternal<float64, OId>(txn);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		finalizeInternal<Timestamp, OId>(txn);
		break;
	case COLUMN_TYPE_OID:
		finalizeInternal<OId, OId>(txn);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED, "");
		break;
	}
	return GS_SUCCESS;
}

/*!
	@brief Insert Row Object
*/
int32_t BtreeMap::insert(
	TransactionContext &txn, const void *constKey, OId oId) {
	assert(oId != UNDEF_OID);
	setDirty();

	int32_t ret = GS_SUCCESS;

	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();

	void *key = const_cast<void *>(constKey);
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(reinterpret_cast<uint8_t *>(key));
		StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCusor));
		ret =
			insertInternal<StringObject, StringKey, OId>(txn, convertKey, oId);
	} break;
	case COLUMN_TYPE_BOOL:
		ret = insertInternal<bool, bool, OId>(
			txn, *reinterpret_cast<bool *>(key), oId);
		break;
	case COLUMN_TYPE_BYTE:
		ret = insertInternal<int8_t, int8_t, OId>(
			txn, *reinterpret_cast<int8_t *>(key), oId);
		break;
	case COLUMN_TYPE_SHORT:
		ret = insertInternal<int16_t, int16_t, OId>(
			txn, *reinterpret_cast<int16_t *>(key), oId);
		break;
	case COLUMN_TYPE_INT:
		ret = insertInternal<int32_t, int32_t, OId>(
			txn, *reinterpret_cast<int32_t *>(key), oId);
		break;
	case COLUMN_TYPE_LONG:
		ret = insertInternal<int64_t, int64_t, OId>(
			txn, *reinterpret_cast<int64_t *>(key), oId);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = insertInternal<float32, float32, OId>(
			txn, *reinterpret_cast<float32 *>(key), oId);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = insertInternal<float64, float64, OId>(
			txn, *reinterpret_cast<float64 *>(key), oId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret = insertInternal<Timestamp, Timestamp, OId>(
			txn, *reinterpret_cast<Timestamp *>(key), oId);
		break;
	case COLUMN_TYPE_OID:
		ret = insertInternal<OId, OId, OId>(
			txn, *reinterpret_cast<OId *>(key), oId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED, "");
		break;
	}

	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}

	return ret;
}

/*!
	@brief Remove Row Object
*/
int32_t BtreeMap::remove(
	TransactionContext &txn, const void *constKey, OId oId) {
	assert(oId != UNDEF_OID);
	setDirty();
	if (isEmpty()) {
		return GS_FAIL;
	}
	int32_t ret = GS_SUCCESS;

	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();

	void *key = const_cast<void *>(constKey);
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(reinterpret_cast<uint8_t *>(key));
		StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCusor));
		ret =
			removeInternal<StringObject, StringKey, OId>(txn, convertKey, oId);
	} break;
	case COLUMN_TYPE_BOOL:
		ret = removeInternal<bool, bool, OId>(
			txn, *reinterpret_cast<bool *>(key), oId);
		break;
	case COLUMN_TYPE_BYTE:
		ret = removeInternal<int8_t, int8_t, OId>(
			txn, *reinterpret_cast<int8_t *>(key), oId);
		break;
	case COLUMN_TYPE_SHORT:
		ret = removeInternal<int16_t, int16_t, OId>(
			txn, *reinterpret_cast<int16_t *>(key), oId);
		break;
	case COLUMN_TYPE_INT:
		ret = removeInternal<int32_t, int32_t, OId>(
			txn, *reinterpret_cast<int32_t *>(key), oId);
		break;
	case COLUMN_TYPE_LONG:
		ret = removeInternal<int64_t, int64_t, OId>(
			txn, *reinterpret_cast<int64_t *>(key), oId);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = removeInternal<float32, float32, OId>(
			txn, *reinterpret_cast<float32 *>(key), oId);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = removeInternal<float64, float64, OId>(
			txn, *reinterpret_cast<float64 *>(key), oId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret = removeInternal<Timestamp, Timestamp, OId>(
			txn, *reinterpret_cast<Timestamp *>(key), oId);
		break;
	case COLUMN_TYPE_OID:
		ret = removeInternal<OId, OId, OId>(
			txn, *reinterpret_cast<OId *>(key), oId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED, "");
		break;
	}

	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}

	return ret;
}

/*!
	@brief Update Row Object
*/
int32_t BtreeMap::update(
	TransactionContext &txn, const void *constKey, OId oId, OId newOId) {
	assert(oId != UNDEF_OID && newOId != UNDEF_OID);
	setDirty();

	if (isEmpty()) {
		return GS_FAIL;
	}

	int32_t ret = GS_SUCCESS;
	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();

	void *key = const_cast<void *>(constKey);
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(reinterpret_cast<uint8_t *>(key));
		StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCusor));
		ret = updateInternal<StringObject, StringKey, OId>(
			txn, convertKey, oId, newOId);
	} break;
	case COLUMN_TYPE_BOOL:
		ret = updateInternal<bool, bool, OId>(
			txn, *reinterpret_cast<bool *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_BYTE:
		ret = updateInternal<int8_t, int8_t, OId>(
			txn, *reinterpret_cast<int8_t *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_SHORT:
		ret = updateInternal<int16_t, int16_t, OId>(
			txn, *reinterpret_cast<int16_t *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_INT:
		ret = updateInternal<int32_t, int32_t, OId>(
			txn, *reinterpret_cast<int32_t *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_LONG:
		ret = updateInternal<int64_t, int64_t, OId>(
			txn, *reinterpret_cast<int64_t *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = updateInternal<float32, float32, OId>(
			txn, *reinterpret_cast<float32 *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = updateInternal<float64, float64, OId>(
			txn, *reinterpret_cast<float64 *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret = updateInternal<Timestamp, Timestamp, OId>(
			txn, *reinterpret_cast<Timestamp *>(key), oId, newOId);
		break;
	case COLUMN_TYPE_OID:
		ret = updateInternal<OId, OId, OId>(
			txn, *reinterpret_cast<OId *>(key), oId, newOId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_INSERT_FAILED, "");
		break;
	}

	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}

	return ret;
}

/*!
	@brief Search Row Object
*/
int32_t BtreeMap::search(
	TransactionContext &txn, const void *constKey, uint32_t keySize, OId &oId) {
	assert(constKey != NULL);
	if (isEmpty()) {
		oId = UNDEF_OID;
		return GS_FAIL;
	}

	oId = UNDEF_OID;
	int32_t ret = GS_SUCCESS;

	void *key = const_cast<void *>(constKey);
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING: {
		StringCursor stringCusor(
			txn, reinterpret_cast<uint8_t *>(key), keySize);
		StringObject convertKey(reinterpret_cast<uint8_t *>(&stringCusor));
		ret = find<StringObject, StringKey, OId>(txn, convertKey, oId);
	} break;
	case COLUMN_TYPE_BOOL:
		ret = find<bool, bool, OId>(txn, *reinterpret_cast<bool *>(key), oId);
		break;
	case COLUMN_TYPE_BYTE:
		ret = find<int8_t, int8_t, OId>(
			txn, *reinterpret_cast<int8_t *>(key), oId);
		break;
	case COLUMN_TYPE_SHORT:
		ret = find<int16_t, int16_t, OId>(
			txn, *reinterpret_cast<int16_t *>(key), oId);
		break;
	case COLUMN_TYPE_INT:
		ret = find<int32_t, int32_t, OId>(
			txn, *reinterpret_cast<int32_t *>(key), oId);
		break;
	case COLUMN_TYPE_LONG:
		ret = find<int64_t, int64_t, OId>(
			txn, *reinterpret_cast<int64_t *>(key), oId);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = find<float32, float32, OId>(
			txn, *reinterpret_cast<float32 *>(key), oId);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = find<float64, float64, OId>(
			txn, *reinterpret_cast<float64 *>(key), oId);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret = find<Timestamp, Timestamp, OId>(
			txn, *reinterpret_cast<Timestamp *>(key), oId);
		break;
	case COLUMN_TYPE_OID:
		ret = find<OId, OId, OId>(txn, *reinterpret_cast<OId *>(key), oId);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_SEARCH_FAILED, "");
		break;
	}
	if (ret != GS_SUCCESS) {
		oId = UNDEF_OID;
	}
	return ret;
}

/*!
	@brief Search Row Objects
*/
int32_t BtreeMap::search(TransactionContext &txn, SearchContext &sc,
	util::XArray<OId> &idList, OutputOrder outputOrder) {
	if (isEmpty()) {
		return GS_FAIL;
	}
	if (sc.limit_ == 0) {
		return GS_SUCCESS;
	}
	int32_t ret = GS_SUCCESS;

	switch (getKeyType()) {
	case COLUMN_TYPE_STRING: {
		void *orgStartKey = const_cast<void *>(sc.startKey_);
		void *orgEndKey = const_cast<void *>(sc.endKey_);
		StringCursor startStringCusor(txn,
			reinterpret_cast<const uint8_t *>(sc.startKey_), sc.startKeySize_);
		StringObject startConvertKey(
			reinterpret_cast<uint8_t *>(&startStringCusor));
		if (sc.startKey_ != NULL) {
			sc.startKey_ = &startConvertKey;
		}
		if (sc.isEqual_ && isUnique()) {
			ret = find<StringObject, StringKey, OId, OId>(
				txn, sc, idList, outputOrder);
			sc.startKey_ = orgStartKey;
		}
		else {
			StringCursor endStringCusor(txn,
				reinterpret_cast<const uint8_t *>(sc.endKey_), sc.endKeySize_);
			StringObject endConvertKey(
				reinterpret_cast<uint8_t *>(&endStringCusor));
			if (sc.endKey_ != NULL) {
				sc.endKey_ = &endConvertKey;
			}
			ret = find<StringObject, StringKey, OId, OId>(
				txn, sc, idList, outputOrder);
			sc.startKey_ = orgStartKey;
			sc.endKey_ = orgEndKey;
		}
	} break;
	case COLUMN_TYPE_BOOL:
		ret = find<bool, bool, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_BYTE:
		ret = find<int8_t, int8_t, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_SHORT:
		ret = find<int16_t, int16_t, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_INT:
		ret = find<int32_t, int32_t, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_LONG:
		ret = find<int64_t, int64_t, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = find<float32, float32, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = find<float64, float64, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret =
			find<Timestamp, Timestamp, OId, OId>(txn, sc, idList, outputOrder);
		break;
	case COLUMN_TYPE_OID:
		ret = find<OId, OId, OId, OId>(txn, sc, idList, outputOrder);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_SEARCH_FAILED, "");
		break;
	}
	return ret;
}

/*!
	@brief Get all Row Objects
*/
int32_t BtreeMap::getAll(
	TransactionContext &txn, ResultSize limit, util::XArray<OId> &idList) {
	int32_t ret = GS_SUCCESS;
	if (isEmpty()) {
		return GS_FAIL;
	}
	if (limit == 0) {
		return GS_SUCCESS;
	}
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING:
		ret = getAllByAscending<StringKey, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_BOOL:
		ret = getAllByAscending<bool, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_BYTE:
		ret = getAllByAscending<int8_t, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_SHORT:
		ret = getAllByAscending<int16_t, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_INT:
		ret = getAllByAscending<int32_t, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_LONG:
		ret = getAllByAscending<int64_t, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = getAllByAscending<float32, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = getAllByAscending<float64, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret = getAllByAscending<Timestamp, OId, OId>(txn, limit, idList);
		break;
	case COLUMN_TYPE_OID:
		ret = getAllByAscending<OId, OId, OId>(txn, limit, idList);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_SEARCH_FAILED, "");
		break;
	}
	return ret;
}

/*!
	@brief Get all Row Objects all at once
*/
int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<OId> &idList, BtreeMap::BtreeCursor &cursor) {
	int32_t ret = GS_SUCCESS;
	if (isEmpty()) {
		return GS_SUCCESS;
	}
	if (limit == 0) {
		return GS_SUCCESS;
	}
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING:
		ret = getAllByAscending<StringKey, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_BOOL:
		ret = getAllByAscending<bool, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_BYTE:
		ret = getAllByAscending<int8_t, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_SHORT:
		ret = getAllByAscending<int16_t, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_INT:
		ret = getAllByAscending<int32_t, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_LONG:
		ret = getAllByAscending<int64_t, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_FLOAT:
		ret = getAllByAscending<float32, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_DOUBLE:
		ret = getAllByAscending<float64, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		ret = getAllByAscending<Timestamp, OId>(txn, limit, idList, cursor);
		break;
	case COLUMN_TYPE_OID:
		ret = getAllByAscending<OId, OId>(txn, limit, idList, cursor);
		break;
	default:
		GS_THROW_SYSTEM_ERROR(GS_ERROR_DS_TM_SEARCH_FAILED, "");
		break;
	}
	return ret;
}


/*!
	@brief Validate the map
*/
std::string BtreeMap::validate(TransactionContext &txn) {
	std::string str;
	switch (getKeyType()) {
	case COLUMN_TYPE_STRING:
		str = validateInternal<StringKey, OId>(txn);
		break;
	case COLUMN_TYPE_BOOL:
		str = validateInternal<bool, OId>(txn);
		break;
	case COLUMN_TYPE_BYTE:
		str = validateInternal<int8_t, OId>(txn);
		break;
	case COLUMN_TYPE_SHORT:
		str = validateInternal<int16_t, OId>(txn);
		break;
	case COLUMN_TYPE_INT:
		str = validateInternal<int32_t, OId>(txn);
		break;
	case COLUMN_TYPE_LONG:
		str = validateInternal<int64_t, OId>(txn);
		break;
	case COLUMN_TYPE_FLOAT:
		str = validateInternal<float32, OId>(txn);
		break;
	case COLUMN_TYPE_DOUBLE:
		str = validateInternal<float64, OId>(txn);
		break;
	case COLUMN_TYPE_TIMESTAMP:
		str = validateInternal<Timestamp, OId>(txn);
		break;
	case COLUMN_TYPE_OID:
		str = validateInternal<OId, OId>(txn);
		break;
	default:
		break;
	}
	return str;
}

template <typename K, typename V>
std::string BtreeMap::validateInternal(TransactionContext &txn) {
	if (isEmpty()) {
		return "empty";
	}
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	for (OId nodeId = getRootOId(); nodeId != UNDEF_OID;) {
		node.load(nodeId);
		if (node.isLeaf()) {
			break;
		}
		nodeId = node.getChild(txn, 0);
	}

	int32_t loc = 0;
	KeyValue<K, V> currentVal, nextVal;
	while (true) {
		currentVal = node.getKeyValue(loc);
		if (!nextPos(txn, node, loc)) {
			break;
		}
		nextVal = node.getKeyValue(loc);
		if (keyCmp<K, V>(txn, *getObjectManager(), currentVal, nextVal) > 0) {
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
		for (int32_t i = 0; i < nodeMaxSize_ + 1; ++i) {
			getTreeStatus<K, V>(txn, node.getChild(txn, i), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode,
				depth + 1);
		}
	}
}

std::string BtreeMap::dump(TransactionContext &txn, uint8_t mode) {
	util::NormalOStringStream strstrm;
	if (mode == SUMMARY_DUMP) {
		int32_t nodeNum = 0;
		int32_t realKeyNum = 0;
		int32_t keySpaceNum = 0;
		int32_t maxDepth = 0;
		int32_t minDepth = INT32_MAX;
		int32_t maxKeyNode = 0;
		int32_t minKeyNode = INT32_MAX;

		switch (getKeyType()) {
		case COLUMN_TYPE_STRING:
			getTreeStatus<StringKey, OId>(txn, getRootOId(), nodeNum,
				realKeyNum, keySpaceNum, maxDepth, minDepth, maxKeyNode,
				minKeyNode, 0);
			break;
		case COLUMN_TYPE_BOOL:
			getTreeStatus<bool, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_BYTE:
			getTreeStatus<int8_t, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_SHORT:
			getTreeStatus<int16_t, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_INT:
			getTreeStatus<int32_t, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_LONG:
			getTreeStatus<int64_t, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_FLOAT:
			getTreeStatus<float32, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_DOUBLE:
			getTreeStatus<float64, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		case COLUMN_TYPE_TIMESTAMP:
			getTreeStatus<Timestamp, OId>(txn, getRootOId(), nodeNum,
				realKeyNum, keySpaceNum, maxDepth, minDepth, maxKeyNode,
				minKeyNode, 0);
			break;
		case COLUMN_TYPE_OID:
			getTreeStatus<OId, OId>(txn, getRootOId(), nodeNum, realKeyNum,
				keySpaceNum, maxDepth, minDepth, maxKeyNode, minKeyNode, 0);
			break;
		default:
			break;
		}

		strstrm << "{BTreeStatus:{" << std::endl;
		strstrm << "\t nodeNum : " << nodeNum << "," << std::endl;
		strstrm << "\t realKeyNum : " << realKeyNum << "," << std::endl;
		strstrm << "\t keySpaceNum : " << keySpaceNum << "," << std::endl;
		strstrm << "\t maxDepth : " << maxDepth << "," << std::endl;
		strstrm << "\t minDepth : " << minDepth << "," << std::endl;
		strstrm << "\t maxKeyNode : " << maxKeyNode << "," << std::endl;
		strstrm << "\t minKeyNode : " << minKeyNode << "," << std::endl;
		strstrm << "}" << std::endl;
	}
	else {
		switch (getKeyType()) {
		case COLUMN_TYPE_STRING:
			print<StringKey, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_BOOL:
			print<bool, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_BYTE:
			print<int8_t, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_SHORT:
			print<int16_t, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_INT:
			print<int32_t, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_LONG:
			print<int64_t, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_FLOAT:
			print<float32, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_DOUBLE:
			print<float64, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_TIMESTAMP:
			print<Timestamp, OId>(txn, strstrm, getRootOId());
			break;
		case COLUMN_TYPE_OID:
			print<OId, OId>(txn, strstrm, getRootOId());
			break;
		default:
			break;
		}
	}
	return strstrm.str();
}

/*!
	@brief Insert general key and value
*/
template int32_t BtreeMap::insert(
	TransactionContext &txn, TransactionId &key, MvccRowImage &value);
template <typename K, typename V>
int32_t BtreeMap::insert(TransactionContext &txn, K &key, V &value) {
	setDirty();

	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();
	bool isSuccess =
		insertInternal<K, K, V>(txn, key, value, &valueCmp, &valueCmp);

	int32_t ret = (isSuccess) ? GS_SUCCESS : GS_FAIL;
	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}
	return ret;
}

/*!
	@brief Remove general key and value
*/
template int32_t BtreeMap::remove(
	TransactionContext &txn, TransactionId &key, MvccRowImage &value);
template <typename K, typename V>
int32_t BtreeMap::remove(TransactionContext &txn, K &key, V &value) {
	setDirty();

	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();
	bool isSuccess =
		removeInternal<K, K, V>(txn, key, value, &valueCmp, &valueCmp);

	int32_t ret = (isSuccess) ? GS_SUCCESS : GS_FAIL;
	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}
	return ret;
}

/*!
	@brief Update general key and value
*/
template int32_t BtreeMap::update(TransactionContext &txn, TransactionId &key,
	MvccRowImage &oldValue, MvccRowImage &newValue);
template <typename K, typename V>
int32_t BtreeMap::update(
	TransactionContext &txn, K &key, V &oldValue, V &newValue) {
	setDirty();

	OId beforeRootOId = getRootOId();
	OId beforeTailOId = getTailNodeOId();
	int32_t ret;
	if (isUnique()) {
		ret = updateInternal<K, K, V>(txn, key, oldValue, newValue, &valueCmp);
	}
	else {
		bool isSuccess;
		isSuccess =
			removeInternal<K, K, V>(txn, key, oldValue, &valueCmp, &valueCmp);
		if (isSuccess) {
			isSuccess = insertInternal<K, K, V>(
				txn, key, newValue, &valueCmp, &valueCmp);
		}
		ret = (isSuccess) ? GS_SUCCESS : GS_FAIL;
	}

	if (beforeRootOId != getRootOId()) {
		ret = (ret | ROOT_UPDATE);
	}
	if (beforeTailOId != getTailNodeOId()) {
		ret = (ret | TAIL_UPDATE);
	}
	return ret;
}

template <typename K, typename V>
std::string BtreeMap::dump(TransactionContext &txn) {
	util::NormalOStringStream strstrm;
	print<K, V>(txn, strstrm, getRootOId());
	return strstrm.str();
}

/*!
	@brief Get all keys and values all at once
*/
template int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<std::pair<TransactionId, MvccRowImage> > &keyValueList);
template <typename K, typename V>
int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<std::pair<K, V> > &keyValueList) {
	if (isEmpty()) {
		return GS_SUCCESS;
	}
	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	getHeadNode<K, V>(txn, node);

	int32_t loc = 0;
	while (true) {
		keyValueList.push_back(std::make_pair(
			node.getKeyValue(loc).key_, node.getKeyValue(loc).value_));
		if (!nextPos(txn, node, loc) || keyValueList.size() >= limit) {
			break;
		}
	}
	return GS_SUCCESS;
}

/*!
	@brief Get all keys and values
*/
template int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<std::pair<TransactionId, MvccRowImage> > &keyValueList,
	BtreeMap::BtreeCursor &cursor);
template <typename K, typename V>
int32_t BtreeMap::getAll(TransactionContext &txn, ResultSize limit,
	util::XArray<std::pair<K, V> > &idList, BtreeMap::BtreeCursor &cursor) {
	if (isEmpty()) {
		return GS_SUCCESS;
	}
	if (limit == 0) {
		return GS_SUCCESS;
	}

	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc = 0;
	if (cursor.nodeId_ == UNDEF_OID) {
		getHeadNode<K, V>(txn, node);
		loc = 0;
	}
	else {
		node.load(cursor.nodeId_);
		loc = cursor.loc_;
	}
	bool hasNext = false;
	while (true) {
		idList.push_back(std::make_pair(
			node.getKeyValue(loc).key_, node.getKeyValue(loc).value_));
		hasNext = nextPos(txn, node, loc);
		if (!hasNext || idList.size() >= limit) {
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