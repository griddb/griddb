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
	@brief Implementation of BtreeMap extension
*/
#include "btree_map_ext.h"

#define GS_BTREE_MAP_DEFINE_SWITCHER_ONLY 1
#include "btree_map.cpp"


int32_t BtreeMap::searchBulk(
		TransactionContext &txn, SearchContext &sc, util::XArray<OId> &idList,
		OutputOrder outputOrder) {
	if (isEmpty()) {
		TermConditionUpdator *condUpdator = sc.getTermConditionUpdator();
		assert(condUpdator != NULL);
		condUpdator->close();
		return GS_FAIL;
	}
	if (sc.getLimit() == 0) {
		return GS_SUCCESS;
	}

	const bool suspendedLast = (sc.getSuspendKey() != NULL);

	SearchBulkFunc searchFunc(txn, sc, idList, outputOrder, this);
	switchToBasicType(getKeyType(), searchFunc);

	if (suspendedLast && !sc.isSuspended()) {
		switchToBasicType(getKeyType(), searchFunc);
	}

	if (!sc.isSuspended()) {
		ResultSize suspendLimit = std::max<ResultSize>(sc.getSuspendLimit(), 1);
		suspendLimit -= std::min<ResultSize>(suspendLimit, idList.size());
		if (suspendLimit < MINIMUM_SUSPEND_SIZE) {
			suspendLimit = MINIMUM_SUSPEND_SIZE;
		}
		sc.setSuspendLimit(suspendLimit);
	}

	return searchFunc.ret_;
}

template<
		typename P, typename K, typename V, typename R,
		BtreeMap::CompareComponent C>
int32_t BtreeMap::findBulk(
		TransactionContext &txn, SearchContext &sc, util::XArray<R> &idList,
		OutputOrder outputOrder) {
	assert(outputOrder != ORDER_DESCENDING);

	UTIL_STATIC_ASSERT((!util::IsSame<V, uint32_t>::VALUE));

	util::StackAllocator &settingAlloc = sc.getSettingAllocator();
	util::LocalUniquePtr<util::StackAllocator::Scope> settingAllocScope;
	Setting setting(getKeyType(), sc.isCaseSensitive(), getFuncInfo());

	assert(!isEmpty());

	BNode<K, V> node(txn, *getObjectManager(), allocateStrategy_);
	int32_t loc = -1;

	TermConditionRewriter<P, K, V, R> rewriter(txn, sc);
	TermConditionUpdator *condUpdator = sc.getTermConditionUpdator();

	assert(condUpdator != NULL);
	condUpdator->bind(sc);

	bool initializedForNext = false;
	const bool suspendedLast = (sc.getSuspendKey() != NULL);

	while (condUpdator->exists()) {
		rewriter.rewrite();

		if (!initializedForNext) {
			settingAllocScope = UTIL_MAKE_LOCAL_UNIQUE(
					settingAllocScope, util::StackAllocator::Scope,
					settingAlloc);
			setting.initialize(
					settingAlloc, sc, outputOrder, &initializedForNext);
		}

		if (findNext<P, K, V, R, C>(txn, sc, idList, setting, node, loc)) {
			break;
		}

		condUpdator->next();

		if (suspendedLast) {
			sc.setSuspendKey(NULL);
			sc.setSuspendValue(NULL);
			sc.setSuspended(false);
			break;
		}
	}

	condUpdator->unbind();

	if (idList.empty()) {
		return GS_FAIL;
	}
	else {
		return GS_SUCCESS;
	}
}

template<
		typename P, typename K, typename V,
		BtreeMap::CompareComponent C>
int32_t BtreeMap::findNext(
		TransactionContext &txn, P &key, KeyValue<K, V> &keyValue,
		Setting &setting, BNode<K, V> &node, int32_t &loc) {
	KeyValue<P, V> val;
	val.key_ = key;
	val.value_ = V();
	CmpFunctor<P, K, V, C> cmpFunctor;
	bool ret = findNodeNext<P, K, V>(txn, val, node, loc, cmpFunctor, setting);

	if (ret) {
		keyValue = node.getKeyValue(loc);
		return GS_SUCCESS;
	}
	else {
		if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
			if (prevPos(txn, node, loc)) {
				keyValue = node.getKeyValue(loc);
				return GS_SUCCESS;
			}
			else {
				return GS_FAIL;
			}
		}
		else {
			return GS_FAIL;
		}
	}
}

template<
		typename P, typename K, typename V, typename R,
		BtreeMap::CompareComponent C>
bool BtreeMap::findNext(
		TransactionContext &txn, SearchContext &sc, util::XArray<R> &idList,
		Setting &setting, BNode<K, V> &node, int32_t &loc) {
	typedef typename MapKeyTraits<K>::TYPE T;

	TermCondition *startCond = setting.getStartKeyCondition();
	TermCondition *endCond = setting.getEndKeyCondition();

	ResultSize suspendLimit = sc.getSuspendLimit();
	KeyValue<K, V> suspendKeyValue;
	bool isSuspend = false;
	if (startCond != NULL && startCond->opType_ == DSExpression::EQ &&
		setting.getGreaterCompareNum() == sc.getKeyColumnNum() && isUnique()) {
		if (startCond == NULL) {
			errorInvalidSearchCondition();
			return false;
		}
		if (idList.size() >= suspendLimit) {
			sc.setSuspended(true);
			isSuspend = true;
			return isSuspend;
		}
		else {
			P key = *reinterpret_cast<const P *>(startCond->value_);
			KeyValue<K, V> keyValue;
			int32_t ret =
					findNext<P, K, V, C>(txn, key, keyValue, setting, node, loc);
			if (ret == GS_SUCCESS) {
				pushResultList<K, V, R>(keyValue, idList);
			}
		}
	}
	else if (startCond != NULL) {
		KeyValue<P, V> startKeyVal(*reinterpret_cast<const P *>(startCond->value_), V());
		if (C == KEY_VALUE_COMPONENT) {
			startKeyVal.value_ = *reinterpret_cast<const V *>(sc.getSuspendValue());
		}
		if (endCond != NULL) {
			KeyValue<P, V> endKeyVal(*reinterpret_cast<const P *>(endCond->value_), V());
			if (C == KEY_VALUE_COMPONENT) {
				assert(sc.getSuspendValue() != NULL);
				endKeyVal.value_ = (setting.isEndIncluded_) ? getMaxValue<V>() : getMinValue<V>();
			}
			isSuspend = findRangeNext<P, K, V, R, C>(
					txn, startKeyVal,
					startCond->isIncluded(), endKeyVal, endCond->isIncluded(),
					sc.getLimit(), idList, suspendLimit,
					suspendKeyValue, setting, node, loc);
		}
		else if (startCond->opType_ == DSExpression::EQ) {
			KeyValue<P, V> endKeyVal(*reinterpret_cast<const P *>(startCond->value_), V());
			if (C == KEY_VALUE_COMPONENT) {
				assert(sc.getSuspendValue() != NULL);
				endKeyVal.value_ = (setting.isEndIncluded_) ? getMaxValue<V>() : getMinValue<V>();
			}
			isSuspend = findRangeNext<P, K, V, R, C>(
					txn, startKeyVal,
					startCond->isIncluded(), endKeyVal, startCond->isIncluded(),
					sc.getLimit(), idList, suspendLimit,
					suspendKeyValue, setting, node, loc);
		}
		else {
			isSuspend = findGreaterNext<P, K, V, R, C>(
					txn, startKeyVal,
					startCond->isIncluded(), sc.getLimit(), idList,
					suspendLimit, suspendKeyValue, setting, node, loc);
		}
	}
	else if (endCond != NULL) {
		KeyValue<P, V> endKeyVal(*reinterpret_cast<const P *>(endCond->value_), V());
		if (C == KEY_VALUE_COMPONENT) {
			assert(sc.getSuspendValue() != NULL);
			endKeyVal.value_ = *reinterpret_cast<const V *>(sc.getSuspendValue());
		}
		isSuspend = findLess<P, K, V, R, C>(
				txn, endKeyVal,
				endCond->isIncluded(), sc.getLimit(), idList,
				suspendLimit, suspendKeyValue, setting);
	}
	else {
		isSuspend = getAllByAscending<P, K, V, R>(
			txn, sc.getLimit(), idList, suspendLimit, suspendKeyValue, setting);
	}
	if (isSuspend) {
		sc.setSuspended(true);
		sc.setSuspendPoint<T, V>(txn, *getObjectManager(), allocateStrategy_, getFuncInfo(),
			suspendKeyValue.key_, suspendKeyValue.value_);
	}
	return isSuspend;
}

template<
		typename P, typename K, typename V, typename R,
		BtreeMap::CompareComponent C>
bool BtreeMap::findGreaterNext(
		TransactionContext &txn, KeyValue<P, V> &keyValue,
		int32_t isIncluded, ResultSize limit, util::XArray<R> &result,
		ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting, BNode<K, V> &node, int32_t &loc) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	setting.setCompareNum(setting.getGreaterCompareNum());

	bool isSuspend = false;
	bool isEqual = findNodeNext<P, K, V, C>(txn, keyValue, node, loc, cmpFuncLeft, setting);

	if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
		if (loc >= node.numkeyValues() || !isEqual) {
			prevPos(txn, node, loc);
		}
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			pushResultList<K, V, R>(currentVal, result);
			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	else {
		if (loc >= node.numkeyValues()) {
			prevPos(txn, node, loc);
		}
		else {
			while (isIncluded) {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue,
						node.getKeyValue(loc), setting) > 0) {
					break;
				}
				if (!prevPos(txn, node, loc)) {
					break;
				}
			}
		}
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
				pushResultList<K, V, R>(currentVal, result);
			}
			else {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, keyValue, currentVal,
					setting) < isIncluded) {
					if (!isComposite<P>() || isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
					start = true;
				}
			}
			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	return isSuspend;
}

template<
		typename P, typename K, typename V, typename R,
		BtreeMap::CompareComponent C>
bool BtreeMap::findRangeNext(
		TransactionContext &txn, KeyValue<P, V> &startKeyValue,
		int32_t isStartIncluded, KeyValue<P, V> &endKeyValue,
		int32_t isEndIncluded, ResultSize limit, util::XArray<R> &result,
		ResultSize suspendLimit, KeyValue<K, V> &suspendKeyValue,
		Setting &setting, BNode<K, V> &node, int32_t &loc) {
	CmpFunctor<P, K, V, C> cmpFuncLeft;
	CmpFunctor<K, P, V, C> cmpFuncRight;

	bool isSuspend = false;

	setting.setCompareNum(setting.getGreaterCompareNum());
	bool isEqual = findNodeNext<P, K, V, C>(
			txn, startKeyValue, node, loc, cmpFuncLeft, setting);
	if (getBtreeMapType() == TYPE_UNIQUE_RANGE_KEY) {
		if (loc >= node.numkeyValues() || !isEqual) {
			prevPos(txn, node, loc);
		}
		setting.setCompareNum(setting.getLessCompareNum());
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, endKeyValue,
					setting) < isEndIncluded) {
				pushResultList<K, V, R>(currentVal, result);
			}
			else {
				break;
			}

			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				isSuspend = true;
				suspendKeyValue = node.getKeyValue(loc);
				break;
			}
		}
	}
	else {
		if (loc >= node.numkeyValues()) {
			prevPos(txn, node, loc);
		}
		else {
			setting.setCompareNum(setting.getGreaterCompareNum());
			while (isStartIncluded) {
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue,
						node.getKeyValue(loc), setting) > 0) {
					break;
				}
				if (!prevPos(txn, node, loc)) {
					break;
				}
			}
		}
		bool start = false;
		while (true) {
			KeyValue<K, V> currentVal = node.getKeyValue(loc);
			if (start) {
				setting.setCompareNum(setting.getLessCompareNum());
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal, endKeyValue,
						setting) < isEndIncluded) {
					if (!isComposite<P>() ||
						isMatch(txn, *getObjectManager(), currentVal, setting))
					pushResultList<K, V, R>(currentVal, result);
				}
				else {
					break;
				}
			}
			else {
				setting.setCompareNum(setting.getGreaterCompareNum());
				if (cmpFuncLeft(txn, *getObjectManager(), allocateStrategy_, startKeyValue, currentVal,
						setting) < isStartIncluded) {
					setting.setCompareNum(setting.getLessCompareNum());
					if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, currentVal,
							endKeyValue, setting) < isEndIncluded) {
						if (!isComposite<P>() ||
							isMatch(txn, *getObjectManager(), currentVal, setting))
						pushResultList<K, V, R>(currentVal, result);
						start = true;
					}
					else {
						break;
					}
				}
			}

			if (!nextPos(txn, node, loc) || result.size() >= limit) {
				break;
			}
			if (result.size() >= suspendLimit) {
				setting.setCompareNum(setting.getLessCompareNum());
				if (cmpFuncRight(txn, *getObjectManager(), allocateStrategy_, node.getKeyValue(loc),
						endKeyValue, setting) < isEndIncluded) {
					isSuspend = true;
					suspendKeyValue = node.getKeyValue(loc);
				}
				break;
			}
		}
	}
	return isSuspend;
}

template<typename P, typename K, typename V, BtreeMap::CompareComponent C>
bool BtreeMap::findNodeNext(
		TransactionContext &txn, KeyValue<P, V> &val, BNode<K, V> &node,
		int32_t &loc, CmpFunctor<P, K, V, C> &cmp, Setting &setting) {
	assert(!isEmpty());

	if (findTopNodeNext<P, K, V, C>(txn, val, node, loc, cmp, setting)) {
		return true;
	}

	const bool isUniqueKey = isUnique();
	int32_t initialLoc = loc;
	for (;;) {
		OId nodeId;
		const int32_t size = node.numkeyValues();
		if (cmp(txn, *getObjectManager(), allocateStrategy_, val,
				node.getKeyValue(size - 1), setting) > 0) {
			if (node.isLeaf()) {
				loc = size;
				return false; 
			}
			else {
				nodeId = node.getChild(txn, size);
			}
		}
		else { 
			assert(0 <= initialLoc && initialLoc < size);
			int32_t l = 0;
			int32_t r = size - initialLoc;
			while (l < r) {
				int32_t i = (l + r) >> 1;
				int32_t currentCmpResult =
					cmp(txn, *getObjectManager(), allocateStrategy_, val,
						node.getKeyValue(initialLoc + i), setting);
				if (currentCmpResult > 0) { 
					l = i + 1;
				}
				else if (isUniqueKey && currentCmpResult == 0) {
					loc = initialLoc + i;
					return true; 
				}
				else {
					r = i; 
				}
			}
			assert(r == l);
			l += initialLoc;
			int32_t cmpResult = cmp(txn, *getObjectManager(),
				allocateStrategy_, val, node.getKeyValue(l), setting);
			if (cmpResult < 0) { 
				if (node.isLeaf()) {
					loc = l;
					return false; 
				}
				else {
					nodeId = node.getChild(txn, l);
				}
			}
			else if (cmpResult > 0) { 
				if (node.isLeaf()) {
					loc = l + 1;
					return false; 
				}
				else {
					nodeId = node.getChild(txn, l + 1);
				}
			}
			else {
				if (node.isRoot()) {
					getRootBNodeObject<K, V>(node);
				}
				loc = l;
				return true; 
			}
		}
		if (nodeId == UNDEF_OID) {
			break;
		}
		node.load(nodeId, false);
		initialLoc = 0;
	}
	assert(false);
	node.reset();
	loc = 0;
	return false;
}

template<typename P, typename K, typename V, BtreeMap::CompareComponent C>
bool BtreeMap::findTopNodeNext(
		TransactionContext &txn, KeyValue<P, V> &val, BNode<K, V> &node,
		int32_t &loc, CmpFunctor<P, K, V, C> &cmp, Setting &setting) {
	assert(!isEmpty());

	const bool isUniqueKey = isUnique();
	do {
		if (node.getBaseAddr() == NULL) {
			break;
		}

		assert(loc >= 0);

		for (;;) {
			const int32_t size = node.numkeyValues();

			if (
					cmp(
							txn, *getObjectManager(), allocateStrategy_, val,
							node.getKeyValue(size - 1), setting) > 0 ||
					cmp(
							txn, *getObjectManager(), allocateStrategy_, val,
							node.getKeyValue(0), setting) < 0) {
				if (node.isRoot()) {
					break;
				}

				node.load(node.getParentOId(), false);
				loc = 0;
				continue;
			}


			const int32_t initialLoc = loc;
			assert(0 <= initialLoc && initialLoc < size);

			int32_t l = 0;
			int32_t r = size - initialLoc;
			while (l < r) {
				const int32_t i = (l + r) >> 1;
				const int32_t currentCmpResult = cmp(
						txn, *getObjectManager(), allocateStrategy_, val,
						node.getKeyValue(initialLoc + i), setting);
				if (currentCmpResult > 0) { 
					l = i + 1;
				}
				else if (isUniqueKey && currentCmpResult == 0) {
					loc = initialLoc + i;
					return true; 
				}
				else {
					r = i; 
				}
			}
			assert(r == l);

			if (!isUniqueKey && cmp(txn, *getObjectManager(),
					allocateStrategy_, val,
					node.getKeyValue(initialLoc + l), setting) == 0) {
				loc = initialLoc + l;
				return true; 
			}
			loc = 0;
			return false;

		}
	}
	while (false);

	getRootBNodeObject<K, V>(node);
	loc = 0;
	return false;
}

void BtreeMap::errorInvalidSearchCondition() {
	GS_THROW_USER_ERROR(
		GS_ERROR_DS_UNEXPECTED_ERROR, "Internal error");
}

BtreeMap::TermConditionUpdator::~TermConditionUpdator() {
}

BtreeMap::TermConditionRewriter<
StringObject, StringKey, OId, OId>::TermConditionRewriter(
		TransactionContext &txn, SearchContext &sc) :
		sc_(sc) {
	util::StackAllocator &alloc = txn.getDefaultAllocator();

	for (size_t i = 0; i < ENTRY_COUNT; i++) {
		Entry &entry = entryList_[i];

		util::LocalUniquePtr<Buffer> &buf = entry.buf_;
		buf = UTIL_MAKE_LOCAL_UNIQUE(buf, Buffer, alloc);
	}
}

BtreeMap::TermConditionRewriter<
StringObject, StringKey, OId, OId>::~TermConditionRewriter() {
	for (size_t i = 0; i < ENTRY_COUNT; i++) {
		Entry &entry = entryList_[i];
		if (entry.cond_ == NULL) {
			continue;
		}

		entry.cond_->value_ = entry.orgKey_;
	}
}

void BtreeMap::TermConditionRewriter<
StringObject, StringKey, OId, OId>::rewrite() {
	for (size_t i = 0; i < ENTRY_COUNT; i++) {
		Entry &entry = entryList_[i];

		entry.cond_ = (i == 0 ?
				sc_.getStartKeyCondition() : sc_.getEndKeyCondition());
		if (entry.cond_ == NULL) {
			continue;
		}

		entry.orgKey_ = entry.cond_->value_;

		util::LocalUniquePtr<StringCursor> &cursor = entry.cursor_;
		cursor = UTIL_MAKE_LOCAL_UNIQUE(
				cursor, StringCursor,
				*entry.buf_, static_cast<const uint8_t *>(entry.cond_->value_),
				entry.cond_->valueSize_);
		entry.obj_.ptr_ = reinterpret_cast<uint8_t *>(cursor.get());
		entry.cond_->value_ = &entry.obj_;
	}
}

template <typename P, typename K, typename V, typename R>
void BtreeMap::SearchBulkFunc::execute() {
	if (sc_.getSuspendValue() != NULL) {
		ret_ = tree_->findBulk<P, K, V, V, KEY_VALUE_COMPONENT>(txn_, sc_, idList_, outputOrder_);
	}
	else {
		ret_ = tree_->findBulk<P, K, V, V, KEY_COMPONENT>(txn_, sc_, idList_, outputOrder_);
	}
}
