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
	@brief Definition of QueryForTimeSeries
*/

#ifndef QUERY_TIMESERIES_H_
#define QUERY_TIMESERIES_H_

#include "query.h"
#include "schema.h"
#include "selection_func.h"

/*!
 * @brief Query object for TimeSeries
 *
 */
class QueryForTimeSeries : public Query {
	friend class lemon_tqlParser::tqlParser;
	friend class QueryStopwatchHook;
	friend class QueryProcessor;
	friend class Expr;
	friend class BoolExpr;

public:
	QueryForTimeSeries(TransactionContext &txn, TimeSeries &timeSeries,
		const TQLInfo &tqlInfo, uint64_t limit = MAX_RESULT_SIZE,
		QueryHookClass *hook = NULL);
	QueryForTimeSeries(TransactionContext &txn, TimeSeries &timeSeries, AllocateStrategy& strategy)
		: Query(txn, *(timeSeries.getObjectManager()), strategy),
		  timeSeries_(&timeSeries), scPass_(txn.getDefaultAllocator(), ColumnInfo::ROW_KEY_COLUMN_ID) {}
	virtual ~QueryForTimeSeries() {}

	void doQuery(
		TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet);
	QueryForTimeSeries *dup(
		TransactionContext &txn, ObjectManagerV4 &objectManager, AllocateStrategy &strategy);

protected:
	virtual Collection *getCollection() {
		return NULL;
	}
	virtual TimeSeries *getTimeSeries() {
		return timeSeries_;
	}

	TimeSeries
		*timeSeries_;  
	void doQueryWithoutCondition(
		TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet);
	void doQueryWithCondition(
		TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet);
	void doSelection(
		TransactionContext &txn, TimeSeries &timeSeries, ResultSet &resultSet);
	void doSelectionByAPI(TransactionContext &txn, TimeSeries &timeSeries,
		ResultSet &resultSet);

	BtreeMap::SearchContext
		scPass_;  
	QpPassMode
		nPassSelectRequested_;  
	bool
		isRowKeySearched_;  
	OutputOrder
		apiOutputOrder_;  

	void tsResultMerge(TransactionContext &txn, TimeSeries &timeSeries,

		util::XArray<PointRowId> &idList1, util::XArray<PointRowId> &idList2,
		util::XArray<PointRowId> &outIdList);
};

/*!
*	@brief Interface for TimeSeries Row
*/
class TimeSeriesRowWrapper : public ContainerRowWrapper {
public:
	TimeSeriesRowWrapper(TransactionContext &txn, TimeSeries &timeSeries,
		PointRowId rowId, uint64_t *pBitmap)
		: txn_(txn),
		  timeSeries_(timeSeries),
		  rowId_(rowId),
		  rowArray_(txn_, &timeSeries_),
		  pBitmap_(pBitmap),
		  varrayCounter_(0) {
		rowArray_.load(txn_, rowId_, &timeSeries_, OBJECT_READ_ONLY);
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		varray_ = reinterpret_cast<ContainerValue *>(
			alloc.allocate(sizeof(ContainerValue) * timeSeries.getColumnNum()));
		try {
			for (uint32_t i = 0; i < timeSeries_.getColumnNum(); i++) {
				new (&(varray_[i])) ContainerValue(
					*(timeSeries
							.getObjectManager()),
					timeSeries.getRowAllocateStrategy());  
				varrayCounter_++;
			}
			memset(pBitmap, 0,
				sizeof(uint64_t) * ((timeSeries_.getColumnNum() / 64) + 1));
		}
		catch (...) {
			for (uint32_t i = 0; i < varrayCounter_; i++) {
				varray_[i]
					.getBaseObject()
					.reset();  
			}
			ALLOC_DELETE((alloc), varray_);
			throw;
		}
	}

	TimeSeriesRowWrapper(TransactionContext &txn, TimeSeries &timeSeries,
		uint64_t *pBitmap)
		: txn_(txn),
		  timeSeries_(timeSeries),
		  rowId_(UNDEF_OID),
		  rowArray_(txn_, &timeSeries_),
		  pBitmap_(pBitmap),
		  varrayCounter_(0) {
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		varray_ = reinterpret_cast<ContainerValue *>(
			alloc.allocate(sizeof(ContainerValue) * timeSeries_.getColumnNum()));
		try {
			for (uint32_t i = 0; i < timeSeries_.getColumnNum(); i++) {
				new (&(varray_[i])) ContainerValue(
					*(timeSeries_
						.getObjectManager()),
					timeSeries.getRowAllocateStrategy());  
				varrayCounter_++;
			}
			memset(pBitmap, 0,
				sizeof(uint64_t) * ((timeSeries_.getColumnNum() / 64) + 1));
		}
		catch (...) {
			for (uint32_t i = 0; i < varrayCounter_; i++) {
				varray_[i]
					.getBaseObject()
					.reset();  
			}
			ALLOC_DELETE((alloc), varray_);
			throw;
		}
	}

	~TimeSeriesRowWrapper() {
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		for (uint32_t i = 0; i < varrayCounter_; i++) {
			varray_[i]
				.getBaseObject()
				.reset();  
		}
		ALLOC_DELETE((alloc), varray_);
	}
	void load(OId oId) {
		rowId_ = oId;
		rowArray_.load(txn_, rowId_, &timeSeries_, OBJECT_READ_ONLY);
		memset(pBitmap_, 0,
			sizeof(uint64_t) * ((timeSeries_.getColumnNum() / 64) + 1));
		for (uint32_t i = 0; i < varrayCounter_; i++) {
			varray_[i].getBaseObject().reset();  
		}
	}

	const Value *getColumn(uint32_t k) {
		ContainerValue &v = varray_[k];
		if (bit_off(pBitmap_[k / 64], k % 64)) {
			BaseContainer::RowArray::Row row(rowArray_.getRow(), &rowArray_);
			row.getField(txn_, timeSeries_.getColumnInfo(k), v);
			set_bit(pBitmap_[k / 64], k % 64);
		}
		return &(v.getValue());
	}

	RowId getRowId() {
		BaseContainer::RowArray::Row row(rowArray_.getRow(), &rowArray_);
		return row.getRowId();
	}
	void getImage(TransactionContext &txn,
		MessageRowStore *messageRowStore, bool isWithRowId) {
		BaseContainer::RowArray::Row row(rowArray_.getRow(), &rowArray_);
		row.getImage(txn, messageRowStore, isWithRowId);
	}

	void setValue(const Value &v, uint32_t columnId) {
		varray_[columnId].set(v.data(), v.getType());
		pBitmap_[columnId / 64] |= (1ULL << columnId % 64);
	}

private:
	inline bool bit_off(uint64_t word, size_t i) {
		return (word & (1ULL << i)) == 0;
	}
	inline void set_bit(uint64_t &word, size_t i) {
		word |= (1ULL << i);
	}
	TransactionContext &txn_;
	TimeSeries &timeSeries_;
	PointRowId rowId_;
	ContainerValue *varray_;
	BaseContainer::RowArray rowArray_;
	uint64_t *pBitmap_;
	size_t varrayCounter_;
};

/*!
*	@brief Compare method for TimeSeries Row
*/
class TimeSeriesOrderByComparator {
public:
	TimeSeriesOrderByComparator(TransactionContext &txn, TimeSeries &timeSeries,
		FunctionMap &fmap, SortExprList &theExprList,
		uint32_t columnId = UNDEF_COLUMNID)
		: txn_(txn),
		  timeSeries_(timeSeries),
		  fmap_(fmap),
		  orderByExprList_(theExprList),
		  interpolateColumnId_(columnId) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		varray1_ = reinterpret_cast<Value *>(
			alloc.allocate(sizeof(Value) * timeSeries_.getColumnNum()));
		varray2_ = reinterpret_cast<Value *>(
			alloc.allocate(sizeof(Value) * timeSeries_.getColumnNum()));
		pBitmap1_ = reinterpret_cast<uint64_t *>(alloc.allocate(
			sizeof(uint64_t) * ((timeSeries_.getColumnNum() / 64) + 1)));
		pBitmap2_ = reinterpret_cast<uint64_t *>(alloc.allocate(
			sizeof(uint64_t) * ((timeSeries_.getColumnNum() / 64) + 1)));
	}

	~TimeSeriesOrderByComparator() {
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		alloc.deallocate(varray1_);
		alloc.deallocate(pBitmap1_);
		alloc.deallocate(varray2_);
		alloc.deallocate(pBitmap2_);
	}

	bool operator()(PointRowId x, PointRowId y) {
		util::StackAllocator::Scope scope(txn_.getDefaultAllocator());
		TimeSeriesRowWrapper row1(txn_, timeSeries_, x, pBitmap1_);
		TimeSeriesRowWrapper row2(txn_, timeSeries_, y, pBitmap2_);
		for (size_t i = 0; i < orderByExprList_.size(); i++) {
			Expr *e = orderByExprList_[i].expr;
			Expr *e1 = e->eval(txn_, *(timeSeries_.getObjectManager()), timeSeries_.getRowAllocateStrategy(), &row1,
				&fmap_, EVAL_MODE_NORMAL);
			Expr *e2 = e->eval(txn_, *(timeSeries_.getObjectManager()), timeSeries_.getRowAllocateStrategy(), &row2,
				&fmap_, EVAL_MODE_NORMAL);
			int ret = e1->compareAsValue(txn_, e2, orderByExprList_[i].nullsLast);
			QP_SAFE_DELETE(e);
			QP_SAFE_DELETE(e1);
			QP_SAFE_DELETE(e2);
			if (ret < 0) {
				return (orderByExprList_[i].order == ASC);
			}
			if (ret > 0) {
				return (orderByExprList_[i].order != ASC);
			}
		}
		const Value *v1 = row1[0];
		const Value *v2 = row2[0];
		return ltTimestampTimestamp(
			txn_, v1->data(), v1->size(), v2->data(), v2->size());
	}

	bool operator()(SamplingRow x, SamplingRow y) {
		util::StackAllocator::Scope scope(txn_.getDefaultAllocator());
		TimeSeriesRowWrapper row1(txn_, timeSeries_, x.rowid, pBitmap1_);
		TimeSeriesRowWrapper row2(txn_, timeSeries_, y.rowid, pBitmap2_);
		Value k1, k2;
		k1.setTimestamp(x.key);
		k2.setTimestamp(y.key);
		row1.setValue(k1, 0);
		row2.setValue(k2, 0);
		if (interpolateColumnId_ != UNDEF_COLUMNID) {
			row1.setValue(*x.value, interpolateColumnId_);
			row2.setValue(*y.value, interpolateColumnId_);
		}

		for (size_t i = 0; i < orderByExprList_.size(); i++) {
			Expr *e = orderByExprList_[i].expr;
			Expr *e1 = e->eval(txn_, *(timeSeries_.getObjectManager()), timeSeries_.getRowAllocateStrategy(), &row1,
				&fmap_, EVAL_MODE_NORMAL);
			Expr *e2 = e->eval(txn_, *(timeSeries_.getObjectManager()), timeSeries_.getRowAllocateStrategy(), &row2,
				&fmap_, EVAL_MODE_NORMAL);
			int ret = e1->compareAsValue(txn_, e2, orderByExprList_[i].nullsLast);
			QP_SAFE_DELETE(e);
			QP_SAFE_DELETE(e1);
			QP_SAFE_DELETE(e2);
			if (ret < 0) {
				return (orderByExprList_[i].order == ASC);
			}
			if (ret > 0) {
				return (orderByExprList_[i].order != ASC);
			}
		}
		return ltTimestampTimestamp(
			txn_, k1.data(), k1.size(), k2.data(), k2.size());
	}

	TransactionContext &txn_;
	TimeSeries &timeSeries_;
	FunctionMap &fmap_;
	SortExprList &orderByExprList_;
	uint32_t interpolateColumnId_;

	Value *varray1_;
	Value *varray2_;
	uint64_t *pBitmap1_;
	uint64_t *pBitmap2_;
};

#endif  
