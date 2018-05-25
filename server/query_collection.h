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
	@brief Definition of QueryForCollection
*/

#ifndef QUERY_COLLECTION_H_
#define QUERY_COLLECTION_H_

#include "query.h"
#include "schema.h"

/*!
 * @brief Query object for Collection
 *
 */
class QueryForCollection : public Query {
	friend class lemon_tqlParser::tqlParser;
	friend class QueryStopwatchHook;
	friend class QueryProcessor;
	friend class Expr;
	friend class BoolExpr;

public:
	QueryForCollection(TransactionContext &txn, Collection &collection,
		const TQLInfo &tqlInfo, uint64_t limit = MAX_RESULT_SIZE,
		QueryHookClass *hook = NULL);
	QueryForCollection(TransactionContext &txn, Collection &collection)
		: Query(txn, *(collection.getObjectManager())),
		  collection_(&collection) {}
	virtual ~QueryForCollection() {}

	void doQuery(
		TransactionContext &txn, Collection &collection, ResultSet &resultSet);
	QueryForCollection *dup(
		TransactionContext &txn, ObjectManager &objectManager);

protected:
	virtual Collection *getCollection() {
		return collection_;
	}
	virtual TimeSeries *getTimeSeries() {
		return NULL;
	}

	Collection
		*collection_;  
	void doQueryWithoutCondition(
		TransactionContext &txn, Collection &collection, ResultSet &resultSet);
	void doQueryWithCondition(
		TransactionContext &txn, Collection &collection, ResultSet &resultSet);
	void doSelection(
		TransactionContext &txn, Collection &collection, ResultSet &resultSet);
};

/*!
*	@brief Interface for Collection Row
*/
class CollectionRowWrapper : public ContainerRowWrapper {
public:
	CollectionRowWrapper(TransactionContext &txn, Collection &collection,
		OId rowId, uint64_t *pBitmap)
		: txn_(txn),
		  collection_(collection),
		  rowId_(rowId),
		  rowArray_(txn_, rowId_, &collection_, OBJECT_READ_ONLY),
		  pBitmap_(pBitmap),
		  varrayCounter_(0) {
		util::StackAllocator &alloc = txn_.getDefaultAllocator();

		varray_ = reinterpret_cast<ContainerValue *>(
			alloc.allocate(sizeof(ContainerValue) * collection.getColumnNum()));
		try {
			for (uint32_t i = 0; i < collection_.getColumnNum(); i++) {
				new (&(varray_[i])) ContainerValue(
					txn,
					*(collection
							.getObjectManager()));  
				varrayCounter_++;
			}
			memset(pBitmap, 0,
				sizeof(uint64_t) * ((collection.getColumnNum() / 64) + 1));
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
	CollectionRowWrapper(TransactionContext &txn, Collection &collection,
		uint64_t *pBitmap)
		: txn_(txn),
		  collection_(collection),
		  rowId_(UNDEF_OID),
		  rowArray_(txn_, &collection_),
		  pBitmap_(pBitmap),
		  varrayCounter_(0) {
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		varray_ = reinterpret_cast<ContainerValue *>(
			alloc.allocate(sizeof(ContainerValue) * collection_.getColumnNum()));
		try {
			for (uint32_t i = 0; i < collection_.getColumnNum(); i++) {
				new (&(varray_[i])) ContainerValue(
					txn,
					*(collection_
							.getObjectManager()));  
				varrayCounter_++;
			}
			memset(pBitmap, 0,
				sizeof(uint64_t) * ((collection_.getColumnNum() / 64) + 1));
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
	~CollectionRowWrapper() {
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
		rowArray_.load(txn_, rowId_, &collection_, OBJECT_READ_ONLY);
//		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		memset(pBitmap_, 0,
			sizeof(uint64_t) * ((collection_.getColumnNum() / 64) + 1));
		for (uint32_t i = 0; i < varrayCounter_; i++) {
			varray_[i].getBaseObject().reset();  
		}
	}

	const Value *getColumn(uint32_t k) {
		ContainerValue &v = varray_[k];
		if (bit_off(pBitmap_[k / 64], k % 64)) {
			Collection::RowArray::Row row(rowArray_.getRow(), &rowArray_);
			row.getField(txn_, collection_.getColumnInfo(k), v);
			set_bit(pBitmap_[k / 64], k % 64);
		}
		return &(v.getValue());
	}
	RowId getRowId() {
		Collection::RowArray::Row row(rowArray_.getRow(), &rowArray_);
		return row.getRowId();
	}
	void getImage(TransactionContext &txn,
		MessageRowStore *messageRowStore, bool isWithRowId) {
		Collection::RowArray::Row row(rowArray_.getRow(), &rowArray_);
		row.getImage(txn, messageRowStore, isWithRowId);
	}
private:
	inline bool bit_off(uint64_t word, size_t i) {
		return (word & (1ULL << i)) == 0;
	}
	inline void set_bit(uint64_t &word, size_t i) {
		word |= (1ULL << i);
	}
	TransactionContext &txn_;
	Collection &collection_;
	OId rowId_;
	ContainerValue *varray_;
	Collection::RowArray rowArray_;
	uint64_t *pBitmap_;
	size_t varrayCounter_;
};

/*!
*	@brief Compare methord for Collection Row
*/
class CollectionOrderByComparator {
public:
	CollectionOrderByComparator(TransactionContext &txn, Collection &collection,
		FunctionMap &fmap, SortExprList &theExprList)
		: txn_(txn),
		  collection_(collection),
		  fmap_(fmap),
		  orderByExprList_(theExprList) {
		util::StackAllocator &alloc = txn.getDefaultAllocator();
		pBitmap1_ = reinterpret_cast<uint64_t *>(alloc.allocate(
			sizeof(uint64_t) * ((collection.getColumnNum() / 64) + 1)));
		pBitmap2_ = reinterpret_cast<uint64_t *>(alloc.allocate(
			sizeof(uint64_t) * ((collection.getColumnNum() / 64) + 1)));
	}

	~CollectionOrderByComparator() {
		util::StackAllocator &alloc = txn_.getDefaultAllocator();
		alloc.deallocate(pBitmap1_);
		alloc.deallocate(pBitmap2_);
	}

	bool operator()(OId x, OId y) {
		util::StackAllocator::Scope scope(txn_.getDefaultAllocator());
		CollectionRowWrapper row1(txn_, collection_, x, pBitmap1_);
		CollectionRowWrapper row2(txn_, collection_, y, pBitmap2_);
		for (size_t i = 0; i < orderByExprList_.size(); i++) {
			Expr *e = orderByExprList_[i].expr;
			Expr *e1 = e->eval(txn_, *(collection_.getObjectManager()), &row1,
				&fmap_, EVAL_MODE_NORMAL);
			Expr *e2 = e->eval(txn_, *(collection_.getObjectManager()), &row2,
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
		return x < y;
	}

	TransactionContext &txn_;
	Collection &collection_;
	FunctionMap &fmap_;
	SortExprList &orderByExprList_;
	uint64_t *pBitmap1_;
	uint64_t *pBitmap2_;
};

#endif  
