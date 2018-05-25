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
	@brief Definition of Index base class
*/
#ifndef BASE_INDEX_H_
#define BASE_INDEX_H_

#include "util/trace.h"
#include "data_type.h"
#include "gs_error.h"
#include "object_manager.h"
#include "schema.h"
#include "string_processor.h"  
#include "value.h"			   
#include <iomanip>
#include <iostream>

class TransactionContext;
class BaseContainer;

/*!
	@brief Index base class
*/
class BaseIndex : public BaseObject {
public:
	BaseIndex(TransactionContext &txn, ObjectManager &objectManager,
		const AllocateStrategy &strategy, BaseContainer *container)
		: BaseObject(txn.getPartitionId(), objectManager),
		  allocateStrategy_(strategy),
		  container_(container) {}
	BaseIndex(TransactionContext &txn, ObjectManager &objectManager, OId oId,
		const AllocateStrategy &strategy, BaseContainer *container)
		: BaseObject(txn.getPartitionId(), objectManager, oId),
		  allocateStrategy_(strategy),
		  container_(container) {}

	virtual int32_t finalize(TransactionContext &txn) = 0;
	virtual int32_t insert(
		TransactionContext &txn, const void *key, OId oId) = 0;
	virtual int32_t remove(
		TransactionContext &txn, const void *key, OId oId) = 0;
	virtual int32_t update(
		TransactionContext &txn, const void *key, OId oId, OId newOId) = 0;

protected:
	struct SearchContext {
		ColumnId columnId_;  
		uint32_t conditionNum_;			
		TermCondition *conditionList_;  
		ResultSize limit_;				
		SearchContext()
			: columnId_(0),
			  conditionNum_(0),
			  conditionList_(NULL),
			  limit_(MAX_RESULT_SIZE)
		{
		}
		SearchContext(ColumnId columnId, uint32_t conditionNum,
			TermCondition *conditionList, ResultSize limit)
			: columnId_(columnId),
			  conditionNum_(conditionNum),
			  conditionList_(conditionList),
			  limit_(limit)
		{
		}
	};

protected:
	AllocateStrategy allocateStrategy_;
	BaseContainer *container_;
};

#endif  
