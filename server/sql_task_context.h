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
#ifndef SQL_TASK_CONTEXT_H_
#define SQL_TASK_CONTEXT_H_

#include "sql_tuple.h"
#include "sql_common.h"
#include "event_engine.h"

class ResourceSet;
struct NoSQLSyncContext;
class Task;
class JobManager;
class SQLPreparedPlan;

class TaskContext {

public:

	class Factory;
	typedef int32_t InputId;

	TaskContext(const ResourceSet *resourceSet = NULL);

	NoSQLSyncContext* getSyncContext() const;

	void setSyncContext(NoSQLSyncContext *syncContext);

	util::StackAllocator& getAllocator();
	
	void setAllocator(util::StackAllocator *alloc);
	
	EventContext* getEventContext() const;
	
	void setEventContext(EventContext *eventContext);
		
	const ResourceSet *getResourceSet();
	
	Task* getTask() const;
	
	void setTask(Task *task);
	
	void checkCancelRequest();
	
	void transfer(TupleList::Block &block);
	
	void transfer(
			TupleList::Block &block,
			bool completed, int32_t outputId);
	
	void finish(int32_t outputId);
	
	void finish();

	bool isTransactionService();

	void setGlobalAllocator(
			SQLVariableSizeGlobalAllocator *globalVarAlloc);

	SQLVariableSizeGlobalAllocator *getGlobalAllocator();

	int64_t getMonotonicTime();

protected:
	
	const ResourceSet *resourceSet_;

	util::StackAllocator *alloc_;
	SQLVariableSizeGlobalAllocator *globalVarAlloc_;
	Task *task_;
	EventContext *eventContext_;
	int64_t counter_;
	NoSQLSyncContext *syncContext_;
};

struct TaskOption {

public:

	typedef EventByteInStream ByteInStream;
	typedef EventByteOutStream ByteOutStream;

	explicit TaskOption(util::StackAllocator &alloc);

	const SQLPreparedPlan *plan_;
	uint32_t planNodeId_;
	util::ObjectInStream<ByteInStream> *byteInStream_;
	util::AbstractObjectInStream *inStream_;
	EventByteInStream *baseStream_;
	const picojson::value *jsonValue_;
};

#endif
