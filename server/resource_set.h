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
#ifndef RESOURCE_SET_H_
#define RESOURCE_SET_H_

#include "util/allocator.h"

typedef util::FixedSizeAllocator<util::Mutex>
		GlobalFixedSizeAllocator;

typedef util::VariableSizeAllocator<util::Mutex>
		GlobalVariableSizeAllocator;

class ClusterService;
class SyncService;
class TransactionService;
class CheckpointService;
class SystemService;
class TriggerService;
class PartitionTable;
class DataStore;
class LogManager;
class ClusterManager;
class SyncManager;
class TransactionManager;
class ChunkManager;
class RecoveryManager;
class ObjectManager;
class ConfigTable;
class StatTable;

class SQLService;
class SQLExecutionManager;
class JobManager;
class LocalTempStore;

/*!
	@brief Option of manager set
*/
struct ResourceSetOption {

	ResourceSetOption() : isNewSQL_(false) {}

	bool isNewSQL_;
};

class ResourceSet {

public:

	ResourceSet(
			ClusterService *clsSvc,
			SyncService *syncSvc,
			TransactionService *txnSvc,
			CheckpointService *cpSvc,
			SystemService *sysSvc,
			TriggerService *trgSvc,
			PartitionTable *pt,
			DataStore *ds,
			LogManager *logMgr,
			ClusterManager *clsMgr,
			SyncManager *syncMgr,
			TransactionManager *txnMgr,
			ChunkManager *chunkMgr,
			RecoveryManager *recoveryMgr,
			ObjectManager *objectMgr,
			GlobalFixedSizeAllocator *fixedSizeAlloc,
			GlobalVariableSizeAllocator *varSizeAlloc,
			ConfigTable *config,
			StatTable *stats
			,
			SQLService *sqlSvc,
			SQLExecutionManager *execMgr,
			JobManager *jobMgr,
			LocalTempStore *tmpStore
			) : 
					clsSvc_(clsSvc),
					syncSvc_(syncSvc),
					txnSvc_(txnSvc),
					cpSvc_(cpSvc),
					sysSvc_(sysSvc),
					trgSvc_(trgSvc),
					pt_(pt),
					ds_(ds),
					logMgr_(logMgr),
					clsMgr_(clsMgr),
					syncMgr_(syncMgr),
					txnMgr_(txnMgr),
					chunkMgr_(chunkMgr),
					recoveryMgr_(recoveryMgr),
					objectMgr_(objectMgr),
					fixedSizeAlloc_(fixedSizeAlloc),
					varSizeAlloc_(varSizeAlloc),
					config_(config),
					stats_(stats)
					,
					sqlSvc_(sqlSvc),
					execMgr_(execMgr),
					jobMgr_(jobMgr),
					tmpStore_(tmpStore) 
					{}
	
	struct Option {
		Option() : isNewSQL_(false) {}
		bool isNewSQL_;
	} option_;

	void setSqlOption(bool flag) {
		option_.isNewSQL_ = flag;
	}

	bool isNewSQL() const {
		return option_.isNewSQL_;
	}

	ClusterService *getClusterService() const  {
		return clsSvc_;
	}

	SyncService *getSyncService() const  {
		return syncSvc_;
	}

	TransactionService *getTransactionService() const  {
		return txnSvc_;
	}

	 CheckpointService *getCheckpointService() const  {
		return cpSvc_;
	}

	SystemService *getSystemService() const  {
		return sysSvc_;
	}

	TriggerService *getTriggerService() const {
		return trgSvc_;
	}

	PartitionTable *getPartitionTable() const  {
		return pt_;
	}

	DataStore *getDataStore() const  {
		return ds_;
	}

	LogManager *getLogManager() const {
		return logMgr_;
	}

	ClusterManager *getClusterManager() const  {
		return clsMgr_;
	}

	SyncManager *getSyncManager() const {
		return syncMgr_;
	}

	TransactionManager *getTransactionManager() const  {
		return txnMgr_;
	}

	ChunkManager *getChunkManager() const  {
		return chunkMgr_;
	}

	RecoveryManager *getRecoveryManager() const  {
		return recoveryMgr_;
	}

	ObjectManager *getObjectManager() const  {
		return objectMgr_;
	}

	GlobalFixedSizeAllocator *getGlobalFixedSizeAllocator() const  {
		return fixedSizeAlloc_;
	}

	GlobalVariableSizeAllocator *getGlobaVariableSizeAllocator() const  {
		return varSizeAlloc_;
	}


	ConfigTable *getConfigTable() const  {
		return config_;
	}

	StatTable *getStatTable() const {
		return stats_;
	}


	SQLService *getSQLService() const  {
		return sqlSvc_;
	}

	SQLExecutionManager *getSQLExecutionManager() const  {
		return execMgr_;
	}

	JobManager *getJobManager() const  {
		return jobMgr_;
	}


	LocalTempStore *getTempolaryStore() const {
		return tmpStore_;
	}


	ClusterService *clsSvc_;
	SyncService *syncSvc_;
	TransactionService *txnSvc_;
	CheckpointService *cpSvc_;
	SystemService *sysSvc_;
	TriggerService *trgSvc_;
	PartitionTable *pt_;
	DataStore *ds_;
	LogManager *logMgr_;
	ClusterManager *clsMgr_;
	SyncManager *syncMgr_;
	TransactionManager *txnMgr_;
	ChunkManager *chunkMgr_;
	RecoveryManager *recoveryMgr_;
	ObjectManager *objectMgr_;
	GlobalFixedSizeAllocator *fixedSizeAlloc_;
	GlobalVariableSizeAllocator *varSizeAlloc_;
	ConfigTable *config_;
	StatTable *stats_;

	SQLService *sqlSvc_;
	SQLExecutionManager *execMgr_;
	JobManager *jobMgr_;
	LocalTempStore *tmpStore_;
};

class ResourceSetReceiver {
public:
	virtual void initialize(const ResourceSet &resourceSet) = 0;
};

#endif
