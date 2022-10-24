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
	@brief Definition of sql processor dml
*/
#ifndef SQL_PROCESSOR_DML_H_
#define SQL_PROCESSOR_DML_H_

#include "sql_common.h"
#include "sql_processor.h"
#include "data_store_common.h"
#include "sql_compiler.h"
#include "nosql_command.h"

class NoSQLContainer;
struct TableContainerInfo;
class ColumnInfo;
struct TableSchemaInfo;
struct NoSQLStoreOption;

/*!
	@brief DMLProcessor
*/
class DMLProcessor : public SQLProcessor {

	typedef SQLContext Context;
	typedef int32_t AccessControlType;

	typedef std::vector<TupleList*, util::StdAllocator<
		TupleList*, SQLVariableSizeGlobalAllocator> > TupleListArray;
	typedef std::vector<TupleList::Group*, util::StdAllocator<
		TupleList::Group*, SQLVariableSizeGlobalAllocator> > GroupListArray;
	typedef std::vector<TupleList::Reader*, util::StdAllocator<
		TupleList::Reader*, SQLVariableSizeGlobalAllocator> > TupleListReaderArray;
	typedef SQLPreparedPlan::Node::PartitioningInfo PartitioningInfo;
	typedef SQLTableInfo::IdInfo TableIdInfo;

	static const int32_t ROW_COLUMN_ID = 1;
	static const int32_t SUBCONTAINER_COLUMN_ID = 0;
	static const int32_t RESERVE_COLUMN_NUM = ROW_COLUMN_ID + 1;
	static const int32_t UNIT_PROCESS_NUM = 5000000;
	static const int32_t UNIT_LIMIT_TIME = 2 * 1000;
	static const int32_t UNIT_CHECK_COUNT = 127;

	static const uint64_t BULK_INSERT_THRESHOLD_COUNT = 4096;
	static const uint64_t BULK_UPDATE_THRESHOLD_COUNT = 4096;
	static const uint64_t BULK_DELETE_THRESHOLD_COUNT = 100000; 
	static const uint64_t BULK_INSERT_THRESHOLD_SIZE = 2 * 1024 * 1024; 
	static const uint64_t BULK_UPDATE_THRESHOLD_SIZE = 2 * 1024 * 1024; 

	friend class DDLProcessor;

public:

	typedef int32_t DmlType;
	static const int32_t DML_INSERT = 0;
	static const int32_t DML_UPDATE = 1;
	static const int32_t DML_DELETE = 2;
	static const int32_t DML_MAX = 3;
	static const int32_t DML_NONE = -1;

	/*!
		@brief コンストラクタ
	*/
	DMLProcessor(Context& cxt, const TypeInfo& typeInfo);

	/*!
		@brief デストラクタ
	*/
	virtual ~DMLProcessor();

	/*!
		@brief パイプ処理
		@note V3.0だとためるだけ。実処理はfinish
	*/
	virtual bool pipe(Context& cxt, InputId inputId, const Block& block);

	/*!
		@brief パイプ処理
		@note 全IputIdがcompleteになった時点で実行
	*/
	virtual bool finish(Context& cxt, InputId inputId);

	/*!
		@brief パイプ処理
		@note 全IputIdがcompleteになった時点で実行
	*/
	virtual bool applyInfo(
		Context& cxt, const Option& option,
		const TupleInfoList& inputInfo, TupleInfo& outputInfo);

	void exportTo(Context& cxt, const OutOption& option) const;

	static void setField(TupleList::TupleColumnType type, ColumnId columnId,
		const TupleValue& value, OutputMessageRowStore& rowStore,
		ColumnType origColumnType = COLUMN_TYPE_NULL) {
		switch (type) {
		case TupleList::TYPE_STRING: {
			if (value.varSize() > static_cast<size_t>(INT32_MAX)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
					"String size limit exceeded");
			}
			const TupleString::BufferInfo& info = TupleString(value).getBuffer();
			rowStore.setFieldForRawData(columnId, info.first, static_cast<uint32_t>(info.second));
		}
								   break;
		case TupleList::TYPE_BLOB: {
			const void* data;
			size_t size;
			size_t totalSize = 0;
			TupleValue::LobReader readerForSize(value);
			while (readerForSize.next(data, size)) {
				totalSize += size;
			}
			if (totalSize > static_cast<size_t>(INT32_MAX)) {
				GS_THROW_USER_ERROR(GS_ERROR_SQL_PROC_LIMIT_EXCEEDED,
					"BLOB size limit exceeded");
			}
			rowStore.setVarDataHeaderField(columnId, static_cast<uint32_t>(totalSize));
			TupleValue::LobReader readerValue(value);
			while (readerValue.next(data, size)) {
				rowStore.addVariableFieldPart(data, static_cast<uint32_t>(size));
			}
		}
								 break;
		case TupleList::TYPE_BOOL:
		case TupleList::TYPE_BYTE:
		case TupleList::TYPE_SHORT:
		case TupleList::TYPE_INTEGER:
		case TupleList::TYPE_LONG:
		case TupleList::TYPE_FLOAT:
		case TupleList::TYPE_NUMERIC:
		case TupleList::TYPE_DOUBLE:
		case TupleList::TYPE_TIMESTAMP:
			rowStore.setField(columnId, value.fixedData(),
				static_cast<uint32_t>(TupleColumnTypeUtils::getFixedSize(type)));
			break;
		case TupleList::TYPE_NULL:
			rowStore.setNull(columnId);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_SQL_DML_INTERNAL, "");
		}
	}

	bool recovery(Context& cxt, NodeAffinityNumber affinity,
		NoSQLContainer& container);

private:

	class BulkManager;
	typedef Factory::Registrar<DMLProcessor> Registrar;

	/*!
		@brief バルクを経由したパイプライン処理の実行
	*/
	bool processInternal(Context& cxt, bool isPipe);

	/*!
		@brief Pipeで更新処理を実行できるか
		@note RAから
	*/
	bool isImmediate() {
		return immediate_;
	}

	/*!
		@brief 入力の全終了を確認する
	*/
	bool checkCompleted() {
		completedNum_++;
		if (completedNum_ >= inputTupleList_.size()) {
			return true;
		}
		else {
			return false;
		}
	}

	util::StackAllocator& jobStackAlloc_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	BulkManager* bulkManager_;
	TupleListArray inputTupleList_;
	TupleListReaderArray inputTupleReaderList_;
	GroupListArray groupList_;
	size_t inputSize_;
	ColumnInfo* nosqlColumnList_;
	int32_t nosqlColumnCount_;
	TupleList::Column* inputTupleColumnList_;
	TupleList::TupleColumnType* columnTypeList_;
	TupleList::Info** tupleInfoList_;
	int32_t inputTupleColumnCount_;
	bool immediate_;
	DmlType dmlType_;
	uint32_t completedNum_;
	InputId currentPos_;
	ClientId clientId_;
	bool isTimeSeries_;
	PartitionTable* pt_;
	PartitioningInfo* partitioningInfo_;
	SQLExecutionManager* executionManager_;
	static const Registrar deleteRegistrar_;
	static const Registrar updateRegistrar_;
	static const Registrar insertRegistrar_;
};

/*!
	@brief バルク登録管理
*/
class DMLProcessor::BulkManager {
public:

	typedef SQLContext Context;
	static const int32_t UNDEF_INPUTID = -1;
	class BulkEntry;
	typedef util::Vector<BulkEntry*> BulkEntryArray;
	typedef util::Vector<int32_t> InsertColumnMap;
	typedef util::Vector<uint8_t> NoSQLColumnInfoList;

	/*!
		@brief コンストラクタ
	*/
	BulkManager(DMLProcessor* processor, util::StackAllocator& jobStackAlloc, SQLVariableSizeGlobalAllocator& globalVarAlloc,
		DMLProcessor::DmlType dmlType, SQLExecutionManager* executionManager) :
		processor_(processor),
		jobStackAlloc_(jobStackAlloc), globalVarAlloc_(globalVarAlloc), type_(dmlType),
		partitioningColumnId_(UNDEF_COLUMNID), partitioningNum_(0),
		columnCount_(0), columnList_(NULL), nosqlColumnList_(NULL),
		nosqlColumnSize_(0), execCount_(0),
		bulkEntryList_(jobStackAlloc), bulkEntryListMap_(jobStackAlloc),
		insertColumnMap_(NULL), targetColumnId_(0),
		currentPos_(0), prevPos_(-1), prevStore_(NULL),
		partitioningType_(0), executionManager_(executionManager) {}
	/*!
		@brief デストラクタ
	*/
	~BulkManager();

	void clear() {
		bulkEntryListMap_.clear();
		for (size_t pos = 0; pos < bulkEntryList_.size(); pos++) {
			ALLOC_DELETE(jobStackAlloc_, bulkEntryList_[pos]);
			if (partitioningType_ <= 1) {
				bulkEntryList_[pos] = NULL;
			}
		}
		if (partitioningType_ > 1) {
			bulkEntryList_.clear();
		}
	}

	/*!
		@brief スキーマ、及びコンテナ情報を与えてバルク情報をセットアップする
	*/
	void setup(Context& cxt, const TableIdInfo& idInfo,
		PartitioningInfo* partitioningInfo,
		TupleList::Column* inputColumnList, int32_t inputColumnSize,
		ColumnInfo* columnList, int32_t columnSize,
		InsertColumnMap* insertColumnMap, bool isReplace
	);

	/*!
		@brief アロケータを指定して出力用のロウストアを生成する
		@note 上位スコープを抜けると自動的に解放されるエリア
	*/

	/*!
		@brief バルクマネージャにデータをエントリする
	*/
	void import(Context& cxt, InputId inputId, TupleList::Reader& reader, bool& needRefresh);

	/*!
		@brief 実行成功件数を返す
	*/
	int64_t getExecCount() {
		return execCount_;
	}

	DMLProcessor* getProcessor() {
		return processor_;
	}

	/*!
		@brief 指定番号のバルクエントリを返す
	*/
	BulkEntry* getBulkEntry(InputId target) {
		return bulkEntryList_[target];
	}

	BulkEntry* add(Context& cxt, NodeAffinityNumber pos,
		TableSchemaInfo* schemaInfo,
		TableContainerInfo* containerInfo,
		TargetContainerInfo& targetInfo);

	BulkEntry* addPosition(Context& cxt, int32_t pos, TargetContainerInfo& targetInfo);
	BulkEntry* addValue(Context& cxt, NodeAffinityNumber affinity, TargetContainerInfo& targetInfo);

	void begin() {
		currentPos_ = 0;
		prevPos_ = 0;
		prevStore_ = NULL;
	}

	BulkEntry* next();

	void incCount(int64_t count) {
		execCount_ += count;
	}

private:

	class Operation;
	class Insert;
	class Update;
	class Delete;
	class BulkStore;

	TupleValue* setTupleValue(TupleList::Reader& reader, ColumnId columnId, TupleValue& value);

	/*!
		@brief カーソルの実ロウデータをエントリする
	*/
	void appendValue(DmlType dmlType, InputId& targetId,
		TupleList::Reader& reader, InputId inputId,
		ColumnId inputColumnId, ColumnId inputNoSQLColumnId,
		ColumnId outputColumnId, OutputMessageRowStore& rowStore);

	/*!
		@brief 実カラム以外のDB位置情報をエントリする
	*/
	int32_t appendDbPosition(DmlType dmlType, InputId targetId,
		TupleList::Reader& reader, ColumnId rowColumn,
		ColumnId SubContainerColumn);

	/*!
		@brief 更新先のエントリを取得する
	*/
	Operation* getOperation(InputId targetId, DmlType type);

	void checkInsertColumnMap();

	util::StackAllocator& jobStackAlloc_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	DmlType type_;
	int32_t partitioningColumnId_;
	int32_t partitioningNum_;
	int32_t columnCount_;
	TupleList::Column* columnList_;
	ColumnInfo* nosqlColumnList_;
	int32_t nosqlColumnSize_;
	int64_t execCount_;
	BulkEntryArray bulkEntryList_;
	util::Map<NodeAffinityNumber, BulkEntry*> bulkEntryListMap_;
	uint8_t partitioningType_;
	PartitioningInfo* partitioningInfo_;
	InsertColumnMap* insertColumnMap_;
	int32_t targetColumnId_;
	bool isInsertReplace_;
	size_t currentPos_;
	size_t prevPos_;

	BulkEntry* prevStore_;
	int32_t partitionNum_;
	SQLExecutionManager* executionManager_;
	DMLProcessor* processor_;
};

/*!
	@brief コンテナに対するDML操作
	@note Insert/Update/Delete
	@note
*/
class DMLProcessor::BulkManager::Operation {
public:

	typedef SQLContext Context;

	/*!
		@brief コンストラクタ
		@note rowStoreのスキーマに注意する
	*/
	Operation(BulkManager* bulkManager, util::StackAllocator& alloc,
		const ColumnInfo* columnInfoList,
		uint32_t columnCount, NoSQLContainer* container, int32_t pos, NodeAffinityNumber affinity);

	/*!
		@brief デストラクタ
	*/
	~Operation();

	void setLimitSize(int64_t limitSize) {
		limitSize_ = limitSize;
	}
	/*!
		@brief コマンドを実行する
	*/
	virtual void execute(Context& cxt) = 0;

	/*!
		@brief コマンドが実行可能状態であるか
	*/
	virtual bool full() = 0;

	/*!
		@brief コンテナに対するコミットを実行する
	*/
	void commit(Context& cxt);

	/*!
		@brief コンテナに対するアボートを実行する
	*/
	void abort(Context& cxt);

	/*!
		@brief ロウストアを取得する
	*/
	OutputMessageRowStore& getRowStore() {
		return *rowStore_;
	}

	/*!
		@brief ロウIDリストを取得する
	*/
	util::XArray<RowId>& getRowIdList() {
		return *rowIdList_;
	}

	/*!
		@brief 次のロウに移動
	*/
	virtual void next() = 0;

	/*!
		@brief アロケータセット
	*/
	virtual void initialize() = 0;

	util::StackAllocator& getAllocatorInfo() {
		return eventStackAlloc_;
	}

	void setPartitioning() {
		partitioning_ = true;
	}

	bool isPartitioning() {
		return partitioning_;
	}

	void setDataStoreConfig(const DataStoreConfig* dsConfig) {
		dsConfig_ = dsConfig;
	}

protected:

	util::StackAllocator& eventStackAlloc_;

	const ColumnInfo* columnInfoList_;
	uint32_t columnCount_;
	NoSQLContainer* container_;
	int32_t pos_;
	util::XArray<uint8_t>* fixedData_;
	util::XArray<uint8_t>* varData_;
	OutputMessageRowStore* rowStore_;
	util::XArray<RowId>* rowIdList_;
	int64_t threshold_;
	int64_t limitSize_;

	NoSQLStoreOption* option_;
	int64_t totalProcessedNum_;
	int64_t currentProcessedNum_;
	bool partitioning_;
	const DataStoreConfig* dsConfig_;
	BulkManager* bulkManager_;
	NodeAffinityNumber affinity_;
};

/*!
	@brief INSERTコマンド処理
*/
class DMLProcessor::BulkManager::Insert : public Operation {
public:
	/*!
		@brief コンストラクタ
	*/
	Insert(BulkManager* bulkManager, util::StackAllocator& alloc,
		const ColumnInfo* columnInfoList,
		uint32_t columnCount, NoSQLContainer* container,
		int32_t pos, bool isInsertReplace, NodeAffinityNumber affinity);

	/*!
		@brief コマンドを実行する
	*/
	void execute(Context& cxt);

	virtual void initialize();

	/*!
		@brief 実行可能条件を満たすか
	*/
	bool full() {
		return ((fixedData_->size() + varData_->size())
			>= limitSize_);
	}

	/*!
		@brief カーソルを進める
	*/
	void next() {
		totalProcessedNum_++;
		currentProcessedNum_++;
		rowStore_->next();
	}
};

/*!
	@brief UPDATEコマンド処理
*/
class DMLProcessor::BulkManager::Update : public Operation {
public:
	/*!
		@brief コンストラクタ
	*/
	Update(BulkManager* bulkManager, util::StackAllocator& alloc,
		const ColumnInfo* columnInfoList,
		uint32_t columnCount, NoSQLContainer* container, int32_t pos, NodeAffinityNumber affinity);

	/*!
		@brief コマンドを実行する
	*/
	void execute(Context& cxt);

	void execute(Context& cxt, RowId rowId);

	void append(Context& cxt, RowId rowId);

	virtual void initialize();

	/*!
		@brief 実行可能条件を満たすか
	*/
	bool full() {
		return ((fixedData_->size() + varData_->size())
			>= limitSize_);
	}

	/*!
		@brief カーソルを進める
	*/
	void next() {
		totalProcessedNum_++;
		currentProcessedNum_++;
		rowStore_->next();
	}
};

/*!
	@brief DELETEコマンド処理
*/
class DMLProcessor::BulkManager::Delete : public Operation {
public:
	/*!
		@brief コンストラクタ
	*/
	Delete(BulkManager* bulkManager, util::StackAllocator& alloc,
		const ColumnInfo* columnInfoList,
		uint32_t columnCount, NoSQLContainer* container, int32_t pos, NodeAffinityNumber affinity) :
		Operation(bulkManager, alloc, columnInfoList, columnCount, container, pos, affinity) {
		threshold_ = BULK_DELETE_THRESHOLD_COUNT;
		initialize();
	}

	/*!
		@brief コマンドを実行する
	*/
	void execute(Context& cxt);

	/*!
		@brief コマンドを実行する
	*/
	void execute(Context& cxt, RowId rowId);

	void append(Context& cxt, RowId rowId);

	/*!
		@brief 実行可能条件を満たすか
	*/
	bool full() {
		return (threshold_ <= static_cast<int64_t>(rowIdList_->size()));
	}

	virtual void initialize();

	/*!
		@brief カーソルを進める
		@note deleteはrowStoreを持たないため、操作しない
	*/
	void next() {
		totalProcessedNum_++;
		currentProcessedNum_++;
	}
};

/*!
	@brief バルクエントリ
	@note 同一コンテナに対する複数の処理をまとたもの
	@note 更新対象のコンテナ分作成される
	@note executeにより、対象コンテナに対する更新処理を一括で実行する
*/
class DMLProcessor::BulkManager::BulkEntry {

	typedef util::XArray<Operation*> OperationArray;

public:

	/*!
		@brief コンストラクタ
	*/
	BulkEntry(BulkManager* bulkManager, util::StackAllocator& eventStackAlloc,
		SQLVariableSizeGlobalAllocator& globalVarAlloc, int32_t pos, DmlType type, NodeAffinityNumber affinity) :
		eventStackAlloc_(eventStackAlloc), globalVarAlloc_(globalVarAlloc),
		entries_(eventStackAlloc_), container_(NULL), pos_(pos), type_(type),
		bulkManager_(bulkManager), affinity_(affinity) {}

	/*!
		@brief デストラクタ
	*/
	~BulkEntry();

	/*!
		@brief セットアップ
	*/
	void setup(Context& cxt, DmlType type, TupleList::Column* inputColumnList,
		int32_t inputSize, ColumnInfo* columnInfoList,
		uint32_t columnCount, TableContainerInfo* containerInfo,
		bool isReplace, bool isPartitioning, TargetContainerInfo& info);

	/*!
		@brief コマンドエントリの取得
	*/
	Operation* getOperation(DmlType type) {
		return entries_[type];
	}

	/*!
		@brief コマンドを実行する
	*/
	void execute(Context& cxt, DmlType type, bool isForce) {
		if (entries_[type] != NULL && (isForce || entries_[type]->full())) {
			entries_[type]->execute(cxt);
		}
	}

	void executeAll(Context& cxt, DmlType type, bool isForce) {
		for (DmlType type = 0; type <
			static_cast<DmlType>(entries_.size()); type++) {
			if (entries_[type] != NULL && (isForce || entries_[type]->full())) {
				entries_[type]->execute(cxt);
			}
		}
	}

	/*!
		@brief コミットを実行する
	*/
	void commit(Context& cxt) {
		for (DmlType type = 0; type <
			static_cast<DmlType>(entries_.size()); type++) {
			if (entries_[type] == NULL) continue;
			entries_[type]->commit(cxt);
		}
	}

	/*!
		@brief アボート実行する
	*/
	void abort(Context& cxt) {
		for (DmlType type = 0; type <
			static_cast<DmlType>(entries_.size()); type++) {
			if (entries_[type] == NULL) continue;
			entries_[type]->abort(cxt);
		}
	}

private:
	BulkManager* bulkManager_;
	util::StackAllocator& eventStackAlloc_;
	SQLVariableSizeGlobalAllocator& globalVarAlloc_;
	OperationArray entries_;
	NoSQLContainer* container_;
	int32_t pos_;
	DmlType type_;
	int32_t partitionNum_;
	int32_t sizeLimit_;
	NodeAffinityNumber affinity_;
};

#endif
