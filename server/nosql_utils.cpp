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

#include "nosql_utils.h"
#include "sql_utils.h"
#include "message_schema.h"
#include "transaction_service.h"
#include "nosql_container.h"
#include "sql_execution.h"
#include "nosql_store.h"
#include "nosql_db.h"

UTIL_TRACER_DECLARE(SQL_SERVICE);

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_VERSION = "0";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_PARTITIONING_INFO = "1";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_EXECUTING = "2";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_INDEX = "3";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_PARTITIONING_ASSIGN_INFO = "4";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA = "5";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_VIEW_INFO = "6";

const char8_t *const NoSQLUtils::
		LARGE_CONTAINER_KEY_TABLE_PROPERTY = "7";

template void NoSQLUtils::makeNormalContainerSchema(
		util::StackAllocator &alloc,
		CreateTableOption &createOption,
		util::XArray<uint8_t> &containerSchema,
		util::XArray<uint8_t> &optionList,
		TablePartitioningInfo<util::StackAllocator> &partitioningInfo);

void NoSQLUtils::getAffinityValue(
		CreateTableOption &createOption,
		util::String &affinityValue) {

	if (createOption.propertyMap_) {
		SyntaxTree::DDLWithParameterMap &paramMap
				= *createOption.propertyMap_;

		SyntaxTree::DDLWithParameterMap::iterator it;
		it = paramMap.find(DDLWithParameter::DATA_AFFINITY);
		
		if (it != paramMap.end()) {
			const char* valueStr =
					static_cast<const char*>(it->second.varData());

			affinityValue.assign(valueStr, it->second.varSize());
		}
	}
}

template<typename Alloc>
void NoSQLUtils::makeNormalContainerSchema(
		util::StackAllocator &alloc,
		CreateTableOption &createOption,
		util::XArray<uint8_t> &containerSchema,
		util::XArray<uint8_t> &optionList,
		TablePartitioningInfo<Alloc> &partitioningInfo) {

	try {
		
		int32_t columnNum = createOption.columnNum_;

		util::Vector<ColumnId> primaryKeyList(alloc);
		NoSQLUtils::checkPrimaryKey(
				alloc, createOption, primaryKeyList);
		
		int16_t rowKeyNum = static_cast<int16_t>(primaryKeyList.size());
		for (size_t j = 0; j < primaryKeyList.size(); j++) {
			(*createOption.columnInfoList_)[primaryKeyList[j]]->option_
					|= SyntaxTree::COLUMN_OPT_NOT_NULL;
		}

		containerSchema.push_back(
				reinterpret_cast<uint8_t*>(&columnNum),
				sizeof(int32_t));
		
		for (int32_t i = 0; i < columnNum; i++) {

			char *columnName = const_cast<char*>(
					(*createOption.columnInfoList_)[i]
							->columnName_->name_->c_str());

			int32_t columnNameLen
					= static_cast<int32_t>(strlen(columnName));
			
			containerSchema.push_back(
					reinterpret_cast<uint8_t*>(&columnNameLen),
					sizeof(int32_t));
			
			containerSchema.push_back(
					reinterpret_cast<uint8_t*>(columnName),
					columnNameLen);

			int8_t tmp = static_cast<int8_t>(
					SQLUtils::convertTupleTypeToNoSQLType(
							(*createOption.columnInfoList_)[i]->type_));

			containerSchema.push_back(
					reinterpret_cast<uint8_t*>(&tmp),
					sizeof(int8_t));

			uint8_t opt = 0;
			if ((*createOption.columnInfoList_)[i]
					->hasNotNullConstraint()) {

				ColumnInfo::setNotNull(opt);
			}

			containerSchema.push_back(
					reinterpret_cast<uint8_t*>(&opt),
					sizeof(uint8_t));

			optionList.push_back(
					(*createOption.columnInfoList_)[i]->option_);
		}

		containerSchema.push_back(
				reinterpret_cast<uint8_t *>(&rowKeyNum),
				sizeof(int16_t));

		for (int16_t pos = 0; pos < rowKeyNum; pos++) {

			int16_t currentColumnId
					= static_cast<int16_t>(primaryKeyList[pos]);

			containerSchema.push_back(
					reinterpret_cast<uint8_t *>(&currentColumnId),
					sizeof(int16_t));
		}

		util::String affinityValue(alloc);
		int32_t affinityStrLen = 0;

		NoSQLUtils::getAffinityValue(
				createOption, affinityValue);

		affinityStrLen = static_cast<int32_t>(
				affinityValue.size());

		containerSchema.push_back(
				reinterpret_cast<uint8_t*>(&affinityStrLen),
				sizeof(int32_t));
		
		if (affinityStrLen > 0) {

			void* valuePtr = reinterpret_cast<void*>(
					const_cast<char8_t *>(affinityValue.c_str()));

			containerSchema.push_back(
					reinterpret_cast<uint8_t*>(valuePtr),
					affinityStrLen);
		}

		bool useExpiration = false;
		bool isSetted = false;
		bool useDevisionCount = false;
		bool withTimeSeriesOption = false;

		if (createOption.propertyMap_) {

			SyntaxTree::DDLWithParameterMap &paramMap
					= *createOption.propertyMap_;
			SyntaxTree::DDLWithParameterMap::iterator it;
			it = paramMap.find(DDLWithParameter::EXPIRATION_TIME);

			if (it != paramMap.end()) {
				
				TupleValue &value = (*it).second;
				if (value.getType() != TupleList::TYPE_LONG) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid fomart type");
				}

				int64_t tmpValue = value.get<int64_t>();
				if (tmpValue <= 0) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid value(> 0)");
				}
				
				if (tmpValue > INT32_MAX) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid value(integer overflow)");
				}

				isSetted = true;
				withTimeSeriesOption = true;
				useExpiration = true;
				partitioningInfo.timeSeriesProperty_.elapsedTime_
						= static_cast<int32_t>(tmpValue);
			}

			it = paramMap.find(DDLWithParameter::EXPIRATION_TIME_UNIT);
			if (it != paramMap.end()) {

				withTimeSeriesOption = true;
				TupleValue &value = (*it).second;
				
				if (value.getType() != TupleList::TYPE_STRING) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid fomart type");
				}

				const char* valueStr
						= static_cast<const char*>(value.varData());
				
				assert(valueStr != NULL);
				util::String valueUnit(
						valueStr, value.varSize(), alloc);
				const util::String &normalizeValueUnit =
						SQLUtils::normalizeName(alloc, valueUnit.c_str());

				TimeUnit targetUnitType;
				if (!strcmp(normalizeValueUnit.c_str(), "DAY")) {
					targetUnitType = TIME_UNIT_DAY;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "HOUR")) {
					targetUnitType = TIME_UNIT_HOUR;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "MINUTE")) {
					targetUnitType = TIME_UNIT_MINUTE;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "SECOND")) {
					targetUnitType = TIME_UNIT_SECOND;
				}
				else if (!strcmp(normalizeValueUnit.c_str(), "MILLISECOND")) {
					targetUnitType = TIME_UNIT_MILLISECOND;
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, 
							"Target time unit '" << valueUnit << "' not supported");
				}

				isSetted = true;
				partitioningInfo.timeSeriesProperty_.timeUnit_
						= targetUnitType;
			}

			it = paramMap.find(
					DDLWithParameter::EXPIRATION_DIVISION_COUNT);

			if (it != paramMap.end()) {
		
				useDevisionCount = true;
				TupleValue &value = (*it).second;
				
				if (value.getType() != TupleList::TYPE_LONG) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid fomart type");
				}
	
				int64_t tmpValue = value.get<int64_t>();
				if (tmpValue <= 0) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid value(> 0)");
				}

				if (tmpValue > INT32_MAX) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid value(integer overflow)");
				}

				withTimeSeriesOption = true;
				partitioningInfo.timeSeriesProperty_.dividedNum_
						= static_cast<uint16_t>(tmpValue);
				isSetted = true;
			}

			ExpirationType targetExpirationType
					= EXPIRATION_TYPE_PARTITION;
			it = paramMap.find(
					DDLWithParameter::EXPIRATION_TYPE);
			
			if (it != paramMap.end()) {
				
				TupleValue &value = (*it).second;
				if (value.getType() != TupleList::TYPE_STRING) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
							"Invalid fomart type");
				}

				withTimeSeriesOption = true;
				const char* valueStr
						= static_cast<const char*>(value.varData());
				assert(valueStr != NULL);
				
				util::String valueType(
						valueStr, value.varSize(), alloc);

				const util::String &normalizeValueType =
						SQLUtils::normalizeName(alloc, valueType.c_str());

				if (!strcmp(normalizeValueType.c_str(), "ROW")) {
					targetExpirationType = EXPIRATION_TYPE_ROW;
				}
				else if (!strcmp(
						normalizeValueType.c_str(), "PARTITION")) {
					targetExpirationType = EXPIRATION_TYPE_PARTITION;
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_COMPILE_SYNTAX_ERROR, 
							"Target expiration type '"
							<< valueType << "' not supported");
				}

				isSetted = true;
				partitioningInfo.tableStatus_
						= static_cast<uint8_t>(targetExpirationType);
			}
			else {
				isSetted = true;
				partitioningInfo.tableStatus_
						= static_cast<uint8_t>(targetExpirationType);
			}
		}

		if (useDevisionCount
				&& partitioningInfo.tableStatus_
						== EXPIRATION_TYPE_PARTITION) {
			
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
					"Division Count must be row expiration");
		}

		if (!createOption.isTimeSeries()
				&& partitioningInfo.tableStatus_ == EXPIRATION_TYPE_ROW) {
			
			GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_PARAMETER,
					"Row expriration definition must be timeseries container");
		}			

		int8_t existTimeSeriesOptionTmp = 0;
		if (createOption.isTimeSeries())  {

			if (isSetted && !useExpiration && withTimeSeriesOption) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_COMPILE_SYNTAX_ERROR,
						"TimeSeries property must be setted elapsed time");
			}

			if (partitioningInfo.isRowExpiration()) {

				existTimeSeriesOptionTmp = 1;
				
				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&existTimeSeriesOptionTmp),
						sizeof(int8_t));

				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&partitioningInfo.timeSeriesProperty_.elapsedTime_),
						sizeof(int32_t));

				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&partitioningInfo.timeSeriesProperty_.timeUnit_),
						sizeof(int8_t));

				int32_t divideNum = static_cast<int32_t>(
						partitioningInfo.timeSeriesProperty_.dividedNum_);

				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(&divideNum),
						sizeof(int32_t));
	
				DurationInfo durationInfo;
				
				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&durationInfo.timeDuration_),
						sizeof(int32_t));

				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&durationInfo.timeUnit_),
						sizeof(int8_t));

				int8_t compressionType = NO_COMPRESSION;
				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&compressionType),
						sizeof(int8_t));

				uint32_t compressionInfoNum = 0;
				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&compressionInfoNum),
						sizeof(uint32_t));
			}
			else {
				containerSchema.push_back(
						reinterpret_cast<uint8_t*>(
								&existTimeSeriesOptionTmp),
						sizeof(int8_t));
			}
		}

		int32_t containerAttribute;
		if (createOption.isPartitioning()) {
			containerAttribute = CONTAINER_ATTR_SUB;
		}
		else {
			containerAttribute = CONTAINER_ATTR_SINGLE;
		}

		containerSchema.push_back(
				reinterpret_cast<uint8_t*>(
						&containerAttribute),
				sizeof(int32_t));

		TablePartitioningVersionId versionId
				= static_cast<TablePartitioningVersionId>(
						partitioningInfo.partitioningVersionId_);
		
		containerSchema.push_back(
				reinterpret_cast<uint8_t*>(&versionId),
				sizeof(TablePartitioningVersionId));
	}

	catch (std::exception &e) {
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}
}

void NoSQLUtils::checkSchemaValidation(
		util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &config,
		const char *containerName, 
		util::XArray<uint8_t> &binarySchema,
		ContainerType containerType) {

	util::ArrayByteInStream normalIn
			= util::ArrayByteInStream(
					util::ArrayInStream(
							binarySchema.data(), binarySchema.size()));
	
	if (containerType == COLLECTION_CONTAINER) {

		MessageCollectionSchema messageSchema(
				alloc,
				config,
				containerName,
				normalIn,
				StatementMessage::FEATURE_V4_3);
	}
	else {

		MessageTimeSeriesSchema messageSchema(
				alloc,
				config,
				containerName,
				normalIn,
				StatementMessage::FEATURE_V4_3);
	}
}

void NoSQLUtils::makeContainerColumns(
		util::StackAllocator &alloc,
		util::XArray<uint8_t> &containerSchema,
		util::Vector<util::String> &columnNameList,
		util::Vector<ColumnType> &columnTypeList,
		util::Vector<uint8_t> &columnOptionList) {

	util::ArrayByteInStream in = util::ArrayByteInStream(
			util::ArrayInStream(
					containerSchema.data(), containerSchema.size()));

	int32_t columnNum;
	int16_t keyColumnId;
	in >> columnNum;
	int8_t tmp;
	uint8_t opt;

	for (int32_t i = 0; i < columnNum; i++) {

		util::String columnName(alloc);

		in >> columnName;
		columnNameList.push_back(columnName);

		in >> tmp;		
		columnTypeList.push_back(
				static_cast<ColumnType>(tmp));

		in >> opt;
		columnOptionList.push_back(
				static_cast<uint8_t>(opt));
	}

	int16_t rowKeyNum;
	in >> rowKeyNum;
	for (int16_t pos = 0; pos < rowKeyNum; pos++) {
		in >> keyColumnId;
	}
}

void NoSQLUtils::makeLargeContainerSchema(
		util::XArray<uint8_t> &binarySchema,
		bool isView,
		util::String &affinityStr) {

	int32_t columnNum = 2;
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&columnNum),
			sizeof(int32_t));

	{
	const char *columnName = "key";
	int32_t columnNameLen = static_cast<int32_t>(
			strlen(columnName));
	
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&columnNameLen),
			sizeof(int32_t));
	
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(const_cast<char*>(columnName)),
			columnNameLen);

	int8_t columnType = COLUMN_TYPE_STRING;
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&columnType),
			sizeof(int8_t));
	
	uint8_t opt = 0;
	ColumnInfo::setNotNull(opt);
	
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&opt),
			sizeof(uint8_t));
	}

	const char *columnName = "value";
	int32_t columnNameLen
			= static_cast<int32_t>(strlen(columnName));

	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&columnNameLen),
			sizeof(int32_t));

	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(
					const_cast<char*>(columnName)),
			columnNameLen);

	int8_t columnType = COLUMN_TYPE_BLOB;
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&columnType),
			sizeof(int8_t));

	bool isArrayVal = false;
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&isArrayVal),
			sizeof(bool));

	int16_t rowKeyNum = 1;
	binarySchema.push_back(
			reinterpret_cast<uint8_t *>(&rowKeyNum),
			sizeof(int16_t));

	int16_t keyColumnId
			= static_cast<int16_t>(
					ColumnInfo::ROW_KEY_COLUMN_ID);
	
	binarySchema.push_back(
			reinterpret_cast<uint8_t *>(&keyColumnId),
			sizeof(int16_t));

	int32_t affinityStrLen
			= static_cast<int32_t>(affinityStr.size());
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&affinityStrLen),
			sizeof(int32_t));
	
	if (affinityStrLen > 0) {
		
		void* valuePtr = reinterpret_cast<void*>(
				const_cast<char8_t *>(affinityStr.c_str()));

		binarySchema.push_back(
				reinterpret_cast<uint8_t*>(valuePtr),
				affinityStrLen);
	}

	ContainerAttribute containerAttribute = CONTAINER_ATTR_LARGE;
	if (isView) {
		containerAttribute = CONTAINER_ATTR_VIEW;
	}

	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&containerAttribute),
			sizeof(int32_t));

	TablePartitioningVersionId versionId = 0;
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&versionId),
			sizeof(TablePartitioningVersionId));
	
	int32_t optionType
			= static_cast<int32_t>(MessageSchema::OPTION_END);
	binarySchema.push_back(
			reinterpret_cast<uint8_t*>(&optionType),
			sizeof(int32_t));
}

void NoSQLUtils::makeLargeContainerColumn(
	util::XArray<ColumnInfo> &columnInfoList) {

		uint32_t nullByteSize = ValueProcessor::calcNullsByteSize(1);
		uint16_t fixDataOffset = static_cast<uint16_t>(nullByteSize);
		uint16_t varDataPos = 0;
		fixDataOffset = static_cast<uint16_t>(fixDataOffset 
				+ static_cast<uint16_t>(sizeof(OId)));
		ColumnInfo columnInfo;
		
		columnInfo.setType(COLUMN_TYPE_STRING, false);
		columnInfo.setColumnId(0);
		columnInfo.setOffset(varDataPos++); 
		columnInfoList.push_back(columnInfo);

		columnInfo.setType(COLUMN_TYPE_BLOB, false);
		columnInfo.setColumnId(1);
		columnInfo.setOffset(varDataPos); 
		columnInfoList.push_back(columnInfo);
}

PartitionId NoSQLUtils::resolvePartitionId(
		util::StackAllocator &alloc,
		PartitionId partitionCount,
		const FullContainerKey &containerKey,
		ContainerHashMode hashMode) {

	return DataStore::resolvePartitionId(
			alloc,
			containerKey,
			partitionCount,
			hashMode);
}

void NoSQLUtils::checkPrimaryKey(
		util::StackAllocator &alloc,
		CreateTableOption &createTableOption,
		util::Vector<ColumnId> &columnIds) {

	ColumnId checkId = 0;

	util::Map<util::String, int32_t> columnMap(alloc);
	util::Map<util::String, int32_t>::iterator it;

	for (int32_t i = 0; i < static_cast<int32_t>(
			createTableOption.columnInfoList_->size()); i++) {
		
		util::String columnName(
				(*createTableOption.columnInfoList_)[i]->
						columnName_->name_->c_str(), alloc);
		columnMap.insert(std::make_pair(columnName, i));
		
		if ((*createTableOption.columnInfoList_)[i]->isPrimaryKey()) {
			if (i != 0) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
						"PRIMARY KEY must be only first column (columnId=0)");
			}
	
			columnIds.push_back(i);
			checkId++;
		}
	}

	if (createTableOption.tableConstraintList_) {

		SyntaxTree::ExprList::iterator itr =
				createTableOption.tableConstraintList_->begin();
		for (; itr != createTableOption.tableConstraintList_->end(); ++itr) {
			
			SyntaxTree::Expr* constraint = *itr;
			if (constraint->op_ != SQLType::EXPR_COLUMN
				|| constraint->next_ == NULL) {

				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
						"Invalid table constraint");
			}

			for (size_t pos = 0;
					pos < constraint->next_->size(); pos++) {

				SyntaxTree::Expr *primaryKey = (*constraint->next_)[pos];
			
				if (primaryKey->qName_ == NULL ||
						primaryKey->qName_->name_ == NULL ||
						primaryKey->qName_->name_->size() == 0) {
					
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
							"No table constraint primary key");
				}

				util::String checkColumn(
						primaryKey->qName_->name_->c_str(), alloc);
				it = columnMap.find(checkColumn);
				
				if (it != columnMap.end()) {
					if (columnIds.size() == 0 && (*it).second != 0) {

						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
							"Invalid primary key column definition");
					}

					columnIds.push_back((*it).second);
				}
				else {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_DDL_INVALID_PRIMARY_KEY,
							"Primary key column not found");
				}
			}
		}
	}
}

void NoSQLUtils::decodePartitioningTableIndexInfo(
		util::StackAllocator &alloc,
		InputMessageRowStore &rowStore,
		util::Vector<IndexInfo> &indexInfoList) {

	while (rowStore.next()) {

		const void* keyData = NULL;
		uint32_t keySize = 0;
		const ColumnId keyColumnNo = 0;
		rowStore.getField(keyColumnNo, keyData, keySize);

		const void* valueData = NULL;
		uint32_t valueSize = 0;
		const ColumnId valueColumnNo = 1;
		rowStore.getField(valueColumnNo, valueData, valueSize);

		const uint8_t *value =
				static_cast<const uint8_t*>(valueData)
						+ ValueProcessor::getEncodedVarSize(valueData);

		util::ArrayByteInStream inStream(
				util::ArrayInStream(value, valueSize));

		uint32_t indexListSize;
		inStream >> indexListSize;
		
		for (size_t pos = 0; pos < indexListSize; pos++) {

			IndexInfo indexInfo(alloc);
			StatementHandler::decodeIndexInfo(
					inStream, indexInfo);
			indexInfoList.push_back(indexInfo);
		}
	}
}

int32_t NoSQLUtils::getColumnId(
		util::StackAllocator &alloc,
		util::Vector<util::String> columnNameList,
		const NameWithCaseSensitivity &columnName) {

	int32_t indexColId = -1;
	int32_t realIndexColId = 0;

	bool isCaseSensitive = columnName.isCaseSensitive_;
	const util::String &normalizeSpecfiedColumnName =
			SQLUtils::normalizeName(
					alloc,
					columnName.name_,
					isCaseSensitive);
	
	for (int32_t col = 0; col < static_cast<int32_t>(
			columnNameList.size()); col++) {
			
		util::String normalizeColumnName =
				SQLUtils::normalizeName(
						alloc,
						columnNameList[col].c_str(),
						isCaseSensitive);

		if (!normalizeColumnName.compare(
				normalizeSpecfiedColumnName)) {

			indexColId = realIndexColId;
			break;
		}
		realIndexColId++;
	}
	return indexColId;
}

template<typename T>
void NoSQLUtils::makeLargeContainerRow(
		util::StackAllocator &alloc,
		const char *key, 
		OutputMessageRowStore &outputMrs, 
		T &targetValue) {

	util::XArray<uint8_t> encodeValue(alloc);
	util::XArrayByteOutStream outStream =
			util::XArrayByteOutStream(
					util::XArrayOutStream<>(encodeValue));

	util::ObjectCoder().encode(outStream, targetValue);
	outputMrs.beginRow();

	outputMrs.setFieldForRawData(
			0,
			key,
			static_cast<uint32_t>(strlen(key)));
	
	outputMrs.setFieldForRawData(
			1,
			encodeValue.data(),
			static_cast<uint32_t>(encodeValue.size()));

	outputMrs.next();
}

void NoSQLUtils::makeLargeContainerRowBinary(
		const char *key,
		OutputMessageRowStore &outputMrs,
		util::XArray<uint8_t> &targetValue) {

	outputMrs.beginRow();
	
	outputMrs.setFieldForRawData(
			0,
			key,
			static_cast<uint32_t>(strlen(key)));

	outputMrs.setFieldForRawData(
			1,
			targetValue.data(),
			static_cast<uint32_t>(targetValue.size()));
	
	outputMrs.next();
}


template void NoSQLUtils::makeLargeContainerRow(
		util::StackAllocator &alloc,
		const char *key,
		OutputMessageRowStore &outputMrs,
		ViewInfo &targetValue);

template void NoSQLUtils::makeLargeContainerRow(
		util::StackAllocator &alloc,
		const char *key,
		OutputMessageRowStore &outputMrs,
		VersionInfo &targetValue);

template void NoSQLUtils::makeLargeContainerRow(
		util::StackAllocator &alloc,
		const char *key,
		OutputMessageRowStore &outputMrs,
		TablePartitioningInfo<
				util::StackAllocator> &targetValue);

template void NoSQLUtils::makeLargeContainerRow(
		util::StackAllocator &alloc,
		const char *key,
		OutputMessageRowStore &outputMrs,
		TablePartitioningIndexInfo &targetValue);

void NoSQLUtils::resolveTargetContainer(
		EventContext &ec,
		const TupleValue *value1,
		const TupleValue *value2, 
		NoSQLDB *db,
		TableSchemaInfo *origTableSchema,
		SQLExecution *execution,
		NameWithCaseSensitivity &dbName,
		NameWithCaseSensitivity &tableName,
		TargetContainerInfo &targetInfo,
		bool &needRefresh) {

	util::StackAllocator &alloc = ec.getAllocator();
	TableContainerInfo *containerInfo = NULL;
	
	NodeAffinityNumber affinity = UNDEF_NODE_AFFINITY_NUMBER;
	NoSQLStoreOption cmdOption(execution);
	
	if (tableName.isCaseSensitive_) {
		cmdOption.caseSensitivity_.setContainerNameCaseSensitive();
	}
	
	NoSQLContainer *currentContainer = NULL;
	SQLExecution::SQLExecutionContext
			&sqlContext = execution->getContext();

	NoSQLStore *store = db->getNoSQLStore(
			sqlContext.getDBId(), sqlContext.getDBName());
	
	TableSchemaInfo *tableSchema = origTableSchema;
	int64_t baseValue;
	
	NodeAffinityNumber baseAffinity;
	const ResourceSet *resoureSet = execution->getResourceSet();
	uint32_t partitionNum
			= resoureSet->getPartitionTable()->getPartitionNum();

	if (value1 != NULL) {
		containerInfo = tableSchema->getTableContainerInfo(
				partitionNum,
				value1,
				value2,
				affinity,
				baseValue,
				baseAffinity);

		if (containerInfo != NULL) {
			targetInfo.set(containerInfo, affinity);
		}
		else {
			if (affinity == UNDEF_NODE_AFFINITY_NUMBER) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_INTERNAL,
						"Affinity must be not null");
			}

			NoSQLContainer targetContainer(
					ec,
					tableName,
					sqlContext.getSyncContext(),
					execution);
			targetContainer.getContainerInfo(cmdOption);

			if (!targetContainer.isExists()) {
				GS_THROW_USER_ERROR(
					GS_ERROR_SQL_TABLE_PARTITION_PARTITIONING_TABLE_NOT_FOUND,
					"Target table is not found");
			}

			util::XArray<ColumnInfo> columnInfoList(alloc);
			NoSQLUtils::makeLargeContainerColumn(columnInfoList);
			LargeExecStatus execStatus(alloc);
			cmdOption.sendBaseValue_ = true;
			cmdOption.baseValue_ = baseValue;

			TablePartitioningInfo<util::StackAllocator>
					partitioningInfo(alloc);

			NoSQLStoreOption option(execution);
			store->getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
					alloc,
					NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
					targetContainer,
					columnInfoList,
					partitioningInfo,
					option);
			partitioningInfo.init();

			if (partitioningInfo.partitionType_
					<= SyntaxTree::TABLE_PARTITION_TYPE_HASH) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH,"");
			}

			affinity = partitioningInfo.getAffinityNumber(
					partitionNum,
					value1,
					value2,
					-1,
					baseValue,
					baseAffinity);

			size_t targetAffinityPos = partitioningInfo.findEntry(affinity);
			if (targetAffinityPos == SIZE_MAX) {
			}
			else if (partitioningInfo.assignStatusList_[targetAffinityPos]
						== PARTITION_STATUS_DROP_START) {
				GS_THROW_USER_ERROR(
						GS_ERROR_SQL_TABLE_PARTITION_ALREADY_REMOVED,
						"Target partition is already removed");
			}
			cmdOption.isSync_ = true;
			cmdOption.ifNotExistsOrIfExists_ = true;
			
			NoSQLUtils::execPartitioningOperation(
					ec,
					db,
					PARTITION_STATUS_CREATE_START,
					execStatus,
					targetContainer,
					baseAffinity,
					affinity,
					cmdOption,
					partitioningInfo,
					tableName,
					execution,
					targetInfo);

			if(targetInfo.affinity_ == UNDEF_NODE_AFFINITY_NUMBER) {
				NoSQLDB::TableLatch latch(
						ec, db, execution, dbName, tableName, false);

				TableSchemaInfo *curentSchema = latch.get();
				if (curentSchema) {
					containerInfo = curentSchema->getTableContainerInfo(
							partitionNum,
							value1,
							value2, 
							affinity,
							baseValue,
							baseAffinity);

					assert(containerInfo != NULL);
					if (containerInfo != NULL) {
						targetInfo.set(containerInfo, affinity);
					}
					else {
						GS_THROW_USER_ERROR(
								GS_ERROR_DS_CONTAINER_NOT_FOUND,
								"Table " << tableName.name_
								<< " (affinity=" << affinity << ") not found");
					}
				}
			}

			if(targetInfo.affinity_ == UNDEF_NODE_AFFINITY_NUMBER) {
				GS_THROW_USER_ERROR(
						GS_ERROR_DS_CONTAINER_NOT_FOUND,
						"Table " << tableName.name_
						<< " (affinity=" << affinity << ") not found");
			}
			
			try {
				currentContainer = NoSQLUtils::createNoSQLContainer(
						ec,
						tableName,
						partitioningInfo.largeContainerId_,
						targetInfo.affinity_,
						execution);
				
				currentContainer->getContainerInfo(cmdOption);
				if (!currentContainer->isExists()) {
					GS_THROW_USER_ERROR(
							GS_ERROR_SQL_CONTAINER_NOT_FOUND, "");
				}
			}
			catch (std::exception &e) {
				if (currentContainer == NULL) {
					GS_RETHROW_USER_OR_SYSTEM(e, "");
				}

				TablePartitioningIndexInfo tablePartitioningIndexInfo(alloc);
				store->getLargeRecord<TablePartitioningIndexInfo>(
						alloc,
						NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
						targetContainer,
						columnInfoList,
						tablePartitioningIndexInfo,
						option);

				util::XArray<uint8_t> containerSchema(alloc);
				store->getLargeBinaryRecord(
						alloc,
						NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
						targetContainer,
						columnInfoList,
						containerSchema,
						option);

				TableExpirationSchemaInfo info;
				partitioningInfo.checkTableExpirationSchema(
						info, baseAffinity, partitionNum);

				store->createSubContainer(
						info, alloc,
						targetContainer,
						currentContainer,
						containerSchema,
						partitioningInfo,
						tablePartitioningIndexInfo,
						tableName,
						partitionNum,
						targetInfo.affinity_);

				NoSQLUtils::dumpRecoverContainer(
						alloc, tableName.name_, currentContainer);
			}
			needRefresh = true;
			store->removeCache(tableName.name_);
		}
	}
	else {
		assert(tableSchema->containerInfoList_.size() != 0);
		containerInfo = &tableSchema->containerInfoList_[0];	
		targetInfo.set(containerInfo, affinity);
	}
}

template<typename Alloc>
bool NoSQLUtils::execPartitioningOperation(
		EventContext &ec,
		NoSQLDB *db,
		LargeContainerStatusType targetOperation,
		LargeExecStatus &execStatus,
		NoSQLContainer &targetContainer,
		NodeAffinityNumber baseAffinity,
		NodeAffinityNumber targetAffinity,
		NoSQLStoreOption &cmdOption,
		TablePartitioningInfo<Alloc> &partitioningInfo,
		const NameWithCaseSensitivity &tableName,
		SQLExecution *execution,
		TargetContainerInfo &targetContainerInfo) {

	util::StackAllocator &alloc = ec.getAllocator();
	NoSQLContainer *currentContainer = NULL;
	IndexInfo *indexInfo = &execStatus.indexInfo_;
	const ResourceSet *resourceSet = execution->getResourceSet();
	uint32_t partitionNum
			= resourceSet->getPartitionTable()->getPartitionNum();
	
	NoSQLStore *store = db->getNoSQLStore(
			execution->getContext().getDBId(),
			execution->getContext().getDBName());

	util::XArray<ColumnInfo> columnInfoList(alloc);
	NoSQLUtils::makeLargeContainerColumn(columnInfoList);
		
	switch (targetOperation) {
		case PARTITION_STATUS_CREATE_START:
		case PARTITION_STATUS_DROP_START: {
			util::Vector<NodeAffinityNumber> affinityList(alloc);
			size_t targetHashPos = 0;
			
			if (partitioningInfo.partitionType_
					== SyntaxTree::TABLE_PARTITION_TYPE_RANGE_HASH) {
				for (size_t hashPos = 0;
						hashPos < partitioningInfo.partitioningNum_; hashPos++) {
					affinityList.push_back(baseAffinity + hashPos);
					if (baseAffinity + hashPos == targetAffinity) {
						targetHashPos = hashPos;
					}
				}
			}
			else {
				affinityList.push_back(targetAffinity);
				targetHashPos = 0;
			}
			
			if (targetOperation == PARTITION_STATUS_CREATE_START) {
				targetContainer.updateLargeContainerStatus(
						PARTITION_STATUS_CREATE_START, 
						baseAffinity,
						cmdOption,
						execStatus,
						indexInfo);

				util::XArray<uint8_t> subContainerSchema(alloc);
				store->getLargeBinaryRecord(
						alloc,
						NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
						targetContainer,
						columnInfoList,
						subContainerSchema,
						cmdOption);

				for (size_t pos = 0; pos < affinityList.size(); pos++) {
					size_t targetAffinityPos
							= partitioningInfo.findEntry(affinityList[pos]);
					if (targetAffinityPos == SIZE_MAX) {
					}
					else if (partitioningInfo.assignStatusList_[targetAffinityPos]
								== PARTITION_STATUS_DROP_START) {
						GS_THROW_USER_ERROR(
								GS_ERROR_SQL_TABLE_PARTITION_ALREADY_REMOVED,
								"Target partition is already removed");
					}

					currentContainer = NoSQLUtils::createNoSQLContainer(
							ec,
							tableName,
							partitioningInfo.largeContainerId_,
							affinityList[pos],
							execution);

					TableExpirationSchemaInfo info;
					partitioningInfo.checkTableExpirationSchema(
							info, baseAffinity, partitionNum);
					
					store->putContainer(
							info,
							subContainerSchema,
							0,
							partitioningInfo.containerType_,
							CONTAINER_ATTR_SUB,
							false,
							*currentContainer, 
							NULL,
							NULL,
							cmdOption);

					if (pos == targetHashPos) {
						targetContainerInfo.containerId_ 
								= currentContainer->getContainerId();
						targetContainerInfo.pId_
								= currentContainer->getPartitionId();
						targetContainerInfo.versionId_ 
								= currentContainer->getSchemaVersionId();
						targetContainerInfo.affinity_ = affinityList[pos];
						targetContainerInfo.pos_ = 0;
					}
					
					TablePartitioningIndexInfo
							tablePartitioningIndexInfo(alloc);

					NoSQLStoreOption option(execution);
					store->getLargeRecord<TablePartitioningIndexInfo>(
							alloc,
							NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
							targetContainer,
							columnInfoList,
							tablePartitioningIndexInfo,
							option);

					TablePartitioningIndexInfoEntry *entry;
					for (size_t indexPos = 0;
							indexPos < tablePartitioningIndexInfo.indexEntryList_.size();
							indexPos++) {

						entry = tablePartitioningIndexInfo.indexEntryList_[indexPos];
						if (!entry->indexName_.empty()) {
							
							currentContainer->createIndex(
								entry->indexName_.c_str(),
								entry->indexType_,
								entry->columnIds_,
								cmdOption);
						}
					}
				}
				{
					TablePartitioningInfo<util::StackAllocator>
							partitioningInfo(alloc);
					NoSQLStoreOption option(execution);
					
					store->getLargeRecord<TablePartitioningInfo<util::StackAllocator> >(
						alloc,
						NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO,
						targetContainer,
						columnInfoList,
						partitioningInfo,
						option);

					partitioningInfo.init();
					util::Set<NodeAffinityNumber> candAffinityNumberList(alloc);
					uint32_t partitioningNum
								= (partitioningInfo.partitioningNum_ == 0 ? 1 :
										partitioningInfo.partitioningNum_);

					for (size_t hashPos = 0; hashPos < partitioningNum; hashPos++) {
						candAffinityNumberList.insert(baseAffinity + hashPos);
					}
					
					partitioningInfo.findNeighbor(
							alloc,
							cmdOption.baseValue_,
							candAffinityNumberList);

					TablePartitioningVersionId tablePartitioningVersionId
							= partitioningInfo.partitioningVersionId_;
					
					for (util::Set<NodeAffinityNumber>::iterator
							it = candAffinityNumberList.begin();
							it != candAffinityNumberList.end(); it++) {
					
						currentContainer = NoSQLUtils::createNoSQLContainer(
								ec,
								tableName,
								partitioningInfo.largeContainerId_,
								(*it), execution);

						currentContainer->getContainerInfo(cmdOption);

						if (!currentContainer->isExists()) {
							TablePartitioningIndexInfo
									tablePartitioningIndexInfo(alloc);
							
							store->getLargeRecord<TablePartitioningIndexInfo>(
									alloc,
									NoSQLUtils::LARGE_CONTAINER_KEY_INDEX,
									targetContainer,
									columnInfoList,
									tablePartitioningIndexInfo,
									option);

							util::XArray<uint8_t> containerSchema(alloc);
							store->getLargeBinaryRecord(
									alloc,
									NoSQLUtils::LARGE_CONTAINER_KEY_SUB_CONTAINER_SCHEMA,
									targetContainer,
									columnInfoList,
									containerSchema,
									option);

							NodeAffinityNumber currentAffinity = (*it);
							TableExpirationSchemaInfo info;
							partitioningInfo.checkTableExpirationSchema(
									info, baseAffinity, partitionNum);
							
							store->createSubContainer<util::StackAllocator>(
									info,
									alloc,
									targetContainer,
									currentContainer,
									containerSchema,
									partitioningInfo,
									tablePartitioningIndexInfo,
									tableName,
									partitionNum,
									currentAffinity);

							NoSQLUtils::dumpRecoverContainer(
									alloc, tableName.name_, currentContainer);
						}

						currentContainer->updateContainerProperty(
								tablePartitioningVersionId, cmdOption);
					}
				}
			}
			else {
				for (size_t pos = 0; pos < affinityList.size(); pos++) {
					
					currentContainer = NoSQLUtils::createNoSQLContainer(
							ec,
							tableName,
							partitioningInfo.largeContainerId_,
							affinityList[pos],
							execution);
					
					currentContainer->setContainerType(
							partitioningInfo.containerType_);
					
					currentContainer->getContainerInfo(cmdOption);
					currentContainer->dropContainer(cmdOption);
				}
			}
		}
		break;
	}
	return true;
}

void NoSQLUtils::dumpRecoverContainer(
		util::StackAllocator &alloc,
		const char *tableName,
		NoSQLContainer *container) {

	if (container != NULL) {
		util::String subNameStr(alloc);
		container->getContainerKey()->toString(alloc, subNameStr);
		GS_TRACE_WARNING(
				SQL_SERVICE, GS_TRACE_SQL_RECOVER_CONTAINER,
				"Recover table '" << tableName
				<< "', partition '" << subNameStr);
	}
}

NoSQLContainer* NoSQLUtils::createNoSQLContainer(
		EventContext &ec,
		const NameWithCaseSensitivity tableName,
		ContainerId largeContainerId,
		NodeAffinityNumber affnitiyNumber,
		SQLExecution *execution) {

	util::StackAllocator &alloc = ec.getAllocator();
	
	FullContainerKeyComponents components;
	components.dbId_ = execution->getContext().getDBId();
	components.affinityNumber_ = affnitiyNumber;
	components.baseName_ = tableName.name_;
	components.baseNameSize_
			= static_cast<uint32_t>(strlen(tableName.name_));
	components.largeContainerId_ =  largeContainerId;
	
	KeyConstraint keyConstraint(
			KeyConstraint::getNoLimitKeyConstraint());
	FullContainerKey *containerKey
			= ALLOC_NEW(alloc) FullContainerKey(
					alloc, keyConstraint, components);

	return  ALLOC_NEW(alloc) NoSQLContainer(ec,
			NameWithCaseSensitivity(
					tableName.name_, tableName.isCaseSensitive_),
			containerKey,
			execution->getContext().getSyncContext(),
			execution);
}

const char8_t* timeUnitToName(TimeUnit unit) {
	switch (unit) {
	case TIME_UNIT_DAY:
		return "DAY";
	case TIME_UNIT_HOUR:
		return "HOUR";
	case TIME_UNIT_MINUTE:
		return "MINUTE";
	case TIME_UNIT_SECOND:
		return "SECOND";
	case TIME_UNIT_MILLISECOND:
		return "MILLISECOND";
	default:
		assert(false);
		return "";
	}
}

void NoSQLUtils::checkConnectedDbName(
		util::StackAllocator &alloc,
		const char *connectedDbName,
		const char *currentDbName,
		bool isCaseSensitive) {

	if (currentDbName == NULL 
			|| (currentDbName && strlen(currentDbName) == 0)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_NAME,
				"Invalid database name=" << currentDbName);
	}

	const util::String &currentNormalizedDbName
			= SQLUtils::normalizeName(alloc, currentDbName, isCaseSensitive);

	const util::String &connectedNormalizedDbName
			= SQLUtils::normalizeName(alloc, connectedDbName, isCaseSensitive);

	if (strcmp(currentNormalizedDbName.c_str(),
			connectedNormalizedDbName.c_str()) != 0) {
		GS_THROW_USER_ERROR(
					GS_ERROR_SQL_DDL_INVALID_CONNECTED_DATABASE,
					"Connected database is not same"
					"as the target database, connected='"
					<< connectedDbName << "', specified='"
					<< currentDbName << "'");
	}
}

NoSQLStoreOption::NoSQLStoreOption(SQLExecution *execution) {

	clear();

	if (execution) {
		storeMemoryAgingSwapRate_
				= execution->getContext().getStoreMemoryAgingSwapRate();
		timezone_ = execution->getContext().getTimezone();
		const char *applicationName
				= execution->getContext().getApplicationName();
		if (applicationName != NULL && strlen(applicationName) > 0) {
			applicationName_ = applicationName;
		}
	}
}

template<typename T>
void NoSQLUtils::decodeRow(InputMessageRowStore &rowStore, T &record,
		VersionInfo &versionInfo, const char *rowKey) {

	bool decodeVersion = false;
	bool decodeInfo = false;

	while (rowStore.next()) {
		
		const void* keyData = NULL;
		uint32_t keySize = 0;
		const ColumnId keyColumnNo = 0;
		rowStore.getField(keyColumnNo, keyData, keySize);
		const char8_t *key =
				static_cast<const char8_t*>(keyData)
						+ ValueProcessor::getEncodedVarSize(keyData);

		const size_t versionKeyLength =
				std::min(strlen(NoSQLUtils::LARGE_CONTAINER_KEY_VERSION),
				static_cast<size_t>(keySize));
		const size_t infoKeyLength = 
			std::min(strlen(rowKey), static_cast<size_t>(keySize));

		const void* valueData = NULL;
		uint32_t valueSize = 0;
		const ColumnId valueColumnNo = 1;
		rowStore.getField(valueColumnNo, valueData, valueSize);
		const uint8_t *value =
			static_cast<const uint8_t*>(valueData)
			+ ValueProcessor::getEncodedVarSize(valueData);

		util::ArrayByteInStream inStream(
				util::ArrayInStream(value, valueSize));

		if (strncmp(key,
				NoSQLUtils::LARGE_CONTAINER_KEY_VERSION,
				versionKeyLength) == 0) {

			util::ObjectCoder().decode(inStream, versionInfo);
			decodeVersion = true;
		}
		else if (strncmp(key, rowKey, infoKeyLength) == 0) {

			util::ObjectCoder::withAllocator(record.getAllocator()).decode(
					inStream, record);
			decodeInfo = true;
		}
	}
 if (!decodeVersion) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_TABLE_FORMAT,
				"Invalid format, version not found.");
	}
}

template
void NoSQLUtils::decodeRow(
		InputMessageRowStore &rowStore,
		TablePartitioningInfo<util::StackAllocator> &record,
		VersionInfo &versionInfo,
		const char *rowKey);

template
void NoSQLUtils::decodeRow(
		InputMessageRowStore &rowStore,
		TablePartitioningIndexInfo &record,
		VersionInfo &versionInfo,
		const char *rowKey);

template<typename T>
void NoSQLUtils::decodeBinaryRow(
		InputMessageRowStore &rowStore,
		T &record,
		VersionInfo &versionInfo,
		const char *rowKey) {

	bool decodeVersion = false;
	bool decodeInfo = false;

	while (rowStore.next()) {
		const void* keyData = NULL;
		uint32_t keySize = 0;
		const ColumnId keyColumnNo = 0;
		rowStore.getField(keyColumnNo, keyData, keySize);
		const char8_t *key =
				static_cast<const char8_t*>(keyData)
				+ ValueProcessor::getEncodedVarSize(keyData);

		const size_t versionKeyLength =
				std::min(strlen(NoSQLUtils::LARGE_CONTAINER_KEY_VERSION),
				static_cast<size_t>(keySize));
		const size_t infoKeyLength = 
			std::min(strlen(rowKey), static_cast<size_t>(keySize));

		const void* valueData = NULL;
		uint32_t valueSize = 0;
		const ColumnId valueColumnNo = 1;
		rowStore.getField(valueColumnNo, valueData, valueSize);
		const uint8_t *value =
			static_cast<const uint8_t*>(valueData)
			+ ValueProcessor::getEncodedVarSize(valueData);

		util::ArrayByteInStream inStream(
				util::ArrayInStream(value, valueSize));

		if (strncmp(key,
				NoSQLUtils::LARGE_CONTAINER_KEY_VERSION,
				versionKeyLength) == 0) {
			
			util::ObjectCoder().decode(inStream, versionInfo);
			decodeVersion = true;
		}
		else if (strncmp(key,rowKey, infoKeyLength) == 0) {
			record.resize(valueSize);
			inStream >> std::make_pair(record.data(), valueSize);
			decodeInfo = true;
		}
	}

	if (!(decodeVersion || decodeInfo)) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_TABLE_FORMAT,
				"Invalid format, both version and record were not found.");
	}
	else if (!decodeVersion) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_TABLE_FORMAT,
				"Invalid format, version not found.");
	}
	else if (!decodeInfo) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_INVALID_TABLE_FORMAT,
				"Invalid format, record not found.");
	}
}

template<typename T>
void NoSQLUtils::getTablePartitioningInfo(
		util::StackAllocator &alloc,
		DataStore *ds,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		T &partitioningInfo,
		NoSQLStoreOption &option) {

	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute()
			!= CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='" 
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION 
			<< "' OR key='" 
			<< NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO
			<< "'";
	
	container.executeSyncQuery(
			queryStr.str().c_str(),
			option,
			fixedPart,
			varPart,
			rowCount,
			hasRowKey);

	InputMessageRowStore rowStore(
			ds->getValueLimitConfig(),
			columnInfoList.data(),
			static_cast<uint32_t>(columnInfoList.size()),
			fixedPart.data(),
			static_cast<uint32_t>(fixedPart.size()),
			varPart.data(),
			static_cast<uint32_t>(varPart.size()),
			rowCount,
			true,
			false);

	VersionInfo versionInfo;
	NoSQLUtils::decodeRow<T>(
			rowStore, partitioningInfo, versionInfo,
			NoSQLUtils::LARGE_CONTAINER_KEY_PARTITIONING_INFO);
}

template<typename T>
void NoSQLUtils::getLargeContainerInfo(
		util::StackAllocator &alloc,
		const char *key, 
		DataStore *ds, NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		T &partitioningInfo) {

	NoSQLStoreOption option;
	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='" 
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION 
			<< "' OR key='" << key << "'";
	container.executeSyncQuery(
			queryStr.str().c_str(),
			option,
			fixedPart,
			varPart,
			rowCount,
			hasRowKey);

		InputMessageRowStore rowStore(
				ds->getValueLimitConfig(),
				columnInfoList.data(),
				static_cast<uint32_t>(columnInfoList.size()),
				fixedPart.data(),
				static_cast<uint32_t>(fixedPart.size()),
				varPart.data(),
				static_cast<uint32_t>(varPart.size()),
				rowCount,
				true,
				false);

		VersionInfo versionInfo;
		NoSQLUtils::decodeRow<T>(
				rowStore, partitioningInfo, versionInfo, key);
}

template<typename T>
void  NoSQLUtils::getLargeContainerInfoBinary(
		util::StackAllocator &alloc,
		const char *key,
		DataStore *ds,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		T &partitioningInfo) {

	NoSQLStoreOption option;
	int64_t rowCount;
	bool hasRowKey = false;
	util::XArray<uint8_t> fixedPart(alloc);
	util::XArray<uint8_t> varPart(alloc);

	if (container.getContainerAttribute() != CONTAINER_ATTR_LARGE) {
		GS_THROW_USER_ERROR(
				GS_ERROR_SQL_TABLE_PARTITION_SCHEMA_UNMATCH, "");
	}

	util::NormalOStringStream queryStr;
	queryStr << "select * where key='" 
			<< NoSQLUtils::LARGE_CONTAINER_KEY_VERSION 
			<< "' OR key='" << key << "'";
	container.executeSyncQuery(
			queryStr.str().c_str(),
			option,
			fixedPart,
			varPart,
			rowCount,
			hasRowKey);

	InputMessageRowStore rowStore(
			ds->getValueLimitConfig(),
			columnInfoList.data(),
			static_cast<uint32_t>(columnInfoList.size()),
			fixedPart.data(),
			static_cast<uint32_t>(fixedPart.size()),
			varPart.data(),
			static_cast<uint32_t>(varPart.size()),
			rowCount,
			true,
			false);

	VersionInfo versionInfo;
	NoSQLUtils::decodeBinaryRow<T>(
			rowStore, partitioningInfo, versionInfo, key);
}

template void NoSQLUtils::getLargeContainerInfo(
		util::StackAllocator &alloc,
		const char *key, DataStore *ds,
		NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		TablePartitioningInfo<class util::StackAllocator> &partitioningInfo);

template void NoSQLUtils::decodeRow(
		InputMessageRowStore &rowStore, ViewInfo &record,
		VersionInfo &versionInfo, const char *rowKey);

template void NoSQLUtils::decodeRow(
		InputMessageRowStore &rowStore,
		TablePartitioningInfo<SQLVariableSizeGlobalAllocator> &record,
		VersionInfo &versionInfo, const char *rowKey);

template void NoSQLUtils::decodeBinaryRow(
		InputMessageRowStore &rowStore,
		util::XArray<uint8_t> &record,
		VersionInfo &versionInfo, const char *rowKey);

template void NoSQLUtils::getTablePartitioningInfo(
		util::StackAllocator &alloc,
		DataStore *ds, NoSQLContainer &container,
		util::XArray<ColumnInfo> &columnInfoList,
		TablePartitioningInfo<util::StackAllocator> &partitioningInfo,
		NoSQLStoreOption &option);

template bool NoSQLUtils::execPartitioningOperation(
		EventContext &ec,
		NoSQLDB *db,
		LargeContainerStatusType targetOperation,
		LargeExecStatus &execStatus,
		NoSQLContainer &targetContainer,
		NodeAffinityNumber baseAffinity,
		NodeAffinityNumber targetAffinity,
		NoSQLStoreOption &cmdOption,
		TablePartitioningInfo<util::StackAllocator> &partitioningInfo,
		const NameWithCaseSensitivity &tableName,
		SQLExecution *execution,
		TargetContainerInfo &targetContainerInfo);