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
	@brief Implementation of MessageSchema
*/

#include "message_schema.h"
#include "value_processor.h"

#include "picojson.h"


const int32_t MessageSchema::DEFAULT_VERSION = 0;
const int32_t MessageSchema::V4_1_VERSION = 1;
const int32_t MessageSchema::V5_8_VERSION = 2;

MessageSchema::MessageSchema(util::StackAllocator &alloc,
	const DataStoreConfig &dsConfig, const char *,
	util::ArrayByteInStream &in, int32_t featureVersion)
	: dsConfig_(dsConfig),
	  containerType_(UNDEF_CONTAINER),
	  affinityStr_(alloc),
	  tablePartitioningVersionId_(UNDEF_TABLE_PARTITIONING_VERSIONID),
	  containerExpirationStartTime_(-1),
	  isRenameColumn_(false),
	  keyColumnIds_(alloc),
	  columnTypeList_(alloc),
	  flagsList_(alloc),
	  columnNameList_(alloc),
	  columnNameMap_(alloc),
	  alloc_(alloc),
	  containerAttribute_(CONTAINER_ATTR_SINGLE),
	  oldColumnNum_(0),
	  oldVarColumnNum_(0),
	  oldRowFixedColumnSize_(0) {
	UNUSED_VARIABLE(featureVersion);
	validateColumnSchema(in);
}


void MessageSchema::validateColumnSchema(util::ArrayByteInStream &in) {
	in >> columnNum_;
	validateColumnSize();

	columnNameList_.reserve(columnNum_);
	columnTypeList_.reserve(columnNum_);
	flagsList_.reserve(columnNum_);
	validateColumn(in);
	validateKeyColumn(in);
}

void MessageSchema::validateColumnSize() {
	if (columnNum_ == 0 ||
		columnNum_ >
			dsConfig_.getLimitColumnNum()) {  
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Number of columns = " << columnNum_ << " is invalid");
	}
}

void MessageSchema::validateColumn(util::ArrayByteInStream& in) {
	uint32_t fixedColumnTotalSize = 0;
	bool hasVariableColumn = false;
	util::Map<util::String, ColumnId, CompareStringI>::iterator itr;
	for (uint32_t i = 0; i < columnNum_; i++) {
		util::String columnName(alloc_);
		in >> columnName;

		NoEmptyKey::validate(
			KeyConstraint::getUserKeyConstraint(LIMIT_COLUMN_NAME_SIZE),
			columnName.c_str(), static_cast<uint32_t>(columnName.size()),
			"columnName");

		columnNameList_.push_back(columnName);

		util::XArray<char8_t> buffer(alloc_);
		buffer.resize(columnName.size() + 1);
		ValueProcessor::convertUpperCase(
			columnName.c_str(), columnName.size() + 1, buffer.data());
		util::String caseColumnName(buffer.data(), alloc_);

		itr = columnNameMap_.find(caseColumnName);
		if (itr != columnNameMap_.end()) {  
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Column name '" << columnName.c_str() << "' already exists");
		}
		columnNameMap_.insert(std::make_pair(caseColumnName, i));

		int8_t typeOrdinal;
		in >> typeOrdinal;

		uint8_t flagsTmp;
		in >> flagsTmp;
		flagsList_.push_back(flagsTmp);
		const bool forArray = getIsArray(i);

		ColumnType columnType;
		if (!ValueProcessor::findColumnTypeByPrimitiveOrdinal(
				typeOrdinal, forArray, false, columnType)) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Unsupported Column type ("
					"ordinal=" << static_cast<int32_t>(typeOrdinal) <<
					", forArray=" << forArray << ")");
		}
		columnTypeList_.push_back(columnType);

		if (!forArray && ValueProcessor::isSimple(columnType)) {
			fixedColumnTotalSize += FixedSizeOfColumnType[columnType];
			if (fixedColumnTotalSize > MAX_FIXED_COLUMN_TOTAL_SIZE) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Total size of Fixed Columns is over, limit =  " << MAX_FIXED_COLUMN_TOTAL_SIZE);
			}
		}
		else if (!hasVariableColumn) {
			hasVariableColumn = true;
		}
	}
}

void MessageSchema::validateKeyColumn(util::ArrayByteInStream& in) {
	int16_t rowKeyNum;
	in >> rowKeyNum;
	for (int16_t i = 0; i < rowKeyNum; i++) {
		int16_t rowKeyColumnId;
		in >> rowKeyColumnId;
		if (rowKeyColumnId != i) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"ColumnNumber of rowkey = " << rowKeyColumnId << " must be sequential from zero");
		}

		const bool isArray = getIsArray(rowKeyColumnId);
		const bool isNotNull = getIsNotNull(rowKeyColumnId);
		if (!ValueProcessor::validateRowKeyType(
			isArray, columnTypeList_[rowKeyColumnId])) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"unsupported RowKey Column type = "
				<< (int32_t)columnTypeList_[rowKeyColumnId]
				<< ", array = " << (int32_t)isArray);
		}
		if (!isNotNull) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"unsupported RowKey Column is not nullable");
		}
		keyColumnIds_.push_back(rowKeyColumnId);
	}
	if (rowKeyNum > MAX_COMPOSITE_COLUMN_NUM) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Number of rowkey must be less than " << MAX_COMPOSITE_COLUMN_NUM);
	}
}

void MessageSchema::validateContainerOption(util::ArrayByteInStream &in) {
	in >> affinityStr_;
	if (affinityStr_.compare(UNIQUE_GROUP_ID_KEY) != 0) {
		EmptyAllowedKey::validate(
			KeyConstraint::getUserKeyConstraint(AFFINITY_STRING_MAX_LENGTH),
			affinityStr_.c_str(), static_cast<uint32_t>(affinityStr_.length()),
			"affinity string");
	}
}

void MessageSchema::validateContainerExpiration(util::ArrayByteInStream &in) {
	int32_t optionSize;
	in >> optionSize;

	in >> containerExpirationStartTime_;
	in >> containerExpirationInfo_.interval_;
	in >> containerExpirationInfo_.info_.duration_;
	if (containerExpirationInfo_.info_.duration_ <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"elapsedTime of ContainerExpiration is Invalid : "
				<< containerExpirationInfo_.info_.duration_);
	}
	if (containerExpirationInfo_.interval_ <= 0) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"interval of ContainerExpiration is Invalid : "
				<< containerExpirationInfo_.interval_);
	}
}

void MessageSchema::validateRenameColumnSchema(util::ArrayByteInStream &in) {
	int32_t optionSize;
	in >> optionSize;
}

void MessageSchema::validateFixColumnSchema(util::ArrayByteInStream &in) {
	int32_t optionSize;
	in >> optionSize;

	if (optionSize != 0) {
		GS_THROW_USER_ERROR(
				GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Invalid fix column option size (size=" << optionSize << ")");
	}
}

MessageCollectionSchema::MessageCollectionSchema(util::StackAllocator &alloc,
	const DataStoreConfig &dsConfig,
	const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion)
	: MessageSchema(alloc, dsConfig, containerName, in, featureVersion) {
	containerType_ = COLLECTION_CONTAINER;
	validateContainerOption(in);
	int32_t attribute = CONTAINER_ATTR_SINGLE;
	if (featureVersion > 0) {
		if (in.base().remaining() != 0) {
			in >> attribute;
		}
		if (in.base().remaining() != 0) {
			in >> tablePartitioningVersionId_;
		}
		if (in.base().remaining() != 0) {
			int32_t tmpValue;
			in >> tmpValue;
			OptionType optionType = static_cast<OptionType>(tmpValue);
			while (optionType != OPTION_END) {
				switch (optionType) {
				case PARTITION_EXPIRATION:
					validateContainerExpiration(in);
					break;
				case RENAME_COLUMN:
					isRenameColumn_ = true;
					validateRenameColumnSchema(in);
					break;
				case FIX_COLUMN:
					validateFixColumnSchema(in);
					break;
				default:
					GS_THROW_USER_ERROR(
							GS_ERROR_DS_DS_SCHEMA_INVALID,
							"Unsupported option type : " <<
							static_cast<int32_t>(optionType));
				}
				in >> tmpValue;
				optionType = static_cast<OptionType>(tmpValue);
			}
		}
	} else {
		if (in.base().remaining() != 0) {
			in >> attribute;
		}
		if (in.base().remaining() != 0) {
			in >> tablePartitioningVersionId_;
		}
	}
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

MessageTimeSeriesSchema::MessageTimeSeriesSchema(util::StackAllocator &alloc,
	const DataStoreConfig &dsConfig,
	const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion)
	:
	  MessageSchema(alloc, dsConfig, containerName, in, featureVersion) {

	containerType_ = TIME_SERIES_CONTAINER;
	validateContainerOption(in);
	validateRowKeySchema();
	validateOption(in);
	int32_t attribute = CONTAINER_ATTR_SINGLE;
	if (featureVersion > 0) {
		if (in.base().remaining() != 0) {
			in >> attribute;
		}
		if (in.base().remaining() != 0) {
			in >> tablePartitioningVersionId_;
		}
		if (in.base().remaining() != 0) {
			int32_t tmpValue;
			in >> tmpValue;
			OptionType optionType = static_cast<OptionType>(tmpValue);
			while (optionType != OPTION_END) {
				switch (optionType) {
				case PARTITION_EXPIRATION:
					validateContainerExpiration(in);
					break;
				case RENAME_COLUMN:
					isRenameColumn_ = true;
					validateRenameColumnSchema(in);
					break;
				case FIX_COLUMN:
					validateFixColumnSchema(in);
					break;
				default:
					GS_THROW_USER_ERROR(
							GS_ERROR_DS_DS_SCHEMA_INVALID,
							"Unsupported option type : " <<
							static_cast<int32_t>(optionType));
				}
				in >> tmpValue;
				optionType = static_cast<OptionType>(tmpValue);
			}
		}
	} else {
		if (in.base().remaining() != 0) {
			in >> attribute;
		}
		if (in.base().remaining() != 0) {
			in >> tablePartitioningVersionId_;
		}
	}
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

void MessageTimeSeriesSchema::validateRowKeySchema() {
	const util::Vector<ColumnId> &keyColumnIds = getRowKeyColumnIdList();
	if (keyColumnIds.size() != 1) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, "must define one rowkey");
	}
	ColumnId columnId = keyColumnIds.front();
	if (columnId >= getColumnCount()) {  
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, "must define rowkey");
	}
	if (getColumnFullType(columnId) !=
		COLUMN_TYPE_TIMESTAMP) {  
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, "Type of rowkey not supported");
	}
	if (getIsArray(columnId)) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, "Type of rowkey not supported");
	}
}



void MessageTimeSeriesSchema::validateOption(util::ArrayByteInStream &in) {
	int8_t existTimeSeriesOptionTmp;
	in >> existTimeSeriesOptionTmp;
	isExistTimeSeriesOption_ = (existTimeSeriesOptionTmp != 0);
	if (!isExistTimeSeriesOption_) {
	}
	else {
		int32_t tmpElapsedTime;
		in >> tmpElapsedTime;
		int8_t timeUnitTmp;
		in >> timeUnitTmp;
		int32_t expirationDivisionCount;
		in >> expirationDivisionCount;

		if (tmpElapsedTime != -1) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, "not support Row Expiration");
		}

		int32_t reserve1;
		in >> reserve1;
		int8_t reserve2;
		in >> reserve2;
		int8_t reserve3;
		in >> reserve3;
		uint32_t reserve4;
		in >> reserve4;
		
		if (reserve3 != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_CM_NOT_SUPPORTED, "not support Timeseries Compression");
		}
	}
}


