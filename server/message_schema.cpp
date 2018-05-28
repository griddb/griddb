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
#include <iostream>

MessageSchema::MessageSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig, const char *,
	util::ArrayByteInStream &in)
	: dsValueLimitConfig_(dsValueLimitConfig),
	  containerType_(UNDEF_CONTAINER),
	  affinityStr_(alloc),
	  keyColumnIds_(alloc),
	  columnTypeList_(alloc),
	  flagsList_(alloc),
	  columnNameList_(alloc),
	  columnNameMap_(alloc),
	  alloc_(alloc) {
	validateColumnSchema(in);
}

ColumnType MessageSchema::getColumnFullType(ColumnId columnId) const {
	if (getIsArray(columnId)) {
		return static_cast<ColumnType>(
			getColumnType(columnId) + COLUMN_TYPE_STRING_ARRAY);
	}
	else {
		return getColumnType(columnId);
	}
}

void MessageSchema::validateColumnSchema(util::ArrayByteInStream &in) {
	in >> columnNum_;
	if (columnNum_ == 0 ||
		columnNum_ >
			dsValueLimitConfig_.getLimitColumnNum()) {  
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Number of columns = " << columnNum_ << " is invalid");
	}


	columnNameList_.reserve(columnNum_);
	columnTypeList_.reserve(columnNum_);
	flagsList_.reserve(columnNum_);

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

		int8_t columTypeTmp;
		in >> columTypeTmp;
		columnTypeList_.push_back(static_cast<ColumnType>(columTypeTmp));

		uint8_t flagsTmp;
		in >> flagsTmp;
		flagsList_.push_back(flagsTmp);
		const bool isArray = getIsArray(i);
//		const bool isNotNull = getIsNotNull(i);

		if (!ValueProcessor::isValidArrayAndType(
				isArray, columnTypeList_[i])) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"unsupported Column type = " << (int32_t)columnTypeList_[i]
											 << ", array = "
											 << (int32_t)isArray);
		}
	}
	int16_t rowKeyNum;
	in >> rowKeyNum;
	for (int16_t i = 0; i < rowKeyNum; i++) {
		int16_t rowKeyColumnId;
		in >> rowKeyColumnId;
		if (rowKeyColumnId != 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"ColumnNumber of rowkey = " << rowKeyColumnId << " is invalid");
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
	if (rowKeyNum < 0 || rowKeyNum > 1) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Number of rowkey must be one or zero");
	}


}

void MessageSchema::validateContainerOption(util::ArrayByteInStream &in) {
	in >> affinityStr_;
	if (affinityStr_.length() > AFFINITY_STRING_MAX_LENGTH) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Affinity num exceeds maximum size : " << affinityStr_.length());
	}
}

MessageCollectionSchema::MessageCollectionSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const char *containerName, util::ArrayByteInStream &in)
	: MessageSchema(alloc, dsValueLimitConfig, containerName, in) {
	containerType_ = COLLECTION_CONTAINER;
	validateContainerOption(in);
	int32_t attribute = CONTAINER_ATTR_SINGLE;
	if (in.base().remaining() != 0) {
		in >> attribute;
	}
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

MessageTimeSeriesSchema::MessageTimeSeriesSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const char *containerName, util::ArrayByteInStream &in)
	:
	  MessageSchema(alloc, dsValueLimitConfig, containerName, in) {

	containerType_ = TIME_SERIES_CONTAINER;
	validateContainerOption(in);
	validateRowKeySchema();
	validateOption(in);
	int32_t attribute = CONTAINER_ATTR_SINGLE;
	if (in.base().remaining() != 0) {
		in >> attribute;
	}
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

void MessageTimeSeriesSchema::validateRowKeySchema() {
	const util::XArray<ColumnId> &keyColumnIds = getRowKeyColumnIdList();
	if (keyColumnIds.size() != 1) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, "must define one rowkey");
	}
	ColumnId columnId = keyColumnIds.front();
	if (columnId >= getColumnCount()) {  
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, "must define rowkey");
	}
	if (getColumnType(columnId) !=
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
		in >> expirationInfo_.elapsedTime_;
		int8_t timeUnitTmp;
		in >> timeUnitTmp;
		expirationInfo_.timeUnit_ =
			static_cast<TimeUnit>(timeUnitTmp);  
		int32_t expirationDivisionCount;
		in >> expirationDivisionCount;
		if (expirationDivisionCount != EXPIRE_DIVIDE_UNDEFINED_NUM) {
			expirationInfo_.dividedNum_ = static_cast<uint16_t>(
				expirationDivisionCount);  
		}
		else {
			expirationInfo_.dividedNum_ =
				EXPIRE_DIVIDE_DEFAULT_NUM;  
		}
		if (expirationInfo_.elapsedTime_ > 0) {
			switch (expirationInfo_.timeUnit_) {
			case TIME_UNIT_DAY:
				expirationInfo_.duration_ =
					static_cast<Timestamp>(expirationInfo_.elapsedTime_) * 24 *
					60 * 60 * 1000LL;
				break;
			case TIME_UNIT_HOUR:
				expirationInfo_.duration_ =
					static_cast<Timestamp>(expirationInfo_.elapsedTime_) * 60 *
					60 * 1000LL;
				break;
			case TIME_UNIT_MINUTE:
				expirationInfo_.duration_ =
					static_cast<Timestamp>(expirationInfo_.elapsedTime_) * 60 *
					1000LL;
				break;
			case TIME_UNIT_SECOND:
				expirationInfo_.duration_ =
					static_cast<Timestamp>(expirationInfo_.elapsedTime_) *
					1000LL;
				break;
			case TIME_UNIT_MILLISECOND:
				expirationInfo_.duration_ =
					static_cast<Timestamp>(expirationInfo_.elapsedTime_);
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Timeunit of RowExpiration not supported"
						<< (int32_t)expirationInfo_.timeUnit_);
				break;
			}
			if (expirationInfo_.dividedNum_ == 0 ||
				expirationInfo_.dividedNum_ > LIMIT_EXPIRATION_DIVIDE_NUM) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Division Count of RowExpiration exceeds maximum size : "
						<< expirationInfo_.dividedNum_);
			}
			if (expirationInfo_.duration_ / expirationInfo_.dividedNum_ < 1) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Duration(" << expirationInfo_.duration_
								<< " msec) must be greater than Division Count("
								<< expirationInfo_.dividedNum_
								<< ") of RowExpiration");
			}
		}
		else if (expirationInfo_.elapsedTime_ == 0) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"elapsedTime of RowExpiration is Invalid : "
					<< expirationInfo_.elapsedTime_);
		}
		else {  
			expirationInfo_.duration_ = INT64_MAX;  
		}

		int32_t reserve1;
		in >> reserve1;
		int8_t reserve2;
		in >> reserve2;
		int8_t reserve3;
		in >> reserve3;
		if (reserve3 != 0) {
			GS_THROW_USER_ERROR(
				GS_ERROR_DS_DS_SCHEMA_INVALID, "Compression is not supported");
		}
		uint32_t reserve4;
		in >> reserve4;
	}
}

