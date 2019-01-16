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

#include "picojson.h"

const int32_t MessageSchema::DEFAULT_VERSION = 0;
const int32_t MessageSchema::V4_1_VERSION = 1;

MessageSchema::MessageSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig, const char *,
	util::ArrayByteInStream &in, int32_t featureVersion)
	: dsValueLimitConfig_(dsValueLimitConfig),
	  containerType_(UNDEF_CONTAINER),
	  affinityStr_(alloc),
	  containerExpirationStartTime_(-1),
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
	validateColumnSchema(in);
}

MessageSchema::MessageSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const BibInfo::Container &bibInfo)
	: dsValueLimitConfig_(dsValueLimitConfig),
	  containerType_(UNDEF_CONTAINER),
	  affinityStr_(alloc),
	  containerExpirationStartTime_(-1),
	  keyColumnIds_(alloc),
	  columnTypeList_(alloc),
	  flagsList_(alloc),
	  columnNameList_(alloc),
	  columnNameMap_(alloc),
	  alloc_(alloc) {
	validateColumnSchema(bibInfo);

	uint32_t columnNum, varColumnNum, rowFixedColumnSize;
	ColumnSchema::convertFromInitSchemaStatus(bibInfo.initSchemaStatus_, columnNum, varColumnNum, rowFixedColumnSize);
	setFirstSchema(columnNum, varColumnNum, rowFixedColumnSize);
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

	uint32_t fixedColumnTotalSize = 0;
	const uint32_t MAX_FIXED_COLUMN_TOTAL_SIXE = 59 * 1024;
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

		int8_t columnTypeTmp;
		in >> columnTypeTmp;
		columnTypeList_.push_back(static_cast<ColumnType>(columnTypeTmp));

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

		if (!isArray && ValueProcessor::isSimple(columnTypeTmp)) {
			fixedColumnTotalSize += FixedSizeOfColumnType[columnTypeTmp];
			if (fixedColumnTotalSize > MAX_FIXED_COLUMN_TOTAL_SIXE) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Total size of Fixed Columns is over, limit =  " << MAX_FIXED_COLUMN_TOTAL_SIXE);
			}
		} else if (!hasVariableColumn) {
			hasVariableColumn = true;
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

void MessageSchema::validateColumnSchema(const BibInfo::Container &bibInfo) {
	columnNum_ = static_cast<uint32_t>(bibInfo.columnSet_.size());
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
		const BibInfo::Container::Column &columnVal = bibInfo.columnSet_[i];
		util::String columnName(alloc_);
		columnName = columnVal.columnName_.c_str();

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

		ColumnType columnTypeTmp = BibInfoUtil::getColumnType(columnVal.type_.c_str());
		columnTypeList_.push_back(ValueProcessor::getSimpleColumnType(columnTypeTmp));

		uint8_t flagsTmp = 0;
		const bool isArray = ValueProcessor::isArray(columnTypeTmp);
		if (isArray) {
			setArray(flagsTmp);
		}
		const bool isNotNull = columnVal.notNull_;
		if (isNotNull) {
			setNotNull(flagsTmp);
		}
		flagsList_.push_back(flagsTmp);
		assert(isArray == getIsArray(i));
		assert(isNotNull == getIsNotNull(i));
		if (!ValueProcessor::isValidArrayAndType(
				isArray, columnTypeList_[i])) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"unsupported Column type = " << (int32_t)columnTypeList_[i]
											 << ", array = "
											 << (int32_t)isArray);
		}
	}

	bool rowKeyAssigned = bibInfo.rowKeyAssigned_;
	if (rowKeyAssigned) {
		ColumnId rowKeyColumnId = ColumnInfo::ROW_KEY_COLUMN_ID;
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
}

void MessageSchema::validateContainerOption(util::ArrayByteInStream &in) {
	in >> affinityStr_;
	EmptyAllowedKey::validate(
		KeyConstraint::getUserKeyConstraint(AFFINITY_STRING_MAX_LENGTH),
		affinityStr_.c_str(), static_cast<uint32_t>(affinityStr_.length()),
		"affinity string");
}

void MessageSchema::validateContainerOption(const BibInfo::Container &bibInfo) {
	affinityStr_ = bibInfo.dataAffinity_.c_str();
	if (affinityStr_.length() > AFFINITY_STRING_MAX_LENGTH) {
		GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
			"Affinity num exceeds maximum size : " << affinityStr_.length());
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

void MessageSchema::validateContainerExpiration(const BibInfo::Container &bibInfo) {
}

MessageCollectionSchema::MessageCollectionSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion)
	: MessageSchema(alloc, dsValueLimitConfig, containerName, in, featureVersion) {
	containerType_ = COLLECTION_CONTAINER;
	validateContainerOption(in);
	int32_t attribute = CONTAINER_ATTR_SINGLE;

	if (featureVersion > 0) {
		if (in.base().remaining() != 0) {
			in >> attribute;
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
				default:
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_DS_SCHEMA_INVALID, "Unsupported option type : " << optionType);
				}
				in >> tmpValue;
				optionType = static_cast<OptionType>(tmpValue);
			}
		}
	} else {
		if (in.base().remaining() != 0) {
			in >> attribute;
		}
	}
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

MessageTimeSeriesSchema::MessageTimeSeriesSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion)
	:
	  MessageSchema(alloc, dsValueLimitConfig, containerName, in, featureVersion),
	  compressionInfoList_(alloc),
	  compressionColumnIdList_(alloc) {

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
			int32_t tmpValue;
			in >> tmpValue;
			OptionType optionType = static_cast<OptionType>(tmpValue);
			while (optionType != OPTION_END) {
				switch (optionType) {
				case PARTITION_EXPIRATION:
					validateContainerExpiration(in);
					break;
				default:
					GS_THROW_USER_ERROR(
						GS_ERROR_DS_DS_SCHEMA_INVALID, "Unsupported option type : " << optionType);
				}
				in >> tmpValue;
				optionType = static_cast<OptionType>(tmpValue);
			}
		}
	} else {
		if (in.base().remaining() != 0) {
			in >> attribute;
		}
	}
	if (getContainerExpirationInfo().info_.duration_ != INT64_MAX && 
		getExpirationInfo().duration_ != INT64_MAX) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, 
			"row and partiion expiration can not be defined at the same time");
	}
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

MessageCollectionSchema::MessageCollectionSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const BibInfo::Container &bibInfo)
	: MessageSchema(alloc, dsValueLimitConfig, bibInfo) {
	containerType_ = COLLECTION_CONTAINER;
	validateContainerOption(bibInfo);
	int32_t attribute = CONTAINER_ATTR_SINGLE;
	validateContainerExpiration(bibInfo);
	setContainerAttribute(static_cast<ContainerAttribute>(attribute));
}

MessageTimeSeriesSchema::MessageTimeSeriesSchema(util::StackAllocator &alloc,
	const DataStoreValueLimitConfig &dsValueLimitConfig,
	const BibInfo::Container &bibInfo)
	:
	  MessageSchema(alloc, dsValueLimitConfig, bibInfo),
	  compressionInfoList_(alloc),
	  compressionColumnIdList_(alloc) {

	containerType_ = TIME_SERIES_CONTAINER;
	validateContainerOption(bibInfo);
	validateRowKeySchema();
	validateOption(bibInfo);
	int32_t attribute = CONTAINER_ATTR_SINGLE;

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
		compressionType_ = NO_COMPRESSION;
		compressionInfoNum_ = 0;
		compressionInfoList_.resize(getColumnCount());
		for (uint32_t i = 0; i < getColumnCount(); i++) {
			compressionInfoList_[i].initialize();
		}
	}
	else {
		in >> expirationInfo_.elapsedTime_;
		int8_t timeUnitTmp;
		in >> timeUnitTmp;
		expirationInfo_.timeUnit_ =
			static_cast<TimeUnit>(timeUnitTmp);  
		int32_t expirationDivisionCount;
		in >> expirationDivisionCount;
		if (expirationDivisionCount != BaseContainer::EXPIRE_DIVIDE_UNDEFINED_NUM) {
			expirationInfo_.dividedNum_ = static_cast<uint16_t>(
				expirationDivisionCount);  
		}
		else {
			expirationInfo_.dividedNum_ =
				BaseContainer::EXPIRE_DIVIDE_DEFAULT_NUM;  
		}
		if (expirationInfo_.elapsedTime_ > 0) {
			expirationInfo_.duration_ =
				getTimestampDuration(expirationInfo_.elapsedTime_, 
				expirationInfo_.timeUnit_);

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

		in >> compressionWindowInfo_.timeDuration_;
		int8_t tmpTimeUnit;
		in >> tmpTimeUnit;
		compressionWindowInfo_.timeUnit_ =
			static_cast<TimeUnit>(tmpTimeUnit);  
		if (compressionWindowInfo_.timeDuration_ > 0) {
			compressionWindowInfo_.timestampDuration_ =
				getTimestampDuration(compressionWindowInfo_.timeDuration_, 
				compressionWindowInfo_.timeUnit_);
		}
		else {  
			compressionWindowInfo_.timestampDuration_ = INT64_MAX;  
		}

		int8_t tmpCompressionType;
		in >> tmpCompressionType;
		compressionType_ = static_cast<COMPRESSION_TYPE>(
			tmpCompressionType);  

		in >> compressionInfoNum_;
		if (compressionInfoNum_ != 0 && compressionType_ != HI_COMPRESSION) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Column Compression Info only support with HI Compression");
		}
		if (compressionInfoNum_ > LIMIT_HICOMPRESSION_COLUMN_NUM) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Number of HiCompression column exceeds maximum size : "
					<< compressionInfoNum_);
		}

		compressionInfoList_.resize(getColumnCount());
		for (uint32_t i = 0; i < getColumnCount(); i++) {
			compressionInfoList_[i].initialize();
		}
		if (compressionInfoNum_ != 0) {
			for (uint32_t i = 0; i < compressionInfoNum_; i++) {
				MessageCompressionInfo::CompressionType type =
					MessageCompressionInfo::DSDC;

				uint32_t columnId;
				in >> columnId;
				compressionColumnIdList_.push_back(columnId);

				int8_t threshholdRelativeTmp;
				in >> threshholdRelativeTmp;
				bool threshholdRelative = (threshholdRelativeTmp != 0);

				double threshhold, rate, span;
				if (threshholdRelative) {
					in >> rate;
					if (rate < 0 || rate > 1.0) {
						GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
							"HI Compression Rate of Column[" << columnId
															 << "] is invalid");
					}
					in >> span;
					threshhold = span * rate;
				}
				else {
					in >> threshhold;
					rate = 0;
					span = 0;
				}

				if (getIsArray(columnId) == true ||
					!ValueProcessor::isNumerical(getColumnType(columnId))) {
					GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
						"Compression of Column[" << columnId
												 << "] not supported");
				}

				compressionInfoList_[columnId].set(
					type, threshholdRelative, threshhold, rate, span);
			}
		}
	}
}

void MessageTimeSeriesSchema::validateOption(const BibInfo::Container &bibInfo) {
	const BibInfo::Container::TimeSeriesProperties &properties = bibInfo.timeSeriesProperties_;
	if (!properties.isExist_) {
		isExistTimeSeriesOption_ = false;
		compressionType_ = NO_COMPRESSION;
	} else {
		isExistTimeSeriesOption_ = true;
		expirationInfo_.elapsedTime_ = static_cast<int32_t>(properties.rowExpirationElapsedTime_);
		expirationInfo_.timeUnit_ = BibInfoUtil::getTimeUnit(properties.rowExpirationTimeUnit_.c_str());
		int32_t expirationDivisionCount = static_cast<int32_t>(properties.expirationDivisionCount_);
		if (expirationDivisionCount != BaseContainer::EXPIRE_DIVIDE_UNDEFINED_NUM) {
			expirationInfo_.dividedNum_ = static_cast<uint16_t>(
				expirationDivisionCount);  
		}
		else {
			expirationInfo_.dividedNum_ =
				BaseContainer::EXPIRE_DIVIDE_DEFAULT_NUM;  
		}
		if (expirationInfo_.elapsedTime_ > 0) {
			expirationInfo_.duration_ =
				getTimestampDuration(expirationInfo_.elapsedTime_, 
				expirationInfo_.timeUnit_);

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
	
		const long &compressionWindowVal = properties.compressionWindowSize_;
		if (compressionWindowVal > 0) {
			compressionWindowInfo_.timeDuration_ = static_cast<int32_t>(compressionWindowVal);
			compressionWindowInfo_.timeUnit_ = BibInfoUtil::getTimeUnit(properties.compressionWindowSizeUnit_.c_str());
		}
		if (compressionWindowInfo_.timeDuration_ > 0) {
			compressionWindowInfo_.timestampDuration_ =
				getTimestampDuration(compressionWindowInfo_.timeDuration_, 
				compressionWindowInfo_.timeUnit_);
		}
		else {  
			compressionWindowInfo_.timestampDuration_ = INT64_MAX;  
		}
		compressionType_ = BibInfoUtil::getCompressionType(properties.compressionMethod_.c_str());
	}

	if (bibInfo.compressionInfoSet_.empty()) {
		compressionInfoNum_ = 0;
		compressionInfoList_.resize(getColumnCount());
		for (uint32_t i = 0; i < getColumnCount(); i++) {
			compressionInfoList_[i].initialize();
		}
	} else {
		compressionInfoNum_ = bibInfo.compressionInfoSet_.size();
		if (compressionInfoNum_ != 0 && compressionType_ != HI_COMPRESSION) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Column Compression Info only support with HI Compression");
		}
		if (compressionInfoNum_ > LIMIT_HICOMPRESSION_COLUMN_NUM) {
			GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
				"Number of HiCompression column exceeds maximum size : "
					<< compressionInfoNum_);
		}

		compressionInfoList_.resize(getColumnCount());
		for (uint32_t i = 0; i < getColumnCount(); i++) {
			compressionInfoList_[i].initialize();
		}
		for (uint32_t i = 0; i < compressionInfoNum_; i++) {
			MessageCompressionInfo::CompressionType type =
				MessageCompressionInfo::DSDC;
			const BibInfo::Container::CompressionInfo &compressionInfo = bibInfo.compressionInfoSet_[i];

			util::String columnName(alloc_);
			columnName = compressionInfo.columnName_.c_str();
			util::Map<util::String, ColumnId, CompareStringI>::iterator itr;
			itr = columnNameMap_.find(columnName);
			if (itr == columnNameMap_.end()) {  
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Column name '" << columnName.c_str() << "' not exist");
			}

			uint32_t columnId = itr->second;
			compressionColumnIdList_.push_back(columnId);

			const std::string compressionTypeStr = compressionInfo.compressionType_;
			if (compressionTypeStr.compare("RELATIVE") != 0 && compressionTypeStr.compare("ABSOLUTE") != 0) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Unknown compressionType : " << compressionTypeStr);
			}
			bool threshholdRelative = (compressionTypeStr.compare("RELATIVE") == 0);

			double threshhold, rate, span;
			if (threshholdRelative) {
				rate = compressionInfo.rate_;
				if (rate < 0 || rate > 1.0) {
					GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
						"HI Compression Rate of Column[" << columnId
														 << "] is invalid");
				}
				span = compressionInfo.span_;
				threshhold = span * rate;
			}
			else {
				threshhold = compressionInfo.width_;
				rate = 0;
				span = 0;
			}

			if (getIsArray(columnId) == true ||
				!ValueProcessor::isNumerical(getColumnType(columnId))) {
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Compression of Column[" << columnId
											 << "] not supported");
			}

			compressionInfoList_[columnId].set(
				type, threshholdRelative, threshhold, rate, span);
		}
	}
	if (getContainerExpirationInfo().info_.duration_ != INT64_MAX && 
		getExpirationInfo().duration_ != INT64_MAX) {
		GS_THROW_USER_ERROR(
			GS_ERROR_DS_DS_SCHEMA_INVALID, 
			"row and partiion expiration can not be defined at the same time");
	}
}

