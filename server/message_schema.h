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
	@brief Definition of MessageSchema
*/
#ifndef MESSAGE_SCHEMA_H_
#define MESSAGE_SCHEMA_H_

#include "time_series.h"
#include "value_operator.h"

/*!
	@brief Case intensive compare method for string
*/
struct CompareStringI {
public:
	bool operator()(const util::String &right, const util::String &left) const {
		return (compareStringStringI(
					right.c_str(),
					static_cast<uint32_t>(right.length()),
					left.c_str(),
					static_cast<uint32_t>(left.length())) < 0);
	}
};

/*!
	@brief Container schema for message format
*/
class MessageSchema {
public:
	static const int32_t DEFAULT_VERSION;
	static const int32_t V4_1_VERSION;
	enum OptionType {

		PARTITION_EXPIRATION = 100,

		OPTION_END = 0xFFFFFFFF
	};
public:
	MessageSchema(util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &dsValueLimitConfig,
		const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion);
	MessageSchema(util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &dsValueLimitConfig,
		const BibInfo::Container &bibInfo);

	virtual ~MessageSchema() {}


	ContainerType getContainerType() const {
		return containerType_;
	}

	TablePartitioningVersionId getTablePartitioningVersionId() {
		return tablePartitioningVersionId_;
	}

	uint32_t getColumnCount() const {
		return columnNum_;
	}

	const util::Vector<ColumnId>& getRowKeyColumnIdList() const {
		return keyColumnIds_;
	}
	uint32_t getRowKeyNum() const {
		return static_cast<uint32_t>(keyColumnIds_.size());
	}

	ColumnType getColumnType(ColumnId columnId) const {
		return columnTypeList_[columnId];
	}

	ColumnType getColumnFullType(ColumnId columnId) const;

	bool getIsArray(ColumnId columnId) const {
		return (flagsList_[columnId] & COLUMN_FLAG_ARRAY) != 0;
	}
	bool getIsNotNull(ColumnId columnId) const {
		return (flagsList_[columnId] & COLUMN_FLAG_NOT_NULL) != 0;
	}
	static void setArray(uint8_t &flag) {
		flag |= COLUMN_FLAG_ARRAY;
	}

	static void setNotNull(uint8_t &flag) {
		flag |= COLUMN_FLAG_NOT_NULL;
	}
	bool getIsVirtual(ColumnId columnId) const {
		return (flagsList_[columnId] & COLUMN_FLAG_VIRTUAL) != 0;
	}

	const util::String &getColumnName(ColumnId columnId) const {
		return columnNameList_[columnId];
	}

	const util::String &getAffinityStr() const {
		return affinityStr_;
	}

	ContainerAttribute getContainerAttribute() const {
		return containerAttribute_;
	}

	static int64_t getTimestampDuration(int32_t elapsedTime, TimeUnit unit) {
		int64_t duration = 0;
		if (elapsedTime > 0) {
			switch (unit) {
			case TIME_UNIT_DAY:
				duration =
					static_cast<int64_t>(elapsedTime) * 24 * 60 * 60 * 1000LL;
				break;
			case TIME_UNIT_HOUR:
				duration =
					static_cast<int64_t>(elapsedTime) * 60 * 60 * 1000LL;
				break;
			case TIME_UNIT_MINUTE:
				duration=
					static_cast<int64_t>(elapsedTime) * 60 * 1000LL;
				break;
			case TIME_UNIT_SECOND:
				duration =
					static_cast<int64_t>(elapsedTime) * 1000LL;
				break;
			case TIME_UNIT_MILLISECOND:
				duration = static_cast<int64_t>(elapsedTime);
				break;
			default:
				GS_THROW_USER_ERROR(GS_ERROR_DS_DS_SCHEMA_INVALID,
					"Timeunit not supported, unit="	<< (int32_t)unit);
				break;
			}
		}
		return duration;
	}
	Timestamp getContainerExpirationStartTime() const {
		return containerExpirationStartTime_;
	}

	BaseContainer::ContainerExpirationInfo &getContainerExpirationInfo() {
		return containerExpirationInfo_;
	}


	void setAffinityStr(const char *affinity) {
		affinityStr_ = affinity;
	}
	void setFirstSchema(uint32_t columnNum, uint32_t varColumnNum, uint32_t rowFixedColumnSize) {
		oldColumnNum_ = columnNum;
		oldVarColumnNum_ = varColumnNum;
		oldRowFixedColumnSize_ = rowFixedColumnSize;
	}
	void getFirstSchema(uint32_t &columnNum, uint32_t &varColumnNum, uint32_t &rowFixedColumnSize) {
		columnNum = oldColumnNum_;
		varColumnNum = oldVarColumnNum_;
		rowFixedColumnSize = oldRowFixedColumnSize_;
	}

protected:
	void validateColumnSchema(util::ArrayByteInStream &in);
	void validateContainerOption(util::ArrayByteInStream &in);
	void validateColumnSchema(const BibInfo::Container &bibInfo);
	void validateContainerOption(const BibInfo::Container &bibInfo);
	void validateContainerExpiration(util::ArrayByteInStream &in);
	void validateContainerExpiration(const BibInfo::Container &bibInfo);
	util::StackAllocator &getAllocator() {
		return alloc_;
	}
	void setTablePartitionVersionId(
		TablePartitioningVersionId versionId) {
		tablePartitioningVersionId_ = versionId;
	}
	void setColumnCount(uint32_t count);

	void setRowKeyColumnId(ColumnId columnId);

	void setColumnType(ColumnId columnId, ColumnType type);

	void setColumnName(ColumnId columnId, const void *data, uint32_t size);

	void setContainerAttribute(ContainerAttribute attribute) {
		containerAttribute_ = attribute;
	}

protected:
	const DataStoreValueLimitConfig &dsValueLimitConfig_;
	ContainerType containerType_;
	util::String affinityStr_;  
	TablePartitioningVersionId tablePartitioningVersionId_;
	Timestamp containerExpirationStartTime_;  
	BaseContainer::ContainerExpirationInfo containerExpirationInfo_;  

	static const uint8_t COLUMN_FLAG_ARRAY = 0x01;
	static const uint8_t COLUMN_FLAG_VIRTUAL = 0x02;
	static const uint8_t COLUMN_FLAG_NOT_NULL = 0x04;

	uint32_t columnNum_;
	util::Vector<ColumnId> keyColumnIds_;
	util::XArray<ColumnType> columnTypeList_;
	util::XArray<uint8_t> flagsList_;
	util::Vector<util::String> columnNameList_;
	util::Map<util::String, ColumnId, CompareStringI> columnNameMap_;
	util::StackAllocator &alloc_;
	ContainerAttribute containerAttribute_;
	uint32_t oldColumnNum_;
	uint32_t oldVarColumnNum_;
	uint32_t oldRowFixedColumnSize_;
};

/*!
	@brief Collection schema for message format
*/
class MessageCollectionSchema : public MessageSchema {
public:
	MessageCollectionSchema(util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &dsValueLimitConfig,
		const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion);
	MessageCollectionSchema(util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &dsValueLimitConfig,
		const BibInfo::Container &bibInfo);

	~MessageCollectionSchema() {}
protected:
};

/*!
	@brief TimeSeries schema for message format
*/
class MessageTimeSeriesSchema : public MessageSchema {
public:
	MessageTimeSeriesSchema(util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &dsValueLimitConfig,
		const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion);
	MessageTimeSeriesSchema(util::StackAllocator &alloc,
		const DataStoreValueLimitConfig &dsValueLimitConfig,
		const BibInfo::Container &bibInfo);

	~MessageTimeSeriesSchema() {}

	BaseContainer::ExpirationInfo &getExpirationInfo() {
		return expirationInfo_;
	}

	uint32_t getCompressionInfoNum() const {
		return compressionInfoNum_;
	}

	DurationInfo &getDurationInfo() {
		return compressionWindowInfo_;
	}

	MessageCompressionInfo &getCompressionInfo(ColumnId columnId) {
		return compressionInfoList_[columnId];
	}

	util::Vector<ColumnId> &getCompressionColumnIdList() {
		return compressionColumnIdList_;
	}

	COMPRESSION_TYPE getCompressionType() const {
		return compressionType_;
	}
	bool isExistTimeSeriesOption() const {
		return isExistTimeSeriesOption_;
	}


	void setExpirationInfo(const BaseContainer::ExpirationInfo &expirationInfo) {
		expirationInfo_ = expirationInfo;
	}

	void setCompressionInfoNum(uint32_t num) {
		compressionInfoNum_ = num;
	}

	void setDurationInfo(DurationInfo &durationInfo) {
		compressionWindowInfo_ = durationInfo;
	}

	void addCompressionInfo(MessageCompressionInfo &compressionInfo);

	void setCompressionType(COMPRESSION_TYPE compressionType) {
		compressionType_ = compressionType;
	}
protected:
private:
	void validateRowKeySchema();
	void validateOption(util::ArrayByteInStream &in);
	void validateOption(const BibInfo::Container &bibInfo);

private:
	BaseContainer::ExpirationInfo expirationInfo_;  
	bool isExistTimeSeriesOption_;
	uint32_t compressionInfoNum_;
	DurationInfo compressionWindowInfo_;  
	util::XArray<MessageCompressionInfo> compressionInfoList_;
	util::Vector<ColumnId> compressionColumnIdList_;
	COMPRESSION_TYPE compressionType_;
};

#endif
