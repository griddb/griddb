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
	static const int32_t V5_8_VERSION;

	enum OptionType {
		PARTITION_EXPIRATION = 100,
		RENAME_COLUMN = 101,
		FIX_COLUMN = 102,


		OPTION_END = 0xFFFFFFFF
	};
public:
	MessageSchema(util::StackAllocator &alloc,
		const DataStoreConfig &dsConfig,
		const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion);

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

	ColumnType getColumnFullType(ColumnId columnId) const {
		return columnTypeList_[columnId];
	}

	static uint8_t makeColumnFlags(
			bool forArray, bool forVirtual, bool notNull) {
		uint8_t flags = 0;
		if (forArray) {
			setArray(flags);
		}
		if (forVirtual) {
			setVirtual(flags);
		}
		if (notNull) {
			setNotNull(flags);
		}
		return flags;
	}

	bool getIsArray(ColumnId columnId) const {
		return getIsArrayByFlags(flagsList_[columnId]);
	}

	static bool getIsArrayByFlags(uint8_t flags) {
		return (flags & COLUMN_FLAG_ARRAY) != 0;
	}

	static void setArray(uint8_t &flags) {
		flags |= COLUMN_FLAG_ARRAY;
	}

	bool getIsVirtual(ColumnId columnId) const {
		return (flagsList_[columnId] & COLUMN_FLAG_VIRTUAL) != 0;
	}
	static void setVirtual(uint8_t &flags) {
		flags |= COLUMN_FLAG_VIRTUAL;
	}

	bool getIsNotNull(ColumnId columnId) const {
		return (flagsList_[columnId] & COLUMN_FLAG_NOT_NULL) != 0;
	}

	static void setNotNull(uint8_t &flags) {
		flags |= COLUMN_FLAG_NOT_NULL;
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

	bool isRenameColumn() {
		return isRenameColumn_;
	}


	void setDataAffinity(const char *affinity) {
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
	void validateColumnSize();
	void validateColumn(util::ArrayByteInStream& in);
	void validateKeyColumn(util::ArrayByteInStream& in);
	void validateContainerOption(util::ArrayByteInStream &in);
	void validateContainerExpiration(util::ArrayByteInStream &in);
	void validateRenameColumnSchema(util::ArrayByteInStream &in); 
	void validateFixColumnSchema(util::ArrayByteInStream &in);
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
	const DataStoreConfig &dsConfig_;
	ContainerType containerType_;
	util::String affinityStr_;  
	TablePartitioningVersionId tablePartitioningVersionId_;
	Timestamp containerExpirationStartTime_;  
	BaseContainer::ContainerExpirationInfo containerExpirationInfo_;  

	bool isRenameColumn_; 

	static const uint8_t COLUMN_FLAG_ARRAY = 0x01;
	static const uint8_t COLUMN_FLAG_VIRTUAL = 0x02;
	static const uint8_t COLUMN_FLAG_NOT_NULL = 0x04;

	const uint32_t MAX_FIXED_COLUMN_TOTAL_SIZE = 59 * 1024;

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
		const DataStoreConfig &dsConfig,
		const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion);
	~MessageCollectionSchema() {}
protected:
};

/*!
	@brief TimeSeries schema for message format
*/
class MessageTimeSeriesSchema : public MessageSchema {
public:
	MessageTimeSeriesSchema(util::StackAllocator &alloc,
		const DataStoreConfig &dsConfig,
		const char *containerName, util::ArrayByteInStream &in, int32_t featureVersion);

	~MessageTimeSeriesSchema() {}


	bool isExistTimeSeriesOption() const {
		return isExistTimeSeriesOption_;
	}
protected:
private:
	void validateRowKeySchema();
	void validateOption(util::ArrayByteInStream &in);

private:
	bool isExistTimeSeriesOption_;
};

#endif
