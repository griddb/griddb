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
	@brief Definition of ArchiveHandler class
*/
#ifndef ARCHIVE_H_
#define ARCHIVE_H_

#include "data_store_common.h"
UTIL_TRACER_DECLARE(LONG_ARCHIVE);

enum ArchiveType {
	ARCHIVE_AVRO,
	ARCHIVE_CSV,
	ARCHIVE_UNKNOWN = 0xFFFFFFFF
};

class ArchiveHandler {
public:
	ArchiveHandler(ArchiveType type, void *output, void *threshold, void *blockSize);
	virtual ~ArchiveHandler() {};
	virtual int32_t getVersion() = 0;
	virtual void initialize() = 0;
	virtual void finalize() = 0;
	virtual void initializeSchema() = 0;
	virtual void finalizeSchema() = 0;
	virtual void setServerVersion(const char *version) = 0;
	virtual void setDbName(const char *dbName) = 0;
	virtual void setContainerName(const char *conainerName) = 0;
	virtual void setColumnSchema(ColumnId columnId, const char *columnName, ColumnType type, bool isNotNull) = 0;
	virtual void initializeRow() = 0;
	virtual void finalizeRow() = 0;
	virtual void initializeField(ColumnId columnId) = 0;
	virtual void setPrimitiveValue(ColumnType type, const void *data, uint32_t size) = 0;
	virtual void initializeArray() = 0;
	virtual void finalizeArray() = 0;
	virtual void appendArray(ColumnType columnType, const void *data, uint32_t size) = 0;
	virtual void finalizeField() = 0;
	ArchiveType getType() {
		return type_;
	}
protected:
	ArchiveHandler() {};
private:
	ArchiveHandler(const ArchiveHandler &);  
	ArchiveHandler &operator=(const ArchiveHandler &);  
private:
	ArchiveType type_;
};

#endif
