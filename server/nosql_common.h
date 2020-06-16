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
#ifndef NOSQL_COMMON_H_
#define NOSQL_COMMON_H_

#include "schema.h"
#include "transaction_statement_message.h"

struct DDLBaseInfo;
class SQLExecution;


struct NoSQLStoreOption {

	NoSQLStoreOption() {
		clear();
	}
	
	NoSQLStoreOption(SQLExecution *execution);

	void setAsyncOption(
			DDLBaseInfo *baseInfo,
			StatementMessage::CaseSensitivity caseSesitive);

	bool isSystemDb_;
	ContainerType containerType_;
	bool ifNotExistsOrIfExists_;
	ContainerAttribute containerAttr_;
	PutRowOption putRowOption_;
	bool isSync_;
	size_t subContainerId_;
	JobExecutionId execId_;
	uint8_t jobVersionId_;
	int64_t baseValue_;
	bool sendBaseValue_;
	const char *indexName_;
	SessionId currentSessionId_;
	StatementMessage::CaseSensitivity caseSensitivity_;
	int32_t featureVersion_;
	int32_t acceptableFeatureVersion_;
	const char *applicationName_;
	double storeMemoryAgingSwapRate_;
	util::TimeZone timezone_;

	void clear() {
		isSystemDb_ = false;
		containerType_ = COLLECTION_CONTAINER;
		ifNotExistsOrIfExists_ = false;
		containerAttr_ = CONTAINER_ATTR_SINGLE;
		putRowOption_ = PUT_INSERT_ONLY;
		isSync_ = true;
		subContainerId_ = static_cast<size_t>(-1);
		execId_ = 0;
		jobVersionId_ = 0;
		sendBaseValue_ = false;
		baseValue_ = 0;
		indexName_ = NULL;
		caseSensitivity_.clear();
		featureVersion_ = -1;
		acceptableFeatureVersion_ = -1;
		applicationName_ = NULL;
		storeMemoryAgingSwapRate_ = -1;
		timezone_ = util::TimeZone();
	}
};

#endif
