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
#ifndef SQL_PROCESSOR_CONFIG_H_
#define SQL_PROCESSOR_CONFIG_H_

#include "sql_type.h"

#include "sql_job_common.h"

class TaskContext;
typedef int32_t TaskId;

struct SQLProcessorConfig {
public:
	class Manager;
	struct PartialOption;
	struct PartialStatus;

	static const int32_t DEFAULT_WORK_MEMORY_MB;

	SQLProcessorConfig();

	const SQLProcessorConfig& getDefault();

	bool merge(const SQLProcessorConfig *base, bool withDefaults);

	int64_t workMemoryLimitBytes_;
	int64_t workMemoryCacheBytes_;
	int64_t hashBucketSmallSize_;
	int64_t hashBucketLargeSize_;
	int64_t hashSeed_;
	int64_t hashMapLimitRate_;
	int64_t hashMapLimit_;
	int64_t hashJoinSingleLimitBytes_;
	int64_t hashJoinBucketLimitBytes_;
	int64_t hashJoinBucketMaxCount_;
	int64_t hashJoinBucketMinCount_;
	int64_t aggregationListLimit_;
	int64_t aggregationSetLimit_;
	int64_t scanBatchThreshold_;
	int64_t scanBatchSetSizeThreshold_;
	int64_t scanBatchUnit_;
	int64_t scanCheckerLimitBytes_;
	int64_t scanUpdateListLimitBytes_;
	int64_t scanUpdateListThreshold_;
	int64_t sortPartialUnitMaxCount_;
	int64_t sortMergeUnitMaxCount_;
	int64_t sortTopNThreshold_;
	int64_t sortTopNBatchThreshold_;
	int64_t sortTopNBatchSize_;
	int64_t interruptionProjectionCount_;
	int64_t interruptionScanCount_;

	UTIL_OBJECT_CODER_MEMBERS(
			UTIL_OBJECT_CODER_OPTIONAL(workMemoryLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(workMemoryCacheBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashBucketSmallSize_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashBucketLargeSize_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashSeed_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashMapLimitRate_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashMapLimit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinSingleLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinBucketLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinBucketMaxCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(hashJoinBucketMinCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(aggregationListLimit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(aggregationSetLimit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanBatchThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanBatchSetSizeThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanBatchUnit_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanCheckerLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanUpdateListLimitBytes_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(scanUpdateListThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortPartialUnitMaxCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortMergeUnitMaxCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortTopNThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortTopNBatchThreshold_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(sortTopNBatchSize_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(interruptionProjectionCount_, -1),
			UTIL_OBJECT_CODER_OPTIONAL(interruptionScanCount_, -1));

private:
	static const SQLProcessorConfig DEFAULT_CONFIG;

	struct DefaultTag {};
	SQLProcessorConfig(const DefaultTag&);

	bool merge(const SQLProcessorConfig &base);
	bool mergeValue(const SQLProcessorConfig &base, const int64_t &baseValue);
};

struct SQLProcessorConfig::PartialOption {
	PartialOption();

	int32_t followingCount_;
	int64_t submissionCode_;
	bool activated_;

	int32_t trialCount_;
};

struct SQLProcessorConfig::PartialStatus {
	PartialStatus();

	int32_t followingCount_;
	int64_t submissionCode_;
	int64_t execCount_;
	int32_t followingProgress_;
	bool activated_;

	int32_t trialCount_;
	int32_t trialProgress_;
	int32_t opPhase_;

	UTIL_OBJECT_CODER_MEMBERS(
			followingCount_,
			submissionCode_,
			execCount_,
			followingProgress_,
			activated_,
			trialCount_,
			trialProgress_,
			opPhase_);
};

class SQLProcessorConfig::Manager {
public:
	struct SimulationEntry;
	struct PartialId;

	Manager();

	static bool isEnabled();
	static Manager& getInstance();

	void apply(const SQLProcessorConfig &config, bool overrideDefaults);
	void mergeTo(SQLProcessorConfig &config);

	bool isSimulating();
	void setSimulation(
			const util::Vector<SimulationEntry> &simulationList,
			SQLType::Id type, int64_t inputCount);
	bool getSimulation(
			util::Vector<SimulationEntry> &simulationList,
			SQLType::Id &type, int64_t &inputCount);
	void clearSimulation();

	bool isPartialMonitoring();
	bool getPartialStatus(PartialStatus &status);
	bool monitorPartial(const PartialOption &option);

	bool initPartial(const PartialId &id, int64_t submissionCode);
	bool checkPartial(const PartialId &id, PartialStatus &status);
	void setPartialOpPhase(const PartialId &id, int32_t opPhase);
	void closePartial(const PartialId &id);

private:
	struct PartialEntry;

	typedef std::vector<SimulationEntry> SimulationList;
	typedef std::vector<PartialEntry> PartialList;

	static Manager instance_;

	util::Atomic<bool> activated_;
	util::Atomic<bool> simulating_;
	util::Atomic<bool> partialMonitoring_;
	util::Mutex mutex_;

	SQLProcessorConfig config_;

	SimulationList simulationList_;
	SQLType::Id simulatingType_;
	int64_t simulatingInputCount_;

	PartialStatus partialStatus_;
	PartialList partialList_;
};

struct SQLProcessorConfig::Manager::SimulationEntry {
	SimulationEntry();

	void set(const picojson::value &value);
	void get(picojson::value &value) const;

	int32_t point_;
	int32_t action_;
	int64_t param_;
};

struct SQLProcessorConfig::Manager::PartialId {
	PartialId();
	PartialId(TaskContext &cxt, int32_t index);

	bool isEmpty() const;
	bool matchesId(const PartialId &id) const;

	JobId jobId_;
	TaskId taskId_;
	int32_t index_;
};

struct SQLProcessorConfig::Manager::PartialEntry {
	PartialEntry();

	PartialId id_;
	int64_t execCount_;
	bool execStarted_;
};

#endif
