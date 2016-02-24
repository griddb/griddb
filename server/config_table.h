/*
    Copyright (c) 2012 TOSHIBA CORPORATION.
    
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
	@brief Definition of ConfigTable
*/
#ifndef CONFIG_TABLE_H_
#define CONFIG_TABLE_H_

#include "data_type.h"
#include "gs_error.h"
#include "util/type.h"
#include "util/container.h"
#include <cassert>

namespace picojson {
class value;
}

/*!
	@brief Parses JSON data
*/
struct JsonUtils {
	static u8string parseAll(
			picojson::value &value, const char8_t *begin, const char8_t *end);
};

/*!
	@brief Represents parameter value
*/
class ParamValue {
public:
	typedef util::StdAllocator<void, void> Allocator;

	/*!
		@brief Parameter data type
	*/	
	enum Type {
		PARAM_TYPE_NULL,
		PARAM_TYPE_STRING,
		PARAM_TYPE_INT64,
		PARAM_TYPE_INT32,
		PARAM_TYPE_DOUBLE,
		PARAM_TYPE_BOOL,
		PARAM_TYPE_JSON
	};

	struct Null {};

	ParamValue(const Null& = Null());
	~ParamValue();

	ParamValue(const char8_t *base); 
	ParamValue(const int64_t &base);
	ParamValue(const int32_t &base);
	ParamValue(const double &base);
	ParamValue(const bool &base);
	ParamValue(const picojson::value &base); 
	template<typename E, typename T, typename A>
	ParamValue(const std::basic_string<E, T, A> &base); 
	template<typename T> ParamValue(const volatile T*); 

	ParamValue(const ParamValue &another, const Allocator &alloc); 
	void assign(const ParamValue &another, const Allocator &alloc); 

	bool operator==(const ParamValue &another) const;
	bool operator!=(const ParamValue &another) const;

	template<typename T> bool is() const;
	template<typename T> const T& get() const;
	Type getType() const;
	void toJSON(picojson::value &dest) const;

	void format(std::ostream &stream) const;

private:
	ParamValue(const ParamValue &another);
	ParamValue& operator=(const ParamValue &another);

	template<typename T> static Type resolveType();
	void clear();
	void assign(const ParamValue &another, const Allocator *alloc);

	template<typename T> static size_t elementCount(const T*);

	template<typename T> struct VariableType {
	public:
		typedef typename Allocator::template rebind<T>::other ValueAllocator;

		void construct(const T *value, const Allocator *alloc);
		void destruct();

		const T *const& get() const;

	private:
		union {
			const T *ref_;
			T *stored_;
		} value_;

		uint8_t allocator_[sizeof(ValueAllocator)];
		bool owner_;
	};

	union {
		VariableType<char8_t> asString_;
		int64_t asInt64_;
		int32_t asInt32_;
		double asDouble_;
		bool asBool_;
		VariableType<picojson::value> asJSON_;
	} storage_;

	Type type_;
};

std::ostream& operator<<(std::ostream &stream, const ParamValue &value);

/*!
	@brief Represents the table of parameters
*/
class ParamTable {
public:
	typedef util::StdAllocator<void, void> Allocator;

	typedef int32_t ParamId;
	static const ParamId PARAM_ID_ROOT;

	template<typename T> class BasicSetUpHandler;
	class ValueAnnotation;
	class ParamHandler;
	class PathFormatter;

	typedef std::pair<
			const ParamValue*, const ValueAnnotation*> AnnotatedValue;

	typedef int32_t ExportMode;
	static const ExportMode EXPORT_MODE_DEFAULT;
	static const ExportMode EXPORT_MODE_DIFF_ONLY;
	static const ExportMode EXPORT_MODE_SHOW_HIDDEN;

	static const int32_t EXPORT_MODE_BASE_BITS;

	ParamTable(const Allocator &alloc, util::Tracer *tracer);
	virtual ~ParamTable();

	void addParam(ParamId groupId, ParamId id, const char8_t *name,
			ValueAnnotation *annotation, bool group, bool failOnFound);
	void addDependency(ParamId id, ParamId baseId);

	void setParamHandler(ParamId id, ParamHandler &handler);
	void setDefaultParamHandler(ParamHandler &handler);

	bool findParam(ParamId groupId, ParamId &id, const char8_t *name) const;
	bool getNextParam(ParamId id, ParamId &nextId, bool skipEmpty) const;
	bool getParentParam(ParamId id, ParamId &parentId) const;

	AnnotatedValue getAnnotatedValue(
			ParamId id, bool valueExpected = true,
			bool annotationExpected = true) const;

	bool isSet(ParamId id) const;
	template<typename T> const T& get(ParamId id) const;

	const char8_t* getName(ParamId id) const;
	const char8_t* getSourceInfo(ParamId id) const;
	bool getBaseParam(ParamId id, ParamId &baseId) const;
	bool getReferredParam(ParamId id, ParamId &refId, size_t ordinal) const;

	void set(ParamId id, const ParamValue &value,
			const char8_t *sourceInfo, ParamHandler *customHandler = NULL);

	void acceptFile(const char8_t *dirPath,
			const char8_t *const *fileList, size_t count, ParamId id);
	void acceptJSON(const picojson::value *srcList, size_t count, ParamId id,
			const char8_t *const *sourceInfoList);
	void toFile(const char8_t *filePath, ParamId id, ExportMode mode);
	void toJSON(picojson::value &dest, ParamId id, ExportMode mode);

	void addProblem(const util::Exception::NamedErrorCode &code,
			const char8_t *message);
	void setTraceEnabled(bool enabled);

	PathFormatter pathFormatter(ParamId id) const;

	static ParamHandler& silentHandler();

	static std::string getParamSymbol(
			const char8_t *src, bool reverse, size_t startDepth);

protected:
	typedef util::BasicString< char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, void> > String;

	Allocator& getAllocator() { return alloc_; }
	util::ObjectPool<ParamValue>& getValuePool() { return valuePool_; }

	void initializeSchema(const ParamTable &table);

private:
	class SilentParamHandler;

	typedef std::map< String, ParamId, std::less<String>, util::StdAllocator<
			std::pair<const String, ParamId>, void> > NameMap;
	typedef std::vector< ParamId, util::StdAllocator<ParamId, void> > IdList;

	struct Entry {
		explicit Entry(const Allocator &alloc = Allocator());

		ParamId parentId_;
		ParamId baseId_;
		String name_;
		NameMap subNameMap_;
		ParamValue *value_;
		ValueAnnotation *annotation_;
		ParamHandler *handler_;
		String sourceInfo_;
		IdList referredList_;
		bool group_;
		bool modified_;
	};

	typedef std::map< ParamId, Entry, std::less<ParamId>, util::StdAllocator<
			std::pair<const ParamId, Entry>, void> > EntryMap;

	typedef std::pair<util::Exception::NamedErrorCode, String> Problem;
	typedef std::vector< Problem, util::StdAllocator<Problem, void> > ProblemList;

	typedef std::pair<ParamId, ParamId> Dependency;
	typedef std::vector<
			Dependency, util::StdAllocator<Problem, void> > DependencyList;

	struct EntryMapCleaner {
	public:
		EntryMapCleaner(
				EntryMap &base, util::ObjectPool<ParamValue> &valuePool);
		~EntryMapCleaner();
		void clean();

	private:
		EntryMap &base_;
		util::ObjectPool<ParamValue> &valuePool_;
	};

	struct PathElem {
		ParamId id_;
		const Entry *entry_;
		NameMap::const_iterator it_;
		const picojson::value *value_;
	};

	void mergeFromJSON(EntryMap &valueMap, const picojson::value &src,
			ParamId id, const char8_t *sourceInfo);
	void accept(const EntryMap &valueMap, const char8_t *defaultSource);

	static void traceProblem(util::Tracer *tracer, const Problem &problem);

	Allocator alloc_;
	util::Tracer *tracer_;

	util::ObjectPool<ParamValue> valuePool_;
	EntryMap entryMap_;
	EntryMapCleaner entryMapCleaner_;
	ParamHandler *defaultHandler_;
	DependencyList pendingDependencyList_;

	ProblemList problemList_;
	bool traceEnabled_;
};

template<typename T> class ParamTable::BasicSetUpHandler {
public:
	BasicSetUpHandler();
	virtual ~BasicSetUpHandler();

	virtual void operator()(T&) = 0;

	static void setUpAll(T &patamTable);

private:
	static std::vector<BasicSetUpHandler*>& handlerList();
};

class ParamTable::ValueAnnotation {
public:
	typedef ParamTable::Allocator Allocator;

	virtual bool isHidden() const = 0;

	virtual bool asStorableValue(
			ParamValue &dest, const ParamValue *src, const ParamValue *base,
			const char8_t *sourceInfo, const Allocator &alloc) const = 0;

	virtual void asDisplayValue(ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, ExportMode mode) const = 0;
};

class ParamTable::ParamHandler {
public:
	virtual void operator()(ParamId id, const ParamValue &value) = 0;
};

class ParamTable::PathFormatter {
public:
	void format(std::ostream &stream) const;

private:
	friend class ParamTable;
	PathFormatter(ParamId id, const EntryMap &enrtyMap);

	ParamId id_;
	const EntryMap &entryMap_;
};

class ParamTable::SilentParamHandler : public ParamHandler {
public:
	virtual void operator()(ParamId, const ParamValue&);
};

std::ostream& operator<<(
		std::ostream &stream, const ParamTable::PathFormatter &formatter);

/*!
	@brief Represents the table of administrators
*/
class UserTable {
public:
	UserTable();

	void load(const char8_t *fileName);

	bool isValidUser(
		const char8_t *name, const char8_t *password, bool convert = false) const;

	bool exists(
		const char8_t *name) const;

private:
	UserTable(const UserTable&);
	UserTable& operator=(const UserTable&);

	std::map<std::string, std::string> digestMap_;
};

/*!
	@brief Represents the table of configure parameters
*/
class ConfigTable : public ParamTable {
public:
	typedef util::StdAllocator<void, void> Allocator;
	typedef BasicSetUpHandler<ConfigTable> SetUpHandler;

	class Constraint;
	class Filter;

	static const ExportMode EXPORT_MODE_NO_UNIT;

	explicit ConfigTable(const Allocator &alloc);
	virtual ~ConfigTable();

	void resolveGroup(ParamId groupId, ParamId id, const char8_t *name);
	Constraint& addParam(ParamId groupId, ParamId id, const char8_t *name);

	uint16_t getUInt16(ParamId id) const;
	uint32_t getUInt32(ParamId id) const;
	uint64_t getUInt64(ParamId id) const;

	static size_t megaBytesToBytes(size_t value);

	/*!
		@brief Extended type about config
	*/
	enum ExtendedType {
		EXTENDED_TYPE_NONE,

		EXTENDED_TYPE_ENUM,
		EXTENDED_TYPE_LAX_BOOL
	};

	/*!
		@brief Unit type of parameter
	*/
	enum ValueUnit {
		VALUE_UNIT_NONE,

		VALUE_UNIT_SIZE_B,
		VALUE_UNIT_SIZE_KB,
		VALUE_UNIT_SIZE_MB,
		VALUE_UNIT_SIZE_GB,
		VALUE_UNIT_SIZE_TB,

		VALUE_UNIT_DURATION_MS,
		VALUE_UNIT_DURATION_S,
		VALUE_UNIT_DURATION_MIN,
		VALUE_UNIT_DURATION_H
	};

private:
	typedef util::XArray<
			Constraint*, util::StdAllocator<Constraint*, void> > ConstraintList;

	util::ObjectPool<Constraint> constraintPool_;
	ConstraintList constraintList_;
};

/*!
	@brief Represents the constraint of each parameter
*/
class ConfigTable::Constraint : protected ParamTable::ValueAnnotation {
public:
	virtual ~Constraint();

	Constraint& setType(ParamValue::Type type);

	Constraint& setExtendedType(ExtendedType type);

	Constraint& setUnit(ValueUnit defaultUnit, bool unitRequired = false);

	Constraint& setDefault(const ParamValue &value);
	Constraint& setMin(const ParamValue &value);
	Constraint& setMax(const ParamValue &value);

	Constraint& add(const ParamValue &value);
	Constraint& addEnum(const ParamValue &value, const char8_t *name);

	Constraint& alternate(ParamId alternativeId, Filter *filter = NULL);
	Constraint& inherit(ParamId baseId);

	Constraint& deprecate(bool enabled = true);
	Constraint& hide(bool enabled = true);

	void checkMinRange(int64_t lower, const ParamValue *target = NULL) const;
	void checkMaxRange(int64_t upper, const ParamValue *target = NULL) const;

protected:
	virtual bool isHidden() const;

	virtual bool asStorableValue(
			ParamValue &dest, const ParamValue *src, const ParamValue *base,
			const char8_t *sourceInfo, const Allocator &alloc) const;

	virtual void asDisplayValue(ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, ExportMode mode) const;

private:
	friend class ConfigTable;

	typedef ParamTable::Allocator Allocator;
	typedef ParamTable::String String;

	struct CandidateInfo {
		String name_;
		ParamValue *value_;
		ValueUnit preferredUnit_;
	};

	typedef std::vector< CandidateInfo,
			util::StdAllocator<CandidateInfo, void> > CandidateList;

	struct UnitInfo {
		ValueUnit unit_;
		const char8_t *nameList_[5];
		int32_t step_;
	};

	class Formatter;
	friend std::ostream& operator<<(
			std::ostream& stream, const Formatter &formatter);

	static const UnitInfo UNIT_INFO_LIST[];
	static const ValueUnit UNIT_MIN_LIST[];
	static const ValueUnit UNIT_MAX_LIST[];

	Constraint(ParamTable::ParamId id, ParamTable &paramTable,
			Allocator &alloc, util::ObjectPool<ParamValue> &valuePool);

	bool asStorableExtendedValue(ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, Formatter &formatter) const;

	bool asStorableUnitValue(ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, Formatter &formatter,
			ValueUnit *srcUnit) const;

	bool asStorableNumericValue(ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, Formatter &formatter,
			const ParamValue::Type *type = NULL) const;

	const ParamValue* filterValue(
			ParamValue &filteredStorage, const ParamValue &value,
			ValueUnit &srcUnit);

	void checkComparable(bool expected) const;

	template<typename D, typename S>
	void convertNumber(D &dest, const S &src, Formatter &formatter) const;

	static int32_t compareValue(
			const ParamValue &value1, const ParamValue &value2);
	template<typename T> static int32_t compareNumber(
			const T &value1, const T &value2);

	static int64_t convertUnit(int64_t src, ValueUnit destUnit,
			ValueUnit srcUnit, Formatter &formatter);

	ParamTable::ParamId id_;
	ParamTable &paramTable_;
	Allocator &alloc_;
	util::ObjectPool<ParamValue> &valuePool_;

	ParamValue::Type type_;
	ExtendedType extendedType_;
	ValueUnit defaultUnit_;

	ParamValue defaultValue_;
	ParamValue minValue_;
	ParamValue maxValue_;

	CandidateList candidateList_;

	Filter *filter_;

	bool deprecated_;
	bool hidden_;
	bool unitRequired_;
	bool alterable_;
	bool inheritable_;
};

class ConfigTable::Constraint::Formatter {
public:
	/*!
		@brief entry type of constraint
	*/
	enum EntryType {
		ENTRY_NORMAL,
		ENTRY_PATH,
		ENTRY_VALUE,
		ENTRY_TYPE,
		ENTRY_JSON_VALUE_TYPE,
		ENTRY_RECOMMENDS,
		ENTRY_VALUE_CANDIDATES,
		ENTRY_UNIT_CANDIDATES,
		ENTRY_UNIT
	};

	explicit Formatter(const Constraint &constraint);

	void add(const char8_t *name, const ParamValue &value,
			EntryType entryType = ENTRY_NORMAL);

	void addAll(const Formatter &formatter);

	void format(std::ostream &stream) const;

private:
	struct Entry {
		const char8_t *name_;
		ParamValue value_;
		EntryType entryType_;
	};

	void format(std::ostream &stream, const Entry &entry) const;

	const Constraint &constraint_;
	Entry entryList_[10];
	size_t entryCount_;
};

std::ostream& operator<<(
		std::ostream& stream,
		const ConfigTable::Constraint::Formatter &formatter);

class ConfigTable::Filter {
public:
	virtual bool operator()(ParamValue &dest, const ParamValue &src) = 0;
};

/*!
	@brief Represents the table of statistics
*/
class StatTable : public ParamTable {
public:
	typedef util::StdAllocator<void, void> Allocator;
	typedef BasicSetUpHandler<StatTable> SetUpHandler;
	typedef int32_t DisplayOption;

	class StatUpdator;

	explicit StatTable(const Allocator &alloc);

	void initialize();
	void initialize(const StatTable &table);

	void resolveGroup(ParamId groupId, ParamId id, const char8_t *name);
	void addParam(ParamId groupId, ParamId id, const char8_t *name);

	void addUpdator(StatUpdator *updator);

	void set(ParamId id, const ParamValue &value);
	void set(ParamId id, const uint32_t &value);
	void set(ParamId id, const uint64_t &value);
	template<typename T> void set(ParamId id, const T &value);

	bool getDisplayOption(DisplayOption option) const;
	void setDisplayOption(DisplayOption option, bool enabled);

	void updateAll();

private:
	typedef std::vector< StatUpdator*,
			util::StdAllocator<StatUpdator*, void> > UpdatorList;
	typedef std::vector< DisplayOption,
			util::StdAllocator<DisplayOption, void> > OptionList;

	UpdatorList updatorList_;
	OptionList optionList_;
};

class StatTable::StatUpdator {
public:
	virtual bool operator()(StatTable &table) = 0;
};

/*!
	@brief Represents config of Partition group
*/
class PartitionGroupConfig {
public:
	PartitionGroupConfig(const ConfigTable &configTable);
	PartitionGroupConfig(uint32_t partitionNum, uint32_t concurrency);
	~PartitionGroupConfig();

	uint32_t getPartitionCount() const;

	uint32_t getPartitionGroupCount() const;

	PartitionId getGroupBeginPartitionId(PartitionGroupId pgId) const;

	PartitionId getGroupEndPartitionId(PartitionGroupId pgId) const;

	PartitionGroupId getPartitionGroupId(PartitionId pId) const;

	uint32_t getGroupPartitonCount(PartitionGroupId pgId) const;

private:
	static void checkParams(uint32_t partitionNum, uint32_t concurrency);
	static uint32_t makeBase(uint32_t partitionNum, uint32_t concurrency);
	static uint32_t makeMod(uint32_t partitionNum, uint32_t concurrency);

	uint32_t partitionNum_;
	uint32_t partitionGroupNum_;
	uint32_t base_;
	uint32_t mod_;

	PartitionGroupConfig();
};

template<typename E, typename T, typename A>
ParamValue::ParamValue(const std::basic_string<E, T, A> &base) :
		type_(PARAM_TYPE_STRING) {
	storage_.asString_.construct(base.c_str(), NULL);
}

template<typename T> ParamValue::ParamValue(const volatile T*) {
	UTIL_STATIC_ASSERT(sizeof(T) < 0);
}

template<typename T> inline bool ParamValue::is() const {
	return (resolveType<T>() == type_);
}

template<typename T> inline const T& ParamValue::get() const {
	UTIL_STATIC_ASSERT(sizeof(T) < 0);
	return *static_cast<const T*>(NULL);
}

template<> inline const char8_t *const& ParamValue::get() const {
	assert(is<const char8_t*>());
	return storage_.asString_.get();
}

template<> inline const int64_t& ParamValue::get() const {
	assert(is<int64_t>());
	return storage_.asInt64_;
}

template<> inline const int32_t& ParamValue::get() const {
	assert(is<int32_t>());
	return storage_.asInt32_;
}

template<> inline const double& ParamValue::get() const {
	assert(is<double>());
	return storage_.asDouble_;
}

template<> inline const bool& ParamValue::get() const {
	assert(is<bool>());
	return storage_.asBool_;
}

template<> inline const picojson::value& ParamValue::get() const {
	assert(is<picojson::value>());
	return *storage_.asJSON_.get();
}

template<typename T> inline ParamValue::Type ParamValue::resolveType() {
	UTIL_STATIC_ASSERT(sizeof(T) < 0);
	return Type();
}

template<>
inline ParamValue::Type ParamValue::resolveType<ParamValue::Null>() {
	return PARAM_TYPE_NULL;
}

template<> inline ParamValue::Type ParamValue::resolveType<const char8_t*>() {
	return PARAM_TYPE_STRING;
}

template<> inline ParamValue::Type ParamValue::resolveType<int64_t>() {
	return PARAM_TYPE_INT64;
}

template<> inline ParamValue::Type ParamValue::resolveType<int32_t>() {
	return PARAM_TYPE_INT32;
}

template<> inline ParamValue::Type ParamValue::resolveType<double>() {
	return PARAM_TYPE_DOUBLE;
}

template<> inline ParamValue::Type ParamValue::resolveType<bool>() {
	return PARAM_TYPE_BOOL;
}

template<> inline ParamValue::Type ParamValue::resolveType<picojson::value>() {
	return PARAM_TYPE_JSON;
}

template<typename T> inline const T& ParamTable::get(ParamId id) const {
	const ParamValue *value = getAnnotatedValue(id, true, false).first;
	return value->get<T>();
}

template<typename T>
inline ParamTable::BasicSetUpHandler<T>::BasicSetUpHandler() {
	handlerList().push_back(this);
}

template<typename T>
inline ParamTable::BasicSetUpHandler<T>::~BasicSetUpHandler() {
}

template<typename T>
inline void ParamTable::BasicSetUpHandler<T>::setUpAll(T &patamTable) {
	static const std::vector<BasicSetUpHandler*> list = handlerList();
	for (typename std::vector<BasicSetUpHandler*>::const_iterator it = list.begin();
			it != list.end(); ++it) {
		(**it)(patamTable);
	}
}

template<typename T>
inline std::vector<ParamTable::BasicSetUpHandler<T>*>&
ParamTable::BasicSetUpHandler<T>::handlerList() {
	static std::vector<BasicSetUpHandler*> list;
	return list;
}

template<typename T>
inline void StatTable::set(ParamId id, const T &value) {
	set(id, ParamValue(value));
}

/*!
	@brief Paramter ID of config table
*/
enum ConfigTableParamId {
	CONFIG_TABLE_ROOT,

	CONFIG_TABLE_DS,
	CONFIG_TABLE_CP,
	CONFIG_TABLE_CS,
	CONFIG_TABLE_SYNC,
	CONFIG_TABLE_SYS,
	CONFIG_TABLE_TXN,
	CONFIG_TABLE_TRIG,
	CONFIG_TABLE_TRACE,
	CONFIG_TABLE_DEV,


	CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS,

	CONFIG_TABLE_DS_PARTITION_NUM,
	CONFIG_TABLE_DS_STORE_BLOCK_SIZE,

	CONFIG_TABLE_CS_REPLICATION_NUM,
	CONFIG_TABLE_CS_NOTIFICATION_ADDRESS,
	CONFIG_TABLE_CS_NOTIFICATION_PORT,
	CONFIG_TABLE_CS_NOTIFICATION_INTERVAL,
	CONFIG_TABLE_CS_HEARTBEAT_INTERVAL,
	CONFIG_TABLE_CS_LOADBALANCE_CHECK_INTERVAL,
	CONFIG_TABLE_CS_CLUSTER_NAME,

	CONFIG_TABLE_CS_OWNER_BACKUP_LSN_GAP,
	CONFIG_TABLE_CS_OWNER_CATCHUP_LSN_GAP,
	CONFIG_TABLE_CS_MAX_LSN_GAP,
	CONFIG_TABLE_CS_MAX_LSN_REPLICATION_NUM,
	CONFIG_TABLE_CS_CATCHUP_NUM,

	CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL,

	CONFIG_TABLE_SYNC_LONG_SYNC_TIMEOUT_INTERVAL,
	CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE,

	CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS,
	CONFIG_TABLE_TXN_NOTIFICATION_PORT,
	CONFIG_TABLE_TXN_NOTIFICATION_INTERVAL,
	CONFIG_TABLE_TXN_REPLICATION_MODE,
	CONFIG_TABLE_TXN_REPLICATION_TIMEOUT_INTERVAL,
	CONFIG_TABLE_TXN_AUTHENTICATION_TIMEOUT_INTERVAL,


	CONFIG_TABLE_ROOT_SERVICE_ADDRESS,

	CONFIG_TABLE_DS_DB_PATH,
	CONFIG_TABLE_DS_CHUNK_MEMORY_LIMIT,
	CONFIG_TABLE_DS_STORE_MEMORY_LIMIT,
	CONFIG_TABLE_DS_CONCURRENCY,
	CONFIG_TABLE_DS_LOG_WRITE_MODE,
	CONFIG_TABLE_DS_STORE_WARM_START,
	CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE,

	CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_MILLIS,
	CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_TIME,
	CONFIG_TABLE_DS_RESULT_SET_MEMORY_LIMIT,
	CONFIG_TABLE_DS_RECOVERY_LEVEL,

	CONFIG_TABLE_DS_PERSISTENCY_MODE,
	CONFIG_TABLE_DS_RETAINED_FILE_COUNT,

	CONFIG_TABLE_CP_CHECKPOINT_INTERVAL,
	CONFIG_TABLE_CP_CHECKPOINT_MEMORY_LIMIT,
	CONFIG_TABLE_CP_USE_PARALLEL_MODE,

	CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL_MILLIS,
	CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL,

	CONFIG_TABLE_CS_LISTEN_ADDRESS,
	CONFIG_TABLE_CS_LISTEN_PORT,
	CONFIG_TABLE_CS_SERVICE_ADDRESS,
	CONFIG_TABLE_CS_SERVICE_PORT,

	CONFIG_TABLE_CS_CONNECTION_LIMIT,

	CONFIG_TABLE_SYNC_LISTEN_ADDRESS,
	CONFIG_TABLE_SYNC_LISTEN_PORT,
	CONFIG_TABLE_SYNC_SERVICE_ADDRESS,
	CONFIG_TABLE_SYNC_SERVICE_PORT,

	CONFIG_TABLE_SYS_LISTEN_ADDRESS,
	CONFIG_TABLE_SYS_LISTEN_PORT,
	CONFIG_TABLE_SYS_SERVICE_ADDRESS,
	CONFIG_TABLE_SYS_SERVICE_PORT,
	CONFIG_TABLE_SYS_EVENT_LOG_PATH,
	CONFIG_TABLE_SYS_STATS_INTERVAL,
	CONFIG_TABLE_SYS_TRACE_MODE,

	CONFIG_TABLE_TXN_LISTEN_ADDRESS,
	CONFIG_TABLE_TXN_LISTEN_PORT,
	CONFIG_TABLE_TXN_SERVICE_ADDRESS,
	CONFIG_TABLE_TXN_SERVICE_PORT,
	CONFIG_TABLE_TXN_CONNECTION_LIMIT,
	CONFIG_TABLE_TXN_TRANSACTION_TIMEOUT_LIMIT,
	CONFIG_TABLE_TXN_REAUTHENTICATION_INTERVAL,

	CONFIG_TABLE_TXN_STACK_MEMORY_LIMIT,
	CONFIG_TABLE_TXN_TOTAL_MEMORY_LIMIT,
	CONFIG_TABLE_TXN_QUEUE_MEMORY_LIMIT,
	CONFIG_TABLE_TXN_TOTAL_MESSAGE_MEMORY_LIMIT,
	CONFIG_TABLE_TXN_USE_KEEPALIVE,
	CONFIG_TABLE_TXN_KEEPALIVE_IDLE,
	CONFIG_TABLE_TXN_KEEPALIVE_INTERVAL,
	CONFIG_TABLE_TXN_KEEPALIVE_COUNT,

	CONFIG_TABLE_TRIG_TIMEOUT_INTERVAL,

	CONFIG_TABLE_TRACE_OUTPUT_TYPE,
	CONFIG_TABLE_TRACE_FILE_COUNT,

	CONFIG_TABLE_TRACE_TRACER_ID_START,
	CONFIG_TABLE_TRACE_DEFAULT,
	CONFIG_TABLE_TRACE_MAIN,
	CONFIG_TABLE_TRACE_HASH_MAP,
	CONFIG_TABLE_TRACE_BASE_CONTAINER,
	CONFIG_TABLE_TRACE_DATA_STORE,
	CONFIG_TABLE_TRACE_COLLECTION,
	CONFIG_TABLE_TRACE_TIME_SERIES,
	CONFIG_TABLE_TRACE_CHUNK_MANAGER,
	CONFIG_TABLE_TRACE_CHUNK_MANAGER_DETAIL,
	CONFIG_TABLE_TRACE_CHUNK_MANAGER_IO_DETAIL,
	CONFIG_TABLE_TRACE_OBJECT_MANAGER,
	CONFIG_TABLE_TRACE_CHECKPOINT_FILE,
	CONFIG_TABLE_TRACE_CHECKPOINT_SERVICE,
	CONFIG_TABLE_TRACE_CHECKPOINT_SERVICE_DETAIL,
	CONFIG_TABLE_TRACE_LOG_MANAGER,
	CONFIG_TABLE_TRACE_IO_MONITOR,
	CONFIG_TABLE_TRACE_CLUSTER_OPERATION,
	CONFIG_TABLE_TRACE_CLUSTER_SERVICE,
	CONFIG_TABLE_TRACE_SYNC_SERVICE,
	CONFIG_TABLE_TRACE_SYSTEM_SERVICE,
	CONFIG_TABLE_TRACE_SYSTEM_SERVICE_DETAIL,
	CONFIG_TABLE_TRACE_TRANSACTION_MANAGER,
	CONFIG_TABLE_TRACE_SESSION_DETAIL,
	CONFIG_TABLE_TRACE_TRANSACTION_DETAIL,
	CONFIG_TABLE_TRACE_TIMEOUT_DETAIL,
	CONFIG_TABLE_TRACE_TRANSACTION_SERVICE,
	CONFIG_TABLE_TRACE_REPLICATION,
	CONFIG_TABLE_TRACE_TRANSACTION_TIMEOUT,
	CONFIG_TABLE_TRACE_SESSION_TIMEOUT,
	CONFIG_TABLE_TRACE_REPLICATION_TIMEOUT,
	CONFIG_TABLE_TRACE_RECOVERY_MANAGER,
	CONFIG_TABLE_TRACE_RECOVERY_MANAGER_DETAIL,
	CONFIG_TABLE_TRACE_EVENT_ENGINE,
	CONFIG_TABLE_TRACE_TRIGGER_SERVICE,
	CONFIG_TABLE_TRACE_MESSAGE_LOG_TEST,
	CONFIG_TABLE_TRACE_TRACER_ID_END,

	CONFIG_TABLE_DEV_AUTO_JOIN_CLUSTER,
	CONFIG_TABLE_DEV_RECOVERY,
	CONFIG_TABLE_DEV_RECOVERY_ONLY_CHECK_CHUNK,
	CONFIG_TABLE_DEV_RECOVERY_DOWN_POINT,
	CONFIG_TABLE_DEV_RECOVERY_DOWN_COUNT,
	CONFIG_TABLE_DEV_CLUSTER_NODE_DUMP,
	CONFIG_TABLE_DEV_TRACE_SECRET,
	CONFIG_TABLE_DEV_SIMPLE_VERSION,
	CONFIG_TABLE_DEV_FULL_VERSION,

	CONFIG_TABLE_PARAM_END
};

/*!
	@brief Utility for setting configure to ConfigTable
*/
class ConfigTableUtils {
public:
	static void resolveGroup(
			ConfigTable &configTable, ConfigTable::ParamId id,
			const char8_t *symbol, const char8_t *name);
	static ConfigTable::Constraint& addParam(
			ConfigTable &configTable, ConfigTable::ParamId id,
			const char8_t *symbol);

private:
	typedef std::map<std::string, std::string> SymbolTable;

	static const char8_t* getGroupSymbol(const char8_t *src, bool reverse);

	static SymbolTable& getGroupSymbolTable();
};

#define CONFIG_TABLE_RESOLVE_GROUP(configTable, id, name) \
	ConfigTableUtils::resolveGroup(configTable, id, #id, name)
#define CONFIG_TABLE_ADD_PARAM(configTable, id, type) \
	ConfigTableUtils::addParam( \
			configTable, id, #id).setType(ParamValue::PARAM_TYPE_ ## type)

#define CONFIG_TABLE_ADD_PORT_PARAM(config, id, defaultPort) \
	CONFIG_TABLE_ADD_PARAM(config, id, INT32). \
	setMin(0). \
	setMax(65535). \
	setDefault(defaultPort)

#define CONFIG_TABLE_ADD_SERVICE_ADDRESS_PARAMS(config, group, defaultPort) \
	do { \
		CONFIG_TABLE_ADD_PARAM( \
				config, CONFIG_TABLE_ ## group ## _LISTEN_ADDRESS, STRING). \
				inherit(CONFIG_TABLE_ROOT_SERVICE_ADDRESS). \
				deprecate(); \
		CONFIG_TABLE_ADD_PARAM( \
				config, CONFIG_TABLE_ ## group ## _LISTEN_PORT, INT32). \
				deprecate(); \
		CONFIG_TABLE_ADD_PARAM( \
				config, CONFIG_TABLE_ ## group ## _SERVICE_ADDRESS, STRING). \
				inherit(CONFIG_TABLE_ ## group ## _LISTEN_ADDRESS); \
		CONFIG_TABLE_ADD_PORT_PARAM( \
				config, CONFIG_TABLE_ ## group ## _SERVICE_PORT, defaultPort). \
				alternate(CONFIG_TABLE_ ## group ## _LISTEN_PORT); \
	} \
	while (false)

/*!
	@brief Paramter ID of statistics table
*/
enum StatTableParamId {
	STAT_TABLE_ROOT = 0,

	STAT_TABLE_CS,
	STAT_TABLE_CS_ERROR,
	STAT_TABLE_CP,
	STAT_TABLE_RM,
	STAT_TABLE_PERF,
	STAT_TABLE_PERF_TXN_EE,
	STAT_TABLE_PERF_DS_DETAIL,
	STAT_TABLE_PERF_MEM,
	STAT_TABLE_PERF_MEM_DS,
	STAT_TABLE_PERF_MEM_WORK,

	STAT_TABLE_ROOT_CURRENT_TIME,
	STAT_TABLE_ROOT_VERSION,
	STAT_TABLE_ROOT_SYNC,
	STAT_TABLE_ROOT_PG_STORE_MEMORY_LIMIT,

	STAT_TABLE_CS_STARTUP_TIME,
	STAT_TABLE_CS_CLUSTER_STATUS,	
	STAT_TABLE_CS_NODE_STATUS,	
	STAT_TABLE_CS_CLUSTER_NAME,
	STAT_TABLE_CS_DESIGNATED_COUNT,	
	STAT_TABLE_CS_SYNC_COUNT,	
	STAT_TABLE_CS_PARTITION_STATUS,	
	STAT_TABLE_CS_LOAD_BALANCER,
	STAT_TABLE_CS_INITIAL_CLUSTER,
	STAT_TABLE_CS_ACTIVE_COUNT,	
	STAT_TABLE_CS_NODE_LIST,
	STAT_TABLE_CS_MASTER,

	STAT_TABLE_CS_ERROR_REPLICATION,
	STAT_TABLE_CS_ERROR_CLUSTER_VERSION,
	STAT_TABLE_CS_ERROR_ALREADY_APPLIED_LOG,
	STAT_TABLE_CS_ERROR_INVALID_LOG,

	STAT_TABLE_CP_START_TIME,
	STAT_TABLE_CP_END_TIME,
	STAT_TABLE_CP_MODE,
	STAT_TABLE_CP_PENDING_PARTITION,
	STAT_TABLE_CP_NORMAL_CHECKPOINT_OPERATION,
	STAT_TABLE_CP_REQUESTED_CHECKPOINT_OPERATION,


	STAT_TABLE_RM_PROGRESS_RATE,

	STAT_TABLE_PERF_CURRENT_TIME,
	STAT_TABLE_PERF_PROCESS_MEMORY,	
	STAT_TABLE_PERF_PEAK_PROCESS_MEMORY,	

	STAT_TABLE_PERF_CHECKPOINT_FILE_SIZE,	
	STAT_TABLE_PERF_CHECKPOINT_FILE_USAGE_RATE,	
	STAT_TABLE_PERF_CURRENT_CHECKPOINT_WRITE_BUFFER_SIZE,
	STAT_TABLE_PERF_CHECKPOINT_WRITE_SIZE,	
	STAT_TABLE_PERF_CHECKPOINT_WRITE_TIME,
	STAT_TABLE_PERF_CHECKPOINT_MEMORY_LIMIT,
	STAT_TABLE_PERF_CHECKPOINT_MEMORY,



	STAT_TABLE_PERF_TXN_NUM_CONNECTION,	
	STAT_TABLE_PERF_TXN_NUM_SESSION,	
	STAT_TABLE_PERF_TXN_NUM_TXN,	
	STAT_TABLE_PERF_TXN_TOTAL_LOCK_CONFLICT_COUNT,	
	STAT_TABLE_PERF_TXN_TOTAL_READ_OPERATION,	
	STAT_TABLE_PERF_TXN_TOTAL_WRITE_OPERATION,	
	STAT_TABLE_PERF_TXN_TOTAL_ROW_READ,	
	STAT_TABLE_PERF_TXN_TOTAL_ROW_WRITE,	

	STAT_TABLE_PERF_TXN_DETAIL_DISABLE_TIMEOUT_CHECK_PARTITION_COUNT,
	STAT_TABLE_PERF_TXN_EE_CLUSTER,
	STAT_TABLE_PERF_TXN_EE_SYNC,
	STAT_TABLE_PERF_TXN_EE_TRANSACTION,

	STAT_TABLE_PERF_DS_STORE_MEMORY_LIMIT,
	STAT_TABLE_PERF_DS_STORE_MEMORY,	
	STAT_TABLE_PERF_DS_STORE_TOTAL_USE,	
	STAT_TABLE_PERF_DS_ALLOCATE_DATA,	
	STAT_TABLE_PERF_DS_BATCH_FREE,	
	STAT_TABLE_PERF_DS_SWAP_READ,	
	STAT_TABLE_PERF_DS_SWAP_WRITE,	
	STAT_TABLE_PERF_DS_SWAP_READ_SIZE,
	STAT_TABLE_PERF_DS_SWAP_READ_TIME,
	STAT_TABLE_PERF_DS_SWAP_WRITE_SIZE,
	STAT_TABLE_PERF_DS_SWAP_WRITE_TIME,
	STAT_TABLE_PERF_DS_SYNC_READ_SIZE,	
	STAT_TABLE_PERF_DS_SYNC_READ_TIME,	
	STAT_TABLE_PERF_DS_RECOVERY_READ_SIZE,	
	STAT_TABLE_PERF_DS_RECOVERY_READ_TIME,

	STAT_TABLE_PERF_DS_DETAIL_POOL_BUFFER_MEMORY,
	STAT_TABLE_PERF_DS_DETAIL_POOL_CHECKPOINT_MEMORY,


	STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START,
	STAT_TABLE_PERF_DS_DETAIL_META_DATA,
	STAT_TABLE_PERF_DS_DETAIL_MAP_DATA,
	STAT_TABLE_PERF_DS_DETAIL_ROW_DATA,
	STAT_TABLE_PERF_DS_DETAIL_BATCH_FREE_MAP_DATA,
	STAT_TABLE_PERF_DS_DETAIL_BATCH_FREE_ROW_DATA,
	STAT_TABLE_PERF_DS_DETAIL_CATEGORY_END,

	STAT_TABLE_PERF_DS_DETAIL_PARAM_START,
	STAT_TABLE_PERF_DS_DETAIL_STORE_MEMORY,
	STAT_TABLE_PERF_DS_DETAIL_STORE_USE,
	STAT_TABLE_PERF_DS_DETAIL_SWAP_READ,
	STAT_TABLE_PERF_DS_DETAIL_SWAP_WRITE,
	STAT_TABLE_PERF_DS_DETAIL_PARAM_END,

	STAT_TABLE_PERF_DS_DETAIL_ALL_START,
	STAT_TABLE_PERF_DS_DETAIL_ALL_END =
			(STAT_TABLE_PERF_DS_DETAIL_ALL_START + 1 +
			(STAT_TABLE_PERF_DS_DETAIL_PARAM_END -
			STAT_TABLE_PERF_DS_DETAIL_PARAM_START - 1) *
			(STAT_TABLE_PERF_DS_DETAIL_CATEGORY_END -
			STAT_TABLE_PERF_DS_DETAIL_CATEGORY_START - 1)),

	STAT_TABLE_PERF_MEM_ALL_TOTAL,	
	STAT_TABLE_PERF_MEM_ALL_CACHED,	
	STAT_TABLE_PERF_MEM_PROCESS_MEMORY_GAP,	
	STAT_TABLE_PERF_MEM_DS_STORE_TOTAL,
	STAT_TABLE_PERF_MEM_DS_LOG_TOTAL,
	STAT_TABLE_PERF_MEM_DS_LOG_CACHED,
	STAT_TABLE_PERF_MEM_WORK_CHECKPOINT_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_CHECKPOINT_CACHED,
	STAT_TABLE_PERF_MEM_WORK_CLUSTER_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_CLUSTER_CACHED,
	STAT_TABLE_PERF_MEM_WORK_MAIN_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_MAIN_CACHED,
	STAT_TABLE_PERF_MEM_WORK_SYNC_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_SYNC_CACHED,
	STAT_TABLE_PERF_MEM_WORK_SYSTEM_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_SYSTEM_CACHED,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_MESSAGE_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_MESSAGE_CACHED,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_RESULT_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_RESULT_CACHED,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_WORK_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_WORK_CACHED,
	STAT_TABLE_PARAM_END
};

/*!
	@brief Category for switch the statistics display
*/
enum StatTableDisplayOption {
	STAT_TABLE_DISPLAY_WEB_OR_DETAIL_TRACE,
	STAT_TABLE_DISPLAY_WEB_ONLY,
	STAT_TABLE_DISPLAY_SELECT_CS,
	STAT_TABLE_DISPLAY_SELECT_CP,
	STAT_TABLE_DISPLAY_SELECT_RM,
	STAT_TABLE_DISPLAY_SELECT_PERF,
	STAT_TABLE_DISPLAY_OPTIONAL_CS,
	STAT_TABLE_DISPLAY_OPTIONAL_DS,
	STAT_TABLE_DISPLAY_OPTIONAL_TXN,
	STAT_TABLE_DISPLAY_OPTIONAL_MEM,
	STAT_TABLE_DISPLAY_OPTIONAL_SYNC,
	STAT_TABLE_DISPLAY_OPTIONAL_PGLIMIT,
	STAT_TABLE_DISPLAY_ADDRESS_CLUSTER,
	STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION,
	STAT_TABLE_DISPLAY_ADDRESS_SYNC
};

#define STAT_TABLE_EXTRACT_SYMBOL(id, depth) \
	ParamTable::getParamSymbol(#id, true, depth).c_str()
#define STAT_TABLE_ADD_PARAM_CUSTOM(stat, parentId, id, depth) \
	stat.addParam(parentId, id, STAT_TABLE_EXTRACT_SYMBOL(id, depth))
#define STAT_TABLE_ADD_PARAM(stat, parentId, id) \
	STAT_TABLE_ADD_PARAM_CUSTOM(stat, parentId, id, 3)
#define STAT_TABLE_ADD_PARAM_SUB(stat, parentId, id) \
	STAT_TABLE_ADD_PARAM_CUSTOM(stat, parentId, id, 4)
#define STAT_TABLE_ADD_PARAM_SUB_SUB(stat, parentId, id) \
	STAT_TABLE_ADD_PARAM_CUSTOM(stat, parentId, id, 5)

/*!
	@brief Group ID about Allocator
*/
enum AllocatorGroupId {
	ALLOCATOR_GROUP_ROOT = 0,

	ALLOCATOR_GROUP_STORE,
	ALLOCATOR_GROUP_LOG,
	ALLOCATOR_GROUP_CP,
	ALLOCATOR_GROUP_CS,
	ALLOCATOR_GROUP_MAIN,
	ALLOCATOR_GROUP_SYNC,
	ALLOCATOR_GROUP_SYS,
	ALLOCATOR_GROUP_TXN_MESSAGE,
	ALLOCATOR_GROUP_TXN_RESULT,
	ALLOCATOR_GROUP_TXN_WORK,

	ALLOCATOR_GROUP_ID_END
};

#endif
