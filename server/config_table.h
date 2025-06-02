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
	template<typename T> T get() const;
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

	template<typename T, bool Pod> struct VariableType {
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
		VariableType<char8_t, true> asString_;
		int64_t asInt64_;
		int32_t asInt32_;
		double asDouble_;
		bool asBool_;
		VariableType<picojson::value, false> asJSON_;
	} storage_;

	Type type_;
};

template<> picojson::value ParamValue::get() const;
std::ostream& operator<<(std::ostream &stream, const ParamValue &value);

/*!
	@brief Represents the table of parameters
*/
class ParamTable {
public:
	typedef util::StdAllocator<void, void> Allocator;

	typedef int32_t ParamId;
	static const ParamId PARAM_ID_ROOT;

	typedef std::set<
			ParamId, std::set<ParamId>::key_compare,
			util::StdAllocator<ParamId, void> > IdSet;
	typedef std::vector<
			const char8_t*, util::StdAllocator<ParamId, void> > SubPath;

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

	bool isSetParamHandler(ParamId id);
	
	void setParamHandler(ParamId id, ParamHandler &handler);
	void setDefaultParamHandler(ParamHandler &handler);

	bool findParam(
			ParamId groupId, ParamId &id, const char8_t *name,
			SubPath *subPath = NULL) const;
	bool getNextParam(ParamId id, ParamId &nextId, bool skipEmpty) const;
	bool getParentParam(ParamId id, ParamId &parentId) const;

	IdSet listDescendantParams(const IdSet &src) const;

	AnnotatedValue getAnnotatedValue(
			ParamId id, bool valueExpected = true,
			bool annotationExpected = true) const;

	bool isSet(ParamId id) const;
	template<typename T> T get(ParamId id) const;

	const char8_t* getName(ParamId id) const;
	const char8_t* getSourceInfo(ParamId id) const;
	bool getBaseParam(ParamId id, ParamId &baseId) const;
	bool getReferredParam(ParamId id, ParamId &refId, size_t ordinal) const;

	void set(ParamId id, const ParamValue &value,
			const char8_t *sourceInfo, ParamHandler *customHandler = NULL,
			const SubPath *subPath = NULL);

	void acceptFile(const char8_t *dirPath,
			const char8_t *const *fileList, size_t count, ParamId id);
	void acceptJSON(const picojson::value *srcList, size_t count, ParamId id,
			const char8_t *const *sourceInfoList);
	void toFile(const char8_t *filePath, ParamId id, ExportMode mode);
	void toJSON(picojson::value &dest, ParamId id, ExportMode mode);

	void addProblem(const util::Exception::NamedErrorCode &code,
			const char8_t *message);
	void setTraceEnabled(bool enabled);

	PathFormatter pathFormatter(
			ParamId id, const SubPath *subPath = NULL) const;

	static ParamHandler& silentHandler();

	static picojson::value* findSubValue(
			picojson::value &value, const SubPath *subPath = NULL);
	static const picojson::value& getSubValue(
			const picojson::value &value, const SubPath *subPath = NULL);

	static std::string getParamSymbol(
			const char8_t *src, bool reverse, size_t startDepth);

	static void fileToJson(
			const util::StdAllocator<void, void> &alloc,
			const char8_t *fileName,
			picojson::value &jsonValue, const size_t *maxFileSize = NULL);

	static void fileToBuffer(
			const char8_t *fileName,
			util::XArray< uint8_t, util::StdAllocator<uint8_t, void> > &buf,
			const size_t *maxFileSize = NULL);

protected:
	typedef util::BasicString< char8_t, std::char_traits<char8_t>,
			util::StdAllocator<char8_t, void> > String;

	Allocator& getAllocator() { return alloc_; }
	util::ObjectPool<ParamValue>& getValuePool() { return valuePool_; }

	void initializeSchema(const ParamTable &table);

private:
	class SilentParamHandler;

	typedef std::map<
			String, ParamId, std::map<String, ParamId>::key_compare,
			util::StdAllocator<
					std::pair<const String, ParamId>, void> > NameMap;
	typedef std::vector< ParamId, util::StdAllocator<ParamId, void> > IdList;

	struct Entry {
		explicit Entry(const Allocator &alloc);

		void assign(const Entry &another);
		void assignNameMap(const NameMap &src, NameMap &dest);

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

	typedef std::map<
			ParamId, Entry, std::map<ParamId, Entry>::key_compare,
			util::StdAllocator<
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

	static void setUpAll(T &paramTable);

private:
	static std::vector<BasicSetUpHandler*>& handlerList();
};

class ParamTable::ValueAnnotation {
public:
	typedef ParamTable::Allocator Allocator;

	virtual bool isHidden() const = 0;

	virtual bool asStorableValue(
			ParamValue &dest, const ParamValue *src, const ParamValue *base,
			const char8_t *sourceInfo, const Allocator &alloc,
			const IdSet *specifiedIds) const = 0;

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
	PathFormatter(
			ParamId id, const EntryMap &entryMap, const SubPath *subPath);

	ParamId id_;
	const EntryMap &entryMap_;
	const SubPath *subPath_;
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
	void save(const char8_t *fileName) const;

	bool isValidUser(
			const char8_t *name, const char8_t *password,
			bool convert = false) const;

	const char8_t* getDigest(const char8_t *name) const;

	void setUser(const char8_t *name, const char8_t *digest);
	void removeUser(const char8_t *name);

	bool exists(const char8_t *name) const;

private:
	UserTable(const UserTable&);
	UserTable& operator=(const UserTable&);

	void replaceAll(
			std::string &str, const std::string &from, const std::string &to);

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

	Constraint& addExclusive(ParamId id);

	Constraint& deprecate(bool enabled = true);
	Constraint& hide(bool enabled = true);

	void checkMinRange(int64_t lower, const ParamValue *target = NULL) const;
	void checkMaxRange(int64_t upper, const ParamValue *target = NULL) const;

protected:
	virtual bool isHidden() const;

	virtual bool asStorableValue(
			ParamValue &dest, const ParamValue *src, const ParamValue *base,
			const char8_t *sourceInfo, const Allocator &alloc,
			const IdSet *specifiedIds) const;

	virtual void asDisplayValue(ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, ExportMode mode) const;

private:
	friend class ConfigTable;

	typedef ParamTable::Allocator Allocator;
	typedef ParamTable::String String;
	typedef std::pair<bool, bool> NumberRounding;

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
			ValueUnit *srcUnit, NumberRounding &rounding) const;

	bool asStorableNumericValue(
			ParamValue &dest, const ParamValue &src,
			const Allocator &alloc, Formatter &formatter,
			NumberRounding &rounding,
			const ParamValue::Type *type = NULL) const;

	const ParamValue* filterValue(
			ParamValue &filteredStorage, const ParamValue &value,
			ValueUnit &srcUnit);

	void checkComparable(bool expected) const;

	template<typename D, typename S>
	void convertNumber(
			D &dest, const S &src, Formatter &formatter,
			NumberRounding &rounding) const;

	template<typename T>
	static void applyNumberRounding(
			const T org, const T reverted, NumberRounding &rounding);

	static int32_t compareValue(
			const ParamValue &value1, const ParamValue &value2,
			const NumberRounding &rounding1);
	template<typename T> static int32_t compareNumber(
			const T &value1, const T &value2);

	static int64_t convertUnit(
			int64_t src, ValueUnit destUnit, ValueUnit srcUnit,
			Formatter &formatter, NumberRounding &rounding);

	void checkExclusiveIdSet(
			const Formatter &formatter, const IdSet &exclusiveIds) const;
	void checkExclusiveValue(
			const Formatter &formatter, const IdSet &exclusiveIds,
			const IdSet *specifiedIds) const;

	ParamId getExclusiveParam(ParamId id) const;
	IdSet listExclusiveParams(const Formatter &formatter) const;

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
	IdSet exclusiveIdSet_;

	Filter *filter_;

	bool deprecated_;
	bool hidden_;
	bool unitRequired_;
	bool alterable_;
	bool inheritable_;
	bool defaultSpecified_;
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
		ENTRY_SPECIFIED,
		ENTRY_TYPE,
		ENTRY_JSON_VALUE_TYPE,
		ENTRY_RECOMMENDS,
		ENTRY_VALUE_CANDIDATES,
		ENTRY_UNIT_CANDIDATES,
		ENTRY_UNIT
	};

	explicit Formatter(const Constraint &constraint);
	~Formatter();

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

	typedef std::vector< Entry*, util::StdAllocator<Entry*, void> > EntryList;

	void format(std::ostream &stream, const Entry &entry) const;
	static void formatValue(std::ostream &stream, const ParamValue &value);

	const Constraint &constraint_;
	util::StdAllocator<Entry, void> entryAlloc_;

	EntryList entryList_;
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

struct LocalStatTypes {
	enum MergeMode {
		MERGE_DEFAULT,
		MERGE_SUM,
		MERGE_MIN,
		MERGE_MAX
	};
};

struct LocalStatValue {
public:
	typedef ParamTable::ParamId ParamId;
	typedef std::pair<LocalStatValue*, LocalStatValue*> Range;
	typedef std::pair<const LocalStatValue*, const LocalStatValue*> RangeRef;

	LocalStatValue();
	void assign(const LocalStatValue &another);

	uint64_t get() const;
	void set(uint64_t value);

	void increment();
	void decrement();

	void add(uint64_t amount);
	void subtract(uint64_t amount);

	static Range toRange(LocalStatValue *it, size_t count);
	static RangeRef toRangeRef(const LocalStatValue *it, size_t count);
	static RangeRef toRangeRef(const Range &src);
	static size_t countOf(const RangeRef &range);

	static LocalStatValue& at(const Range &range, ParamId id);
	static const LocalStatValue& at(const RangeRef &range, ParamId id);

private:
	LocalStatValue(const LocalStatValue&);
	LocalStatValue& operator=(const LocalStatValue&);

	uint64_t value_;
};

class BasicLocalStatMapper {
public:
	typedef ParamTable::ParamId ParamId;
	typedef ConfigTable::ValueUnit ValueUnit;
	typedef StatTable::DisplayOption DisplayOption;
	typedef LocalStatTypes::MergeMode MergeMode;
	typedef LocalStatValue::Range ValueRange;
	typedef LocalStatValue::RangeRef ValueRangeRef;

	struct Source;
	class Options;
	class Cursor;

	BasicLocalStatMapper(
			const util::StdAllocator<void, void> &alloc, size_t paramCount);
	~BasicLocalStatMapper();

	Source getSource();

	void addSub(const BasicLocalStatMapper &sub);
	void addFilter(DisplayOption option);

	Options defaultOptions();
	Options bind(ParamId id, ParamId targetId);
	void setFraction(ParamId id, ParamId denomId);

	void applyValue(
			ParamId id, const LocalStatValue &src1, const LocalStatValue &src2,
			StatTable &dest) const;
	static void mergeValue(
			const BasicLocalStatMapper *mapper, ParamId id,
			const LocalStatValue &src, LocalStatValue &dest);
	bool findRelatedParam(ParamId id, ParamId &relatedId) const;

	static void setAllValues(
			const ValueRangeRef &src, const ValueRange &dest);
	static void applyAllValues(
			const BasicLocalStatMapper *mapper, const ValueRangeRef &src,
			StatTable &dest);
	static void mergeAllValues(
			const BasicLocalStatMapper *mapper, const ValueRangeRef &src,
			const ValueRange &dest);

private:
	struct Entry;
	struct Group;
	class OptionResolver;
	class ValueProducer;

	typedef util::AllocSet<ParamId> IdSet;
	typedef util::AllocVector<Entry*> EntryList;
	typedef util::AllocVector<Group*> GroupList;

	typedef std::pair<const Entry*, const Entry*> EntryPair;

	BasicLocalStatMapper(const BasicLocalStatMapper&);
	BasicLocalStatMapper& operator=(const BasicLocalStatMapper&);

	Entry& resolveEntry(ParamId id);
	Entry& newEntry(ParamId id);

	Group& prepareDefaultGroup();
	Group& addGroup(size_t &groupIndex);

	EntryPair getEntries(ParamId id) const;
	Group& getGroup(size_t groupIndex);
	const Group& getGroup(size_t groupIndex) const;
	size_t checkGroupIndex(size_t groupIndex) const;

	template<typename E>
	void clearList(util::AllocVector<E*> &list);

	size_t resolveEntryIndex(ParamId id) const;
	bool isEntryAssigned(ParamId id) const;

	void checkParamId(ParamId id) const;
	static void checkParamIdBasic(ParamId id);

	void checkParamCount(size_t count) const;
	static void checkParamCount(size_t count1, size_t count2);

	util::StdAllocator<void, void> alloc_;
	EntryList entryList_;
	GroupList groupList_;
	size_t paramCount_;
};

struct BasicLocalStatMapper::Source {
	Source(const util::StdAllocator<void, void> &alloc, size_t paramCount);

	const util::StdAllocator<void, void> &alloc_;
	size_t paramCount_;
};

class BasicLocalStatMapper::Options {
public:
	Options(util::StdAllocator<void, void> &alloc, Entry &entry);

	const Options& setMergeMode(MergeMode mode) const;

	const Options& setValueType(ParamValue::Type type) const;
	const Options& setDateTimeType(bool withDefaults = true) const;
	const Options& setDefaultValue(
			const ParamValue &value, const uint64_t *srcValue = NULL) const;

	const Options& setTimeUnit(ValueUnit unit) const;
	const Options& setConversionUnit(uint64_t unit, bool dividing) const;
	const Options& setNameCoder(const util::GeneralNameCoder &coder) const;
	template<typename C> const Options& setNameCoder(const C *coder) const;

private:
	util::StdAllocator<void, void> &alloc_;
	Entry &entry_;
};

template<typename T, size_t C>
class LocalStatMapper {
public:
	typedef ParamTable::ParamId ParamId;
	typedef StatTable::DisplayOption DisplayOption;
	typedef BasicLocalStatMapper::Source Source;
	typedef BasicLocalStatMapper::Options Options;

	explicit LocalStatMapper(const util::StdAllocator<void, void> &alloc);
	explicit LocalStatMapper(const Source &source);

	Source getSource();

	void addSub(const LocalStatMapper &sub);
	void addFilter(DisplayOption option);

	Options defaultOptions();
	Options bind(T srcId, ParamId targetId);
	void setFraction(T id, T denomId);

	static const BasicLocalStatMapper* findBase(const LocalStatMapper *mapper);

private:
	LocalStatMapper(const LocalStatMapper&);
	LocalStatMapper& operator=(const LocalStatMapper&);

	BasicLocalStatMapper base_;
};

template<typename T, size_t C>
class LocalStatTable {
public:
	typedef LocalStatMapper<T, C> Mapper;

	explicit LocalStatTable(const Mapper *mapper);

	void assign(const LocalStatTable &src);
	void apply(StatTable &statTable) const;
	void merge(const LocalStatTable &src);

	const LocalStatValue& operator()(T id) const;
	LocalStatValue& operator()(T id);

private:
	LocalStatTable(const LocalStatTable&);
	LocalStatTable& operator=(const LocalStatTable&);

	LocalStatValue values_[C];
	const Mapper *mapper_;
};

class StatStopwatch {
public:
	class Holder;
	class NonStat;

	void start();
	uint32_t stop();

private:
	StatStopwatch(const StatStopwatch&);
	StatStopwatch& operator=(const StatStopwatch&);

	explicit StatStopwatch(LocalStatValue *value);

	util::Stopwatch base_;
	LocalStatValue *value_;
};

class StatStopwatch::Holder {
public:
	Holder();

	void initialize(LocalStatValue &value);
	StatStopwatch& get();

private:
	StatStopwatch watch_;
};

class StatStopwatch::NonStat {
public:
	NonStat();
	StatStopwatch& get();

private:
	LocalStatValue nonStatValue_;
	StatStopwatch watch_;
};

template<typename T, size_t C>
class TimeStatTable {
public:
	typedef LocalStatTable<T, C> BaseTable;

	explicit TimeStatTable(BaseTable &baseTable);

	StatStopwatch& operator()(T id);

private:
private:
	TimeStatTable(const TimeStatTable&);
	TimeStatTable& operator=(const TimeStatTable&);

	StatStopwatch::Holder watchList_[C];
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

	uint32_t getGroupPartitionCount(PartitionGroupId pgId) const;

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

template<typename T> inline T ParamValue::get() const {
	UTIL_STATIC_ASSERT(sizeof(T) < 0);
	return *static_cast<const T*>(NULL);
}

template<> inline const char8_t* ParamValue::get() const {
	assert(is<const char8_t*>());
	return storage_.asString_.get();
}

template<> inline int64_t ParamValue::get() const {
	assert(is<int64_t>());
	return storage_.asInt64_;
}

template<> inline int32_t ParamValue::get() const {
	assert(is<int32_t>());
	return storage_.asInt32_;
}

template<> inline double ParamValue::get() const {
	assert(is<double>());
	return storage_.asDouble_;
}

template<> inline bool ParamValue::get() const {
	assert(is<bool>());
	return storage_.asBool_;
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

template<typename T> inline T ParamTable::get(ParamId id) const {
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
inline void ParamTable::BasicSetUpHandler<T>::setUpAll(T &paramTable) {
	static const std::vector<BasicSetUpHandler*> list = handlerList();
	for (typename std::vector<BasicSetUpHandler*>::const_iterator it = list.begin();
			it != list.end(); ++it) {
		(**it)(paramTable);
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


inline LocalStatValue::LocalStatValue() : value_(0) {
}

inline void LocalStatValue::assign(const LocalStatValue &another) {
	set(another.value_);
}

inline uint64_t LocalStatValue::get() const {
	return value_;
}

inline void LocalStatValue::set(uint64_t value) {
	value_ = value;
}

inline void LocalStatValue::increment() {
	++value_;
}

inline void LocalStatValue::decrement() {
	--value_;
}

inline void LocalStatValue::add(uint64_t amount) {
	value_ += amount;
}

inline void LocalStatValue::subtract(uint64_t amount) {
	value_ -= amount;
}


template<typename C>
inline const BasicLocalStatMapper::Options&
BasicLocalStatMapper::Options::setNameCoder(const C *coder) const {
	return setNameCoder(util::GeneralNameCoder(coder));
}


template<typename T, size_t C>
inline LocalStatMapper<T, C>::LocalStatMapper(
		const util::StdAllocator<void, void> &alloc) :
		base_(alloc, C) {
}

template<typename T, size_t C>
inline LocalStatMapper<T, C>::LocalStatMapper(const Source &source) :
		base_(source.alloc_, C) {
	assert(source.paramCount_ == C);
}

template<typename T, size_t C>
inline BasicLocalStatMapper::Source LocalStatMapper<T, C>::getSource() {
	return base_.getSource();
}

template<typename T, size_t C>
inline void LocalStatMapper<T, C>::addSub(const LocalStatMapper &sub) {
	return base_.addSub(sub.base_);
}

template<typename T, size_t C>
inline void LocalStatMapper<T, C>::addFilter(DisplayOption option) {
	return base_.addFilter(option);
}

template<typename T, size_t C>
inline BasicLocalStatMapper::Options LocalStatMapper<T, C>::defaultOptions() {
	return base_.defaultOptions();
}

template<typename T, size_t C>
inline BasicLocalStatMapper::Options LocalStatMapper<T, C>::bind(
		T srcId, ParamId targetId) {
	return base_.bind(srcId, targetId);
}

template<typename T, size_t C>
inline void LocalStatMapper<T, C>::setFraction(T id, T denomId) {
	base_.setFraction(id, denomId);
}

template<typename T, size_t C>
inline const BasicLocalStatMapper* LocalStatMapper<T, C>::findBase(
		const LocalStatMapper *mapper) {
	if (mapper == NULL) {
		return NULL;
	}
	return &mapper->base_;
}


template<typename T, size_t C>
inline LocalStatTable<T, C>::LocalStatTable(const Mapper *mapper) :
		mapper_(mapper) {
}

template<typename T, size_t C>
inline void LocalStatTable<T, C>::assign(const LocalStatTable &src) {
	BasicLocalStatMapper::setAllValues(
			LocalStatValue::toRangeRef(src.values_, C),
			LocalStatValue::toRange(values_, C));
}

template<typename T, size_t C>
inline void LocalStatTable<T, C>:: apply(StatTable &statTable) const {
	BasicLocalStatMapper::applyAllValues(
			Mapper::findBase(mapper_),
			LocalStatValue::toRangeRef(values_, C), statTable);
}

template<typename T, size_t C>
inline void LocalStatTable<T, C>::merge(const LocalStatTable &src) {
	BasicLocalStatMapper::mergeAllValues(
			Mapper::findBase(mapper_),
			LocalStatValue::toRangeRef(src.values_, C),
			LocalStatValue::toRange(values_, C));
}

template<typename T, size_t C>
const LocalStatValue& LocalStatTable<T, C>::operator()(T id) const {
	assert(0 <= id && static_cast<size_t>(id) < C);
	return values_[id];
}

template<typename T, size_t C>
LocalStatValue& LocalStatTable<T, C>::operator()(T id) {
	assert(0 <= id && static_cast<size_t>(id) < C);
	return values_[id];
}


inline void StatStopwatch::start() {
	base_.start();
}

inline uint32_t StatStopwatch::stop() {
	const uint32_t intervalMillis = base_.stop();
	const uint64_t microTime = base_.elapsedNanos() / 1000;
	assert(value_ != NULL);
	value_->set(microTime);
	return intervalMillis;
}

inline StatStopwatch::StatStopwatch(LocalStatValue *value) :
		value_(value) {
}


inline StatStopwatch::Holder::Holder() :
		watch_(NULL) {
}

inline void StatStopwatch::Holder::initialize(LocalStatValue &value) {
	assert(watch_.value_ == NULL);
	watch_.value_ = &value;
}

inline StatStopwatch& StatStopwatch::Holder::get() {
	assert(watch_.value_ != NULL);
	return watch_;
}


inline StatStopwatch::NonStat::NonStat() :
		watch_(&nonStatValue_) {
}

inline StatStopwatch& StatStopwatch::NonStat::get() {
	return watch_;
}


template<typename T, size_t C>
inline TimeStatTable<T, C>::TimeStatTable(BaseTable &baseTable) {
	for (size_t i = 0; i < C; i++) {
		watchList_[i].initialize(baseTable(static_cast<T>(i)));
	}
}

template<typename T, size_t C>
inline StatStopwatch& TimeStatTable<T, C>::operator()(T id) {
	assert(0 <= id && static_cast<size_t>(id) < C);
	return watchList_[id].get();
}

/*!
	@brief Parameter ID of config table
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
	CONFIG_TABLE_SEC,


	CONFIG_TABLE_ROOT_NOTIFICATION_ADDRESS,

	CONFIG_TABLE_DS_PARTITION_NUM,
	CONFIG_TABLE_DS_STORE_BLOCK_SIZE,
	CONFIG_TABLE_DS_STORE_BLOCK_EXTENT_SIZE, 

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
	CONFIG_TABLE_CS_CHECK_RULE_INTERVAL,
	CONFIG_TABLE_CS_DROP_CHECK_INTERVAL,

	CONFIG_TABLE_CS_NOTIFICATION_INITIAL_INTERVAL,


	CONFIG_TABLE_SYNC_TIMEOUT_INTERVAL,

	CONFIG_TABLE_SYNC_LONG_SYNC_TIMEOUT_INTERVAL,
	CONFIG_TABLE_SYNC_LONG_SYNC_MAX_MESSAGE_SIZE,
	CONFIG_TABLE_SYNC_LOG_MAX_MESSAGE_SIZE,
	CONFIG_TABLE_SYNC_CHUNK_MAX_MESSAGE_SIZE,

	CONFIG_TABLE_SYNC_APPROXIMATE_GAP_LSN,
	CONFIG_TABLE_SYNC_LOCKCONFLICT_INTERVAL,
	CONFIG_TABLE_SYNC_APPROXIMATE_WAIT_INTERVAL,
	CONFIG_TABLE_SYNC_SHORTTERM_LIMIT_QUEUE_SIZE,
	CONFIG_TABLE_SYNC_SHORTTERM_LOWLOAD_LOG_INTERVAL,
	CONFIG_TABLE_SYNC_SHORTTERM_HIGHLOAD_LOG_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_LIMIT_QUEUE_SIZE,
	CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_LOG_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_LOG_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_CHUNK_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_CHUNK_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_DUMP_CHUNK_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_LOWLOAD_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_HIGHLOAD_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL_COUNT,
	CONFIG_TABLE_SYNC_LOG_INTERRUPTION_INTERVAL,
	CONFIG_TABLE_SYNC_REDO_LOG_REPLICATION_TIMEOUT_INTERVAL,
	CONFIG_TABLE_SYNC_REDO_LOG_ERROR_KEEP_INTERVAL,
	CONFIG_TABLE_SYNC_REDO_LOG_CHECK_REPLICATION_INTERVAL,
	CONFIG_TABLE_SYNC_REDO_LOG_MAX_MESSAGE_SIZE,
	CONFIG_TABLE_SYNC_REDO_LOG_RETRY_TIMEOUT_INTERVAL,
	CONFIG_TABLE_SYNC_REDO_LOG_RETRY_CHECK_INTERVAL,
	CONFIG_TABLE_SYNC_ENABLE_KEEP_LOG,
	CONFIG_TABLE_SYNC_KEEP_LOG_INTERVAL,
	CONFIG_TABLE_SYNC_KEEP_LOG_LSN_DIFFERENCE,
	CONFIG_TABLE_SYNC_ENABLE_PARALLEL_SYNC,
	CONFIG_TABLE_SYNC_MAX_PARALLEL_SYNC_NUM,
	CONFIG_TABLE_SYNC_MAX_PARALLEL_CHUNK_SYNC_NUM,
	CONFIG_TABLE_SYNC_LONGTERM_CHECK_INTERVAL,
	CONFIG_TABLE_SYNC_SYNC_POLICY,
	CONFIG_TABLE_SYNC_KEEP_LOG_CHECK_INTERVAL,
	CONFIG_TABLE_SYNC_LONGTERM_LOG_WAIT_INTERVAL,
	CONFIG_TABLE_SYNC_LOG_SYNC_LSN_DIFFERENCE,

	CONFIG_TABLE_SYS_SERVER_SSL_MODE,
	CONFIG_TABLE_SYS_CLUSTER_SSL_MODE,
	CONFIG_TABLE_SYS_SSL_PROTOCOL_MAX_VERSION,

	CONFIG_TABLE_TXN_NOTIFICATION_ADDRESS,
	CONFIG_TABLE_TXN_NOTIFICATION_PORT,
	CONFIG_TABLE_TXN_NOTIFICATION_INTERVAL,
	CONFIG_TABLE_TXN_REPLICATION_MODE,
	CONFIG_TABLE_TXN_REPLICATION_TIMEOUT_INTERVAL,
	CONFIG_TABLE_TXN_AUTHENTICATION_TIMEOUT_INTERVAL,

	CONFIG_TABLE_CS_NOTIFICATION_MEMBER,
	CONFIG_TABLE_CS_NOTIFICATION_PROVIDER,
	CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_URL,
	CONFIG_TABLE_CS_NOTIFICATION_PROVIDER_UPDATE_INTERVAL,

	CONFIG_TABLE_CS_NOTIFICATION_INTERFACE_ADDRESS,
	CONFIG_TABLE_TXN_NOTIFICATION_INTERFACE_ADDRESS,

	CONFIG_TABLE_CS_ABNORMAL_AUTO_SHUTDOWN,

	CONFIG_TABLE_CS_RACK_ZONE_AWARENESS,
	CONFIG_TABLE_CS_RACK_ZONE_ID,

	CONFIG_TABLE_CS_GOAL_ASSIGNMENT_RULE,
	CONFIG_TABLE_CS_ENABLE_STABLE_GOAL,
	CONFIG_TABLE_CS_STABLE_GOAL_POLICY,
	CONFIG_TABLE_CS_STABLE_GOAL_FILE_NAME,
	CONFIG_TABLE_CS_GENERATE_INITIAL_STABLE_GOAL,

	CONFIG_TABLE_CS_ENABLE_STANDBY_MODE,
	CONFIG_TABLE_CS_STANDBY_MODE,
	CONFIG_TABLE_CS_STANDBY_MODE_FILE_NAME,
	
	CONFIG_TABLE_CS_IMMEDIATE_PARTITION_CHECK,
	CONFIG_TABLE_CS_PARTITION_NODE_BALANCE,


	CONFIG_TABLE_SEC_AUTHENTICATION,
	CONFIG_TABLE_SEC_LDAP_ROLE_MANAGEMENT,
	CONFIG_TABLE_SEC_LDAP_URL,
	CONFIG_TABLE_SEC_LDAP_USER_DN_PREFIX,
	CONFIG_TABLE_SEC_LDAP_USER_DN_SUFFIX,
	CONFIG_TABLE_SEC_LDAP_BIND_DN,
	CONFIG_TABLE_SEC_LDAP_BIND_PASSWORD,
	CONFIG_TABLE_SEC_LDAP_BASE_DN,
	CONFIG_TABLE_SEC_LDAP_SEARCH_ATTRIBUTE,
	CONFIG_TABLE_SEC_LDAP_MEMBER_OF_ATTRIBUTE,
	CONFIG_TABLE_SEC_LDAP_WAIT_TIME,
	CONFIG_TABLE_SEC_LOGIN_WAIT_TIME,
	CONFIG_TABLE_SEC_LOGIN_REPETITION_NUM,


	CONFIG_TABLE_ROOT_SERVICE_ADDRESS,

	CONFIG_TABLE_DS_DB_PATH,
	CONFIG_TABLE_DS_BACKUP_PATH,
	CONFIG_TABLE_DS_SYNC_TEMP_PATH,  
	CONFIG_TABLE_DS_ARCHIVE_TEMP_PATH,  
	CONFIG_TABLE_DS_CHUNK_MEMORY_LIMIT,
	CONFIG_TABLE_DS_STORE_MEMORY_LIMIT,
	CONFIG_TABLE_DS_CONCURRENCY,
	CONFIG_TABLE_DS_LOG_WRITE_MODE,
	CONFIG_TABLE_DS_AFFINITY_GROUP_SIZE,
	CONFIG_TABLE_DS_STORE_COMPRESSION_MODE,
	CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_LOAD_CHECK_PERIOD,
	CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_MILLIS,
	CONFIG_TABLE_DS_IO_WARNING_THRESHOLD_TIME,
	CONFIG_TABLE_DS_RESULT_SET_MEMORY_LIMIT,
	CONFIG_TABLE_DS_RESULT_SET_CACHE_MEMORY, 
	CONFIG_TABLE_DS_RECOVERY_LEVEL,

	CONFIG_TABLE_DS_PERSISTENCY_MODE,
	CONFIG_TABLE_DS_RETAINED_FILE_COUNT,

	CONFIG_TABLE_DS_BACKGROUND_MIN_RATE,
	CONFIG_TABLE_DS_ERASABLE_EXPIRED_TIME,
	CONFIG_TABLE_DS_ESTIMATED_ERASABLE_EXPIRED_TIME,
	CONFIG_TABLE_DS_BATCH_SCAN_NUM,
	CONFIG_TABLE_DS_ROW_ARRAY_RATE_EXPONENT,
	CONFIG_TABLE_DS_ROW_ARRAY_SIZE_CONTROL_MODE,
	CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_INTERVAL,
	CONFIG_TABLE_DS_PARTITION_BATCH_FREE_CHECK_CONTAINER_COUNT,
	CONFIG_TABLE_DS_STORE_MEMORY_AGING_SWAP_RATE,
	CONFIG_TABLE_DS_STORE_MEMORY_COLD_RATE,
	CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_SHIFTABLE_MEMORY_RATE,
	CONFIG_TABLE_DS_STORE_MEMORY_REDISTRIBUTE_EMA_HALF_LIFE_PERIOD,
	CONFIG_TABLE_DS_CHECKPOINT_FILE_FLUSH_SIZE,  
	CONFIG_TABLE_DS_CHECKPOINT_FILE_AUTO_CLEAR_CACHE,  
	CONFIG_TABLE_DS_STORE_BUFFER_TABLE_SIZE_RATE,  
	CONFIG_TABLE_DS_DB_FILE_SPLIT_COUNT,
	CONFIG_TABLE_DS_DB_FILE_SPLIT_STRIPE_SIZE,
	CONFIG_TABLE_DS_LOG_FILE_CLEAR_CACHE_INTERVAL,  
	CONFIG_TABLE_DS_TRANSACTION_LOG_PATH,  
	CONFIG_TABLE_DS_LOG_MEMORY_LIMIT,  
	CONFIG_TABLE_DS_ENABLE_AUTO_ARCHIVE, 
	CONFIG_TABLE_DS_AUTO_ARCHIVE_NAME,  
	CONFIG_TABLE_DS_ENABLE_AUTO_ARCHIVE_OUTPUT_CLUSTER_INFO,  
	CONFIG_TABLE_DS_AUTO_ARCHIVE_OUTPUT_CLUSTER_INFO_PATH,  
	CONFIG_TABLE_DS_AUTO_ARCHIVE_OUTPUT_UUID_INFO,  
	CONFIG_TABLE_DS_RECOVERY_CONCURRENCY, 

	CONFIG_TABLE_CP_CHECKPOINT_INTERVAL,

	CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL_MILLIS,
	CONFIG_TABLE_CP_CHECKPOINT_COPY_INTERVAL,

	CONFIG_TABLE_CP_PARTIAL_CHECKPOINT_INTERVAL,  

	CONFIG_TABLE_CS_LISTEN_ADDRESS,
	CONFIG_TABLE_CS_LISTEN_PORT,
	CONFIG_TABLE_CS_SERVICE_ADDRESS,
	CONFIG_TABLE_CS_SERVICE_PORT,

	CONFIG_TABLE_CS_CONNECTION_LIMIT,
	CONFIG_TABLE_CS_CHECKPOINT_DELAY_INTERVAL,
	CONFIG_TABLE_CS_CATCHUP_PROMOTION_CHECK_INTERVAL,

	CONFIG_TABLE_CS_PARTITION_ASSIGN_MODE,

	CONFIG_TABLE_SYNC_LISTEN_ADDRESS,
	CONFIG_TABLE_SYNC_LISTEN_PORT,
	CONFIG_TABLE_SYNC_SERVICE_ADDRESS,
	CONFIG_TABLE_SYNC_SERVICE_PORT,

	CONFIG_TABLE_SYS_LISTEN_ADDRESS,
	CONFIG_TABLE_SYS_LISTEN_PORT,
	CONFIG_TABLE_SYS_SERVICE_ADDRESS,
	CONFIG_TABLE_SYS_SERVICE_PORT,
	CONFIG_TABLE_SYS_SERVICE_SSL_PORT,
	CONFIG_TABLE_SYS_EVENT_LOG_PATH,
	CONFIG_TABLE_SYS_SECURITY_PATH,
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
	CONFIG_TABLE_TXN_WORK_MEMORY_LIMIT,
	CONFIG_TABLE_TXN_TOTAL_MESSAGE_MEMORY_LIMIT,
	CONFIG_TABLE_TXN_USE_KEEPALIVE,
	CONFIG_TABLE_TXN_KEEPALIVE_IDLE,
	CONFIG_TABLE_TXN_KEEPALIVE_INTERVAL,
	CONFIG_TABLE_TXN_KEEPALIVE_COUNT,

	CONFIG_TABLE_TXN_LOCAL_SERVICE_ADDRESS,
	CONFIG_TABLE_TXN_USE_MULTITENANT_MODE,
	CONFIG_TABLE_TXN_USE_REQUEST_CONSTRAINT,
	CONFIG_TABLE_TXN_CHECK_DATABASE_STATS_INTERVAL,
	CONFIG_TABLE_TXN_LIMIT_DELAY_TIME,
	CONFIG_TABLE_TXN_USE_SCAN_STAT,
	CONFIG_TABLE_TRIG_TIMEOUT_INTERVAL,

	CONFIG_TABLE_TXN_SITE_REPLICATION,
	CONFIG_TABLE_TXN_SITE_REPLICATION_ENABLE,
	CONFIG_TABLE_TXN_SITE_REPLICATION_SITE_CONNECTION_ID,
	CONFIG_TABLE_TXN_SITE_REPLICATION_SITE_HEARTBEAT_INTERVAL,
	CONFIG_TABLE_TXN_SITE_REPLICATION_SITE_HEARTBEAT_RETRY_COUNT,
	CONFIG_TABLE_TXN_SITE_REPLICATION_PARTITION_CHECK_INTERVAL,
	CONFIG_TABLE_TXN_SITE_REPLICATION_LIMIT_LSN_DIFFERENCE,
	CONFIG_TABLE_TXN_SITE_REPLICATION_LIMIT_UPDATE_DELAY_TIME,
	CONFIG_TABLE_TXN_SITE_REPLICATION_MEMBER,
	CONFIG_TABLE_TXN_SITE_REPLICATION_KEEP_LOG_FILE_COUNT,
	CONFIG_TABLE_TXN_SITE_REPLICATION_SYNC_WAIT_TIME,

	CONFIG_TABLE_TRACE_OUTPUT_TYPE,
	CONFIG_TABLE_TRACE_FILE_COUNT,

	CONFIG_TABLE_TRACE_AUDIT_LOGS,
	CONFIG_TABLE_TRACE_AUDIT_LOGS_PATH,
	CONFIG_TABLE_TRACE_AUDIT_FILE_LIMIT,
	CONFIG_TABLE_TRACE_AUDIT_MESSAGE_LIMIT,
	CONFIG_TABLE_TRACE_AUDIT_FILE_COUNT,

	CONFIG_TABLE_TRACE_TRACER_ID_START,
	CONFIG_TABLE_TRACE_DEFAULT,
	CONFIG_TABLE_TRACE_MAIN,
	CONFIG_TABLE_TRACE_BASE_CONTAINER,
	CONFIG_TABLE_TRACE_DATA_STORE,
	CONFIG_TABLE_TRACE_KEY_DATA_STORE,
	CONFIG_TABLE_TRACE_COLLECTION,
	CONFIG_TABLE_TRACE_TIME_SERIES,
	CONFIG_TABLE_TRACE_CHUNK_MANAGER,
	CONFIG_TABLE_TRACE_CHUNK_MANAGER_DETAIL,
	CONFIG_TABLE_TRACE_CHUNK_MANAGER_IO_DETAIL,
	CONFIG_TABLE_TRACE_OBJECT_MANAGER,
	CONFIG_TABLE_TRACE_CHECKPOINT_FILE,
	CONFIG_TABLE_TRACE_CHECKPOINT_SERVICE,
	CONFIG_TABLE_TRACE_CHECKPOINT_SERVICE_DETAIL,
	CONFIG_TABLE_TRACE_CHECKPOINT_SERVICE_STATUS_DETAIL,
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
	CONFIG_TABLE_TRACE_CLUSTER_INFO_TRACE,
	CONFIG_TABLE_TRACE_SYSTEM,
	CONFIG_TABLE_TRACE_BTREE_MAP,
	CONFIG_TABLE_TRACE_AUTHENTICATION_TIMEOUT,
	CONFIG_TABLE_TRACE_DATASTORE_BACKGROUND,
	CONFIG_TABLE_TRACE_SYNC_DETAIL,
	CONFIG_TABLE_TRACE_CLUSTER_DETAIL,
	CONFIG_TABLE_TRACE_SQL_DETAIL,
	CONFIG_TABLE_TRACE_LONG_ARCHIVE,

	CONFIG_TABLE_TRACE_ZLIB_UTILS,
	CONFIG_TABLE_TRACE_SIZE_MONITOR,
	CONFIG_TABLE_TRACE_CLUSTER_DUMP,
	CONFIG_TABLE_TRACE_AUTH_OPERATION,
	CONFIG_TABLE_TRACE_PARTITION,
	CONFIG_TABLE_TRACE_PARTITION_DETAIL,
	CONFIG_TABLE_TRACE_DATA_EXPIRATION_DETAIL,

	CONFIG_TABLE_TRACE_CONNECTION_DETAIL,
	CONFIG_TABLE_TRACE_REDO_MANAGER,
	CONFIG_TABLE_TRACE_SITE_REPLICATION,

	CONFIG_TABLE_TRACE_TRACER_ID_END,

	CONFIG_TABLE_TRACE_AUDIT_TRACER_ID_START,
	CONFIG_TABLE_TRACE_AUDIT_SYSTEM,
	CONFIG_TABLE_TRACE_AUDIT_STAT,
	CONFIG_TABLE_TRACE_AUDIT_SQL_READ,
	CONFIG_TABLE_TRACE_AUDIT_SQL_WRITE,
	CONFIG_TABLE_TRACE_AUDIT_NOSQL_READ,
	CONFIG_TABLE_TRACE_AUDIT_NOSQL_WRITE,
	CONFIG_TABLE_TRACE_AUDIT_CONNECT,
	CONFIG_TABLE_TRACE_AUDIT_DDL,
	CONFIG_TABLE_TRACE_AUDIT_DCL,
	CONFIG_TABLE_TRACE_AUDIT_TRACER_ID_END,

	CONFIG_TABLE_DEV_AUTO_JOIN_CLUSTER,
	CONFIG_TABLE_DEV_RECOVERY,
	CONFIG_TABLE_DEV_RECOVERY_ONLY_CHECK_CHUNK,
	CONFIG_TABLE_DEV_RECOVERY_DOWN_POINT,
	CONFIG_TABLE_DEV_RECOVERY_DOWN_COUNT,
	CONFIG_TABLE_DEV_CLUSTER_NODE_DUMP,
	CONFIG_TABLE_DEV_TRACE_SECRET,
	CONFIG_TABLE_DEV_SIMPLE_VERSION,
	CONFIG_TABLE_DEV_FULL_VERSION,

	
	CONFIG_TABLE_SEC_USER_CACHE_SIZE,
	CONFIG_TABLE_SEC_USER_CACHE_UPDATE_INTERVAL,
	CONFIG_TABLE_TXN_PUBLIC_SERVICE_ADDRESS,

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

	static const char8_t* getGroupSymbol(const char8_t *src, bool reverse);

private:
	typedef std::map<std::string, std::string> SymbolTable;

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
	@brief Parameter ID of statistics table
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
	STAT_TABLE_ROOT_SQL_TEMP_STORE,
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
	STAT_TABLE_CS_INVALID_PARTITION_STATUS,

	STAT_TABLE_CS_NOTIFICATION_MODE,
	STAT_TABLE_CS_NOTIFICATION_MEMBER,

	STAT_TABLE_CS_CLUSTER_REVISION_ID,

	STAT_TABLE_CS_CURRENT_RULE,
	STAT_TABLE_CS_APPLY_RULE_LIMIT_TIME,
	STAT_TABLE_CS_CLUSTER_REVISION_NO,

	STAT_TABLE_CS_AUTO_GOAL,
	STAT_TABLE_CS_RACKZONE_ID,
	STAT_TABLE_CS_PARTITION_REPLICATION_PROGRESS,
	STAT_TABLE_CS_PARTITION_GOAL_PROGRESS,
	STAT_TABLE_CS_GOAL_DEFAULT_RULE,
	STAT_TABLE_CS_GOAL_CURRENT_RULE,
	STAT_TABLE_CS_ENABLE_STABLE_GOAL,
	STAT_TABLE_CS_STABLE_GOAL_POLICY,
	STAT_TABLE_CS_GENERATE_INITIAL_STABLE_GOAL,
	STAT_TABLE_CS_STANDBY_MODE,

	STAT_TABLE_PERF_OWNER_COUNT,
	STAT_TABLE_PERF_BACKUP_COUNT,
	STAT_TABLE_PERF_TOTAL_OWNER_LSN,
	STAT_TABLE_PERF_TOTAL_BACKUP_LSN,
	STAT_TABLE_PERF_TOTAL_OTHER_LSN,
	STAT_TABLE_PERF_TOTAL_CLUSTER_OTHER_LSN,

	STAT_TABLE_CP_START_TIME,
	STAT_TABLE_CP_END_TIME,
	STAT_TABLE_CP_MODE,
	STAT_TABLE_CP_PENDING_PARTITION,
	STAT_TABLE_CP_NORMAL_CHECKPOINT_OPERATION,
	STAT_TABLE_CP_REQUESTED_CHECKPOINT_OPERATION,
	STAT_TABLE_CP_PERIODIC_CHECKPOINT,  
	STAT_TABLE_CP_BACKUP_OPERATION,
	STAT_TABLE_CP_ARCHIVE_LOG,
	STAT_TABLE_CP_DUPLICATE_LOG,
	STAT_TABLE_CP_AUTO_ARCHIVE,
	STAT_TABLE_CP_AUTO_ARCHIVE_NAME,


	STAT_TABLE_RM_PROGRESS_RATE,

	STAT_TABLE_PERF_CURRENT_TIME,
	STAT_TABLE_PERF_PROCESS_MEMORY,	
	STAT_TABLE_PERF_PEAK_PROCESS_MEMORY,	

	STAT_TABLE_PERF_DATA_FILE_SIZE,	
	STAT_TABLE_PERF_DATA_FILE_USAGE_RATE,	

	STAT_TABLE_PERF_STORE_COMPRESSION_MODE,
	STAT_TABLE_PERF_DATA_FILE_ALLOCATE_SIZE,	

	STAT_TABLE_PERF_CURRENT_CHECKPOINT_WRITE_BUFFER_SIZE,
	STAT_TABLE_PERF_CHECKPOINT_WRITE, 
	STAT_TABLE_PERF_CHECKPOINT_WRITE_SIZE,	
	STAT_TABLE_PERF_CHECKPOINT_WRITE_TIME,
	STAT_TABLE_PERF_CHECKPOINT_WRITE_COMPRESS_TIME, 
	STAT_TABLE_PERF_CHECKPOINT_MEMORY_LIMIT,
	STAT_TABLE_PERF_CHECKPOINT_MEMORY,



	STAT_TABLE_PERF_TXN_NUM_CONNECTION,	
	STAT_TABLE_PERF_TXN_NUM_SESSION,	
	STAT_TABLE_PERF_TXN_NUM_TXN,	
	STAT_TABLE_PERF_TXN_NUM_BACKGROUND,	
	STAT_TABLE_PERF_TXN_NUM_NO_EXPIRE_TXN,	
	STAT_TABLE_PERF_TXN_TOTAL_LOCK_CONFLICT_COUNT,	
	STAT_TABLE_PERF_TXN_TOTAL_READ_OPERATION,	
	STAT_TABLE_PERF_TXN_TOTAL_WRITE_OPERATION,	
	STAT_TABLE_PERF_TXN_TOTAL_ROW_READ,	
	STAT_TABLE_PERF_TXN_TOTAL_ROW_WRITE,	
	STAT_TABLE_PERF_TXN_DETAIL,
	STAT_TABLE_PERF_TXN_TOTAL_BACKGROUND_OPERATION,	
	STAT_TABLE_PERF_TXN_TOTAL_NO_EXPIRE_OPERATION, 
	STAT_TABLE_PERF_TXN_TOTAL_ABORT_DDL,	
	STAT_TABLE_PERF_TXN_TOTAL_REP_TIMEOUT,	
	STAT_TABLE_PERF_TXN_BACKGROUND_MIN_RATE,

	STAT_TABLE_PERF_TXN_DETAIL_DISABLE_TIMEOUT_CHECK_PARTITION_COUNT,
	STAT_TABLE_PERF_TXN_EE_CLUSTER,
	STAT_TABLE_PERF_TXN_EE_SYNC,
	STAT_TABLE_PERF_TXN_EE_TRANSACTION,
	STAT_TABLE_PERF_TXN_EE_SQL,

	STAT_TABLE_PERF_TXN_TOTAL_INTERNAL_CONNECTION_COUNT,
	STAT_TABLE_PERF_TXN_TOTAL_EXTERNAL_CONNECTION_COUNT,

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

	STAT_TABLE_PERF_DS_SWAP_READ_UNCOMPRESS_TIME,
	STAT_TABLE_PERF_DS_SWAP_WRITE_COMPRESS_TIME,
	STAT_TABLE_PERF_DS_SYNC_READ_UNCOMPRESS_TIME,
	STAT_TABLE_PERF_DS_RECOVERY_READ_UNCOMPRESS_TIME,

	STAT_TABLE_PERF_DS_BUFFER_HASH_COLLISION_COUNT,
	STAT_TABLE_PERF_DS_DETAIL_POOL_BUFFER_MEMORY,
	STAT_TABLE_PERF_DS_DETAIL_POOL_CHECKPOINT_MEMORY,

	STAT_TABLE_PERF_DS_EXP,
	STAT_TABLE_PERF_DS_EXP_ERASABLE_EXPIRED_TIME,
	STAT_TABLE_PERF_DS_EXP_LATEST_EXPIRATION_CHECK_TIME,
	STAT_TABLE_PERF_DS_EXP_ESTIMATED_ERASABLE_EXPIRED_TIME,
	STAT_TABLE_PERF_DS_EXP_ESTIMATED_BATCH_FREE,
	STAT_TABLE_PERF_DS_EXP_LAST_BATCH_FREE,
	STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_NUM,
	STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_TOTAL_TIME,
	STAT_TABLE_PERF_DS_EXP_BATCH_SCAN_NUM,
	STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_COUNT,
	STAT_TABLE_PERF_DS_CHECKPOINT_FILE_FLUSH_TIME,

	STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_COUNT,
	STAT_TABLE_PERF_DS_LOG_FILE_FLUSH_TIME,

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
	STAT_TABLE_PERF_MEM_ALL_LOCAL_CACHED,
	STAT_TABLE_PERF_MEM_ALL_ELEMENT_COUNT,
	STAT_TABLE_PERF_MEM_PROCESS_MEMORY_GAP,	
	STAT_TABLE_PERF_MEM_DS_STORE_TOTAL,
	STAT_TABLE_PERF_MEM_DS_STORE_CACHED,
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
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_STATE_TOTAL,
	STAT_TABLE_PERF_MEM_WORK_TRANSACTION_STATE_CACHED,
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
	STAT_TABLE_DISPLAY_OPTIONAL_NOTIFICATION_MEMBER,
	STAT_TABLE_DISPLAY_ADDRESS_CLUSTER,
	STAT_TABLE_DISPLAY_ADDRESS_TRANSACTION,
	STAT_TABLE_DISPLAY_ADDRESS_SYNC,
	STAT_TABLE_DISPLAY_ADDRESS_SQL,
	STAT_TABLE_DISPLAY_ADDRESS_SECURE
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
	ALLOCATOR_GROUP_TXN_STATE,
	ALLOCATOR_GROUP_ID_END
};

#endif
