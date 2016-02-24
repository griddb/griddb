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
	@brief Implementation of ConfigTable
*/
#include "config_table.h"

#include "util/file.h"
#include "util/container.h"
#include "sha2.h"
#include "picojson.h"
#include <limits>
#include <algorithm>
#include <cstring>

UTIL_TRACER_DECLARE(MAIN);
UTIL_TRACER_DECLARE(SYSTEM);

#define GS_PARAM_TABLE_ADD_PROBLEM(code, message) \
	addProblem( \
		GS_EXCEPTION_NAMED_CODE(code), \
		UTIL_EXCEPTION_CREATE_MESSAGE_CHARS(message))

static void loadAllAsString(
		const char8_t *fileName, util::NormalXArray<uint8_t> &buf) {
	const int32_t maxFileSize = 1024 * 1024;

	try {
		if (!util::FileSystem::exists(fileName)) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_FILE_NOT_FOUND, "");
		}

		util::NamedFile file;
		file.open(fileName, util::FileFlag::TYPE_READ_ONLY);

		util::FileStatus status;
		file.getStatus(&status);
		const int64_t size = status.getSize();
		if (size > maxFileSize) {
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_FILE_TOO_LARGE,
				"Too large file size (limitBytes=" << maxFileSize << ")");
		}

		buf.resize(static_cast<size_t>(size + 1));
		file.read(buf.data(), static_cast<size_t>(size));
		file.close();
		buf[buf.size() - 1] = '\0';
	}
	catch (std::exception &e) {
		GS_RETHROW_USER_ERROR(e, "Failed to load file (fileName=" <<
				fileName << ", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
	}
}

static void replaceAll(std::string &str, const std::string &from, const std::string &to) {
    std::string::size_type pos = 0;
    while (pos = str.find(from, pos), pos != std::string::npos) {
        str.replace(pos, from.size(), to);
        pos += to.size();
    }
}

u8string JsonUtils::parseAll(
		picojson::value &value, const char8_t *begin, const char8_t *end) {
	u8string err;
	const char8_t *last = picojson::parse(value, begin, end, &err);

	if (!err.empty()) {
		value = picojson::value();
		return err;
	}
	else if (last != end) {
		bool found = false;
		for (const char8_t *i = last; i != end; i++) {
			found |= (strchr("\r\n\t ", *i) == NULL);
		}

		if (found) {
			value = picojson::value();
			u8string content;
			size_t line = 0;
			{
				util::NormalIStringStream iss(u8string(begin, last));
				while (std::getline(iss, content)) {
					line++;
				}
			}
			{
				util::NormalIStringStream iss(u8string(begin, end));
				for (size_t i = 0; i < line && std::getline(iss, content); i++) {
				}
			}
			util::NormalOStringStream oss;
			oss << "Failed to parse json by extra part (line=" << line <<
					", content=\"" << content << "\")";
			err = oss.str();
		}
	}

	return err;
}

ParamValue::ParamValue(const Null&) : type_(PARAM_TYPE_NULL) {
}

ParamValue::~ParamValue() {
	clear();
}

ParamValue::ParamValue(const char8_t *base) : type_(PARAM_TYPE_STRING) {
	assert(base != NULL);
	storage_.asString_.construct(base, NULL);
}

ParamValue::ParamValue(const int64_t &base) : type_(PARAM_TYPE_INT64) {
	storage_.asInt64_ = base;
}

ParamValue::ParamValue(const int32_t &base) : type_(PARAM_TYPE_INT32) {
	storage_.asInt32_ = base;
}

ParamValue::ParamValue(const double &base) : type_(PARAM_TYPE_DOUBLE) {
	storage_.asDouble_ = base;
}

ParamValue::ParamValue(const bool &base) : type_(PARAM_TYPE_BOOL) {
	storage_.asBool_ = base;
}

ParamValue::ParamValue(const picojson::value &base) : type_(PARAM_TYPE_NULL) {
	if (base.is<bool>()) {
		type_ = PARAM_TYPE_BOOL;
		storage_.asBool_ = base.get<bool>();
	}
	else if (base.is<double>()) {
		type_ = PARAM_TYPE_DOUBLE;
		storage_.asDouble_ = base.get<double>();
	}
	else if (base.is<std::string>()) {
		type_ = PARAM_TYPE_STRING;
		storage_.asString_.construct(base.get<std::string>().c_str(), NULL);
	}
	else if (!base.is<picojson::null>()) {
		type_ = PARAM_TYPE_JSON;
		storage_.asJSON_.construct(&base, NULL);
	}
}

ParamValue::ParamValue(const ParamValue &another, const Allocator &alloc) :
		type_(PARAM_TYPE_NULL) {
	assign(another, &alloc);
}

void ParamValue::assign(const ParamValue &another, const Allocator &alloc) {
	assign(another, &alloc);
}

bool ParamValue::operator==(const ParamValue &another) const {
	if (type_ != another.type_) {
		return false;
	}

	switch (type_) {
	case PARAM_TYPE_NULL:
		return true;
	case PARAM_TYPE_STRING:
		return (strcmp(storage_.asString_.get(),
				another.storage_.asString_.get()) == 0);
	case PARAM_TYPE_INT64:
		return (storage_.asInt64_ == another.storage_.asInt64_);
	case PARAM_TYPE_INT32:
		return (storage_.asInt32_ == another.storage_.asInt32_);
	case PARAM_TYPE_DOUBLE:
		return (storage_.asDouble_ == another.storage_.asDouble_);
		break;
	case PARAM_TYPE_BOOL:
		return !(storage_.asBool_ ^ another.storage_.asBool_);
	case PARAM_TYPE_JSON:
		return (*storage_.asJSON_.get() ==
				*another.storage_.asJSON_.get());
	default:
		assert(false);
		return false;
	}
}

bool ParamValue::operator!=(const ParamValue &another) const {
	return !(*this == another);
}

ParamValue::Type ParamValue::getType() const {
	return type_;
}

void ParamValue::toJSON(picojson::value &dest) const {
	switch (type_) {
	case PARAM_TYPE_NULL:
		dest = picojson::value();
		break;
	case PARAM_TYPE_STRING:
		{
			picojson::value src(std::string(storage_.asString_.get()));
			dest.swap(src);
		}
		break;
	case PARAM_TYPE_INT64:
		dest = picojson::value(static_cast<double>(storage_.asInt64_));
		break;
	case PARAM_TYPE_INT32:
		dest = picojson::value(static_cast<double>(storage_.asInt32_));
		break;
	case PARAM_TYPE_DOUBLE:
		dest = picojson::value(storage_.asDouble_);
		break;
	case PARAM_TYPE_BOOL:
		dest = picojson::value(storage_.asBool_);
		break;
	case PARAM_TYPE_JSON:
		dest = *storage_.asJSON_.get();
		break;
	default:
		assert(false);
		break;
	}
}

void ParamValue::format(std::ostream &stream) const {
	try {
		switch (type_) {
		case PARAM_TYPE_INT64:
			stream << storage_.asInt64_;
			break;
		case PARAM_TYPE_INT32:
			stream << storage_.asInt32_;
			break;
		default:
			{
				picojson::value jsonValue;
				toJSON(jsonValue);
				stream << jsonValue.to_str();
			}
			break;
		}
	}
	catch (...) {
		stream.setstate(std::ios::badbit);
		if ((stream.exceptions() & std::ios::badbit) != 0) {
			throw;
		}
	}
}

void ParamValue::clear() {
	switch (type_) {
	case PARAM_TYPE_STRING:
		storage_.asString_.destruct();
		break;
	case PARAM_TYPE_JSON:
		storage_.asJSON_.destruct();
		break;
	default:
		break;
	}
	type_ = PARAM_TYPE_NULL;
}

void ParamValue::assign(const ParamValue &another, const Allocator *alloc) {
	if (this == &another) {
		return;
	}

	clear();
	try {
		type_ = another.type_;
		switch (type_) {
		case PARAM_TYPE_STRING:
			storage_.asString_.construct(
					another.storage_.asString_.get(), alloc);
			break;
		case PARAM_TYPE_INT64:
			storage_.asInt64_ = another.storage_.asInt64_;
			break;
		case PARAM_TYPE_INT32:
			storage_.asInt32_ = another.storage_.asInt32_;
			break;
		case PARAM_TYPE_DOUBLE:
			storage_.asDouble_ = another.storage_.asDouble_;
			break;
		case PARAM_TYPE_BOOL:
			storage_.asBool_ = another.storage_.asBool_;
			break;
		case PARAM_TYPE_JSON:
			storage_.asJSON_.construct(
					another.storage_.asJSON_.get(), alloc);
			break;
		default:
			break;
		}
	}
	catch (...) {
		type_ = PARAM_TYPE_NULL;
		throw;
	}
}

template<> size_t ParamValue::elementCount(const char8_t *value) {
	return strlen(value) + 1;
}

template<> size_t ParamValue::elementCount(const picojson::value*) {
	return 1;
}

template<typename T>
void ParamValue::VariableType<T>::construct(
		const T *value, const Allocator *alloc) {

	if (alloc == NULL) {
		value_.ref_ = value;
		owner_ = false;
	}
	else {
		ValueAllocator *valueAllocator =
				new (static_cast<void*>(&allocator_)) ValueAllocator(*alloc);

		const size_t count = elementCount(value);
		T *dest = valueAllocator->allocate(count);

		try {
			for (size_t i = 0; i < count; i++) {
				new (static_cast<void*>(&dest[i])) T(value[i]);
			}
		}
		catch (...) {
			valueAllocator->deallocate(dest, count);
			throw;
		}

		value_.stored_ = dest;
		owner_ = true;
	}
}

template<typename T>
void ParamValue::VariableType<T>::destruct() {
	if (owner_) {
		ValueAllocator *valueAllocator =
				reinterpret_cast<ValueAllocator*>(&allocator_);

		valueAllocator->deallocate(
				value_.stored_, elementCount(value_.stored_));
		valueAllocator->~ValueAllocator();

		value_.stored_ = NULL;
	}
}

template<typename T>
const T *const& ParamValue::VariableType<T>::get() const {
	return value_.ref_;
}

template struct ParamValue::VariableType<char8_t>;
template struct ParamValue::VariableType<picojson::value>;

std::ostream& operator<<(std::ostream &stream, const ParamValue &value) {
	value.format(stream);
	return stream;
}

UserTable::UserTable() {
}

/*!
	@brief Loads user definition file
*/
void UserTable::load(const char8_t *fileName) {
	util::NormalXArray<uint8_t> buf;
	loadAllAsString(fileName, buf);

	std::string allStr(buf.begin(), buf.end());
	replaceAll(allStr, "\r\n", "\n");
	replaceAll(allStr, "\r", "\n");
	const char8_t *entry = allStr.c_str();
	const char8_t *end = entry + allStr.size();
	while (entry != end) {
		const char8_t *const entryEnd = std::find(entry, end, '\n');
		if (entryEnd == entry) {
			entry++;
			continue;
		}
		const char8_t *const nameEnd = std::find(entry, entryEnd, ',');
		if (nameEnd != entryEnd) {
			const std::string name(entry, nameEnd);
			const std::string digestStr(nameEnd + 1, entryEnd);
			const char *emptyDigest = "";
			if (digestStr.compare(emptyDigest) == 0) {
				GS_THROW_USER_ERROR(GS_ERROR_TXN_AUTH_FAILED, "Password of \"" << name << "\" has not been set");
			}
			digestMap_.insert(std::make_pair(name, digestStr));
		}
		entry = entryEnd;
	}
}

/*!
	@brief Checks user name and digest
*/
bool UserTable::isValidUser(
		const char8_t *name, const char8_t *digest, bool convert) const {
	std::string digestStr;
	if (convert) {
		char data[SHA256_DIGEST_STRING_LENGTH + 1];
		SHA256_Data(reinterpret_cast<const uint8_t*>(digest), strlen(digest), data);
		data[SHA256_DIGEST_STRING_LENGTH] = '\0';
		digestStr = data;
	}
	else {
		digestStr = digest;
	}

	std::map<std::string, std::string>::const_iterator it = digestMap_.find(name);
	if (it != digestMap_.end() && it->second == digestStr) {
		return true;
	}
	return false;
}

/*!
	@brief Checks if a specified user name exists
*/
bool UserTable::exists(
		const char8_t *name) const {

	std::map<std::string, std::string>::const_iterator it = digestMap_.find(name);
	if (it != digestMap_.end()) {
		return true;
	}
	return false;
}


const ParamTable::ParamId ParamTable::PARAM_ID_ROOT = ParamId();

const ParamTable::ExportMode ParamTable::EXPORT_MODE_DEFAULT = 0;
const ParamTable::ExportMode ParamTable::EXPORT_MODE_DIFF_ONLY = (1 << 0);
const ParamTable::ExportMode ParamTable::EXPORT_MODE_SHOW_HIDDEN = (1 << 1);
const int32_t ParamTable::EXPORT_MODE_BASE_BITS = 2;

ParamTable::ParamTable(const Allocator &alloc, util::Tracer *tracer) :
		alloc_(alloc),
		tracer_(tracer),
		valuePool_(util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "paramValue")),
		entryMap_(EntryMap::key_compare(), alloc),
		entryMapCleaner_(entryMap_, valuePool_),
		defaultHandler_(NULL),
		pendingDependencyList_(alloc),
		problemList_(alloc),
		traceEnabled_(true) {

	Entry entry(alloc_);
	entry.group_ = true;
	entryMap_.insert(std::make_pair(PARAM_ID_ROOT, entry));
}

ParamTable::~ParamTable() {
}

/*!
	@brief Adds parameter to ParamTable
*/
void ParamTable::addParam(ParamId groupId, ParamId id, const char8_t *name,
		ValueAnnotation *annotation, bool group, bool failOnFound) {

	EntryMap::iterator groupIt = entryMap_.find(groupId);
	if (groupIt == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	bool found = false;
	EntryMap::iterator entryIt = entryMap_.find(id);
	if (entryIt != entryMap_.end()) {
		found = true;
	}

	const String nameStr(name, alloc_);
	if (nameStr.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	Entry &groupEntry = groupIt->second;
	if (!groupEntry.group_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (groupEntry.subNameMap_.find(nameStr) !=
			groupEntry.subNameMap_.end()) {
		found = true;
	}

	if (found) {
		if (failOnFound ||
				strcmp(entryIt->second.name_.c_str(), name) != 0 ||
				entryIt->second.annotation_ != annotation ||
				entryIt->second.group_ ^ group) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
		return;
	}

	groupEntry.subNameMap_.insert(std::make_pair(nameStr, id));

	Entry entry(alloc_);
	entry.parentId_ = groupId;
	entry.name_ = nameStr;
	entry.annotation_ = annotation;
	entry.group_ = group;
	entryMap_.insert(std::make_pair(id, entry));

	for (DependencyList::iterator it = pendingDependencyList_.begin();
			it != pendingDependencyList_.end();) {
		if (it->second == id) {
			const Dependency dependency = *it;
			it = pendingDependencyList_.erase(it);
			addDependency(dependency.first, dependency.second);
		}
		else {
			++it;
		}
	}
}

/*!
	@brief Adds dependency between parameters
*/
void ParamTable::addDependency(ParamId id, ParamId baseId) {
	if (id == PARAM_ID_ROOT || baseId == PARAM_ID_ROOT) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	EntryMap::iterator it = entryMap_.find(id);
	EntryMap::iterator baseIt = entryMap_.find(baseId);

	if (it == entryMap_.end() || it->second.group_ ||
			it->second.handler_ != NULL ||
			it->second.baseId_ != PARAM_ID_ROOT) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (baseIt == entryMap_.end()) {
		for (DependencyList::iterator it = pendingDependencyList_.begin();
				it != pendingDependencyList_.end(); ++it) {
			if (it->first == id) {
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}
		}
		pendingDependencyList_.push_back(Dependency(id, baseId));
		return;
	}

	if (baseIt->second.group_ || baseIt->second.handler_ != NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	for (EntryMap::const_iterator refIt = baseIt;
			refIt->first != PARAM_ID_ROOT;
			refIt = entryMap_.find(refIt->second.baseId_)) {
		if (refIt->first == id) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	baseIt->second.referredList_.push_back(id);
	it->second.baseId_ = baseId;
}

/*!
	@brief Sets ParamHandler
*/
void ParamTable::setParamHandler(ParamId id, ParamHandler &handler) {
	EntryMap::iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	Entry &entry = it->second;
	if (!entry.referredList_.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (entry.handler_ != NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	entry.handler_ = &handler;
}

/*!
	@brief Sets default ParamHandler
*/
void ParamTable::setDefaultParamHandler(ParamHandler &handler) {
	if (defaultHandler_ != NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	defaultHandler_ = &handler;
}

/*!
	@brief Checks if a specified parameter exists
*/
bool ParamTable::findParam(
		ParamId groupId, ParamId &id, const char8_t *name) const {
	id = PARAM_ID_ROOT;

	EntryMap::const_iterator groupIt = entryMap_.find(groupId);
	if (groupIt == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	const String nameStr(name, alloc_);
	const Entry &groupEntry = groupIt->second;

	const NameMap::const_iterator nameIt =
			groupEntry.subNameMap_.find(nameStr);
	if (nameIt == groupEntry.subNameMap_.end()) {
		return false;
	}

	id = nameIt->second;
	return true;
}

/*!
	@brief Gets next parameter
*/
bool ParamTable::getNextParam(
		ParamId id, ParamId &nextId, bool skipEmpty) const {
	nextId = PARAM_ID_ROOT;

	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	do {
		if (++it == entryMap_.end()) {
			return false;
		}
	}
	while (skipEmpty && it->second.value_ == NULL);

	nextId = it->first;
	return true;
}

/*!
	@brief Gets parent parameter
*/
bool ParamTable::getParentParam(ParamId id, ParamId &parentId) const {
	parentId = PARAM_ID_ROOT;

	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	parentId = it->second.parentId_;
	return (parentId != PARAM_ID_ROOT);
}

/*!
	@brief Gets annotated value
*/
ParamTable::AnnotatedValue ParamTable::getAnnotatedValue(
		ParamId id, bool valueExpected, bool annotationExpected) const {
	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (valueExpected && it->second.value_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (annotationExpected && it->second.annotation_ == NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	return AnnotatedValue(it->second.value_, it->second.annotation_);
}

/*!
	@brief Checks if a specified parameter was already set
*/
bool ParamTable::isSet(ParamId id) const {
	AnnotatedValue value = getAnnotatedValue(id, false, false);
	return (value.first != NULL);
}

/*!
	@brief Gets parameter name
*/
const char8_t* ParamTable::getName(ParamId id) const {
	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	return it->second.name_.c_str();
}

const char8_t* ParamTable::getSourceInfo(ParamId id) const {
	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	return it->second.sourceInfo_.c_str();
}

/*!
	@brief Gets base parameter id of a specified parameter
*/
bool ParamTable::getBaseParam(ParamId id, ParamId &baseId) const {
	baseId = PARAM_ID_ROOT;

	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	baseId = it->second.baseId_;
	return (baseId != PARAM_ID_ROOT);
}

/*!
	@brief Gets referred parameter id
*/
bool ParamTable::getReferredParam(
		ParamId id, ParamId &refId, size_t ordinal) const {
	refId = PARAM_ID_ROOT;

	EntryMap::const_iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (ordinal >= it->second.referredList_.size()) {
		return false;
	}

	refId = it->second.referredList_[ordinal];
	return true;
}

/*!
	@brief Sets ParamValue, input file name and ParamHandler with parameter id
*/
void ParamTable::set(ParamId id, const ParamValue &value,
		const char8_t *sourceInfo, ParamHandler *customHandler) {

	EntryMap::iterator it = entryMap_.find(id);
	if (it == entryMap_.end()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	Entry &entry = it->second;

	ParamHandler *handler = (customHandler == NULL ?
			(entry.handler_ == NULL ? defaultHandler_ : entry.handler_) :
			customHandler);
	String sourceInfoStr(sourceInfo, alloc_);

	if (handler == NULL || entry.group_ || !entry.referredList_.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"The parameter can not be modified ("
				"path=" << pathFormatter(id) <<
				", source=" << sourceInfoStr.c_str() << ")");
	}

	ParamValue *newValue = UTIL_OBJECT_POOL_NEW(valuePool_) ParamValue();
	try {
		if (entry.annotation_ == NULL) {
			newValue->assign(value, alloc_);
		}
		else if (!entry.annotation_->asStorableValue(
				*newValue, &value, NULL, sourceInfo, alloc_)) {
			UTIL_OBJECT_POOL_DELETE(valuePool_, newValue);
			return;
		}

		if (handler != NULL) {
			(*handler)(id, *newValue);
		}
	}
	catch (...) {
		UTIL_OBJECT_POOL_DELETE(valuePool_, newValue);
		std::exception e;
		GS_RETHROW_USER_OR_SYSTEM(e, "");
	}

	UTIL_OBJECT_POOL_DELETE(valuePool_, entry.value_);
	entry.value_ = newValue;
	entry.sourceInfo_.swap(sourceInfoStr);
	entry.modified_ = true;
}

/*!
	@brief Loads list of files
*/
void ParamTable::acceptFile(
		const char8_t *dirPath, const char8_t *const *fileList, size_t count,
		ParamId id) {

	std::vector< picojson::value,
		util::StdAllocator<picojson::value, void> > jsonValueList(alloc_);

	for (size_t i = 0; i < count; i++) {
		u8string filePath;
		util::FileSystem::createPath(dirPath, fileList[i], filePath);

		util::NormalXArray<uint8_t> buf;
		loadAllAsString(filePath.c_str(), buf);
		const char8_t *str = reinterpret_cast<char8_t*>(buf.data());

		jsonValueList.push_back(picojson::value());
		picojson::value &jsonValue = jsonValueList.back();

		const std::string err =
				JsonUtils::parseAll(jsonValue, str, str + buf.size() - 1);
		if (!err.empty()) {
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"Failed to parse json (file=" << fileList[i] <<
					", reason=" << err << ")");
		}
	}

	acceptJSON(&jsonValueList[0], count, id, fileList);
}

/*!
	@brief Sets json data to ParamTable
*/
void ParamTable::acceptJSON(
		const picojson::value *srcList, size_t count, ParamId id,
		const char8_t *const *sourceInfoList) {

	EntryMap valueMap(EntryMap::key_compare(), alloc_);
	EntryMapCleaner cleaner(valueMap, valuePool_);

	util::NormalOStringStream oss;
	for (size_t i = 0; i < count; i++) {
		mergeFromJSON(valueMap, srcList[i], id, sourceInfoList[i]);

		oss << (i > 0 ? ":" : "") << sourceInfoList[i];
	}

	accept(valueMap, oss.str().c_str());
}

/*!
	@brief Writes ParamTable information to file
*/
void ParamTable::toFile(const char8_t *filePath, ParamId id, ExportMode mode) {
	const size_t maxOpenTrial = 1000;

	picojson::value jsonValue;
	toJSON(jsonValue, id, mode);
	const std::string jsonStr = jsonValue.serialize();

	util::NamedFile fileObj;
	std::string tmpFileName;
	for (uint64_t trial = 1;; trial++) {
		util::NormalOStringStream oss;
		oss << filePath << "." << trial << ".tmp";
		tmpFileName = oss.str();
		try {
			fileObj.open(tmpFileName.c_str(), util::FileFlag::TYPE_CREATE |
					util::FileFlag::TYPE_EXCLUSIVE |
					util::FileFlag::TYPE_READ_WRITE);
			break;
		}
		catch (util::PlatformException &e) {
			if (trial >= maxOpenTrial) {
				GS_RETHROW_USER_OR_SYSTEM(e,
						"Too many temporary files may exist or other open"
						" error occred (tmpFileName=" << tmpFileName.c_str() <<
						", trial=" << trial <<
						", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
			}
		}
	}

	try {
		fileObj.write(jsonStr.c_str(), jsonStr.size());
		fileObj.close();
		util::FileSystem::move(tmpFileName.c_str(), filePath);
	}
	catch (...) {
		try {
			util::FileSystem::removeFile(tmpFileName.c_str());
		}
		catch (...) {
			std::exception e;
			GS_RETHROW_USER_OR_SYSTEM(e,
					"Failed to export to file"
					" (tmpFileName=" << tmpFileName.c_str() <<
					", reason=" << GS_EXCEPTION_MESSAGE(e) << ")");
		}
	}
}

/*!
	@brief Converts ParamTable information to json data
*/
void ParamTable::toJSON(picojson::value &dest, ParamId id, ExportMode mode) {
	util::NormalXArray<EntryMap::value_type*> entryPath;
	for (EntryMap::iterator it = entryMap_.begin();
			it != entryMap_.end(); ++it) {
		entryPath.clear();

		const ParamValue *srcValue = it->second.value_;
		if (srcValue == NULL || (!it->second.modified_ &&
				(mode & EXPORT_MODE_DIFF_ONLY) != 0)) {
			continue;
		}

		if (it->second.annotation_ != NULL &&
				it->second.annotation_->isHidden() &&
				(mode & EXPORT_MODE_SHOW_HIDDEN) == 0) {
			continue;
		}

		bool found = (it->first == id);
		for (EntryMap::value_type *entry = &(*it);
				!found && entry->first != PARAM_ID_ROOT;) {

			entryPath.push_back(entry);
			entry = &(*entryMap_.find(entry->second.parentId_));

			found |= (entry->first == id);
		}

		if (!found) {
			continue;
		}

		picojson::value *subValue = &dest;
		for (size_t i = entryPath.size(); i > 0; i--) {
			const char8_t *name = entryPath[i - 1]->second.name_.c_str();

			if (!subValue->is<picojson::object>()) {
				*subValue = picojson::value(picojson::object());
			}
			subValue = &subValue->get<picojson::object>()[name];
		}

		ValueAnnotation *annotation = it->second.annotation_;
		if (annotation == NULL) {
			srcValue->toJSON(*subValue);
		}
		else {
			ParamValue paramValue;
			annotation->asDisplayValue(paramValue, *srcValue, alloc_, mode);
			paramValue.toJSON(*subValue);
		}
	}
}

/*!
	@brief Adds error information
*/
void ParamTable::addProblem(const util::Exception::NamedErrorCode &code,
		const char8_t *message) {
	const Problem problem(code, String(message, alloc_));
	if (traceEnabled_) {
		traceProblem(tracer_, problem);
	}
	else {
		problemList_.push_back(problem);
	}
}

/*!
	@brief Sets trace enabled
*/
void ParamTable::setTraceEnabled(bool enabled) {
	traceEnabled_ = enabled;
	if (enabled) {
		for (ProblemList::iterator it = problemList_.begin();
				it != problemList_.end(); ++it) {
			traceProblem(tracer_, *it);
		}
		problemList_.clear();
	}
}

/*!
	@brief Returns path format
*/
ParamTable::PathFormatter ParamTable::pathFormatter(ParamId id) const {
	return PathFormatter(id, entryMap_);
}

/*!
	@brief Returns SilentParamHandler
*/
ParamTable::ParamHandler& ParamTable::silentHandler() {
	static SilentParamHandler handler;
	return handler;
}

/*!
	@brief Gets parameter name from parameter symbol
*/
std::string ParamTable::getParamSymbol(
		const char8_t *src, bool reverse, size_t startDepth) {
	const char8_t *specialWordList[] = { "IO", NULL };

	std::string dest;
	if (reverse) {
		size_t underScoreCount = 0;
		for (const char8_t *i = src;; i++) {
			if (*i == '\0') {
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}
			else if (*i != '_' || ++underScoreCount < startDepth) {
				continue;
			}

			std::string rawWord;
			std::string camelWord;
			for (;;) {
				++i;
				if (*i == '_' || *i == '\0') {
					if (!dest.empty()) {
						for (const char8_t **w = specialWordList; *w != NULL; w++) {
							if (*w == rawWord) {
								camelWord = rawWord;
								break;
							}
						}
					}
					dest += camelWord;
					rawWord.clear();
					camelWord.clear();
					if (*i == '\0') {
						break;
					}
				}
				else if ('A' <= *i && *i <= 'Z' &&
						(dest.empty() || !camelWord.empty())) {
					rawWord += *i;
					camelWord += static_cast<char8_t>(
							static_cast<int32_t>(*i) + 'a' - 'A');
				}
				else {
					rawWord += *i;
					camelWord += *i;
				}
			}
			break;
		}
	}
	else {
		bool largeLast = false;
		for (const char8_t *i = src; *i != '\0'; i++) {
			if ('A' <= *i && *i <= 'Z') {
				if (!largeLast) {
					dest += '_';
				}
				dest += *i;
				largeLast = true;
			}
			else if ('a' <= *i && *i <= 'z') {
				dest += static_cast<char8_t>(
						static_cast<int32_t>(*i) + 'A' - 'a');
				largeLast = false;
			}
			else {
				dest += *i;
				largeLast = false;
			}
		}
	}
	return dest;
}

void ParamTable::initializeSchema(const ParamTable &table) {
	if (entryMap_.size() != 1) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	for (EntryMap::const_iterator it = table.entryMap_.begin();
			it != table.entryMap_.end(); ++it) {
		if (it->first == PARAM_ID_ROOT) {
			continue;
		}

		Entry &entry = entryMap_.insert(std::pair<ParamId, Entry>(
				it->first, Entry(alloc_))).first->second;
		entry = it->second;
		entry.value_ = NULL;
	}
}

void ParamTable::mergeFromJSON(
		EntryMap &valueMap, const picojson::value &src,
		ParamId id, const char8_t *sourceInfo) {

	std::vector< PathElem, util::StdAllocator<PathElem, void> > itList(alloc_);
	for (ParamId pathId = id;;) {
		Entry &entry = entryMap_.find(pathId)->second;
		PathElem elem = { pathId, &entry, entry.subNameMap_.begin(), &src };
		itList.insert(itList.begin(), elem);

		if (pathId == PARAM_ID_ROOT) {
			break;
		}
		pathId = entry.parentId_;
	}
	const size_t basePathSize = itList.size();

	for (;;) {
		PathElem &elem = itList.back();
		const ParamId curId = elem.id_;

		if (!elem.entry_->group_) {
			if (valueMap.find(curId) == valueMap.end()) {
				Entry &entry = valueMap.insert(
						std::make_pair(curId, Entry(alloc_))).first->second;
				entry.value_ = UTIL_OBJECT_POOL_NEW(valuePool_)
						ParamValue(*elem.value_, alloc_);
				entry.sourceInfo_.assign(sourceInfo);
			}
		}
		else if (elem.it_ == elem.entry_->subNameMap_.end()) {
			if (!elem.value_->is<picojson::object>()) {
				GS_THROW_USER_ERROR(
						GS_ERROR_CT_PARAMETER_INVALID,
						"Invalid json input (source=" << sourceInfo <<
						", group=" << pathFormatter(curId) << ")");
			}
			const picojson::object &obj = elem.value_->get<picojson::object>();
			const NameMap &nameMap = elem.entry_->subNameMap_;
			for (picojson::object::const_iterator it = obj.begin();
					it != obj.end(); ++it) {
				const char8_t *name = it->first.c_str();
				if (nameMap.find(String(name, alloc_)) == nameMap.end()) {
					GS_PARAM_TABLE_ADD_PROBLEM(
							GS_ERROR_CT_PARAMETER_INVALID,
							"Unknown parameter (source=" << sourceInfo <<
							", group=" << pathFormatter(curId) <<
							", name=" << name << ")");
				}
			}
		}
		else {
			if (!elem.value_->is<picojson::object>()) {
				GS_PARAM_TABLE_ADD_PROBLEM(
						GS_ERROR_CT_PARAMETER_INVALID,
						"Unknown parameter (source=" << sourceInfo <<
						", group=" << pathFormatter(curId) << ")");
				++elem.it_;
				continue;
			}

			const picojson::object &obj = elem.value_->get<picojson::object>();
			picojson::object::const_iterator valueIt =
					obj.find(elem.it_->first.c_str());
			if (valueIt == obj.end()) {
				++elem.it_;
				continue;
			}

			const ParamId subId = elem.it_->second;
			const Entry &entry = entryMap_.find(subId)->second;
			PathElem subElem = {
					subId, &entry, entry.subNameMap_.begin(),
					&valueIt->second };
			itList.push_back(subElem);
			continue;
		}

		itList.pop_back();
		if (itList.size() < basePathSize) {
			break;
		}
		++itList.back().it_;
	}
}

void ParamTable::accept(
		const EntryMap &valueMap, const char8_t *defaultSource) {
	if (!pendingDependencyList_.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	EntryMap tmpMap(EntryMap::key_compare(), alloc_);
	EntryMapCleaner cleaner(tmpMap, valuePool_);

	typedef std::set<ParamId> IdSet;
	IdSet idSet;
	for (EntryMap::iterator it = entryMap_.begin();
			it != entryMap_.end(); ++it) {
		idSet.insert(it->first);
	}

	do {
		for (IdSet::iterator it = idSet.begin(); it != idSet.end();) {
			Entry &entry = entryMap_.find(*it)->second;
			if (entry.group_) {
				idSet.erase(it++);
				continue;
			}

			const char8_t *sourceInfo = NULL;
			const ParamValue *baseValue = NULL;
			if (entry.baseId_ != PARAM_ID_ROOT) {
				EntryMap::iterator tmpIt = tmpMap.find(entry.baseId_);
				if (tmpIt == tmpMap.end()) {
					++it;
					continue;
				}
				baseValue = tmpIt->second.value_;
				sourceInfo = tmpIt->second.sourceInfo_.c_str();
			}

			const ParamValue *srcValue = NULL;
			{
				EntryMap::const_iterator valueIt = valueMap.find(*it);
				if (valueIt != valueMap.end()) {
					srcValue = valueIt->second.value_;
					sourceInfo = valueIt->second.sourceInfo_.c_str();
				}
			}

			if (sourceInfo == NULL) {
				sourceInfo = defaultSource;
			}

			Entry &destEntry = tmpMap.insert(std::make_pair(
					*it, Entry(alloc_))).first->second;
			destEntry.sourceInfo_.assign(sourceInfo);

			ParamValue *&destValue = destEntry.value_;
			destValue = UTIL_OBJECT_POOL_NEW(valuePool_) ParamValue();

			EntryMap::iterator orgIt;
			if (baseValue == NULL && srcValue == NULL &&
					(orgIt = entryMap_.find(*it))->second.value_ != NULL) {
				destValue->assign(*orgIt->second.value_, alloc_);
			}
			else if (entry.annotation_ == NULL) {
				if (srcValue != NULL) {
					destValue->assign(*srcValue, alloc_);
				}
			}
			else if (!entry.annotation_->asStorableValue(
						*destValue, srcValue, baseValue, sourceInfo, alloc_)) {
				UTIL_OBJECT_POOL_DELETE(valuePool_, destValue);
				destValue = NULL;
			}

			idSet.erase(it++);
		}
	}
	while (!idSet.empty());

	for (EntryMap::iterator it = tmpMap.begin(); it != tmpMap.end(); ++it) {
		Entry &entry = entryMap_[it->first];
		ParamValue *&value = entry.value_;
		UTIL_OBJECT_POOL_DELETE(valuePool_, value);
		value = it->second.value_;

		it->second.value_ = NULL;
		it->second.sourceInfo_.swap(entry.sourceInfo_);
	}
}

void ParamTable::traceProblem(util::Tracer *tracer, const Problem &problem) {
	if (tracer == NULL) {
		return;
	}

	tracer->put(util::TraceOption::LEVEL_WARNING,
			problem.second.c_str(), UTIL_EXCEPTION_POSITION_ARGS, NULL,
			problem.first);
}

ParamTable::Entry::Entry(const Allocator &alloc) :
		parentId_(PARAM_ID_ROOT),
		baseId_(PARAM_ID_ROOT),
		name_(alloc),
		subNameMap_(NameMap::key_compare(), alloc),
		value_(NULL),
		annotation_(NULL),
		handler_(NULL),
		sourceInfo_(alloc),
		referredList_(alloc),
		group_(false),
		modified_(false) {
}

ParamTable::EntryMapCleaner::EntryMapCleaner(
		EntryMap &base, util::ObjectPool<ParamValue> &valuePool) :
		base_(base), valuePool_(valuePool) {
}

ParamTable::EntryMapCleaner::~EntryMapCleaner() {
	try {
		clean();
	}
	catch (...) {
	}
}

void ParamTable::EntryMapCleaner::clean() {
	for (EntryMap::iterator it = base_.begin(); it != base_.end(); ++it) {
		UTIL_OBJECT_POOL_DELETE(valuePool_, it->second.value_);
		it->second.value_ = NULL;
	}
}

void ParamTable::SilentParamHandler::operator()(ParamId, const ParamValue&) {
}

void ParamTable::PathFormatter::format(std::ostream &stream) const {
	if (id_ == PARAM_ID_ROOT) {
		stream << "(root)";
		return;
	}

	typedef std::vector<const Entry*> EntryList;
	EntryList entryList;
	for (ParamId id = id_; id != PARAM_ID_ROOT;) {
		const Entry &entry = entryMap_.find(id)->second;
		entryList.push_back(&entry);
		id = entry.parentId_;
	}

	for (EntryList::reverse_iterator it = entryList.rbegin();
			it != entryList.rend(); ++it) {
		stream << (it == entryList.rbegin() ? "" : ".") <<
				(**it).name_.c_str();
	}
}

ParamTable::PathFormatter::PathFormatter(
		ParamId id, const EntryMap &entryMap) :
		id_(id), entryMap_(entryMap) {
}

std::ostream& operator<<(
		std::ostream &stream, const ParamTable::PathFormatter &formatter) {
	formatter.format(stream);
	return stream;
}


const int32_t ConfigTable::EXPORT_MODE_NO_UNIT =
		(1 << (EXPORT_MODE_BASE_BITS + 0));

ConfigTable::ConfigTable(const Allocator &alloc) :
		ParamTable(alloc, &UTIL_TRACER_RESOLVE(MAIN)),
		constraintPool_(util::AllocatorInfo(ALLOCATOR_GROUP_MAIN, "configConstraint")),
		constraintList_(alloc) {
	setTraceEnabled(false);
	SetUpHandler::setUpAll(*this);
}

ConfigTable::~ConfigTable() {
	for (ConstraintList::iterator it = constraintList_.begin();
			it != constraintList_.end(); ++it) {
		UTIL_OBJECT_POOL_DELETE(constraintPool_, *it);
	}
}

/*!
	@brief Adds config parameter group
*/
void ConfigTable::resolveGroup(ParamId groupId, ParamId id, const char8_t *name) {
	ParamTable::addParam(groupId, id, name, NULL, true, false);
}

/*!
	@brief Adds config parameter and returns constrait object
*/
ConfigTable::Constraint& ConfigTable::addParam(
		ParamId groupId, ParamId id, const char8_t *name) {

	constraintList_.reserve(constraintList_.size() + 1);

	Constraint *constraint = UTIL_OBJECT_POOL_NEW(constraintPool_) Constraint(
			id, *this, getAllocator(), getValuePool());
	constraintList_.push_back(constraint);

	ParamTable::addParam(groupId, id, name, constraint, false, true);

	return *constraint;
}

/*!
	@brief Gets UInt16 type parameter value
*/
uint16_t ConfigTable::getUInt16(ParamId id) const {
	const AnnotatedValue value = getAnnotatedValue(id);
	static_cast<const Constraint*>(value.second)->checkMinRange(0);
	static_cast<const Constraint*>(value.second)->checkMaxRange(
			static_cast<int64_t>(std::numeric_limits<uint16_t>::max()));

	return static_cast<uint16_t>(value.first->get<int32_t>());
}

/*!
	@brief Gets UInt32 type parameter value
*/
uint32_t ConfigTable::getUInt32(ParamId id) const {
	const AnnotatedValue value = getAnnotatedValue(id);
	static_cast<const Constraint*>(value.second)->checkMinRange(0);
	static_cast<const Constraint*>(value.second)->checkMaxRange(
			static_cast<int64_t>(std::numeric_limits<uint32_t>::max()));

	return static_cast<uint32_t>(value.first->get<int32_t>());
}

/*!
	@brief Gets UInt64 type parameter value
*/
uint64_t ConfigTable::getUInt64(ParamId id) const {
	const AnnotatedValue value = getAnnotatedValue(id);
	static_cast<const Constraint*>(value.second)->checkMinRange(0);

	return static_cast<uint64_t>(value.first->get<int64_t>());
}

/*!
	@brief Converts megabytes to bytes
*/
size_t ConfigTable::megaBytesToBytes(size_t value) {
	if (value > std::numeric_limits<size_t>::max() / (1024 * 1024)) {
		return std::numeric_limits<size_t>::max();
	}

	return value * (1024 * 1024);
}


const ConfigTable::Constraint::UnitInfo
ConfigTable::Constraint::UNIT_INFO_LIST[] = {
	{ VALUE_UNIT_NONE, { "", NULL }, 0 },

	{ VALUE_UNIT_SIZE_B, { "B", "b", NULL }, 0 },
	{ VALUE_UNIT_SIZE_KB, { "KB", "kb", "K", "k", NULL }, 1024 },
	{ VALUE_UNIT_SIZE_MB, { "MB", "mb", "M", "m", NULL }, 1024 },
	{ VALUE_UNIT_SIZE_GB, { "GB", "gb", "G", "g", NULL }, 1024 },
	{ VALUE_UNIT_SIZE_TB, { "TB", "tb", "T", "t", NULL }, 1024 },

	{ VALUE_UNIT_DURATION_MS, { "ms", NULL }, 0 },
	{ VALUE_UNIT_DURATION_S, { "s", NULL }, 1000 },
	{ VALUE_UNIT_DURATION_MIN, { "min", NULL }, 60 },
	{ VALUE_UNIT_DURATION_H, { "h", NULL }, 60 }
};

const ConfigTable::ValueUnit ConfigTable::Constraint::UNIT_MIN_LIST[] = {
	VALUE_UNIT_SIZE_B,
	VALUE_UNIT_DURATION_MS
};

const ConfigTable::ValueUnit ConfigTable::Constraint::UNIT_MAX_LIST[] = {
	VALUE_UNIT_SIZE_TB,
	VALUE_UNIT_DURATION_H
};

ConfigTable::Constraint::~Constraint() {
	for (CandidateList::iterator it = candidateList_.begin();
			it != candidateList_.end(); ++it) {
		UTIL_OBJECT_POOL_DELETE(valuePool_, it->value_);
	}
}

ConfigTable::Constraint& ConfigTable::Constraint::setType(
		ParamValue::Type type) {
	if (type_ != ParamValue::PARAM_TYPE_NULL ||
			type == ParamValue::PARAM_TYPE_NULL) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	type_ = type;
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::setExtendedType(
		ExtendedType type) {
	if (extendedType_ != EXTENDED_TYPE_NONE || type == EXTENDED_TYPE_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (defaultUnit_ != VALUE_UNIT_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (extendedType_ == EXTENDED_TYPE_ENUM) {
		checkComparable(false);
	}

	extendedType_ = type;
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::setUnit(
		ValueUnit defaultUnit, bool unitRequired) {
	if (defaultUnit_ != VALUE_UNIT_NONE || defaultUnit == VALUE_UNIT_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	if (extendedType_ != EXTENDED_TYPE_NONE) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	checkComparable(true);

	defaultUnit_ = defaultUnit;
	unitRequired_ = unitRequired;
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::setDefault(
		const ParamValue &value) {
	ParamValue filtered;
	ValueUnit srcUnit;
	defaultValue_.assign(*filterValue(filtered, value, srcUnit), alloc_);
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::setMin(
		const ParamValue &value) {
	checkComparable(true);
	ParamValue filtered;
	ValueUnit srcUnit;
	minValue_.assign(*filterValue(filtered, value, srcUnit), alloc_);
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::setMax(
		const ParamValue &value) {
	checkComparable(true);
	ParamValue filtered;
	ValueUnit srcUnit;
	maxValue_.assign(*filterValue(filtered, value, srcUnit), alloc_);
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::add(
		const ParamValue &value) {
	if (extendedType_ == EXTENDED_TYPE_ENUM) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	checkComparable(false);

	ParamValue filteredStorage;
	ValueUnit srcUnit;
	const ParamValue *filtered = filterValue(filteredStorage, value, srcUnit);

	for (CandidateList::iterator it = candidateList_.begin();
			it != candidateList_.end(); ++it) {
		if (*it->value_ == *filtered) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	candidateList_.reserve(candidateList_.size() + 1);
	CandidateInfo info = {
		String(),
		UTIL_OBJECT_POOL_NEW(valuePool_) ParamValue(*filtered, alloc_),
		srcUnit };
	candidateList_.push_back(info);

	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::addEnum(
		const ParamValue &value, const char8_t *name) {
	if (extendedType_ != EXTENDED_TYPE_ENUM) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	checkComparable(false);

	ParamValue filteredStorage;
	ValueUnit srcUnit;
	const ParamValue *filtered = filterValue(filteredStorage, value, srcUnit);

	String nameStr(name, alloc_);
	if (nameStr.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	for (CandidateList::iterator it = candidateList_.begin();
			it != candidateList_.end(); ++it) {
		if (it->name_ == nameStr || *it->value_ == *filtered) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	candidateList_.reserve(candidateList_.size() + 1);
	CandidateInfo info = {
		nameStr,
		UTIL_OBJECT_POOL_NEW(valuePool_) ParamValue(*filtered, alloc_),
		VALUE_UNIT_NONE };
	candidateList_.push_back(info);

	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::alternate(
		ParamId alternativeId, Filter *filter) {
	if (alterable_ || inheritable_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	paramTable_.addDependency(id_, alternativeId);

	alterable_ = true;
	filter_ = filter;

	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::inherit(ParamId baseId) {
	if (alterable_ || inheritable_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	paramTable_.addDependency(id_, baseId);

	inheritable_ = true;

	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::deprecate(bool enabled) {
	deprecated_ = enabled;
	return *this;
}

ConfigTable::Constraint& ConfigTable::Constraint::hide(bool enabled) {
	hidden_ = enabled;
	return *this;
}

void ConfigTable::Constraint::checkMinRange(
		int64_t lower, const ParamValue *target) const {
	bool succeeded = false;

	if (target == NULL) {
		if (minValue_.getType() != ParamValue::PARAM_TYPE_NULL) {
			checkMinRange(lower, &minValue_);
			return;
		}

		if (!candidateList_.empty()) {
			for (CandidateList::const_iterator it = candidateList_.begin();
					it != candidateList_.end(); ++it) {
				checkMinRange(lower, it->value_);
			}
			return;
		}

		if (type_ == ParamValue::PARAM_TYPE_INT32) {
			succeeded = (lower <= std::numeric_limits<int32_t>::min());
		}
	}
	else {
		switch (target->getType()) {
		case ParamValue::PARAM_TYPE_INT32:
			succeeded = (lower <= target->get<int32_t>());
			break;
		case ParamValue::PARAM_TYPE_INT64:
			succeeded = (lower <= target->get<int64_t>());
			break;
		case ParamValue::PARAM_TYPE_DOUBLE:
			succeeded = (lower <= target->get<double>());
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	if (!succeeded) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
}

void ConfigTable::Constraint::checkMaxRange(
		int64_t upper, const ParamValue *target) const {
	bool succeeded = false;

	if (target == NULL) {
		if (maxValue_.getType() != ParamValue::PARAM_TYPE_NULL) {
			checkMaxRange(upper, &maxValue_);
			return;
		}

		if (!candidateList_.empty()) {
			for (CandidateList::const_iterator it = candidateList_.begin();
					it != candidateList_.end(); ++it) {
				checkMaxRange(upper, it->value_);
			}
			return;
		}

		if (type_ == ParamValue::PARAM_TYPE_INT32) {
			succeeded = (std::numeric_limits<int32_t>::max() <= upper);
		}
	}
	else {
		switch (target->getType()) {
		case ParamValue::PARAM_TYPE_INT32:
			succeeded = (target->get<int32_t>() <= upper);
			break;
		case ParamValue::PARAM_TYPE_INT64:
			succeeded = (target->get<int64_t>() <= upper);
			break;
		case ParamValue::PARAM_TYPE_DOUBLE:
			succeeded = (target->get<double>() <= upper);
			break;
		default:
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}

	if (!succeeded) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}
}

bool ConfigTable::Constraint::isHidden() const {
	return deprecated_ || hidden_;
}

bool ConfigTable::Constraint::asStorableValue(
		ParamValue &dest, const ParamValue *src, const ParamValue *base,
		const char8_t *sourceInfo, const Allocator &alloc) const {

	Formatter formatter(*this);
	formatter.add("source", sourceInfo);
	formatter.add("name", id_, Formatter::ENTRY_PATH);

	ParamValue filteredStorage;
	const ParamValue *filtered = NULL;
	ParamId filteredId = PARAM_ID_ROOT;
	if (base != NULL) {
		if (filter_ != NULL) {
			if ((*filter_)(filteredStorage, *base)) {
				filtered = &filteredStorage;
				paramTable_.getBaseParam(id_, filteredId);
			}
		}
		else {
			filtered = base;
		}
	}

	const ParamValue *selected;
	if (src == NULL) {
		if (filtered == NULL) {
			if (defaultValue_.is<ParamValue::Null>()) {
				if (deprecated_) {
					return false;
				}
				GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
						"Parameter must be specified " << formatter);
			}
			dest.assign(defaultValue_, alloc);
			return true;
		}
		else {
			formatter.add("baseName", filteredId, Formatter::ENTRY_PATH);
			formatter.add("baseValue", *filtered);

			if (!alterable_ && !inheritable_) {
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}
			selected = filtered;
		}
	}
	else {
		formatter.add("value", *src);

		if (filtered != NULL && alterable_) {
			formatter.add("baseName", filteredId, Formatter::ENTRY_PATH);
			formatter.add("baseValue", *filtered);
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"Parameter conflicted " << formatter);
		}

		if (deprecated_) {
			Formatter subFormatter(*this);
			subFormatter.addAll(formatter);
			subFormatter.add(
					"recommended", ParamValue(), Formatter::ENTRY_RECOMMENDS);
			paramTable_.GS_PARAM_TABLE_ADD_PROBLEM(
					GS_ERROR_CT_PARAMETER_INVALID,
					"Parameter deprecated " << subFormatter);
		}

		selected = src;
	}

	ParamValue converted;
	if (extendedType_ != EXTENDED_TYPE_NONE) {
		if (asStorableExtendedValue(converted, *selected, alloc, formatter)) {
			selected = &converted;
		}
	}
	else if (defaultUnit_ != VALUE_UNIT_NONE) {
		ValueUnit srcUnit;
		if (asStorableUnitValue(converted, *selected, alloc, formatter, &srcUnit)) {
			selected = &converted;
		}
	}

	ParamValue numeric;
	if (asStorableNumericValue(numeric, *selected, alloc, formatter)) {
		selected = &numeric;
	}

	if (selected->getType() != type_) {
		formatter.add("targetType",
				static_cast<int32_t>(type_), Formatter::ENTRY_TYPE);
		formatter.add("srcType", *selected, Formatter::ENTRY_JSON_VALUE_TYPE);

		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_TYPE_MISMATCH,
				"Type unmatched " << formatter);
	}

	if (!minValue_.is<ParamValue::Null>() &&
			compareValue(*selected, minValue_) < 0) {
		formatter.add("min", minValue_, Formatter::ENTRY_VALUE);
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_LESS_THAN_LOWER_LIMIT,
				"Too small value " << formatter);
	}

	if (!maxValue_.is<ParamValue::Null>() &&
			compareValue(*selected, maxValue_) > 0) {
		formatter.add("max", minValue_, Formatter::ENTRY_VALUE);
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large value " << formatter);
	}

	if (!candidateList_.empty()) {
		bool matched = false;
		for (CandidateList::const_iterator it = candidateList_.begin();
				it != candidateList_.end(); ++it) {
			if (*selected == *it->value_) {
				matched = true;
				break;
			}
		}

		if (!matched) {
			formatter.add("candidates",
					ParamValue(), Formatter::ENTRY_VALUE_CANDIDATES);
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"No candidates matched " << formatter);
		}
	}

	dest.assign(*selected, alloc);
	return true;
}

void ConfigTable::Constraint::asDisplayValue(
		ParamValue &dest, const ParamValue &src,
		const Allocator &alloc, ExportMode mode) const {

	ValueUnit preferredUnit = VALUE_UNIT_NONE;
	for (CandidateList::const_iterator it = candidateList_.begin();
			it != candidateList_.end(); ++it) {
		if (src == *it->value_) {
			if (extendedType_ == EXTENDED_TYPE_ENUM) {
				dest.assign(it->name_.c_str(), alloc);
				return;
			}
			preferredUnit = it->preferredUnit_;
			break;
		}
	}

	if (defaultUnit_ != VALUE_UNIT_NONE &&
			((mode & EXPORT_MODE_NO_UNIT) == 0)) {
		Formatter formatter(*this);
		ParamValue numeric;
		ParamValue filtered;
		const ParamValue::Type numericType = ParamValue::PARAM_TYPE_INT64;

		ValueUnit displayUnit = preferredUnit;
		try {
			if (displayUnit == VALUE_UNIT_NONE || !asStorableNumericValue(
						numeric, src, alloc_, formatter, &numericType)) {
				filtered.assign(src, alloc);
				displayUnit = defaultUnit_;
			}
			else {
				filtered.assign(convertUnit(numeric.get<int64_t>(),
							preferredUnit, defaultUnit_, formatter), alloc);
			}
		}
		catch (UserException &e) {
			GS_RETHROW_USER_OR_SYSTEM_CODED(
					GS_ERROR_CM_INTERNAL_ERROR, e, "");
		}

		util::NormalOStringStream oss;
		oss << filtered << UNIT_INFO_LIST[displayUnit].nameList_[0];
		dest.assign(oss.str().c_str(), alloc);
		return;
	}

	dest.assign(src, alloc);
}

ConfigTable::Constraint::Constraint(
		ParamTable::ParamId id, ParamTable &paramTable,
		Allocator &alloc, util::ObjectPool<ParamValue> &valuePool) :
		id_(id),
		paramTable_(paramTable),
		alloc_(alloc),
		valuePool_(valuePool),
		type_(ParamValue::PARAM_TYPE_NULL),
		extendedType_(EXTENDED_TYPE_NONE),
		defaultUnit_(VALUE_UNIT_NONE),
		candidateList_(alloc),
		filter_(NULL),
		deprecated_(false),
		hidden_(false),
		unitRequired_(false),
		alterable_(false),
		inheritable_(false) {
}

bool ConfigTable::Constraint::asStorableExtendedValue(
		ParamValue &dest, const ParamValue &src,
		const Allocator &alloc, Formatter &formatter) const {

	if (extendedType_ == EXTENDED_TYPE_ENUM) {
		if (src.getType() == ParamValue::PARAM_TYPE_STRING) {
			for (CandidateList::const_iterator it = candidateList_.begin();; ++it) {
				if (it == candidateList_.end()) {
					formatter.add("candidates", ParamValue(),
							Formatter::ENTRY_VALUE_CANDIDATES);
					GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
							"Value not matched in candidate names " << formatter);
				}
				else if (strcmp(it->name_.c_str(),
						src.get<const char8_t*>()) == 0) {
					dest.assign(*it->value_, alloc);
					return true;
				}
			}
		}
		else if (src.getType() == type_) {
			Formatter subFormatter(*this);
			subFormatter.addAll(formatter);
			subFormatter.add("candidates", ParamValue(),
					Formatter::ENTRY_VALUE_CANDIDATES);

			paramTable_.GS_PARAM_TABLE_ADD_PROBLEM(
					GS_ERROR_CT_PARAMETER_INVALID,
					"Specifying as string is recommended " << subFormatter);
		}
	}
	else if (extendedType_ == EXTENDED_TYPE_LAX_BOOL) {
		const ParamValue::Type type = ParamValue::PARAM_TYPE_DOUBLE;

		ParamValue numeric;
		const ParamValue *filtered;
		if (src.getType() == type) {
			filtered = &src;
		}
		else if (asStorableNumericValue(numeric, src, alloc, formatter, &type)) {
			filtered = &numeric;
		}
		else {
			return false;
		}

		if (filtered->get<double>() == 1) {
			dest.assign(true, alloc);
			return true;
		}
		else if (filtered->get<double>() == 0) {
			dest.assign(false, alloc);
			return true;
		}
		else {
			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_TYPE_MISMATCH,
					"Failed to convert as boolean " << formatter);
		}
	}
	return false;
}

bool ConfigTable::Constraint::asStorableUnitValue(
		ParamValue &dest, const ParamValue &src,
		const Allocator &alloc, Formatter &formatter,
		ValueUnit *srcUnit) const {
	*srcUnit = VALUE_UNIT_NONE;

	if (defaultUnit_ == VALUE_UNIT_NONE) {
		return false;
	}

	if (src.getType() != ParamValue::PARAM_TYPE_STRING) {
		if (unitRequired_) {
			formatter.add("availableUnits", ParamValue(),
					Formatter::ENTRY_UNIT_CANDIDATES);
			formatter.add("defaultUnit",
					static_cast<int32_t>(defaultUnit_),
					Formatter::ENTRY_UNIT);

			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"Unit must be specified " << formatter);
		}
		else {
			Formatter subFormatter(*this);
			subFormatter.addAll(formatter);
			subFormatter.add("availableUnits", ParamValue(),
					Formatter::ENTRY_UNIT_CANDIDATES);
			subFormatter.add("defaultUnit",
					static_cast<int32_t>(defaultUnit_),
					Formatter::ENTRY_UNIT);

			paramTable_.GS_PARAM_TABLE_ADD_PROBLEM(
					GS_ERROR_CT_PARAMETER_INVALID,
					"Specifying with unit is recommended " << subFormatter);
		}
		return false;
	}

	String str(src.get<const char8_t*>(), alloc);

	if (str.empty()) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Empty value " << formatter);
	}

	size_t unitPos = str.size();
	for (String::reverse_iterator it = str.rbegin();
			it != str.rend(); it++, unitPos--) {
		if ('0' <= *it && *it <= '9') {
			break;
		}
	}

	String unitStr(str, unitPos, String::npos, alloc);
	if (unitStr.empty()) {
		formatter.add("availableUnits", ParamValue(),
				Formatter::ENTRY_UNIT_CANDIDATES);
		formatter.add("defaultUnit",
				static_cast<int32_t>(defaultUnit_),
				Formatter::ENTRY_UNIT);
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Unit not specified " << formatter);
	}

	const UnitInfo *infoListEnd = UNIT_INFO_LIST +
			sizeof(UNIT_INFO_LIST) / sizeof(*UNIT_INFO_LIST);
	for (const UnitInfo *it = UNIT_INFO_LIST; it != infoListEnd; ++it) {
		for (const char8_t *const *nameIt = it->nameList_;
				*nameIt != NULL; ++nameIt) {
			if (unitStr == *nameIt) {
				*srcUnit = it->unit_;
				break;
			}
		}
		if (*srcUnit != VALUE_UNIT_NONE) {
			break;
		}
	}

	if (*srcUnit == VALUE_UNIT_NONE) {
		formatter.add("specifiedUnit", unitStr);
		formatter.add("availableUnits", ParamValue(),
				Formatter::ENTRY_UNIT_CANDIDATES);
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Unknown unit " << formatter);
	}

	{
		bool unitMatched = false;
		const size_t rangeCount =
				sizeof(UNIT_MIN_LIST) / sizeof(*UNIT_MIN_LIST);

		for (size_t i = 0; i < rangeCount; i++) {
			const ValueUnit min = UNIT_MIN_LIST[i];
			const ValueUnit max = UNIT_MAX_LIST[i];

			unitMatched |=
					(min <= *srcUnit && *srcUnit <= max &&
					min <= defaultUnit_ && defaultUnit_ <= max);
		}

		if (!unitMatched) {
			formatter.add("availableUnits", ParamValue(),
					Formatter::ENTRY_UNIT_CANDIDATES);
			formatter.add("specifiedUnit",
					static_cast<int32_t>(*srcUnit),
					Formatter::ENTRY_UNIT);

			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"Unit not matched " << formatter);
		}
	}

	int64_t number;
	{
		std::string numberStr(str.c_str(), unitPos);
		util::NormalIStringStream iss(numberStr);
		iss >> number;
		util::NormalOStringStream oss;
		oss << number;
		if (iss.fail() || !iss.eof() || numberStr != oss.str()) {
			formatter.add("targetNumber", str.c_str());
			formatter.add("specifiedUnit",
					static_cast<int32_t>(*srcUnit),
					Formatter::ENTRY_UNIT);

			GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
					"Failed to parse number " << formatter);
		}
	}

	dest.assign(convertUnit(number, defaultUnit_, *srcUnit, formatter), alloc);
	return true;
}

bool ConfigTable::Constraint::asStorableNumericValue(
		ParamValue &dest, const ParamValue &src,
		const Allocator &alloc, Formatter &formatter,
		const ParamValue::Type *type) const {

	const ParamValue::Type targetType = (type == NULL ? type_ : *type);
	if (targetType == src.getType()) {
		return false;
	}

	if (targetType == ParamValue::PARAM_TYPE_INT64) {
		int64_t destNumber;
		if (src.getType() == ParamValue::PARAM_TYPE_INT32) {
			convertNumber(destNumber, src.get<int32_t>(), formatter);
		}
		else if (src.getType() == ParamValue::PARAM_TYPE_DOUBLE) {
			convertNumber(destNumber, src.get<double>(), formatter);
		}
		else {
			return false;
		}
		dest.assign(destNumber, alloc);
	}
	else if (targetType == ParamValue::PARAM_TYPE_INT32) {
		int32_t destNumber;
		if (src.getType() == ParamValue::PARAM_TYPE_INT64) {
			convertNumber(destNumber, src.get<int64_t>(), formatter);
		}
		else if (src.getType() == ParamValue::PARAM_TYPE_DOUBLE) {
			convertNumber(destNumber, src.get<double>(), formatter);
		}
		else {
			return false;
		}
		dest.assign(destNumber, alloc);
	}
	else if (targetType == ParamValue::PARAM_TYPE_DOUBLE) {
		double destNumber;
		if (src.getType() == ParamValue::PARAM_TYPE_INT32) {
			convertNumber(destNumber, src.get<int32_t>(), formatter);
		}
		else if (src.getType() == ParamValue::PARAM_TYPE_INT64) {
			convertNumber(destNumber, src.get<int64_t>(), formatter);
		}
		else {
			return false;
		}
		dest.assign(destNumber, alloc);
	}
	else {
		return false;
	}
	return true;
}

const ParamValue* ConfigTable::Constraint::filterValue(
		ParamValue &filteredStorage, const ParamValue &value,
		ValueUnit &srcUnit) {
	srcUnit = defaultUnit_;
	Formatter formatter(*this);

	const ParamValue *filtered = &value;
	if (filtered->getType() != type_ &&
			value.getType() == ParamValue::PARAM_TYPE_STRING) {
		try {
			if (!asStorableUnitValue(
					filteredStorage, *filtered, alloc_, formatter, &srcUnit)) {
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}
			filtered = &filteredStorage;
		}
		catch (UserException &e) {
			GS_RETHROW_USER_OR_SYSTEM_CODED(
					GS_ERROR_CM_INTERNAL_ERROR, e, "");
		}
	}

	if (value.getType() != ParamValue::PARAM_TYPE_DOUBLE &&
			asStorableNumericValue(
					filteredStorage, *filtered, alloc_, formatter)) {
		filtered = &filteredStorage;
	}

	if (type_ == ParamValue::PARAM_TYPE_NULL ||
			filtered->getType() != type_) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	return filtered;
}

void ConfigTable::Constraint::checkComparable(bool expected) const {
	if (expected) {
		if (!candidateList_.empty() || extendedType_ == EXTENDED_TYPE_ENUM) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}
	else {
		if (!minValue_.is<ParamValue::Null>() ||
				!maxValue_.is<ParamValue::Null>()) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}
}

template<typename D, typename S>
void ConfigTable::Constraint::convertNumber(
		D &dest, const S &src, Formatter &formatter) const {
	if (src > std::numeric_limits<D>::max()) {
		if (maxValue_.is<ParamValue::Null>()) {
			formatter.add("upperRange",
					std::numeric_limits<D>::max(), Formatter::ENTRY_VALUE);
		}
		else {
			formatter.add("max", maxValue_, Formatter::ENTRY_VALUE);
		}

		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_GREATER_THAN_UPPER_LIMIT,
				"Too large number " << formatter);
	}
	else if (std::numeric_limits<D>::is_integer &&
			src < std::numeric_limits<D>::min()) {
		if (minValue_.is<ParamValue::Null>()) {
			formatter.add("lowerRange",
					std::numeric_limits<D>::min(), Formatter::ENTRY_VALUE);
		}
		else {
			formatter.add("min", minValue_, Formatter::ENTRY_VALUE);
		}

		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_LESS_THAN_LOWER_LIMIT,
				"Too small number " << formatter);
	}

	dest = static_cast<D>(src);
}

int32_t ConfigTable::Constraint::compareValue(
		const ParamValue &value1, const ParamValue &value2) {
	if (value1.getType() != value2.getType()) {
		return 0;
	}

	switch (value1.getType()) {
	case ParamValue::PARAM_TYPE_INT64:
		return compareNumber(value1.get<int64_t>(), value2.get<int64_t>());
	case ParamValue::PARAM_TYPE_INT32:
		return compareNumber(value1.get<int32_t>(), value2.get<int32_t>());
	case ParamValue::PARAM_TYPE_DOUBLE:
		return compareNumber(value1.get<double>(), value2.get<double>());
	default:
		return 0;
	}
}

template<typename T>
int32_t ConfigTable::Constraint::compareNumber(
		const T &value1, const T &value2) {
	if (value1 < value2) {
		return -1;
	}
	else if (value1 > value2) {
		return 1;
	}
	else {
		return 0;
	}
}

int64_t ConfigTable::Constraint::convertUnit(int64_t src, ValueUnit destUnit,
		ValueUnit srcUnit, Formatter &formatter) {

	const UnitInfo *destInfo = &UNIT_INFO_LIST[destUnit];
	const UnitInfo *srcInfo = &UNIT_INFO_LIST[srcUnit];

	int64_t dest = src;
	for (const UnitInfo *unitInfo = srcInfo; unitInfo != destInfo;) {
		const UnitInfo *nextInfo;
		if (unitInfo < destInfo) {
			nextInfo = unitInfo + 1;
			if (nextInfo->step_ == 0) {
				assert(false);
				GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
			}

			dest /= nextInfo->step_;
		}
		else {
			nextInfo = unitInfo - 1;
			const int32_t step = unitInfo->step_;
			assert(step > 0);

			if (dest > std::numeric_limits<int64_t>::max() / step) {
				formatter.add("targetUnit",
						static_cast<int32_t>(destInfo->unit_),
						Formatter::ENTRY_UNIT);

				GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
						"Too large to convert unit " << formatter);
			}
			else if (dest < std::numeric_limits<int64_t>::min() / step) {
				formatter.add("targetUnit",
						static_cast<int32_t>(destInfo->unit_),
						Formatter::ENTRY_UNIT);

				GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
						"Too small to convert unit" << formatter);
			}
			dest *= step;
		}
		unitInfo = nextInfo;
	}

	return dest;
}

ConfigTable::Constraint::Formatter::Formatter(const Constraint &constraint) :
		constraint_(constraint), entryCount_(0) {
}

void ConfigTable::Constraint::Formatter::add(
		const char8_t *name, const ParamValue &value, EntryType entryType) {

	if (entryCount_ >= sizeof(entryList_) / sizeof(*entryList_)) {
		assert(false);
		return;
	}

	Entry &entry = entryList_[entryCount_];

	entry.name_ = name;
	entry.value_.assign(value, constraint_.alloc_);
	entry.entryType_ = entryType;

	entryCount_++;
}

void ConfigTable::Constraint::Formatter::addAll(const Formatter &formatter) {
	assert(this != &formatter);
	const size_t count = formatter.entryCount_;
	for (size_t i = 0; i < count; i++) {
		const Entry &entry = formatter.entryList_[i];
		add(entry.name_, entry.value_, entry.entryType_);
	}
}

void ConfigTable::Constraint::Formatter::format(std::ostream &stream) const {
	stream << "(";
	for (size_t i = 0; i < entryCount_; i++) {
		if (i > 0) {
			stream << ", ";
		}
		format(stream, entryList_[i]);
	}
	stream << ")";
}

void ConfigTable::Constraint::Formatter::format(
		std::ostream &stream, const Entry &entry) const try {
	if (entry.name_ != NULL) {
		stream << entry.name_ << "=";
	}

	switch (entry.entryType_) {
	case ENTRY_NORMAL:
		stream << entry.value_;
		break;
	case ENTRY_PATH:
		stream << constraint_.paramTable_.pathFormatter(
				entry.value_.get<int32_t>());
		break;
	case ENTRY_VALUE:
		{
			ParamValue value;
			constraint_.asDisplayValue(
					value, entry.value_, constraint_.alloc_,
					EXPORT_MODE_DEFAULT);
			stream << value;
		}
		break;
	case ENTRY_TYPE:
		switch (entry.value_.get<int32_t>()) {
		case ParamValue::PARAM_TYPE_NULL:
			stream << "null";
			break;
		case ParamValue::PARAM_TYPE_STRING:
			stream << "string";
			break;
		case ParamValue::PARAM_TYPE_INT64:
			stream << "numeric";
			break;
		case ParamValue::PARAM_TYPE_INT32:
			stream << "numeric";
			break;
		case ParamValue::PARAM_TYPE_DOUBLE:
			stream << "numeric";
			break;
		case ParamValue::PARAM_TYPE_BOOL:
			stream << "boolean";
			break;
		case ParamValue::PARAM_TYPE_JSON:
			stream << "(complexed json type)";
			break;
		default:
			stream << "(unknown)";
			break;
		}
		break;
	case ENTRY_JSON_VALUE_TYPE:
		if (entry.value_.getType() == ParamValue::PARAM_TYPE_JSON) {
			if (entry.value_.get<picojson::value>().is<picojson::object>()) {
				stream << "object";
			}
			else if (entry.value_.get<picojson::value>().is<picojson::array>()) {
				stream << "array";
			}
			else {
				stream << "(unknown)";
			}
		}
		else {
			Entry subEntry;
			subEntry.name_ = NULL;
			subEntry.value_.assign(
					static_cast<int32_t>(entry.value_.getType()), constraint_.alloc_);
			subEntry.entryType_ = ENTRY_TYPE;

			format(stream, subEntry);
		}
		break;
	case ENTRY_RECOMMENDS:
		stream << "[";
		{
			bool found = false;
			ParamId refId;
			ParamTable &paramTable = constraint_.paramTable_;
			for (size_t i = 0; paramTable.getReferredParam(
					constraint_.id_, refId, i); i++) {
				const Constraint *other = static_cast<const Constraint*>(
						paramTable.getAnnotatedValue(refId, false, true).second);

				if (other->deprecated_ || !other->alterable_) {
					continue;
				}
				if (found) {
					stream << ", ";
				}
				stream << paramTable.pathFormatter(refId);
				found = true;
			}
		}
		stream << "]";
		break;
	case ENTRY_VALUE_CANDIDATES:
		stream << "[";
		{
			const CandidateList &list = constraint_.candidateList_;
			for (CandidateList::const_iterator it =
					list.begin(); it != list.end(); ++it) {
				if (it != list.begin()) {
					stream << ", ";
				}

				if (it->name_.empty()) {
					ParamValue value;
					constraint_.asDisplayValue(
							value, *it->value_, constraint_.alloc_,
							EXPORT_MODE_DEFAULT);
					stream << value;
				}
				else {
					stream << it->name_.c_str();
				}
			}
		}
		stream << "]";
		break;
	case ENTRY_UNIT_CANDIDATES:
		stream << "[";
		for (size_t i = 0;
				i < sizeof(UNIT_MIN_LIST) / sizeof(*UNIT_MIN_LIST); i++) {

			const int32_t unitValue = constraint_.defaultUnit_;
			const int32_t min = UNIT_MIN_LIST[i];
			const int32_t max = UNIT_MAX_LIST[i];

			if (min <= unitValue && unitValue <= max) {
				for (int32_t unit = min; unit <= max; unit++) {
					if (unit != min) {
						stream << ", ";
					}
					stream << UNIT_INFO_LIST[unit].nameList_[0];
				}
			}
		}
		stream << "]";
		break;
	case ENTRY_UNIT:
		{
			const int32_t unitValue = entry.value_.get<int32_t>();
			stream << UNIT_INFO_LIST[unitValue].nameList_[0];
		}
		break;
	default:
		assert(false);
		break;
	}
}
catch (...) {
	stream.setstate(std::ios::badbit);
	if ((stream.exceptions() & std::ios::badbit) != 0) {
		throw;
	}
}

std::ostream& operator<<(
		std::ostream& stream,
		const ConfigTable::Constraint::Formatter &formatter) {
	formatter.format(stream);
	return stream;
}


StatTable::StatTable(const Allocator &alloc) :
		ParamTable(alloc,  &UTIL_TRACER_RESOLVE(SYSTEM)),
		updatorList_(alloc),
		optionList_(alloc) {
}

void StatTable::initialize() {
	SetUpHandler::setUpAll(*this);
}

void StatTable::initialize(const StatTable &table) {
	ParamTable::initializeSchema(table);
	updatorList_.assign(table.updatorList_.begin(), table.updatorList_.end());
	optionList_.assign(table.optionList_.begin(), table.optionList_.end());
}

void StatTable::resolveGroup(
		ParamId groupId, ParamId id, const char8_t *name) {
	ParamTable::addParam(groupId, id, name, NULL, true, false);
}

void StatTable::addParam(ParamId groupId, ParamId id, const char8_t *name) {
	ParamTable::addParam(groupId, id, name, NULL, false, true);
}

void StatTable::addUpdator(StatUpdator *updator) {
	assert(updator != NULL);
	updatorList_.push_back(updator);
}

void StatTable::set(ParamId id, const ParamValue &value) {
	ParamTable::set(id, value, "", &silentHandler());
}

void StatTable::set(ParamId id, const uint32_t &value) {
	set(id, ParamValue(static_cast<int64_t>(value)));
}

void StatTable::set(ParamId id, const uint64_t &value) {
	if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
		set(id, ParamValue(std::numeric_limits<int64_t>::max()));
	}

	set(id, ParamValue(static_cast<int64_t>(value)));
}

bool StatTable::getDisplayOption(DisplayOption option) const {
	return (std::find(optionList_.begin(), optionList_.end(), option) !=
			optionList_.end());
}

void StatTable::setDisplayOption(DisplayOption option, bool enabled) {
	OptionList::iterator it =
			std::find(optionList_.begin(), optionList_.end(), option);

	if (it == optionList_.end()) {
		if (enabled) {
			optionList_.push_back(option);
		}
	}
	else {
		if (!enabled) {
			optionList_.erase(it);
		}
	}
}

void StatTable::updateAll() {
	UpdatorList pendingList(
			updatorList_.begin(), updatorList_.end(), getAllocator());

	while (!pendingList.empty()) {
		const size_t orgSize = pendingList.size();
		for (UpdatorList::iterator it = pendingList.begin();
				it != pendingList.end();) {
			if ((**it)(*this)) {
				it = pendingList.erase(it);
			}
			else {
				++it;
			}
		}

		if (orgSize == pendingList.size()) {
			GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
		}
	}
}


PartitionGroupConfig::PartitionGroupConfig(const ConfigTable &configTable) :
		partitionNum_(configTable.getUInt32(CONFIG_TABLE_DS_PARTITION_NUM)),
		partitionGroupNum_(configTable.getUInt32(CONFIG_TABLE_DS_CONCURRENCY)),
		base_(makeBase(partitionNum_, partitionGroupNum_)),
		mod_(makeMod(partitionNum_, partitionGroupNum_))
{
}

PartitionGroupConfig::PartitionGroupConfig(
		uint32_t partitionNum, uint32_t concurrency) :
		partitionNum_(partitionNum),
		partitionGroupNum_(concurrency),
		base_(makeBase(partitionNum_, partitionGroupNum_)),
		mod_(makeMod(partitionNum_, partitionGroupNum_))
{
}

PartitionGroupConfig::PartitionGroupConfig() :
		partitionNum_(1),
		partitionGroupNum_(1),
		base_(makeBase(partitionNum_, partitionGroupNum_)),
		mod_(makeMod(partitionNum_, partitionGroupNum_))
{
}

PartitionGroupConfig::~PartitionGroupConfig()
{
}

uint32_t PartitionGroupConfig::getPartitionCount() const
{
	return partitionNum_;
}

uint32_t PartitionGroupConfig::getPartitionGroupCount() const
{
	return partitionGroupNum_;
}

PartitionId PartitionGroupConfig::getGroupBeginPartitionId(PartitionGroupId pgId) const
{
	return static_cast<PartitionId>(base_ * pgId + std::min(mod_, pgId));
}

PartitionId PartitionGroupConfig::getGroupEndPartitionId(PartitionGroupId pgId) const
{
	return getGroupBeginPartitionId(pgId + 1);
}

PartitionGroupId PartitionGroupConfig::getPartitionGroupId(PartitionId pId) const
{
	if (pId < (base_ + 1) * mod_) {
		return static_cast<PartitionGroupId>(pId / (base_ + 1));
	} else {
		return static_cast<PartitionGroupId>((pId - mod_) / base_);
	}
}

uint32_t PartitionGroupConfig::getGroupPartitonCount(PartitionGroupId pgId) const
{
	if (pgId < mod_) {
		return base_ + 1;
	} else {
		return base_;
	}
}

void PartitionGroupConfig::checkParams(
	uint32_t partitionNum, uint32_t concurrency)
{
	if (partitionNum <= 0 || concurrency <= 0 ||
			partitionNum < concurrency) {
		GS_THROW_USER_ERROR(GS_ERROR_CT_PARAMETER_INVALID,
				"Invalid params (partitionNum=" << partitionNum <<
				", concurrency=" << concurrency << ")");
	}
}

uint32_t PartitionGroupConfig::makeBase(
	uint32_t partitionNum, uint32_t concurrency)
{
	checkParams(partitionNum, concurrency);
	return partitionNum / concurrency;
}

uint32_t PartitionGroupConfig::makeMod(
	uint32_t partitionNum, uint32_t concurrency)
{
	checkParams(partitionNum, concurrency);
	return partitionNum % concurrency;
}


/*!
	@brief Adds config parameter group to ConfigTable
*/
void ConfigTableUtils::resolveGroup(
		ConfigTable &configTable, ConfigTable::ParamId id, const char8_t *symbol,
		const char8_t *name) {
	configTable.resolveGroup(CONFIG_TABLE_ROOT, id, name);

	SymbolTable &symbolTable = getGroupSymbolTable();
	symbolTable[name] = symbol;
}

/*!
	@brief Adds parameter id and symbol to ConfigTable, and returns constrait object
*/
ConfigTable::Constraint& ConfigTableUtils::addParam(
		ConfigTable &configTable, ConfigTable::ParamId id, const char8_t *symbol) {
	const char8_t *groupName = getGroupSymbol(symbol, true);

	ConfigTable::ParamId groupId;
	if (strlen(groupName) == 0) {
		groupId = CONFIG_TABLE_ROOT;
	}
	else if (!configTable.findParam(CONFIG_TABLE_ROOT, groupId, groupName)) {
		GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
	}

	const std::string name = ParamTable::getParamSymbol(symbol, true, 3);
	return configTable.addParam(groupId, id, name.c_str());
}

/*!
	@brief Gets parameter group symbol
*/
const char8_t* ConfigTableUtils::getGroupSymbol(
		const char8_t *src, bool reverse) {
	const SymbolTable &symbolTable = getGroupSymbolTable();

	if (reverse) {
		if (strstr(src, "CONFIG_TABLE_ROOT") == src) {
			return "";
		}
		for (SymbolTable::const_iterator it = symbolTable.begin();
				it != symbolTable.end(); ++it) {
			if (strstr(src, it->second.c_str()) == src) {
				return it->first.c_str();
			}
		}
	}
	else {
		if (strlen(src) == 0) {
			return "CONFIG_TABLE_ROOT";
		}
		SymbolTable::const_iterator it = symbolTable.find(src);
		if (it != symbolTable.end()) {
			return it->second.c_str();
		}
	}

	GS_THROW_USER_ERROR(GS_ERROR_CM_INTERNAL_ERROR, "");
}

ConfigTableUtils::SymbolTable& ConfigTableUtils::getGroupSymbolTable() {
	static SymbolTable symbolTable;
	return symbolTable;
}
