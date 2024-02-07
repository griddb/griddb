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
	@brief Implementation of JsonUtils
*/
#include "json.h"

#include "gs_error_common.h"
#include "picojson.h"

struct JsonUtils::Impl {
	static const picojson::value NULL_VALUE;
};

const picojson::value JsonUtils::Impl::NULL_VALUE;

const size_t JsonUtils::MAX_ERROR_LINE_LENGTH = 80;

template const picojson::value& JsonUtils::asValue(
		const picojson::value &src, const u8string &key, Path *path);
template picojson::value& JsonUtils::asValue(
		picojson::value &src, const u8string &key, Path *path);

template const picojson::value* JsonUtils::findValue(
		const picojson::value &src, const u8string &key, Path *path);
template picojson::value* JsonUtils::findValue(
		picojson::value &src, const u8string &key, Path *path);

template void JsonUtils::errorByIntRange(
		double value, int64_t min, int64_t max, const Path *path);
template void JsonUtils::errorByIntRange(
		double value, uint64_t min, uint64_t max, const Path *path);

picojson::value JsonUtils::parseAll(const char8_t *begin, const char8_t *end) {
	picojson::value value;
	const u8string &err = parseAll(value, begin, end);	

	if (!err.empty()) {
		GS_COMMON_THROW_USER_ERROR(
				GS_ERROR_JSON_INVALID_SYNTAX,
				"Failed to parse JSON (reason=" << err << ")");
	}

	return value;
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
		const char8_t *extra = NULL;
		for (const char8_t *i = last; i != end; i++) {
			if (strchr("\r\n\t ", *i) == NULL) {
				extra = i;
				break;
			}
		}

		if (extra != NULL) {
			value = picojson::value();
			u8string content;
			size_t line = 0;
			{
				util::NormalIStringStream iss(u8string(begin, extra + 1));
				while (std::getline(iss, content)) {
					line++;
				}
			}
			{
				util::NormalIStringStream iss(u8string(begin, end));
				for (size_t i = 0; i < line && std::getline(iss, content); i++) {
				}
			}

			if (content.size() > MAX_ERROR_LINE_LENGTH) {
				content.erase(MAX_ERROR_LINE_LENGTH);
				content += "...";
			}

			util::NormalOStringStream oss;
			oss << "Non whitespace characters found after the value "
					"(line=" << line << ", content=\"" << content << "\")";
			err = oss.str();
		}
	}

	return err;
}

template<>
void JsonUtils::checkType<picojson::value>(
		const picojson::value &src, const Path *path) {
	static_cast<void>(src);
	static_cast<void>(path);
}

template<>
const char8_t* JsonUtils::typeToString<picojson::null>() {
	return "null";
}

template<>
const char8_t* JsonUtils::typeToString<bool>() {
	return "boolean";
}

template<>
const char8_t* JsonUtils::typeToString<double>() {
	return "number";
}

template<>
const char8_t* JsonUtils::typeToString<std::string>() {
	return "string";
}

template<>
const char8_t* JsonUtils::typeToString<picojson::array>() {
	return "array";
}

template<>
const char8_t* JsonUtils::typeToString<picojson::object>() {
	return "object";
}

const char8_t* JsonUtils::typeToString(const picojson::value &value) {
	if (value.is<picojson::null>()) {
		return typeToString<picojson::null>();
	}
	else if (value.is<bool>()) {
		return typeToString<bool>();
	}
	else if (value.is<double>()) {
		return typeToString<double>();
	}
	else if (value.is<std::string>()) {
		return typeToString<std::string>();
	}
	else if (value.is<picojson::array>()) {
		return typeToString<picojson::array>();
	}
	else if (value.is<picojson::object>()) {
		return typeToString<picojson::object>();
	}
	else {
		assert(false);
		return "(unknown)";
	}
}

template<typename V, typename S>
V& JsonUtils::asValue(S &src, const u8string &key, Path *path) {
	typedef typename ConstTraits<V, picojson::object>::Type Obj;
	typedef typename ConstTraits<V, picojson::object>::Itr Itr;

	Obj &obj = as<picojson::object>(src, path);

	const Itr it = obj.find(key);
	if (it == obj.end()) {
		GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_KEY_NOT_FOUND,
				"Json object does not contain the specified key (" <<
				(path == NULL ? "" : "path=") << path << 
				(path == NULL ? "" : ", ") << "key=" << key <<")");
	}

	if (path != NULL) {
		*path = path->named(it->first.c_str());
	}

	return it->second;
}

template<typename V, typename S>
V* JsonUtils::findValue(S &src, const u8string &key, Path *path) {
	typedef typename ConstTraits<V, picojson::object>::Type Obj;
	typedef typename ConstTraits<V, picojson::object>::Itr Itr;

	Obj *obj = find<picojson::object>(src);
	if (obj == NULL) {
		return NULL;
	}

	const Itr it = obj->find(key);
	if (it == obj->end()) {
		return NULL;
	}

	if (path != NULL) {
		*path = path->named(it->first.c_str());
	}

	return &it->second;
}


u8string JsonUtils::attributeToString(
		const util::ObjectCoder::Attribute &attr) {
	return u8string(
			util::ObjectCoder::Impl::nameBegin(attr),
			util::ObjectCoder::Impl::nameEnd(attr));
}


template<typename T>
void JsonUtils::errorByIntRange(double value, T min, T max, const Path *path) {
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_VALUE_OUT_OF_RANGE,
			"Json value out of range (value=" << value <<
			", min=" << (min + 0) <<
			", max=" << (max + 0) <<
			(path == NULL ? "" : ", path=") << path << ")");
}

void JsonUtils::errorByNoInt(double value, const Path *path) {
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json value is not integral (value=" << value <<
			(path == NULL ? "" : ", path=") << path << ")");
}

void JsonUtils::errorByType(
		const char8_t *expected, const char8_t *actual, const Path *path) {
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Json type unmatched (expected=" << expected <<
			", actual=" << actual <<
			(path == NULL ? "" : ", path=") << path << ")");
}

JsonUtils::Path::Path() :
		parent_(NULL),
		name_(NULL),
		index_(0),
		indexed_(false) {
}

JsonUtils::Path JsonUtils::Path::child() const {
	Path path;
	path.parent_ = this;
	return path;
}

JsonUtils::Path JsonUtils::Path::named(const char8_t *name) const {
	Path path;
	path.parent_ = parent_;
	path.name_ = name;
	return path;
}

JsonUtils::Path JsonUtils::Path::indexed(size_t index) const {
	Path path;
	path.parent_ = parent_;
	path.index_ = index;
	path.indexed_ = true;
	return path;
}

const JsonUtils::Path* JsonUtils::Path::getParent() const {
	return parent_;
}

const char8_t* JsonUtils::Path::getName() const {
	return name_;
}

size_t JsonUtils::Path::getIndex() const {
	return index_;
}

bool JsonUtils::Path::isEmpty(bool recursive) const {
	if (indexed_ || name_ != NULL) {
		return false;
	}

	if (recursive && parent_ != NULL) {
		return parent_->isEmpty(recursive);
	}

	return true;
}

bool JsonUtils::Path::isIndexed(bool recursive) const {
	if (indexed_) {
		return true;
	}
	else if (name_ != NULL) {
		return false;
	}

	if (recursive && parent_ != NULL) {
		return parent_->isIndexed(recursive);
	}

	return false;
}

std::ostream& operator<<(std::ostream &stream, const JsonUtils::Path *path) {
	if (path != NULL) {
		stream << *path;
	}

	return stream;
}

std::ostream& operator<<(std::ostream &stream, const JsonUtils::Path &path) {
	const JsonUtils::Path *parent = path.getParent();
	const char8_t *name = path.getName();

	if (parent != NULL) {
		stream << *parent;

		if (!parent->isEmpty(true) && name != NULL) {
			stream << ".";
		}
	}

	if (name != NULL) {
		stream << name;
	}
	else if (path.isIndexed(false)) {
		stream << "[" << path.getIndex() << "]";
	}

	return stream;
}


template<typename V>
JsonUtils::BaseInStream<V>::BaseInStream(V base, const Path *path) :
		base_(base), basePath_(path) {
}

template<typename V>
JsonUtils::BaseInStream<V>::BaseInStream(
		const Value &value, const Path *path) :
		base_(V()), basePath_(path) {
	assert(false);
	static_cast<void>(value);
}

template<>
JsonUtils::BaseInStream<const JsonUtils::Value*>::BaseInStream(
		const Value &value, const Path *path) :
		base_(&value), basePath_(path) {
}

template<typename V>
void JsonUtils::BaseInStream<V>::reserve(size_t size) {
	static_cast<void>(size);
}

template<typename V>
bool JsonUtils::BaseInStream<V>::peekType(Type &type, const Attribute &attr) {
	Path path;
	type = getType(peek(attr, path), path);
	return true;
}

template<typename V>
bool JsonUtils::BaseInStream<V>::readBool(const Attribute &attr) {
	return as<bool>(read(attr), &path_);
}

template<typename V>
const JsonUtils::Object* JsonUtils::BaseInStream<V>::readObject(
		const Attribute &attr) {
	return &as<Object>(read(attr), &path_);
}

template<typename V>
const JsonUtils::Value* JsonUtils::BaseInStream<V>::readValue(
		Type &type, const Attribute &attr) {
	const Value &dest = read(attr, false);
	type = getType(dest, path_);

	return &dest;
}

template<typename V>
JsonUtils::Array::const_iterator JsonUtils::BaseInStream<V>::readList(
		size_t &size, const Attribute &attr) {
	const Array &dest = as<Array>(read(attr), &path_);
	size = dest.size();
	return dest.begin();
}

template<typename V>
typename JsonUtils::BaseInStream<V>::Locator
JsonUtils::BaseInStream<V>::locator() {
	return Locator(*this);
}

template<typename V>
const JsonUtils::Path& JsonUtils::BaseInStream<V>::getLastPath() const {
	return path_;
}

template<>
const JsonUtils::Value&
JsonUtils::BaseInStream<const JsonUtils::Object*>::peek(
		const Attribute &attr, Path &path) const {
	assert(base_ != NULL);
	path = (basePath_ == NULL ? Path() : basePath_->child());

	const Value *value = findValue<const Value, const Object>(
			*base_, attributeToString(attr), &path);
	return (value == NULL ? Impl::NULL_VALUE : *value);
}

template<>
const JsonUtils::Value&
JsonUtils::BaseInStream<const JsonUtils::Value*>::peek(
		const Attribute &attr, Path &path) const {
	assert(base_ != NULL);
	static_cast<void>(attr);

	path = (basePath_ == NULL ? Path() : basePath_->child());
	return *base_;
}

template<>
const JsonUtils::Value&
JsonUtils::BaseInStream<JsonUtils::Array::const_iterator>::peek(
		const Attribute &attr, Path &path) const {
	static_cast<void>(attr);

	path = path_.indexed(path_.isIndexed(false) ? path_.getIndex() + 1 : 0);
	return *base_;
}

template<>
const JsonUtils::Value&
JsonUtils::BaseInStream<const JsonUtils::Object*>::read(
		const Attribute &attr, bool required) {
	assert(base_ != NULL);
	path_ = (basePath_ == NULL ? Path() : basePath_->child());

	if (required) {
		return asValue<const Value, const Object>(
				*base_, attributeToString(attr), &path_);
	}
	else {
		const Value *value = findValue<const Value, const Object>(
				*base_, attributeToString(attr), &path_);
		return (value == NULL ? Impl::NULL_VALUE : *value);
	}
}

template<>
const JsonUtils::Value&
JsonUtils::BaseInStream<const JsonUtils::Value*>::read(
		const Attribute &attr, bool required) {
	assert(base_ != NULL);
	static_cast<void>(attr);
	static_cast<void>(required);

	return *base_;
}

template<>
const JsonUtils::Value&
JsonUtils::BaseInStream<JsonUtils::Array::const_iterator>::read(
		const Attribute &attr, bool required) {
	static_cast<void>(attr);
	static_cast<void>(required);

	path_ = path_.indexed(path_.isIndexed(false) ? path_.getIndex() + 1 : 0);
	return *(base_++);
}

template<typename V>
typename JsonUtils::BaseInStream<V>::Type
JsonUtils::BaseInStream<V>::getType(const Value &value, const Path &path) {
	if (value.is<picojson::null>()) {
		return util::ObjectCoder::TYPE_NULL;
	}
	else if (value.is<bool>()) {
		return util::ObjectCoder::TYPE_BOOL;
	}
	else if (value.is<double>()) {
		return util::ObjectCoder::TYPE_NUMERIC;
	}
	else if (value.is<std::string>()) {
		return util::ObjectCoder::TYPE_STRING;
	}
	else if (value.is<Array>()) {
		return util::ObjectCoder::TYPE_LIST;
	}
	else if (value.is<Object>()) {
		return util::ObjectCoder::TYPE_OBJECT;
	}
	else {
		JsonUtils::errorByType("(any)", "(unknown)", &path);
		return util::ObjectCoder::TYPE_NULL;
	}
}

template<typename V>
JsonUtils::BaseInStream<V>::Locator::Locator() :
		stream_(NULL), base_(V()) {
}

template<typename V>
JsonUtils::BaseInStream<V>::Locator::Locator(BaseInStream &stream) :
		stream_(&stream), base_(stream.base_) {
}

template<typename V>
void JsonUtils::BaseInStream<V>::Locator::locate() const {
	if (stream_ == NULL) {
		return;
	}

	stream_->base_ = base_;
}

template<typename V>
void JsonUtils::BaseInStream<V>::errorByUnknownEnum(
		const char8_t *value, const Path &path) {
	GS_COMMON_THROW_USER_ERROR(GS_ERROR_JSON_UNEXPECTED_TYPE,
			"Unknown enum constant (value=" << value <<
			", path=" << path << ")");
}

template<typename V>
JsonUtils::BaseOutStream<V>::BaseOutStream(V base) : base_(base) {
}

template<typename V>
JsonUtils::BaseOutStream<V>::BaseOutStream(Value &value) : base_(V()) {
	assert(false);
	static_cast<void>(value);
}

template<>
JsonUtils::BaseOutStream<JsonUtils::Value*>::BaseOutStream(
		Value &value) : base_(&value) {
}

template<typename V>
void JsonUtils::BaseOutStream<V>::reserve(size_t size) {
	static_cast<void>(size);
}

template<typename V>
void JsonUtils::BaseOutStream<V>::writeBool(
		bool value, const Attribute &attr) {
	write(attr) = Value(value);
}

template<typename V>
void JsonUtils::BaseOutStream<V>::writeString(
		const char8_t *data, size_t size, const Attribute &attr) {
	write(attr) = Value(size > 0 ? u8string(data, size) : u8string());
}

template<typename V>
void JsonUtils::BaseOutStream<V>::writeBinary(
		const void *data, size_t size, const Attribute &attr) {
	Value &dest = write(attr);
	if (size == 0) {
		dest = Value(u8string());
	}
	else {
		util::NormalIStringStream iss(
				u8string(static_cast<const char8_t*>(data), size));
		util::NormalOStringStream oss;
		util::HexConverter::encode(oss, iss);

		dest = Value(oss.str());
	}
}

template<typename V>
JsonUtils::Object* JsonUtils::BaseOutStream<V>::writeObject(
		const Attribute &attr) {
	return &(write(attr) = Value(Object())).template get<Object>();
}

template<typename V>
JsonUtils::Value* JsonUtils::BaseOutStream<V>::writeValue(
		Type type, const Attribute &attr) {
	if (type == util::ObjectCoder::TYPE_NULL) {
		return NULL;
	}

	return &write(attr);
}

template<typename V>
JsonUtils::Array* JsonUtils::BaseOutStream<V>::writeList(
		size_t size, const Attribute &attr) {
	JsonUtils::Array &dest = (write(attr) = Value(Array())).template get<Array>();
	dest.reserve(size);
	return &dest;
}

template<typename V>
void JsonUtils::BaseOutStream<V>::writeDouble(
		const double &value, const Attribute &attr) {
	write(attr) = Value(value);
}

template<>
JsonUtils::Value&
JsonUtils::BaseOutStream<JsonUtils::Object*>::write(const Attribute &attr) {
	assert(base_ != NULL);
	return (*base_)[attributeToString(attr)];
}

template<>
JsonUtils::Value&
JsonUtils::BaseOutStream<JsonUtils::Value*>::write(const Attribute &attr) {
	assert(base_ != NULL);
	static_cast<void>(attr);
	return (*base_);
}

template<>
JsonUtils::Value&
JsonUtils::BaseOutStream<JsonUtils::Array*>::write(const Attribute &attr) {
	assert(base_ != NULL);
	static_cast<void>(attr);
	base_->push_back(Value());
	return base_->back();
}

template class JsonUtils::BaseInStream<const JsonUtils::Object*>;
template class JsonUtils::BaseInStream<const JsonUtils::Value*>;
template class JsonUtils::BaseInStream<JsonUtils::Array::const_iterator>;

template class JsonUtils::BaseOutStream<JsonUtils::Object*>;
template class JsonUtils::BaseOutStream<JsonUtils::Value*>;
template class JsonUtils::BaseOutStream<JsonUtils::Array*>;

