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
	@brief Definition of JsonUtils
*/
#ifndef JSON_H_
#define JSON_H_

#include "util/code.h"
#include <limits>
#include <vector>
#include <map>

namespace picojson {
class value;
struct null;
}

struct JsonUtils {
public:
	typedef picojson::value Value;
	typedef picojson::null Null;
	typedef std::vector<picojson::value> Array;
	typedef std::map<std::string, picojson::value> Object;

	struct Path;

	template<typename V> class BaseInStream;
	template<typename V> class BaseOutStream;
	template<typename V> class BaseInScope;
	template<typename V> class BaseOutScope;

	typedef BaseInStream<const Value*> InStream;
	typedef BaseOutStream<Value*> OutStream;

	static picojson::value parseAll(const char8_t *begin, const char8_t *end);
	static u8string parseAll(
			picojson::value &value, const char8_t *begin, const char8_t *end);

	template<typename T>
	static const T& as(const picojson::value &src, const Path *path = NULL);
	template<typename T>
	static T& as(picojson::value &src, const Path *path = NULL);

	template<typename T>
	static const T& as(
			const picojson::value &src, const u8string &key,
			Path *path = NULL);
	template<typename T>
	static T& as(
			picojson::value &src, const u8string &key, Path *path = NULL);

	template<typename T>
	static T asInt(const picojson::value &src, const Path *path = NULL);
	template<typename T>
	static T asInt(
			const picojson::value &src, const u8string &key,
			Path *path = NULL);

	template<typename T>
	static T asInt(const double &src, const Path *path = NULL);

	template<typename T>
	static void checkType(const picojson::value &src, const Path *path = NULL);

	template<typename T>
	static const T* find(const picojson::value &src);
	template<typename T>
	static T* find(picojson::value &src);

	template<typename T>
	static const T* find(
			const picojson::value &src,
			const u8string &key, Path *path = NULL);
	template<typename T>
	static T* find(
			picojson::value &src, const u8string &key, Path *path = NULL);

	template<typename T>
	static bool findInt(const picojson::value &src, T *result, Path *path = NULL);
	template<typename T>
	static bool findInt(
			const picojson::value &src, const u8string &key, T *result,
			Path *path = NULL);

	template<typename T>
	static const char8_t* typeToString();
	static const char8_t* typeToString(const picojson::value &value);

private:
	struct Impl;

	static const size_t MAX_ERROR_LINE_LENGTH;

	template<typename T, typename U> struct ConstTraits;

	template<typename T, bool = std::numeric_limits<T>::is_signed>
	struct LargestIntTraits;

	template<typename T>
	static const T& as(const T &src, const Path*);
	template<typename T>
	static T& as(T &src, const Path*);

	template<typename T>
	static const T* find(const T &src);
	template<typename T>
	static T* find(T &src);

	template<typename T>
	static bool is(const picojson::value &value);
	template<typename V, typename T>
	static bool is(const V &value, const util::TrueType&);
	template<typename V, typename T>
	static bool is(const V &value, const util::FalseType&);

	template<typename T>
	static const T& get(const picojson::value &value);
	template<typename T>
	static T& get(picojson::value &value);
	template<typename V, typename T, typename U>
	static T& get(V &value, const util::TrueType&);
	template<typename V, typename T, typename U>
	static T& get(V &value, const util::FalseType&);

	template<typename V, typename T, typename U>
	static T& valueAs(V &src, const u8string &key, Path *path);
	template<typename V, typename T, typename U>
	static T* findFromValue(V &src, const u8string &key, Path *path);
	template<typename T>
	static T* applyPath(T *value, const Path &localPath, Path *path);

	template<typename V, typename S>
	static V& asValue(S &src, const u8string &key, Path *path);
	template<typename V, typename S>
	static V* findValue(S &src, const u8string &key, Path *path);

	static u8string attributeToString(
			const util::ObjectCoder::Attribute &attr);

	template<typename T>
	static void errorByIntRange(double value, T min, T max, const Path *path);

	static void errorByNoInt(double value, const Path *path);
	static void errorByType(
			const char8_t *expected, const char8_t *actual, const Path *path);
};

template<>
void JsonUtils::checkType<picojson::value>(
		const picojson::value &src, const Path *path);

struct JsonUtils::Path {
public:
	Path();

	Path child() const;
	Path named(const char8_t *name) const;
	Path indexed(size_t index) const;

	const Path* getParent() const;
	const char8_t* getName() const;
	size_t getIndex() const;

	bool isEmpty(bool recursive) const;
	bool isIndexed(bool recursive) const;

private:
	const Path *parent_;
	const char8_t *name_;
	size_t index_;
	bool indexed_;
};

std::ostream& operator<<(std::ostream &stream, const JsonUtils::Path *path);
std::ostream& operator<<(std::ostream &stream, const JsonUtils::Path &path);


template<typename V>
class JsonUtils::BaseInStream {
public:
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoder::Type Type;
	typedef util::ObjectCoder::NumericPrecision NumericPrecision;

	typedef BaseInScope<const Object*> ObjectScope;
	typedef BaseInScope<const Value*> ValueScope;
	typedef BaseInScope<Array::const_iterator> ListScope;

	class Locator;

	explicit BaseInStream(V base, const Path *path = NULL);
	explicit BaseInStream(const Value &value, const Path *path = NULL);

	void reserve(size_t size);

	bool peekType(Type &type, const Attribute &attr);

	bool readBool(const Attribute &attr);
	template<typename T> T readNumeric(
			const Attribute &attr,
			NumericPrecision precision = util::ObjectCoder::PRECISION_NORMAL);
	template<typename T> void readNumericGeneral(
			T &value, const Attribute &attr, NumericPrecision precision);
	template<typename T> void readEnum(T &value, const Attribute &attr);
	template<typename A> void readString(A &action, const Attribute &attr);
	template<typename A> void readBinary(A &action, const Attribute &attr);

	const Object* readObject(const Attribute &attr);
	const Value* readValue(Type &type, const Attribute &attr);
	Array::const_iterator readList(size_t &size, const Attribute &attr);

	Locator locator();
	const Path& getLastPath() const;

private:
	const Value& peek(const Attribute &attr, Path &path) const;
	const Value& read(const Attribute &attr, bool required = true);

	static Type getType(const Value &value, const Path &path);

	template<typename T>
	static T getNumeric(
			const Value &value, const Path &path, const util::TrueType&);
	template<typename T>
	static T getNumeric(
			const Value &value, const Path &path, const util::FalseType&);

	static void errorByUnknownEnum(const char8_t *value, const Path &path);

	V base_;
	const Path *const basePath_;
	Path path_;
};

template<typename V>
class JsonUtils::BaseInStream<V>::Locator {
public:
	Locator();
	explicit Locator(BaseInStream &stream);

	void locate() const;

private:
	BaseInStream *stream_;
	V base_;
};

template<typename V>
class JsonUtils::BaseOutStream {
public:
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoder::Type Type;
	typedef util::ObjectCoder::NumericPrecision NumericPrecision;

	typedef BaseOutScope<Value*> ValueScope;
	typedef BaseOutScope<Object*> ObjectScope;
	typedef BaseOutScope<Array*> ListScope;

	explicit BaseOutStream(V base);
	explicit BaseOutStream(Value &value);

	bool isTyped() const { return true; }
	void reserve(size_t size);

	void writeBool(bool value, const Attribute &attr);
	template<typename T> void writeNumeric(
			const T &value, const Attribute &attr,
			NumericPrecision precision = util::ObjectCoder::PRECISION_NORMAL);
	template<typename T> void writeNumericGeneral(
			const T &value, const Attribute &attr, NumericPrecision precision);
	template<typename T> void writeEnum(const T &value, const Attribute &attr);
	void writeString(const char8_t *data, size_t size, const Attribute &attr);
	void writeBinary(const void *data, size_t size, const Attribute &attr);

	Object* writeObject(const Attribute &attr);
	Value* writeValue(Type type, const Attribute &attr);
	Array* writeList(size_t size, const Attribute &attr);

private:
	void writeDouble(const double &value, const Attribute &attr);

	Value& write(const Attribute &attr);

	V base_;
};

template<typename V>
class JsonUtils::BaseInScope {
public:
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoder::Type Type;
	typedef BaseInStream<V> Stream;

	template<typename S>
	BaseInScope(S &base, const Attribute &attr);
	template<typename S>
	BaseInScope(S &base, Type &type, const Attribute &attr);
	template<typename S>
	BaseInScope(S &base, size_t &size, const Attribute &attr);

	Stream& stream();

private:
	Stream base_;
};

template<typename V>
class JsonUtils::BaseOutScope {
public:
	typedef util::ObjectCoder::Attribute Attribute;
	typedef util::ObjectCoder::Type Type;
	typedef BaseOutStream<V> Stream;

	template<typename S>
	BaseOutScope(S &base, const Attribute &attr);
	template<typename S>
	BaseOutScope(S &base, Type type, const Attribute &attr);
	template<typename S>
	BaseOutScope(S &base, size_t size, const Attribute &attr);

	Stream& stream();

private:
	Stream base_;
};


template<typename T, typename U>
struct JsonUtils::ConstTraits {
	typedef U Type;
	typedef typename U::iterator Itr;
};

template<typename T, typename U>
struct JsonUtils::ConstTraits<const T, U> {
	typedef const U Type;
	typedef typename U::const_iterator Itr;
};

template<typename T, bool>
struct JsonUtils::LargestIntTraits {
	typedef int64_t Type;
};

template<typename T>
struct JsonUtils::LargestIntTraits<T, false> {
	typedef uint64_t Type;
};

template<typename T>
const T& JsonUtils::as(const picojson::value &src, const Path *path) {
	checkType<T>(src, path);
	return get<T>(src);
}

template<typename T>
T& JsonUtils::as(picojson::value &src, const Path *path) {
	checkType<T>(src, path);
	return get<T>(src);
}

template<typename T>
const T& JsonUtils::as(
		const picojson::value &src, const u8string &key, Path *path) {
	return valueAs<const picojson::value, const T, T>(src, key, path);
}

template<typename T>
T& JsonUtils::as(
		picojson::value &src, const u8string &key, Path *path) {
	return valueAs<picojson::value, T, T>(src, key, path);
}

template<typename T>
T JsonUtils::asInt(const picojson::value &src, const Path *path) {
	return asInt<T>(as<double>(src, path), path);
}

template<typename T>
T JsonUtils::asInt(
		const picojson::value &src, const u8string &key, Path *path) {
	Path localPath = (path == NULL ? Path() : *path);
	Path *localPathAddr = (path == NULL ? NULL : &localPath);

	const T &result =
			asInt<T>(as<double>(src, key, localPathAddr), localPathAddr);
	return *applyPath(&result, localPath, path);
}

template<typename T>
T JsonUtils::asInt(const double &src, const Path *path) {
	UTIL_STATIC_ASSERT(std::numeric_limits<T>::is_integer);
	typedef typename LargestIntTraits<T>::Type Largest;

	const Largest min = std::numeric_limits<T>::min();
	const Largest max = std::numeric_limits<T>::max();
	const volatile Largest intValue = static_cast<Largest>(src);

	if (!(static_cast<double>(min) <= src &&
			src <= static_cast<double>(max) &&
			min <= intValue && intValue <= max)) {
		errorByIntRange(src, min, max, path);
	}

	if (!(static_cast<double>(intValue) == src)) {
		errorByNoInt(src, path);
	}

	return static_cast<T>(intValue);
}

template<typename T>
void JsonUtils::checkType(const picojson::value &src, const Path *path) {
	UTIL_STATIC_ASSERT(
			(util::IsSame<T, picojson::null>::VALUE) ||
			(util::IsSame<T, bool>::VALUE) ||
			(util::IsSame<T, double>::VALUE) ||
			(util::IsSame<T, std::string>::VALUE) ||
			(util::IsSame< T, std::vector<Value> >::VALUE) ||
			(util::IsSame< T, std::map<std::string, Value> >::VALUE));

	if (!is<T>(src)) {
		errorByType(typeToString<T>(), typeToString(src), path);
	}
}

template<typename T>
const T* JsonUtils::find(const picojson::value &src) {
	return (is<T>(src) ? &get<T>(src) : NULL);
}

template<typename T>
T* JsonUtils::find(picojson::value &src) {
	return (is<T>(src) ? &get<T>(src) : NULL);
}

template<typename T>
const T* JsonUtils::find(
		const picojson::value &src, const u8string &key, Path *path) {
	return findFromValue<const picojson::value, const T, T>(src, key, path);
}

template<typename T>
T* JsonUtils::find(
		picojson::value &src, const u8string &key, Path *path) {
	return findFromValue<picojson::value, T, T>(src, key, path);
}

template<typename T>
bool JsonUtils::findInt(const picojson::value &src, T *result, Path *path) {
	assert(result != NULL);
	*result = T();

	const double *value = find<double>(src);
	if (value == NULL) {
		return false;
	}

	*result = asInt<T>(*value, path);
	return true;
}

template<typename T>
bool JsonUtils::findInt(
		const picojson::value &src, const u8string &key, T *result,
		Path *path) {
	assert(result != NULL);
	*result = T();

	Path localPath = (path == NULL ? Path() : *path);
	Path *localPathAddr = (path == NULL ? NULL : &localPath);

	const double *value = find<double>(src, key, localPathAddr);
	if (value == NULL) {
		return false;
	}

	*result = asInt<T>(*value, localPathAddr);
	applyPath(&result, localPath, path);
	return true;
}

template<typename T>
const T& JsonUtils::as(const T &src, const Path*) {
	return src;
}

template<typename T>
T& JsonUtils::as(T &src, const Path*) {
	return src;
}

template<typename T>
const T* JsonUtils::find(const T &src) {
	return &src;
}

template<typename T>
T* JsonUtils::find(T &src) {
	return &src;
}

template<typename T>
bool JsonUtils::is(const picojson::value &value) {
	typedef typename util::IsSame<picojson::value, T>::Type SameType;
	return is<picojson::value, T>(value, SameType());
}

template<typename V, typename T>
bool JsonUtils::is(const V &value, const util::TrueType&) {
	static_cast<void>(value);
	return true;
}

template<typename V, typename T>
bool JsonUtils::is(const V &value, const util::FalseType&) {
	return value.template is<T>();
}

template<typename T>
const T& JsonUtils::get(const picojson::value &value) {
	typedef typename util::IsSame<picojson::value, T>::Type SameType;
	return get<const picojson::value, const T, T>(value, SameType());
}

template<typename T>
T& JsonUtils::get(picojson::value &value) {
	typedef typename util::IsSame<picojson::value, T>::Type SameType;
	return get<picojson::value, T, T>(value, SameType());
}

template<typename V, typename T, typename U>
T& JsonUtils::get(V &value, const util::TrueType&) {
	return value;
}

template<typename V, typename T, typename U>
T& JsonUtils::get(V &value, const util::FalseType&) {
	return value.template get<U>();
}

template<typename V, typename T, typename U>
T& JsonUtils::valueAs(V &src, const u8string &key, Path *path) {
	Path localPath = (path == NULL ? Path() : *path);
	Path *localPathAddr = (path == NULL ? NULL : &localPath);

	V &value = asValue<V, V>(src, key, localPathAddr);
	return *applyPath(&as<U>(value, localPathAddr), localPath, path);
}

template<typename V, typename T, typename U>
T* JsonUtils::findFromValue(V &src, const u8string &key, Path *path) {
	Path localPath = (path == NULL ? Path() : *path);
	V *value = findValue<V, V>(src, key, &localPath);
	return applyPath(
			(value == NULL ? NULL : find<U>(*value)), localPath, path);
}

template<typename T>
T* JsonUtils::applyPath(T *value, const Path &localPath, Path *path) {
	if (value != NULL && path != NULL) {
		*path = localPath;
	}
	return value;
}


template<typename V>
template<typename T>
T JsonUtils::BaseInStream<V>::readNumeric(
		const Attribute &attr, NumericPrecision precision) {
	typedef typename util::BoolType<std::numeric_limits<T>::is_integer>::Result
			IntegerType;

	const Value &value = read(attr);
	if (precision == util::ObjectCoder::PRECISION_EXACT &&
			IntegerType::VALUE) {
		const u8string *str = find<u8string>(value);
		if (str != NULL) {
			return util::LexicalConverter<T>()(*str);
		}
	}

	return getNumeric<T>(value, path_, IntegerType());
}

template<typename V>
template<typename T>
void JsonUtils::BaseInStream<V>::readNumericGeneral(
		T &value, const Attribute &attr, NumericPrecision precision) {
	if (value.isInteger()) {
		value.accept(readNumeric<int64_t>(attr, precision));
	}
	else {
		value.accept(readNumeric<double>(attr, precision));
	}
}

template<typename V>
template<typename T>
void JsonUtils::BaseInStream<V>::readEnum(
		T &value, const Attribute &attr) {

	const Value &src = read(attr);
	const u8string *str = find<u8string>(src);
	if (str == NULL) {
		value.accept(asInt<typename T::IntegerType>(src, &path_));
	}
	else {
		if (!value.accept(str->c_str())) {
			errorByUnknownEnum(str->c_str(), path_);
		}
	}
}

template<typename V>
template<typename A>
void JsonUtils::BaseInStream<V>::readString(
		A &action, const Attribute &attr) {
	const u8string &src = as<u8string>(read(attr), &path_);
	const size_t size = src.size();
	action(size);
	action(static_cast<const void*>(src.c_str()), size);
}

template<typename V>
template<typename A>
void JsonUtils::BaseInStream<V>::readBinary(
		A &action, const Attribute &attr) {
	util::NormalIStringStream iss(as<u8string>(read(attr), &path_));
	util::NormalOStringStream oss;
	util::HexConverter::decode(oss, iss);

	const u8string &src = oss.str();
	const size_t size = src.size();
	action(size);
	action(static_cast<const void*>(src.c_str()), size);
}

template<typename V>
template<typename T>
T JsonUtils::BaseInStream<V>::getNumeric(
		const Value &value, const Path &path, const util::TrueType&) {
	return asInt<T>(value, &path);
}

template<typename V>
template<typename T>
T JsonUtils::BaseInStream<V>::getNumeric(
		const Value &value, const Path &path, const util::FalseType&) {
	return static_cast<T>(as<double>(value, &path));
}

template<typename V>
template<typename T>
void JsonUtils::BaseOutStream<V>::writeNumeric(
		const T &value, const Attribute &attr, NumericPrecision precision) {
	if (precision == util::ObjectCoder::PRECISION_EXACT &&
			std::numeric_limits<T>::is_integer) {
		const u8string &str = util::LexicalConverter<u8string>()(value);
		writeString(str.c_str(), str.size(), attr);
	}
	else {
		writeDouble(static_cast<double>(value), attr);
	}
}

template<typename V>
template<typename T>
void JsonUtils::BaseOutStream<V>::writeNumericGeneral(
		const T &value, const Attribute &attr, NumericPrecision precision) {
	if (value.isInteger()) {
		writeNumeric(value.getInt64(), attr, precision);
	}
	else {
		writeNumeric(value.getDouble(), attr, precision);
	}
}

template<typename V>
template<typename T>
void JsonUtils::BaseOutStream<V>::writeEnum(
		const T &value, const Attribute &attr) {
	const char8_t *str = value.toString();
	if (str == NULL) {
		writeNumeric(value.toInteger(), attr);
	}
	else {
		writeString(str, strlen(str), attr);
	}
}

template<typename V>
template<typename S>
JsonUtils::BaseInScope<V>::BaseInScope(S &base, const Attribute &attr) :
		base_(base.readObject(attr), &base.getLastPath()) {
}

template<typename V>
template<typename S>
JsonUtils::BaseInScope<V>::BaseInScope(
		S &base, Type &type, const Attribute &attr) :
		base_(base.readValue(type, attr), &base.getLastPath()) {
}

template<typename V>
template<typename S>
JsonUtils::BaseInScope<V>::BaseInScope(
		S &base, size_t &size, const Attribute &attr) :
		base_(base.readList(size, attr), &base.getLastPath()) {
}

template<typename V>
JsonUtils::BaseInStream<V>& JsonUtils::BaseInScope<V>::stream() {
	return base_;
}

template<typename V>
template<typename S>
JsonUtils::BaseOutScope<V>::BaseOutScope(
		S &base, const Attribute &attr) :
		base_(base.writeObject(attr)) {
}

template<typename V>
template<typename S>
JsonUtils::BaseOutScope<V>::BaseOutScope(
		S &base, Type type, const Attribute &attr) :
		base_(base.writeValue(type, attr)) {
}

template<typename V>
template<typename S>
JsonUtils::BaseOutScope<V>::BaseOutScope(
		S &base, size_t size, const Attribute &attr) :
		base_(base.writeList(size, attr)) {
}

template<typename V>
JsonUtils::BaseOutStream<V>& JsonUtils::BaseOutScope<V>::stream() {
	return base_;
}


#endif
